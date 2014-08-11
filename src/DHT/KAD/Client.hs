module DHT.KAD.Client (
                       start
                      , findValue
                      , store
                      ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (liftM)
import Control.Monad.Parallel as P (mapM)
import Data.List (sortBy)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Data.Maybe (catMaybes, isJust)
import Data.Serialize
import Nanomsg
import System.IO (hPutStrLn, stderr)
import System.Timeout (timeout)

import DHT.KAD.Data hiding (findNode)
import DHT.KAD.NanomsgTransport
import DHT.KAD.RPC as RPC
import DHT.KAD.Transport as Transport

start :: MVar Bucket -> [Node] -> IO ()
start = flip joinSelf

findNode :: NID -> MVar Bucket -> IO [Node]
findNode nid bucket = do
  bkt@(Bucket local _) <- readMVar bucket
  let nodes = nearNodes nid bkt 3
  (targets, nrps) <- go local Map.empty Map.empty nodes
  modifyMVar_ bucket (return . deleteNoResponses nrps)
  return targets
    where
      sendFindNode' :: Node -> Node -> IO (Node, Maybe [Node])
      sendFindNode' local n =
          withTransport Req $ \t ->
              Transport.connect t n $
                      either (\err -> hPutStrLn stderr err >> return (n, Nothing)) $ \c -> do
                        sn <- RPC.genSN
                        runSendM (sendFindNode (MsgHead sn local) nid) c
                        m <- timeout (30 * 1000000) $ Transport.recv c
                        maybe
                          (hPutStrLn stderr "Send FindNode timeout" >> return (n, Nothing))
                          (either
                           (\err -> hPutStrLn stderr err >> return (n, Nothing))
                           (\(Message (MsgHead sn' _) msg) ->
                                if sn' == sn then
                                    case msg of
                                      FoundNode ns -> return (n, Just ns)
                                      _ -> return (n, Just [])
                                else
                                    return (n, Nothing)) . RPC.unpackMessage)
                          m
      go :: Node -> Map.Map Node Int -> Map.Map Node Int -> [Node] -> IO ([Node], [Node])
      go _ result failed [] =
          return (if Map.size result > 8 then
                      take 8 $ sortBy (\a@(Node anid _ _) b@(Node bnid _ _) -> compare (dist2idx (nidDist nid anid)) (dist2idx (nidDist nid bnid))) (Map.keys result)
                  else
                      Map.keys result, Map.keys failed)
      go local result failed toSend = do
                        found <- P.mapM (sendFindNode' local) toSend
                        let (tm, fm) = foldr (\(src, x) (tm, fm) -> maybe (tm, Map.insert src 0 fm) (\ns -> (foldr (\y m -> Map.insert y 0 m) tm ns, fm)) x) (Map.empty :: Map.Map Node Int, failed) found
                        let rm = foldr (\x m -> Map.insert x 0 m) result $ filter (\x -> x `Map.notMember` failed && x `Map.notMember` result) toSend
                        let unsent = filter (\x -> x `notElem` toSend && x `Map.notMember` fm && x `Map.notMember` rm) (Map.keys tm)
                        go local rm fm unsent

findValue :: Key -> MVar Bucket -> MVar Cache -> IO (Maybe (Deadline, Value))
findValue key bucket cache = do
  kache <- readMVar cache
  case Map.lookup key kache of
    v@(Just _) -> return v
    _ -> do
      bkt@(Bucket local _) <- readMVar bucket
      let nodes = nearNodes key bkt 3
      -- mapM_ (\n -> hPutStrLn stderr ("found " ++ show n ++ " near key " ++ show key)) nodes
      (p, nrps, nvs) <- go local nodes key Nothing Map.empty Map.empty
      -- mapM_ (\(Node _ ip port) -> hPutStrLn stderr (ip2string ip ++ ":" ++ show port ++ " no response to " ++ ip2string lip ++ ":" ++ show lport)) nrps
      modifyMVar_ bucket (return . deleteNoResponses nrps)
      maybe (return p) (\(d, v) -> do nrps' <- doSendStore local key v d nvs; modifyMVar_ bucket (return . deleteNoResponses nrps'); return p) p
    where
      sendFindValue' :: Node -> Key -> Node -> IO (Node, Maybe (Either [Node] (Deadline, Value)))
      sendFindValue' local k n =
          withTransport Req $ \t ->
              Transport.connect t n $
                       either (\err -> hPutStrLn stderr err >> return (n, Nothing)) $ \conn ->
                            do
                              sn <- RPC.genSN
                              runSendM (sendFindValue (MsgHead sn local) k) conn
                              m <- timeout (30 * 1000000) $ Transport.recv conn
                              maybe
                                (hPutStrLn stderr ("Send FindValue to " ++ show n ++ " timeout") >> return (n, Nothing))
                                (either
                                 ((>> return (n, Nothing)) . hPutStrLn stderr)
                                 (\(Message (MsgHead sn' _) msg) ->
                                      if sn' == sn then
                                          case msg of
                                            FoundValue _ v d -> return (n, Just (Right (d, v)))
                                            FoundNode ns -> return (n, Just (Left ns))
                                            _ -> return (n, Nothing)
                                      else
                                          return (n, Nothing)) . RPC.unpackMessage)
                                m
      go :: Node -> [Node] -> Key -> Maybe (Deadline, Value) -> Map.Map Node Int -> Map.Map Node Int -> IO (Maybe (Deadline, Value), [Node], [Node])
      go _ [] _ p noResponses noValues = return (p, Map.keys noResponses, Map.keys noValues)
      go local toSend k p noResponses noValues =
          do
            r <- P.mapM (sendFindValue' local k) toSend
            let (p', nodes, noResponses', noValues') = foldr (\(src, x) (p'', nodes, noResponses'', noValues'') -> maybe (p'', nodes, Map.insert src 0 noResponses'', noValues'') (either (\ns' -> (p'', ns' ++ nodes, noResponses'', Map.insert src 0 noValues'')) (\p''' -> (Just p''', nodes, noResponses'', noValues''))) x) (Nothing, [], noResponses, noValues) r
            let toSend' = foldr (\x m -> Map.insert x 0 m) Map.empty $ filter (\x -> x `Map.notMember` noResponses' && x `Map.notMember` noValues' && x `notElem` toSend) nodes
            go local (Map.keys toSend') k (maybe p Just p') noResponses' noValues'

store :: Key -> Value -> Deadline -> MVar Bucket -> MVar Cache -> IO ()
store key value deadline bucket cache = do
  nodes <- findNode key bucket
  if null nodes then
      modifyMVar_ cache $ return . Map.insert key (deadline, value)
  else
      do
        (Bucket local _) <- readMVar bucket
        nrps <- doSendStore local key value deadline nodes
        modifyMVar_ bucket (return . deleteNoResponses nrps)
        return ()

joinSelf :: [Node] -> MVar Bucket -> IO ()
joinSelf roots bucket = do
  modifyMVar_ bucket $ \bkt -> return $ foldr (\x b -> addNode x b 8) bkt roots
  bkt@(Bucket local@(Node nid _ _) map') <- readMVar bucket
  nodes <- findNode nid bucket
  modifyMVar_ bucket $ \bkt -> return $ foldr (\x b -> addNode x b 8) bkt nodes

doSendStore :: Node -> Key -> Value -> Deadline -> [Node] -> IO [Node]
doSendStore local k v d toSend = do
  now <- getTimestamp
  r <- P.mapM (sendStore' local k v d now) toSend
  let ns = foldr (\x ns' -> maybe ns' (: ns') x) [] r
  return ns
    where
      sendStore' :: Node -> Key -> Value -> Deadline -> Deadline -> Node -> IO (Maybe Node)
      sendStore' local k v d now n =
          withTransport Req $ \t ->
              Transport.connect t n $
                       either
                         (\err -> hPutStrLn stderr err >> return Nothing)
                         (\conn ->
                              RPC.genSN >>=
                                     \sn ->
                                         runSendM (sendStore (MsgHead sn local) k v $ adjustDeadline d now k n) conn >>
                                                  timeout (30 * 1000000) (Transport.recv conn) >>=
                                                          maybe
                                                            (hPutStrLn stderr ("Send Store to " ++ show n ++ " timeout") >> return (Just n))
                                                            (either (\err -> hPutStrLn stderr err >> return (Just n)) (\(Message (MsgHead sn' _) _) -> return $ if sn' == sn then Nothing else Just n) . RPC.unpackMessage))
      adjustDeadline :: Deadline -> Deadline -> Key -> Node -> Deadline
      adjustDeadline d now k (Node nid _ _) =
          if d > now then
              now + (fromIntegral (dist2idx (nidDist k nid) * delta d now `div` 160) :: Deadline)
          else
              0
      delta :: Deadline -> Deadline -> Int
      delta d now = fromIntegral (d - now) :: Int

deleteNoResponses :: [Node] -> Bucket -> Bucket
deleteNoResponses [] b = b
deleteNoResponses xs b = foldr deleteNode b xs
