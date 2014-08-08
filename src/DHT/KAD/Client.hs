module DHT.KAD.Client (
                       start
                      , findValue
                      , store
                      ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (liftM, void)
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
  (targets, nrps) <- go Map.empty Map.empty nodes
  modifyMVar_ bucket (return . deleteNoResponses nrps)
  return targets
    where
      sendFindNode' :: Node -> IO (Node, Maybe [Node])
      sendFindNode' n =
          bracket (createTransport Req) Transport.free $ \t ->
              bracket (Transport.connect t n) (either (\_ -> return ()) Transport.close) $
                      either (\err -> hPutStrLn stderr err >> return (n, Nothing)) $ \c -> do
                        runSendM (sendFindNode nid) c
                        m <- timeout (30 * 1000000) $ Transport.recv c
                        maybe (hPutStrLn stderr "Send FindNode timeout" >> return (n, Nothing)) (either (\err -> hPutStrLn stderr err >> return (n, Nothing)) (\msg -> case msg of FoundNode ns -> return (n, Just ns); _ -> return (n, Just [])) . decode) m
      go :: Map.Map Node Int -> Map.Map Node Int -> [Node] -> IO ([Node], [Node])
      go result failed [] = do
                        return (if Map.size result > 8 then
                                    take 8 $ sortBy (\a@(Node anid _ _) b@(Node bnid _ _) -> compare (dist2idx (nidDist nid anid)) (dist2idx (nidDist nid bnid))) (Map.keys result)
                                else
                                    Map.keys result, Map.keys failed)
      go result failed toSend = do
                        found <- P.mapM sendFindNode' toSend
                        let (tm, fm) = foldr (\(src, x) (tm, fm) -> maybe (tm, Map.insert src 0 fm) (\ns -> (foldr (\y m -> Map.insert y 0 m) tm ns, fm)) x) (Map.empty :: Map.Map Node Int, failed) found
                        let rm = foldr (\x m -> Map.insert x 0 m) result $ filter (\x -> x `Map.notMember` failed && x `Map.notMember` result) toSend
                        let unsent = filter (\x -> x `notElem` toSend && x `Map.notMember` fm && x `Map.notMember` rm) (Map.keys tm)
                        go rm fm unsent

findValue :: Key -> MVar Bucket -> MVar Cache -> IO (Maybe Value)
findValue key bucket cache = do
  kache <- readMVar cache
  case Map.lookup key kache of
    v@(Just _) -> return v
    _ -> do
      bkt <- readMVar bucket
      let nodes = nearNodes key bkt 3
      -- mapM_ (\n -> hPutStrLn stderr ("found " ++ show n ++ " near key " ++ show key)) nodes
      (v, nrps, nvs) <- go nodes key Nothing Map.empty Map.empty
      -- mapM_ (\(Node _ ip port) -> hPutStrLn stderr (ip2string ip ++ ":" ++ show port ++ " no response to " ++ ip2string lip ++ ":" ++ show lport)) nrps
      modifyMVar_ bucket (return . deleteNoResponses nrps)
      maybe (return v) (\v' -> do nrps' <- doSendStore key v' nvs; modifyMVar_ bucket (return . deleteNoResponses nrps'); return v) v
    where
      sendFindValue' :: Key -> Node -> IO (Node, Maybe (Either [Node] Value))
      sendFindValue' k n =
          bracket (createTransport Req) Transport.free $ \t ->
              bracket (Transport.connect t n) (either (\_ -> return ()) Transport.close) $
                      either (\err -> hPutStrLn stderr err >> return (n, Nothing)) $ \conn -> do
                               runSendM (sendFindValue k) conn
                               m <- timeout (30 * 1000000) $ Transport.recv conn
                               maybe (hPutStrLn stderr ("Send FindValue to " ++ (show n) ++ " timeout") >> return (n, Nothing)) (either ((>> return (n, Nothing)) . (hPutStrLn stderr)) (\msg -> case msg of FoundValue _ v -> return (n, Just (Right v)); FoundNode ns -> return (n, Just (Left ns)); _ -> return (n, Nothing)) . decode) m
      go :: [Node] -> Key -> Maybe Value -> Map.Map Node Int -> Map.Map Node Int -> IO (Maybe Value, [Node], [Node])
      go [] _ v noResponses noValues = return (v, Map.keys noResponses, Map.keys noValues)
      go toSend k v noResponses noValues =
          do
            r <- P.mapM (sendFindValue' k) toSend
            let (v', nodes, noResponses', noValues') = foldr (\(src, x) (v'', nodes, noResponses'', noValues'') -> maybe (v'', nodes, Map.insert src 0 noResponses'', noValues'') (either (\ns' -> (v'', ns' ++ nodes, noResponses'', Map.insert src 0 noValues'')) (\v''' -> (Just v''', nodes, noResponses'', noValues''))) x) (Nothing, [], noResponses, noValues) r
            let toSend' = foldr (\x m -> Map.insert x 0 m) Map.empty $ filter (\x -> x `Map.notMember` noResponses' && x `Map.notMember` noValues' && x `notElem` toSend) nodes
            case v' of
              Just _ -> go (Map.keys toSend') k v' noResponses' noValues'
              Nothing -> go (Map.keys toSend') k v noResponses' noValues'

store :: Key -> Value -> MVar Bucket -> MVar Cache -> IO ()
store key value bucket cache = do
  nodes <- findNode key bucket
  if null nodes then
      modifyMVar_ cache $ return . Map.insert key value
  else
      do
        nrps <- doSendStore key value nodes
        modifyMVar_ bucket (return . deleteNoResponses nrps)
        return ()

joinSelf :: [Node] -> MVar Bucket -> IO ()
joinSelf roots bucket = do
  modifyMVar_ bucket $ \bkt -> return $ foldr (\x b -> addNode x b 8) bkt roots
  bkt@(Bucket local@(Node nid _ _) map') <- readMVar bucket
  nodes <- findNode nid bucket
  liftM catMaybes (P.mapM (sendPing' local) (nodes ++ roots)) >>= \ns -> modifyMVar_ bucket $ \bkt -> return $ foldr (\x b -> addNode x b 8) bkt ns
    where
      sendPing' :: Node -> Node -> IO (Maybe Node)
      sendPing' l n =
          bracket (createTransport Req) Transport.free $ \t ->
              bracket (Transport.connect t n) (either (\_ -> return ()) Transport.close) $
                      either (\err -> hPutStrLn stderr err >> return Nothing) $ \conn -> do
                          runSendM (sendPing l) conn
                          m <- timeout (30 * 1000000) $ Transport.recv conn
                          maybe (hPutStrLn stderr ("Send Ping to " ++ (show n) ++ " timeout") >> return Nothing) (either ((>> return Nothing) . (hPutStrLn stderr)) (\msg -> case msg of Pong n -> return (Just n); _ -> return Nothing) . decode) m

doSendStore :: Key -> Value -> [Node] -> IO [Node]
doSendStore k v toSend = do
  r <- P.mapM (sendStore' k v) toSend
  let ns = foldr (\x ns' -> maybe ns' (\n -> n : ns') x) [] r
  return ns
    where
      sendStore' :: Key -> Value -> Node -> IO (Maybe Node)
      sendStore' k v n =
          bracket (createTransport Req) Transport.free $ \t ->
              bracket (Transport.connect t n) (either (\_ -> return ()) Transport.close) $
                      either (\err -> hPutStrLn stderr err >> return Nothing) (\conn -> runSendM (sendStore k v) conn >> (timeout (30 * 1000000) (Transport.recv conn)) >>= maybe (hPutStrLn stderr ("Send Store to " ++ (show n) ++ " timeout") >> return (Just n)) (\m -> return Nothing))

deleteNoResponses :: [Node] -> Bucket -> Bucket
deleteNoResponses [] b = b
deleteNoResponses xs b = foldr deleteNode b xs
