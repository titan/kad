module DHT.KAD.Client (
                       start
                      , findValue
                      , store
                      ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (forever, liftM)
import Control.Monad.Reader
import Control.Monad.Parallel as P (mapM)
import Data.Bits
import Data.List (sortBy)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Data.Maybe (catMaybes, isJust, fromJust)
import Data.Serialize
import Nanomsg
import System.Timeout (timeout)

import DHT.KAD.Data hiding (findNode)
import DHT.KAD.NanomsgTransport
import DHT.KAD.RPC as RPC
import DHT.KAD.Transport as Transport

start :: [Node] -> App ()
start nodes = do
  joinSelf nodes
  forever $ do
    liftIO $ threadDelay $ 60 * 60 * 1000000
    refresh

findNode :: NID -> App [Node]
findNode nid = do
  (AppConfig timeout' nodeCount threshold bucket _ local) <- ask
  bkt <- liftIO $ readMVar bucket
  let nodes = nearNodes nid bkt nodeCount
  (targets, nrps) <- liftIO $ go local Map.empty Map.empty nodes timeout' threshold
  liftIO $ modifyMVar_ bucket (return . deleteNoResponses nrps)
  return targets
    where
      sendFindNode' :: Node -> Int -> Node -> IO (Node, Maybe [Node])
      sendFindNode' local timeout' n =
          withTransport Req $ \t ->
              Transport.connect t n $
                      either (\err -> logMsg err >> return (n, Nothing)) $ \c -> do
                        sn <- RPC.genSN
                        runSendM (sendFindNode (MsgHead sn local) nid) c
                        m <- timeout timeout' $ Transport.recv c
                        maybe
                          (logMsg "Send FindNode timeout" >> return (n, Nothing))
                          (either
                           (\err -> logMsg err >> return (n, Nothing))
                           (\(Message (MsgHead sn' _) msg) ->
                                if sn' == sn then
                                    case msg of
                                      FoundNode ns -> return (n, Just ns)
                                      _ -> return (n, Just [])
                                else
                                    return (n, Nothing)) . RPC.unpackMessage)
                          m
      go :: Node -> Map.Map Node Int -> Map.Map Node Int -> [Node] -> Int -> Int -> IO ([Node], [Node])
      go _ result failed [] _ threshold =
          return (if Map.size result > threshold then
                      take threshold $ sortBy (\a@(Node anid _ _) b@(Node bnid _ _) -> compare (dist2idx (nidDist nid anid)) (dist2idx (nidDist nid bnid))) (Map.keys result)
                  else
                      Map.keys result, Map.keys failed)
      go local result failed toSend timeout' threshold = do
                        found <- P.mapM (sendFindNode' local timeout') toSend
                        let (tm, fm) = foldr (\(src, x) (tm, fm) -> maybe (tm, Map.insert src 0 fm) (\ns -> (foldr (\y m -> Map.insert y 0 m) tm ns, fm)) x) (Map.empty :: Map.Map Node Int, failed) found
                        let rm = foldr (\x m -> Map.insert x 0 m) result $ filter (\x -> x `Map.notMember` failed && x `Map.notMember` result) toSend
                        let unsent = filter (\x -> x `notElem` toSend && x `Map.notMember` fm && x `Map.notMember` rm) (Map.keys tm)
                        go local rm fm unsent timeout' threshold

findValue :: Key -> App (Maybe (Deadline, Value))
findValue key = do
  (AppConfig timeout' nodeCount _ bucket cache local) <- ask
  kache <- liftIO $ readMVar cache
  now <- liftIO getTimestamp
  let vp = Map.lookup key kache
  if isJust vp && now < fst (fromJust vp) then
      return vp
  else do
    bkt <- liftIO $ readMVar bucket
    let nodes = nearNodes key bkt nodeCount
    (p, nrps, nvs) <- liftIO $ go local nodes key Nothing Map.empty Map.empty timeout'
    liftIO $ modifyMVar_ bucket (return . deleteNoResponses nrps)
    maybe
      (return p)
      (\(d, v) -> do
         nrps' <- doSendStore key v d nvs
         liftIO $ modifyMVar_ bucket (return . deleteNoResponses nrps')
         liftIO $ modifyMVar_ cache (return . Map.insert key (if now < d then now + (d - now) `shiftR` 1 else d, v))
         return p)
      p
    where
      sendFindValue' :: Node -> Key -> Int -> Node -> IO (Node, Maybe (Either [Node] (Deadline, Value)))
      sendFindValue' local k timeout' n =
          withTransport Req $ \t ->
              Transport.connect t n $
                       either (\err -> logMsg err >> return (n, Nothing)) $ \conn ->
                            do
                              sn <- RPC.genSN
                              runSendM (sendFindValue (MsgHead sn local) k) conn
                              m <- timeout timeout' $ Transport.recv conn
                              maybe
                                (logMsg ("Send FindValue to " ++ show n ++ " timeout") >> return (n, Nothing))
                                (either
                                 ((>> return (n, Nothing)) . logMsg)
                                 (\(Message (MsgHead sn' _) msg) ->
                                      if sn' == sn then
                                          case msg of
                                            FoundValue _ v d -> return (n, Just (Right (d, v)))
                                            FoundNode ns -> return (n, Just (Left ns))
                                            _ -> return (n, Nothing)
                                      else
                                          return (n, Nothing)) . RPC.unpackMessage)
                                m
      go :: Node -> [Node] -> Key -> Maybe (Deadline, Value) -> Map.Map Node Int -> Map.Map Node Int -> Int -> IO (Maybe (Deadline, Value), [Node], [Node])
      go _ [] _ p noResponses noValues _ = return (p, Map.keys noResponses, Map.keys noValues)
      go local toSend k p noResponses noValues timeout' =
          do
            r <- P.mapM (sendFindValue' local k timeout') toSend
            let (p', nodes, noResponses', noValues') = foldr (\(src, x) (p'', nodes, noResponses'', noValues'') -> maybe (p'', nodes, Map.insert src 0 noResponses'', noValues'') (either (\ns' -> (p'', ns' ++ nodes, noResponses'', Map.insert src 0 noValues'')) (\p''' -> (Just p''', nodes, noResponses'', noValues''))) x) (Nothing, [], noResponses, noValues) r
            let toSend' = foldr (\x m -> Map.insert x 0 m) Map.empty $ filter (\x -> x `Map.notMember` noResponses' && x `Map.notMember` noValues' && x `notElem` toSend) nodes
            go local (Map.keys toSend') k (maybe p Just p') noResponses' noValues' timeout'

store :: Key -> Value -> Deadline -> App ()
store key value deadline = do
  (AppConfig _ _ _ bucket cache _) <- ask
  nodes <- findNode key
  if null nodes then
      liftIO $ modifyMVar_ cache $ return . Map.insert key (deadline, value)
  else
      do
        nrps <- doSendStore key value deadline nodes
        liftIO $ modifyMVar_ bucket (return . deleteNoResponses nrps)
        return ()

joinSelf :: [Node] -> App ()
joinSelf roots = do
  (AppConfig _ _ threshold bucket _ local) <- ask
  addNode' threshold roots bucket
  nodes <- findNode (nid local)
  addNode' threshold nodes bucket
    where
      addNode' th ns bucket = liftIO $ modifyMVar_ bucket $ \bkt -> return $ foldr (\x b -> addNode x b th) bkt ns

doSendStore :: Key -> Value -> Deadline -> [Node] -> App [Node]
doSendStore k v d toSend = do
  (AppConfig timeout' _ _ bucket _ local) <- ask
  now <- liftIO getTimestamp
  r <- liftIO $ P.mapM (sendStore' local k v d now timeout') toSend
  let ns = foldr (\x ns' -> maybe ns' (: ns') x) [] r
  return ns
    where
      sendStore' :: Node -> Key -> Value -> Deadline -> Deadline -> Int -> Node -> IO (Maybe Node)
      sendStore' local k v d now timeout' n =
          withTransport Req $ \t ->
              Transport.connect t n $
                       either
                         (\err -> logMsg err >> return Nothing)
                         (\conn ->
                              RPC.genSN >>=
                                     \sn ->
                                         runSendM (sendStore (MsgHead sn local) k v $ adjustDeadline d now k n) conn >>
                                                  timeout timeout' (Transport.recv conn) >>=
                                                          maybe
                                                            (logMsg ("Send Store to " ++ show n ++ " timeout") >> return (Just n))
                                                            (either (\err -> logMsg err >> return (Just n)) (\(Message (MsgHead sn' _) _) -> return $ if sn' == sn then Nothing else Just n) . RPC.unpackMessage))
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

refresh :: App ()
refresh = do
  (AppConfig timeout' nodeCount threshold bucket cache local) <- ask
  kache <- liftIO $ readMVar cache
  now <- liftIO $ getTimestamp
  forM_ (Map.keys kache) $ \x ->
      case Map.lookup x kache of
        Just (d, v) ->
            if now < d then
                store x v d
            else
              liftIO $ modifyMVar_ cache (return . Map.delete x)
        Nothing -> return ()
