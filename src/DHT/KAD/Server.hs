module DHT.KAD.Server (start) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (forever, void, when)
import Control.Monad.Reader
import Control.Monad.Writer
import Control.Monad.Parallel as P (mapM)
import Data.Maybe
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Nanomsg
import System.Timeout (timeout)

import DHT.KAD.Data
import DHT.KAD.NanomsgTransport
import DHT.KAD.RPC as RPC
import DHT.KAD.Transport as Transport

type ServerM = ReaderT (Int, Int, Int, MVar Bucket, MVar Cache, Node) IO
runServerM t nc th b c l a = runReaderT a (t, nc, th, b, c, l)

start :: App ()
start = do
  (AppConfig timeout' nodeCount threshold bucket cache local) <- ask
  liftIO $ withTransport Rep $ \t ->
        Transport.bind t local $
        either logMsg $ \conn -> getTimestamp >>= (runServerM timeout' nodeCount threshold bucket cache local) . loop conn
    where
      sendNearNodes :: Connection -> Word160 -> NID -> ServerM ()
      sendNearNodes conn sn nid = do
          (_, nc, _, bucket, _, local) <- ask
          bkt <- liftIO $ readMVar bucket
          let nodes = nearNodes nid bkt nc
          liftIO $ runSendM (sendFoundNode (MsgHead sn local) nodes) conn
          return ()
      loop :: Connection -> Timestamp -> ServerM ()
      loop conn lastTimestamp = do
          (_, _, _, bucket, cache, local) <- ask
          m <- liftIO $ timeout (60 * 1000000) $ Transport.recv conn
          maybe (liftIO getTimestamp >>= \now -> when (now - lastTimestamp > 3600) $ refresh)
                    (either (liftIO . logMsg) (\(RPC.Message (MsgHead sn from) msg) -> do
                                      now <- liftIO getTimestamp
                                      case msg of
                                        Ping -> liftIO (runSendM (sendPong (MsgHead sn local)) conn) >> tryAddNode from >> loop conn now
                                        Store k v d -> liftIO (modifyMVar_ cache (return . Map.insert k (d, v)) >> runSendM (sendStored (MsgHead sn local) k) conn) >> tryAddNode from >> loop conn now
                                        FindNode nid -> sendNearNodes conn sn nid >> tryAddNode from >> loop conn now
                                        FindValue k -> liftIO (readMVar cache) >>= maybe (sendNearNodes conn sn k) (\(d, v) -> liftIO (void $ runSendM (sendFoundValue (MsgHead sn local) k v d) conn)) . Map.lookup k >> tryAddNode from >> loop conn now
                                        Error err -> liftIO (logMsg err) >> loop conn now
                                   ) . RPC.unpackMessage) m

refresh :: ServerM ()
refresh = do
  (t, _, th, bucket, _, local) <- ask
  liftIO $ do
            b <- readMVar bucket
            r <- P.mapM (\x -> sendPing' local x t) $ allNodes b
            modifyMVar_ bucket $ \bkt -> return $ addNodes (catMaybes r) (Bucket local IntMap.empty) th

tryAddNode :: Node -> ServerM ()
tryAddNode node = do
  (t, _, th, bucket, _, local) <- ask
  liftIO $ modifyMVar_ bucket $ \bkt@(Bucket _ nodemap) ->
      if nid node == nid local then
          return bkt
      else
          maybe
            (newItemBucket local nodemap)
            (updatedBucket bkt t th)
            (IntMap.lookup (idx node local) nodemap)
    where
      idx node local = dist2idx $ nodeDist local node
      newItemBucket local nodemap = return $ Bucket local $ IntMap.insert (idx node local) [node] nodemap
      updatedBucket bkt@(Bucket local nodemap) timeout' threshold nodes
          | length nodes < threshold && node `notElem` nodes = return $ Bucket local $ IntMap.insert (idx node local) (node : nodes) nodemap
          | length nodes < threshold && node `elem` nodes = return bkt
          | node `notElem` nodes =
              sendPing' local (last nodes) timeout' >>=
                maybe
                  (return $ Bucket local $ IntMap.insert (idx node local) (node : take (length nodes - 1) nodes) nodemap)
                  (\_ -> return $ Bucket local $ IntMap.insert (idx node local) (last nodes : take (length nodes - 1) nodes) nodemap)
          | otherwise = return $ Bucket local $ IntMap.insert (idx node local) (node : dropWhile (\x -> nid x == nid node) nodes) nodemap

sendPing' :: Node -> Node -> Int -> IO (Maybe Node)
sendPing' l n timeout' =
    withTransport Req $ \t ->
        Transport.connect t n $
                 either ((>> return Nothing) . logMsg) $ \conn -> do
                   sn <- RPC.genSN
                   runSendM (sendPing (MsgHead sn l)) conn
                   m <- timeout timeout' $ Transport.recv conn
                   maybe
                     (logMsg ("Send Ping to " ++ show n ++ " timeout") >> return Nothing)
                     (either
                      ((>> return Nothing) . logMsg)
                      (\(Message (MsgHead sn' from) msg) ->
                           if sn' == sn then
                               case msg of
                                 Pong -> return (Just from)
                                 _ -> return Nothing
                           else
                               return Nothing) . RPC.unpackMessage) m
