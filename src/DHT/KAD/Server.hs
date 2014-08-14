module DHT.KAD.Server (start) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (forever, void, when)
import Control.Monad.Reader
import Control.Monad.Writer
import qualified Control.Monad.Parallel as P (mapM)
import Data.Maybe
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Nanomsg
import System.Timeout (timeout)

import qualified DHT.KAD.Client as Client (refresh)
import DHT.KAD.Data
import DHT.KAD.NanomsgTransport
import qualified DHT.KAD.RPC as RPC
import qualified DHT.KAD.Transport as Transport

start :: App ()
start = do
  (AppConfig timeout' nodeCount threshold bucket cache local) <- ask
  liftIO $ withTransport Rep $ \t ->
        Transport.bind t local $
        either logMsg $ \conn -> logMsg (show local ++ " started")>> getTimestamp >>= (runApp timeout' nodeCount threshold bucket cache local) . loop conn
    where
      sendNearNodes :: Transport.Connection -> Word160 -> NID -> App ()
      sendNearNodes conn sn nid = do
          (AppConfig _ nc _ bucket _ local) <- ask
          bkt <- liftIO $ readMVar bucket
          let nodes = nearNodes nid bkt nc
          liftIO $ RPC.sendFoundNode sn local nodes conn
          return ()
      loop :: Transport.Connection -> Timestamp -> App ()
      loop conn lastTimestamp = do
          (AppConfig _ _ _ bucket cache local) <- ask
          m <- liftIO $ timeout (60 * 1000000) $ Transport.recv conn
          maybe
            (liftIO getTimestamp >>= \now -> when (now - lastTimestamp > 3600) refresh)
            (either
             (liftIO . logMsg)
             (\(RPC.Message (RPC.MsgHead sn from) msg) -> do
                now <- liftIO getTimestamp
                case msg of
                  RPC.Ping -> liftIO (RPC.sendPong sn local conn) >> tryAddNode from >> loop conn now
                  RPC.Store k v d -> liftIO (modifyMVar_ cache (return . Map.insert k (d, v)) >> RPC.sendStored sn local k conn) >> tryAddNode from >> loop conn now
                  RPC.FindNode nid -> sendNearNodes conn sn nid >> tryAddNode from >> loop conn now
                  RPC.FindValue k -> liftIO (readMVar cache) >>= maybe (sendNearNodes conn sn k) (\(d, v) -> liftIO (void $ RPC.sendFoundValue sn local k v d conn)) . Map.lookup k >> tryAddNode from >> loop conn now
                  RPC.Error err -> liftIO (logMsg err) >> loop conn now
             ) . RPC.unpackMessage)
            m

refresh :: App ()
refresh = do
  (AppConfig t _ th bucket _ local) <- ask
  liftIO $ do
            b <- readMVar bucket
            r <- P.mapM (\x -> sendPing' local x t) $ allNodes b
            modifyMVar_ bucket $ \bkt -> return $ addNodes (catMaybes r) (Bucket local IntMap.empty) th

tryAddNode :: Node -> App ()
tryAddNode node = do
  (AppConfig t nc th bucket _ local) <- ask
  n1 <- liftIO $ withMVar bucket $ \bkt -> return $ findNode (nid node) bkt
  bkt' <- liftIO $ modifyMVar bucket $ \bkt@(Bucket _ nodemap) ->
         if nid node == nid local then
             return (bkt, bkt)
         else do
           bkt'' <- maybe
                      (newItemBucket local nodemap)
                      (updatedBucket bkt t th)
                      (IntMap.lookup (idx node local) nodemap)
           return (bkt'', bkt'')
  n2 <- liftIO $ return $ findNode (nid node) bkt'
  when (isNothing n1 && isJust n2) $
       do
         let nodes = nearNodes (nid local) bkt' nc
         when (node `elem` nodes) Client.refresh
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
        Transport.connect t n (RPC.sendPing l n timeout')
