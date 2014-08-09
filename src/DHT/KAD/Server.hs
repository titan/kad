module DHT.KAD.Server (start) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (forever, void)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Nanomsg
import System.IO (hPutStrLn, stderr)
import System.Timeout (timeout)

import DHT.KAD.Data
import DHT.KAD.NanomsgTransport
import DHT.KAD.RPC as RPC
import DHT.KAD.Transport as Transport

start :: MVar Bucket -> MVar Cache -> IO ()
start bucket cache =
  bracket (createTransport Rep)
          Transport.free $ \t ->
              bracket (readMVar bucket >>= \b@(Bucket local _) -> Transport.bind t local) (either (\_ -> return ()) Transport.close) $
                      either putStrLn $ \conn ->
                          forever $
                            either putStrLn (\(RPC.Message (MsgHead sn from) msg) ->
                                                case msg of
                                                  Ping -> readMVar bucket >>= \(Bucket local map') -> runSendM (sendPong (MsgHead sn local)) conn >> tryAddNode from bucket 8
                                                  Store k v -> modifyMVar_ cache (return . Map.insert k v) >> readMVar bucket >>= \(Bucket local _) -> runSendM (sendStored (MsgHead sn local) k) conn >> tryAddNode from bucket 8
                                                  FindNode nid -> readMVar bucket >>= sendNearNodes conn sn nid >> tryAddNode from bucket 8
                                                  FindValue k -> readMVar cache >>= maybe (sendNearNodes conn sn k =<< readMVar bucket) (\v -> readMVar bucket >>= \(Bucket local _) -> void $ runSendM (sendFoundValue (MsgHead sn local) k v) conn) . Map.lookup k >> tryAddNode from bucket 8
                                                  Error err -> putStrLn err
                                           ) . RPC.unpackMessage =<< Transport.recv conn
    where
      sendNearNodes conn sn nid bucket@(Bucket local _) = do
        let nodes = nearNodes nid bucket 3
        runSendM (sendFoundNode (MsgHead sn local) nodes) conn
        return ()

tryAddNode :: Node -> MVar Bucket -> Int -> IO ()
tryAddNode node bucket threshold =
    modifyMVar_ bucket $ \bkt@(Bucket local nodemap) ->
        if nid node == nid local then
            return bkt
        else
            maybe (newItemBucket local nodemap) (updatedBucket bkt) (IntMap.lookup (idx node local) nodemap)
    where
      idx node local = dist2idx $ nodeDist local node
      newItemBucket local nodemap = return $ Bucket local $ IntMap.insert (idx node local) [node] nodemap
      updatedBucket bkt@(Bucket local nodemap) nodes =
        if length nodes < threshold then
            if node `notElem` nodes then
                return $ Bucket local $ IntMap.insert (idx node local) (node : nodes) nodemap
            else
                return bkt
        else
            if node `notElem` nodes then
                sendPing' local (last nodes) >>= maybe
                              (return $ Bucket local $ IntMap.insert (idx node local) (node : take (length nodes - 1) nodes) nodemap)
                              (\_ -> return $ Bucket local $ IntMap.insert (idx node local) ((last nodes) : take (length nodes - 1) nodes) nodemap)
            else
                return $ Bucket local $ IntMap.insert (idx node local) (node : dropWhile (\x -> nid x == nid node) nodes) nodemap
      sendPing' :: Node -> Node -> IO (Maybe Node)
      sendPing' l n =
          bracket (createTransport Req) Transport.free $ \t ->
              bracket (Transport.connect t n) (either (\_ -> return ()) Transport.close) $
                      either (\err -> hPutStrLn stderr err >> return Nothing) $ \conn -> do
                          sn <- RPC.genSN
                          runSendM (sendPing (MsgHead sn l)) conn
                          m <- timeout (30 * 1000000) $ Transport.recv conn
                          maybe (hPutStrLn stderr ("Send Ping to " ++ (show n) ++ " timeout") >> return Nothing) (either ((>> return Nothing) . (hPutStrLn stderr)) (\(Message (MsgHead sn' from) msg) -> if sn' == sn then case msg of Pong -> return (Just from); _ -> return Nothing else return Nothing) . RPC.unpackMessage) m
