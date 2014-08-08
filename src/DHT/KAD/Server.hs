module DHT.KAD.Server (start) where

import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (forever, void)
import qualified Data.Map.Strict as Map
import Nanomsg

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
                            either putStrLn (\(RPC.Message h msg) ->
                                                case msg of
                                                  Ping n -> modifyMVar_ bucket (return . flip (addNode n) 8) >> readMVar bucket >>= \(Bucket local map') -> runSendM (sendPong h local) conn >> return ()
                                                  Store k v -> modifyMVar_ cache (return . Map.insert k v) >> void (runSendM (sendStored h k) conn)
                                                  FindNode nid -> readMVar bucket >>= sendNearNodes conn h nid -- \b -> (sendNearNodes conn nid b) (\node -> void $ runSendM (sendFoundNode [node]) conn) (findNode nid b)
                                                  FindValue k -> readMVar cache >>= maybe (sendNearNodes conn h k =<< readMVar bucket) (void . flip runSendM conn . sendFoundValue h k) . Map.lookup k
                                                  -- FindValue k -> readMVar cache >>= \ch -> maybe (readMVar bucket >>= sendNearNodes conn k) (\v -> void $ runSendM (sendFoundValue k v) conn) (Map.lookup k ch)
                                                  Error err -> putStrLn err
                                           ) . RPC.unpackMessage =<< Transport.recv conn
    where
      sendNearNodes conn h nid bucket = do
        let nodes = nearNodes nid bucket 3
        runSendM (sendFoundNode h nodes) conn
        return ()
