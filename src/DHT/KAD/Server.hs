module DHT.KAD.Server (start) where

import Control.Concurrent.MVar
import Control.Exception (bracket)
import Control.Monad (forever, void)
import qualified Data.Map.Strict as Map
import Data.Serialize (decode)
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
                            either putStrLn (\msg ->
                                                case msg of
                                                  Ping n -> modifyMVar_ bucket (return . flip (addNode n) 8) >> readMVar bucket >>= \(Bucket local map') -> runSendM (sendPong local) conn >> return ()
                                                  Store k v -> modifyMVar_ cache (return . Map.insert k v) >> void (runSendM (sendStored k) conn)
                                                  FindNode nid -> readMVar bucket >>= sendNearNodes conn nid -- \b -> (sendNearNodes conn nid b) (\node -> void $ runSendM (sendFoundNode [node]) conn) (findNode nid b)
                                                  FindValue k -> readMVar cache >>= maybe (sendNearNodes conn k =<< readMVar bucket) (void . flip runSendM conn . sendFoundValue k) . Map.lookup k
                                                  -- FindValue k -> readMVar cache >>= \ch -> maybe (readMVar bucket >>= sendNearNodes conn k) (\v -> void $ runSendM (sendFoundValue k v) conn) (Map.lookup k ch)
                                                  Error err -> putStrLn err
                                           ) . decode =<< Transport.recv conn
    where
      sendNearNodes conn nid bucket = do
        let nodes = nearNodes nid bucket 3
        runSendM (sendFoundNode nodes) conn
        return ()
