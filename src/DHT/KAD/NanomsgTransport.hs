module DHT.KAD.NanomsgTransport(withTransport) where

import Control.Exception (bracket, try)
import Data.ByteString
import Nanomsg

import DHT.KAD.Data
import DHT.KAD.Transport as Transport

data TransportState a = State {sock :: Socket a}

withTransport :: (Sender a, Receiver a) => a -> (Transport -> IO b) -> IO b
withTransport t = bracket (createTransport t) Transport.free

createTransport :: (Sender a, Receiver a) => a -> IO Transport
createTransport t = do
  s <- socket t
  let ts = State { sock = s }
  return Transport { Transport.connect = nanoConnect ts
                   , Transport.bind = nanoBind ts
                   , Transport.ipc = nanoIpc ts
                   , Transport.free = nanoFree ts
                   }

nanoConnect :: (Sender a, Receiver a) => TransportState a -> Node -> (Either String Connection -> IO b) -> IO b
nanoConnect ts (Node _ ip port) action = do
  let addr = "tcp://" ++ ip2string ip ++ ":" ++ show port
  nanoConn ts (Nanomsg.connect (sock ts) addr) action

nanoBind :: (Sender a, Receiver a) => TransportState a -> Node -> (Either String Connection -> IO b) -> IO b
nanoBind ts (Node _ ip port) action = do
  let addr = "tcp://" ++ ip2string ip ++ ":" ++ show port
  nanoConn ts (Nanomsg.bind (sock ts) addr) action

nanoIpc :: (Sender a, Receiver a) => TransportState a -> Path -> (Either String Connection -> IO b) -> IO b
nanoIpc ts path action = do
  let addr = "ipc://" ++ path
  nanoConn ts (Nanomsg.bind (sock ts) addr) action

nanoConn :: (Sender a, Receiver a) => TransportState a -> IO Endpoint -> (Either String Connection -> IO b) -> IO b
nanoConn ts ep action = do
  r <- try ep
  bracket (either (\x -> return $ Left $ show (x :: NNException))
           (\_ -> return $
                  Right Connection {
                              Transport.send = nanoSend $ sock ts
                            , Transport.recv = nanoRecv $ sock ts
                            }) r) (either (\_ -> return ()) (\_ -> either (\_ -> return ()) (nanoClose (sock ts)) r)) action

nanoFree :: TransportState a -> IO ()
nanoFree = Nanomsg.close . sock

nanoClose :: Socket a -> Endpoint -> IO ()
nanoClose = shutdown

nanoSend :: Sender a => Socket a -> ByteString -> IO (Either String Int)
nanoSend s b = do
  Nanomsg.send s b
  return $ Right $ Data.ByteString.length b

nanoRecv :: Receiver a => Socket a -> IO ByteString
nanoRecv = Nanomsg.recv
