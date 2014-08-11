module DHT.KAD.NanomsgTransport(createTransport) where

import Data.ByteString
import Nanomsg
import System.Posix.Types (Fd(..))

import DHT.KAD.Data
import DHT.KAD.Transport as Transport

data TransportState a = State {sock :: Socket a}

createTransport :: (Sender a, Receiver a) => a -> IO Transport
createTransport t = do
  s <- socket t
  let ts = State { sock = s }
  return Transport { Transport.connect = nanoConnect ts
                   , Transport.bind = nanoBind ts
                   , Transport.ipc = nanoIpc ts
                   , Transport.free = nanoFree ts
                   }

nanoConnect :: (Sender a, Receiver a) => TransportState a -> Node -> IO (Either String Connection)
nanoConnect ts (Node _ ip port) = do
  let addr = "tcp://" ++ (ip2string ip) ++ ":" ++ show port
  e <- Nanomsg.connect (sock ts) addr
  return $ Right Connection { Transport.send = nanoSend $ sock ts
                            , Transport.recv = nanoRecv $ sock ts
                            , Transport.close = nanoClose (sock ts) e
                            }

nanoBind :: (Sender a, Receiver a) => TransportState a -> Node -> IO (Either String Connection)
nanoBind ts (Node _ ip port) = do
  let addr = "tcp://" ++ (ip2string ip) ++ ":" ++ show port
  e <- Nanomsg.bind (sock ts) addr
  return $ Right Connection { Transport.send = nanoSend $ sock ts
                            , Transport.recv = nanoRecv $ sock ts
                            , Transport.close = nanoClose (sock ts) e
                            }

nanoIpc :: (Sender a, Receiver a) => TransportState a -> Path -> IO (Either String Connection)
nanoIpc ts path = do
  let addr = "ipc://" ++ path
  e <- Nanomsg.bind (sock ts) addr
  return $ Right Connection { Transport.send = nanoSend $ sock ts
                            , Transport.recv = nanoRecv $ sock ts
                            , Transport.close = nanoClose (sock ts) e
                            }

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
