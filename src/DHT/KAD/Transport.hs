module DHT.KAD.Transport
    (
     Transport(..)
    , Connection(..)
    , Path
    ) where

import Data.Bits
import Data.ByteString
import Data.Word

import DHT.KAD.Data

type Path = String

data Transport = Transport {
      connect :: Node -> IO (Either String Connection)
    , bind :: Node -> IO (Either String Connection)
    , ipc :: Path -> IO (Either String Connection)
    , free :: IO ()
    }

data Connection = Connection {
      send :: ByteString -> IO (Either String Int)
    , recv :: IO ByteString
    , close :: IO ()
}
