{-# LANGUAGE RankNTypes #-}
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
      connect :: forall a. Node -> (Either String Connection -> IO a) -> IO a
    , bind :: forall a. Node -> (Either String Connection -> IO a) -> IO a
    , ipc :: forall a. Path -> (Either String Connection -> IO a) -> IO a
    , free :: IO ()
    }

data Connection = Connection {
      send :: ByteString -> IO (Either String Int)
    , recv :: IO ByteString
}
