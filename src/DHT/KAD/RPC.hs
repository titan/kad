module DHT.KAD.RPC (
                    Message(..)
                   , SendM
                   , runSendM
                   , sendPing
                   , sendPong
                   , sendStore
                   , sendStored
                   , sendFindNode
                   , sendFoundNode
                   , sendFindValue
                   , sendFoundValue
                   ) where

import Control.Monad.Reader
import Data.Bits
import Data.ByteString
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.Char
import Data.Map as Map
import Data.Serialize
import Data.Word
import DHT.KAD.Data
import DHT.KAD.Transport

data Message = Ping Node
             | Pong Node
             | Store Key Value
             | Stored Key
             | FindNode NID
             | FoundNode [Node]
             | FindValue Key
             | FoundValue Key Value
             | Error String

putVarint :: Putter Int
putVarint x | x <= 0x7f = putWord8 (fromIntegral x :: Word8)
            | x <= 0x3fff = do putWord8 (0x80 .|. a)
                               putWord8 (0x7f .&. b)
            | x <= 0x1fffff = do putWord8 (0x80 .|. a)
                                 putWord8 (0x80 .|. b)
                                 putWord8 (0x7f .&. c)
            | x <= 0xfffffff = do putWord8 (0x80 .|. a)
                                  putWord8 (0x80 .|. b)
                                  putWord8 (0x80 .|. c)
                                  putWord8 (0x7f .&. d)
            | otherwise = do putWord8 (0x80 .|. a)
                             putWord8 (0x80 .|. b)
                             putWord8 (0x80 .|. c)
                             putWord8 (0x80 .|. d)
                             putWord8 (0x7f .&. e)
    where
      a, b, c, d, e :: Word8
      a = fromIntegral x
      b = fromIntegral (shiftR x 7)
      c = fromIntegral (shiftR x 14)
      d = fromIntegral (shiftR x 21)
      e = fromIntegral (shiftR x 28 .&. 0x0f)

getVarint :: Get Int
getVarint = loop 0 0
    where loop :: Int -> Int -> Get Int
          loop s r = do
            b <- getWord8
            case 0x80 .&. b of
              0 -> return $ r .|. (fromIntegral (b .&. 0x7f) `shiftL` s)
              _ -> loop (s + 7) (r .|. (fromIntegral (b .&. 0x7f) `shiftL` s))

putWord160 :: Putter Word160
putWord160 nid = do
  putWord32be $ w0 nid
  putWord64be $ w1 nid
  putWord64be $ w2 nid

getWord160 :: Get Word160
getWord160 = do
  w0 <- getWord32be
  w1 <- getWord64be
  w2 <- getWord64be
  return $ Word160 w0 w1 w2

putKV :: Putter (Key, Value)
putKV (k, v) = do
  putWord160 k
  putVarint $ B.length v
  putByteString v

getKV :: Get (Key, Value)
getKV = do
  ks <- getWord160
  vl <- getVarint
  vs <- getByteString vl
  return (ks, vs)

putNode :: Putter Node
putNode (Node nid ip port) = do
  putWord160 nid
  putWord32be ip
  putWord16be port

getNode :: Get Node
getNode = do
  nid <- getWord160
  ip <- getWord32be
  port <- getWord16be
  return (Node nid ip port)

instance Serialize Message where
    put (Ping node) = putWord8 0x01 >> putNode node
    put (Pong node) = putWord8 0x81 >> putNode node
    put (Store k v) = putWord8 0x02 >> putKV (k, v)
    put (Stored k) = putWord8 0x82 >> putWord160 k
    put (FindNode nid) = putWord8 0x03 >> putWord160 nid
    put (FoundNode nodes) = do
      putWord8 0x83
      putVarint $ Prelude.length nodes
      mapM_ putNode nodes
    put (FindValue k) = putWord8 0x04 >> putWord160 k
    put (FoundValue k v) = putWord8 0x84 >> putKV (k, v)
    put (Error err) = do
      putWord8 0x00
      putVarint $ B.length b
      putByteString b
          where b = BC.pack err
    get = do code <- getWord8
             case code of
               0x00 -> getError
               0x01 -> getPing
               0x81 -> getPong
               0x02 -> getStore
               0x82 -> getStored
               0x03 -> getFindNode
               0x83 -> getFoundNode
               0x04 -> getFindValue
               0x84 -> getFoundValue
               _ -> return $ Error "Unknown message"
        where
          getPing :: Get Message
          getPing = liftM Ping getNode
          getPong :: Get Message
          getPong = liftM Pong getNode
          getError :: Get Message
          getError = do len <- getVarint
                        bs <- getByteString len
                        return $ Error $ BC.unpack bs
          getStore :: Get Message
          getStore = getKV >>= uncurry ((return .) . Store)
          getStored :: Get Message
          getStored = liftM Stored getWord160
          getFindNode :: Get Message
          getFindNode = liftM FindNode getWord160
          getFoundNode :: Get Message
          getFoundNode = do len <- getVarint
                            nodes <- getNodes (return []) len
                            return $ FoundNode nodes
              where getNodes :: Get [Node] -> Int -> Get [Node]
                    getNodes nodes 0 = nodes
                    getNodes nodes loop = do
                             n <- getNode
                             ns <- nodes
                             getNodes (return (n:ns)) (loop - 1)
          getFindValue :: Get Message
          getFindValue = liftM FindValue getWord160
          getFoundValue :: Get Message
          getFoundValue = getKV >>= uncurry ((return .) . FoundValue)

packMessage :: Message -> ByteString
packMessage = encode

type SendM = ReaderT Connection IO (Either String Int)
runSendM :: SendM -> Connection -> IO (Either String Int)
runSendM = runReaderT

sendMessage :: Message -> SendM
sendMessage m = do
  c <- ask
  liftIO $ sendMsg c m
  where sendMsg :: Connection -> Message -> IO (Either String Int)
        sendMsg c m = send c $ packMessage m

sendPing :: Node -> SendM
sendPing = sendMessage . Ping

sendPong :: Node -> SendM
sendPong = sendMessage . Pong

sendStore :: Key -> Value -> SendM
sendStore = (sendMessage .) . Store

sendStored :: Key -> SendM
sendStored = sendMessage . Stored

sendFindNode :: NID -> SendM
sendFindNode = sendMessage . FindNode

sendFoundNode :: [Node] -> SendM
sendFoundNode = sendMessage . FoundNode

sendFindValue :: Key -> SendM
sendFindValue = sendMessage . FindValue

sendFoundValue :: Key -> Value -> SendM
sendFoundValue = (sendMessage .) . FoundValue
