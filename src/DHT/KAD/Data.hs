module DHT.KAD.Data (
                     addNode
                    , addNodes
                    , deleteNode
                    , findNode
                    , nearNodes
                    , allNodes
                    , Node(..)
                    , NID
                    , makeNid
                    , calcNid
                    , nodeDist
                    , nidDist
                    , dist2idx
                    , string2nid
                    , ip2string
                    , IP
                    , PORT
                    , Key
                    , Value
                    , Word160(..)
                    , Bucket(..)
                    , Cache
                    , Deadline
                    , Timestamp
                    , getTimestamp
                    , logMsg
                    , AppConfig(..)
                    , App
                    , runApp
                    ) where

import Control.Concurrent.MVar
import Control.Monad.Reader
import Crypto.Hash.RIPEMD160 as R160
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.List hiding (lookup, insert)
import qualified Data.IntMap.Strict as IntMap hiding (delete)
import qualified Data.Map.Strict as Map hiding (delete)
import Data.Time.Clock.POSIX
import Data.Word
import Prelude hiding (lookup)
import System.IO (hPutStrLn, stderr)

type IP = Word32
type PORT = Word16
type Key = Word160
type Value = B.ByteString

data Word160 = Word160 {
      w0 :: Word32
    , w1 :: Word64
    , w2 :: Word64
    } deriving (Eq, Ord)

instance Show Word160 where
    show (Word160 w0 w1 w2) = hex32 w0 ++ hex64 w1 ++ hex64 w2

byte2hex :: Word8 -> String
byte2hex x = [h, l]
    where
      h = tran $ fromIntegral x `shiftR` 4
      l = tran $ fromIntegral x .&. 0x0F
      tran i = "0123456789ABCDEF" !! i

hex32 :: Word32 -> String
hex32 x = byte2hex b0 ++ byte2hex b1 ++ byte2hex b2 ++ byte2hex b3
    where
      b0 = fromIntegral (x `shiftR` 24) :: Word8
      b1 = fromIntegral (x `shiftR` 16) :: Word8
      b2 = fromIntegral (x `shiftR` 08) :: Word8
      b3 = fromIntegral  x              :: Word8

hex64 :: Word64 -> String
hex64 x = byte2hex b0 ++ byte2hex b1 ++ byte2hex b2 ++ byte2hex b3 ++ byte2hex b4 ++ byte2hex b5 ++ byte2hex b6 ++ byte2hex b7
    where
      b0 = fromIntegral (x `shiftR` 56) :: Word8
      b1 = fromIntegral (x `shiftR` 48) :: Word8
      b2 = fromIntegral (x `shiftR` 40) :: Word8
      b3 = fromIntegral (x `shiftR` 32) :: Word8
      b4 = fromIntegral (x `shiftR` 24) :: Word8
      b5 = fromIntegral (x `shiftR` 16) :: Word8
      b6 = fromIntegral (x `shiftR` 08) :: Word8
      b7 = fromIntegral  x              :: Word8

word160ToByteString :: Word160 -> B.ByteString
word160ToByteString (Word160 w0 w1 w2) = B.pack $ (word32ToWord8s w0) ++ (word64ToWord8s w1) ++ (word64ToWord8s w2)
    where
      word32ToWord8s w = [b0, b1, b2, b3]
          where
            b0 = fromIntegral (w `shiftR` 24) :: Word8
            b1 = fromIntegral (w `shiftR` 16) :: Word8
            b2 = fromIntegral (w `shiftR` 08) :: Word8
            b3 = fromIntegral  w              :: Word8
      word64ToWord8s w = [b0, b1, b2, b3, b4, b5, b6, b7]
          where
            b0 = fromIntegral (w `shiftR` 56) :: Word8
            b1 = fromIntegral (w `shiftR` 48) :: Word8
            b2 = fromIntegral (w `shiftR` 40) :: Word8
            b3 = fromIntegral (w `shiftR` 32) :: Word8
            b4 = fromIntegral (w `shiftR` 24) :: Word8
            b5 = fromIntegral (w `shiftR` 16) :: Word8
            b6 = fromIntegral (w `shiftR` 08) :: Word8
            b7 = fromIntegral  w              :: Word8

byteString2Word32 :: B.ByteString -> Word32
byteString2Word32 bs | B.length bs == 4 = (fromIntegral b0 :: Word32) `shiftL` 24 .|.
                                          (fromIntegral b1 :: Word32) `shiftL` 16 .|.
                                          (fromIntegral b2 :: Word32) `shiftL` 08 .|.
                                          (fromIntegral b3 :: Word32)
                     | otherwise = 0
    where [b0, b1, b2, b3] = B.unpack bs

byteString2Word64 :: B.ByteString -> Word64
byteString2Word64 bs | B.length bs == 8 = (fromIntegral b0 :: Word64) `shiftL` 56 .|.
                                          (fromIntegral b1 :: Word64) `shiftL` 48 .|.
                                          (fromIntegral b2 :: Word64) `shiftL` 40 .|.
                                          (fromIntegral b3 :: Word64) `shiftL` 32 .|.
                                          (fromIntegral b4 :: Word64) `shiftL` 24 .|.
                                          (fromIntegral b5 :: Word64) `shiftL` 16 .|.
                                          (fromIntegral b6 :: Word64) `shiftL` 08 .|.
                                          (fromIntegral b7 :: Word64)
                     | otherwise = 0
    where [b0, b1, b2, b3, b4, b5, b6, b7] = B.unpack bs

byteString2Word160 :: B.ByteString -> Word160
byteString2Word160 bs | B.length bs == 20 = Word160 w0 w1 w2
                      | otherwise = Word160 0 0 0
    where
      w0 = byteString2Word32 $ B.take 4 bs
      w1 = byteString2Word64 $ B.take 8 $ B.drop 4  bs
      w2 = byteString2Word64 $ B.take 8 $ B.drop 12 bs

-- the number of leading zeros
nlz32 :: Word32 -> Int
nlz32 x
    | x == 0 = 32
    | otherwise = num - fromIntegral (x `shiftR` 31)
    where
      num = calc 16 8 x 1
      calc a d x n
          | d == 1 = n
          | otherwise =
              if (x `shiftR` a) == 0 then
                  calc (a + d) (d `shiftR` 1) (x `shiftL` (32 - a)) (n + (32 - a))
              else
                  calc (a + d) (d `shiftR` 1) x n

nlz64 :: Word64 -> Int
nlz64 x
    | x == 0 = 64
    | otherwise = num - fromIntegral (x `shiftR` 63)
    where
      num = calc 32 16 x 1
      calc a d x n
          | d == 1 = n
          | otherwise =
              if (x `shiftR` a) == 0 then
                  calc (a + d) (d `shiftR` 1) (x `shiftL` (64 - a)) (n + (64 - a))
              else
                  calc (a + d) (d `shiftR` 1) x n

nlz160 :: Word160 -> Int
nlz160 (Word160 w0 w1 w2) =
    if w0 == 0 then
        if w1 == 0 then
            32 + 64 + nlz64 w2
        else
            32 + nlz64 w1
    else
        nlz32 w0

type NID = Word160

makeNid :: Word32 -> Word64 -> Word64 -> NID
makeNid = Word160

calcNid :: IP -> PORT -> NID
calcNid ip port = byteString2Word160 $ R160.hash $ B.pack bytes
    where
      bytes = [ip0, ip1, ip2, ip3, port0, port1]
      ip0   = fromIntegral (ip `shiftR` 24)     :: Word8
      ip1   = fromIntegral (ip `shiftR` 16)     :: Word8
      ip2   = fromIntegral (ip `shiftR` 08)     :: Word8
      ip3   = fromIntegral  ip                  :: Word8
      port0 = fromIntegral (port `shiftR` 08)   :: Word8
      port1 = fromIntegral  port                :: Word8


nidXor :: NID -> NID -> NID
nidXor n1 n2 = Word160 (w0 n1 `xor` w0 n2)
               (w1 n1 `xor` w1 n2)
               (w2 n1 `xor` w2 n2)

data Node = Node {
      nid :: NID
    , ip :: IP
    , port :: PORT
    } deriving (Eq, Ord)

instance Show Node where
    show (Node nid ip port) = "Node(" ++ show nid ++ ", " ++ ip2string ip ++ ", " ++ show port ++ ")"

nodeDist :: Node -> Node -> Word160
nodeDist a b = nidDist (nid a) (nid b)

nidDist :: NID -> NID -> Word160
nidDist = nidXor

type Cache = Map.Map Key (Deadline, Value)

data Bucket = Bucket Node (IntMap.IntMap [Node])

dist2idx :: Word160 -> Int
dist2idx = (160 -) . nlz160

addNode :: Node -> Bucket -> Int -> Bucket
addNode target b@(Bucket local nodemap) threshold
    | threshold > 0 =
        if nid target == nid local then
            b
        else
            maybe newItemBucket updatedBucket (IntMap.lookup idx nodemap)
    where idx = dist2idx $ nodeDist local target
          newItemBucket = Bucket local $ IntMap.insert idx [target] nodemap
          updatedBucket nodes = Bucket local $ IntMap.update (\xs -> Just (if target `notElem` xs then target : (if length nodes > threshold then take (threshold - 1) xs else xs) else xs)) idx nodemap

addNodes :: [Node] -> Bucket -> Int -> Bucket
addNodes nodes b@(Bucket local nodemap) threshold
    | threshold > 0 =
        Bucket local $ foldr (\x m -> let index = idx x in maybe (new x) (update index x) (IntMap.lookup index nodemap)) nodemap $ filter (\x -> nid x /= nid local) nodes
    where idx n = dist2idx $ nodeDist local n
          new node = IntMap.insert (idx node) [node] nodemap
          update index node nodes' = IntMap.update (\xs -> Just (if node `notElem` xs then node : (if length nodes' > threshold then take (threshold - 1) xs else xs) else xs)) index nodemap

deleteNode :: Node -> Bucket -> Bucket
deleteNode target b@(Bucket local nodemap) =
    if IntMap.member idx nodemap then
        newBucket
    else
        b
    where idx = dist2idx $ nodeDist local target
          newBucket = Bucket local $ IntMap.adjust (delete target) idx nodemap

findNode :: NID -> Bucket -> Maybe Node
findNode target (Bucket local nodemap) =
    if target == nid local then
        Just local
    else
        maybe Nothing foundNode (IntMap.lookup idx nodemap)
    where idx = dist2idx $ nidXor target $ nid local
          foundNode = find (\n -> target == nid n)

nearNodes :: NID -> Bucket -> Int -> [Node]
nearNodes target (Bucket local nodemap) threshold
    | threshold > 0 =
        filter (\x -> nid x /= localnid) $ maybe allNodes aroundNodes (IntMap.lookup idx nodemap)
    where idx = dist2idx $ nidXor target $ nid local
          allNodes = take threshold $ concat $ IntMap.elems $ IntMap.mapKeysWith (++) (\k' -> abs(k' - idx)) nodemap
          aroundNodes ns = if length ns <= threshold then ns else take threshold ns
          localnid = nid local

allNodes :: Bucket -> [Node]
allNodes (Bucket _ nodemap) = concat $ IntMap.elems nodemap

string2nid :: String -> Word160
string2nid = byteString2Word160 . R160.hash . BC.pack

ip2string :: Word32 -> String
ip2string ip = (show b0) ++ "." ++ (show b1) ++ "." ++ (show b2) ++ "." ++ (show b3)
    where
      b0 = fromIntegral (ip `shiftR` 24) :: Word8
      b1 = fromIntegral (ip `shiftR` 16) :: Word8
      b2 = fromIntegral (ip `shiftR` 08) :: Word8
      b3 = fromIntegral  ip              :: Word8

type Timestamp = Word32
type Deadline = Timestamp

getTimestamp :: IO Timestamp
getTimestamp = do
  t <- getPOSIXTime
  return (fromIntegral (round t) :: Timestamp)

logMsg :: String -> IO ()
logMsg s = do
  t <- getTimestamp
  hPutStrLn stderr $ show t ++ " | " ++ s

data AppConfig = AppConfig {
      netTimeout :: Int
    , nodeCount :: Int
    , threshold :: Int
    , bucket :: MVar Bucket
    , cache :: MVar Cache
    , local :: Node
}

type App = ReaderT AppConfig IO

runApp :: Int -> Int -> Int -> MVar Bucket -> MVar Cache -> Node -> App a -> IO a
runApp t nc th b c l a =
    let config = AppConfig t nc th b c l
    in runReaderT a config
