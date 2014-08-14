module DHT.KAD.RPC (
                    Message(..)
                   , MsgHead(..)
                   , MsgBody(..)
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
                   , unpackMessage
                   , genSN
                   ) where

import Control.Monad.Reader
import Data.Bits
import Data.ByteString
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.Char
import qualified Data.Map as Map
import Data.Serialize
import Data.Word
import System.Random
import System.Timeout (timeout)

import DHT.KAD.Data
import DHT.KAD.Transport

data Message = Message MsgHead MsgBody

data MsgHead = MsgHead Word160 Node

data MsgBody = Ping
             | Pong
             | Store Key Value Deadline
             | Stored Key
             | FindNode NID
             | FoundNode [Node]
             | FindValue Key
             | FoundValue Key Value Deadline
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
    put (Message head body) = put head >> put body
    get = get >>= \h -> get >>= \b -> return $ Message h b

instance Serialize MsgHead where
    put (MsgHead sn from) = putWord160 sn >> putNode from
    get = getWord160 >>= \sn -> getNode >>= \from -> return $ MsgHead sn from

instance Serialize MsgBody where
    put Ping = putWord8 0x01
    put Pong = putWord8 0x81
    put (Store k v d) = putWord8 0x02 >> putKV (k, v) >> putWord32be d
    put (Stored k) = putWord8 0x82 >> putWord160 k
    put (FindNode nid) = putWord8 0x03 >> putWord160 nid
    put (FoundNode nodes) = do
      putWord8 0x83
      putVarint $ Prelude.length nodes
      mapM_ putNode nodes
    put (FindValue k) = putWord8 0x04 >> putWord160 k
    put (FoundValue k v d) = putWord8 0x84 >> putKV (k, v) >> putWord32be d
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
          getPing :: Get MsgBody
          getPing = return Ping
          getPong :: Get MsgBody
          getPong = return Pong
          getError :: Get MsgBody
          getError = do len <- getVarint
                        bs <- getByteString len
                        return $ Error $ BC.unpack bs
          getStore :: Get MsgBody
          getStore = getKV >>= \(k , v) -> getWord32be >>= \d -> return $ Store k v d
          getStored :: Get MsgBody
          getStored = liftM Stored getWord160
          getFindNode :: Get MsgBody
          getFindNode = liftM FindNode getWord160
          getFoundNode :: Get MsgBody
          getFoundNode = do len <- getVarint
                            nodes <- getNodes (return []) len
                            return $ FoundNode nodes
              where getNodes :: Get [Node] -> Int -> Get [Node]
                    getNodes nodes 0 = nodes
                    getNodes nodes loop = do
                             n <- getNode
                             ns <- nodes
                             getNodes (return (n:ns)) (loop - 1)
          getFindValue :: Get MsgBody
          getFindValue = liftM FindValue getWord160
          getFoundValue :: Get MsgBody
          getFoundValue = getKV >>= \(k, v) -> getWord32be >>= \d -> return (FoundValue k v d)

packMessage :: Message -> ByteString
packMessage = encode

unpackMessage :: ByteString -> Either String Message
unpackMessage = decode

type SendM = ReaderT Connection IO (Either String Int)
runSendM :: SendM -> Connection -> IO (Either String Int)
runSendM = runReaderT

sendMessage :: MsgHead -> MsgBody -> SendM
sendMessage h b = do
  c <- ask
  liftIO $ sendMsg c $ Message h b
  where sendMsg :: Connection -> Message -> IO (Either String Int)
        sendMsg c m = send c $ packMessage m

sendPing' :: MsgHead -> SendM
sendPing' h = sendMessage h Ping

sendPong' :: MsgHead -> SendM
sendPong' h = sendMessage h Pong

sendStore' :: MsgHead -> Key -> Value -> Deadline -> SendM
sendStore' h k v d = sendMessage h $ Store k v d

sendStored' :: MsgHead -> Key -> SendM
sendStored' h k = sendMessage h $ Stored k

sendFindNode' :: MsgHead -> NID -> SendM
sendFindNode' h n = sendMessage h $ FindNode n

sendFoundNode' :: MsgHead -> [Node] -> SendM
sendFoundNode' h ns = sendMessage h $ FoundNode ns

sendFindValue' :: MsgHead -> Key -> SendM
sendFindValue' h k = sendMessage h $ FindValue k

sendFoundValue' :: MsgHead -> Key -> Value -> Deadline -> SendM
sendFoundValue' h k v d = sendMessage h $ FoundValue k v d

genSN :: IO Word160
genSN = do
  w0 <- getStdRandom random :: IO Word32
  w1 <- getStdRandom random :: IO Word64
  w2 <- getStdRandom random :: IO Word64
  return $ makeNid w0 w1 w2

sendPing :: Node -> Node -> Int -> Either String Connection -> IO (Maybe Node)
sendPing l n timeout' =
    either ((>> return Nothing) . logMsg) $ \c -> do
      sn <- genSN
      runSendM (sendPing' (MsgHead sn l)) c
      m <- timeout timeout' $ recv c
      maybe
        (logMsg ("Send Ping to " ++ show n ++ " timeout") >> return Nothing)
        (either
         ((>> return Nothing) . logMsg)
         (\(Message (MsgHead sn' from) msg) ->
              if sn' == sn then
                  case msg of
                    Pong -> return (Just from)
                    _ -> return Nothing
              else
                  return Nothing) . unpackMessage) m

sendPong :: Word160 -> Node -> Connection -> IO (Either String Int)
sendPong sn local = runSendM (sendPong' (MsgHead sn local))

sendFindNode :: Node -> Int -> Node -> NID -> Either String Connection -> IO (Node, Maybe [Node])
sendFindNode l timeout' n nid =
    either (\err -> logMsg err >> return (n, Nothing)) $ \c -> do
      sn <- genSN
      runSendM (sendFindNode' (MsgHead sn l) nid) c
      m <- timeout timeout' $ recv c
      maybe
        (logMsg ("Send FindNode to " ++ show n ++ " timeout") >> return (n, Nothing))
        (either
         (\err -> logMsg err >> return (n, Nothing))
         (\(Message (MsgHead sn' _) msg) ->
              if sn' == sn then
                  case msg of
                    FoundNode ns -> return (n, Just ns)
                    _ -> return (n, Just [])
              else
                  return (n, Nothing)) . unpackMessage)
        m

sendFoundNode :: Word160 -> Node -> [Node] -> Connection -> IO (Either String Int)
sendFoundNode sn local nodes = runSendM (sendFoundNode' (MsgHead sn local) nodes)

sendFindValue :: Node -> Key -> Int -> Node -> Either String Connection -> IO (Node, Maybe (Either [Node] (Deadline, Value)))
sendFindValue l k timeout' n =
    either (\err -> logMsg err >> return (n, Nothing)) $ \c -> do
      sn <- genSN
      runSendM (sendFindValue' (MsgHead sn l) k) c
      m <- timeout timeout' $ recv c
      maybe
        (logMsg ("Send FindValue to " ++ show n ++ " timeout") >> return (n, Nothing))
        (either
         ((>> return (n, Nothing)) . logMsg)
         (\(Message (MsgHead sn' _) msg) ->
              if sn' == sn then
                  case msg of
                    FoundValue _ v d -> return (n, Just (Right (d, v)))
                    FoundNode ns -> return (n, Just (Left ns))
                    _ -> return (n, Nothing)
              else
                  return (n, Nothing)) . unpackMessage)
        m

sendFoundValue :: Word160 -> Node -> Key -> Value -> Timestamp -> Connection -> IO (Either String Int)
sendFoundValue sn local k v d = runSendM (sendFoundValue' (MsgHead sn local) k v d)

sendStore :: Node -> Key -> Value -> Deadline -> Deadline -> Int -> Node -> Either String Connection -> IO (Maybe Node)
sendStore l k v d now timeout' n =
    either (\err -> logMsg err >> return Nothing) $ \c -> do
      sn <- genSN
      runSendM (sendStore' (MsgHead sn l) k v $ adjustDeadline d now k n) c
      m <- timeout timeout' (recv c)
      maybe
        (logMsg ("Send Store to " ++ show n ++ " timeout") >> return (Just n))
        (either
         (\err -> logMsg err >> return (Just n))
         (\(Message (MsgHead sn' _) _) ->
              return $
                     if sn' == sn then
                         Nothing
                     else
                         Just n) . unpackMessage)
        m
    where
      adjustDeadline :: Deadline -> Deadline -> Key -> Node -> Deadline
      adjustDeadline d now k (Node nid _ _) =
          if d > now then
              now + (fromIntegral (dist2idx (nidDist k nid) * delta d now `div` 160) :: Deadline)
          else
              0
      delta :: Deadline -> Deadline -> Int
      delta d now = fromIntegral (d - now) :: Int

sendStored :: Word160 -> Node -> Key -> Connection -> IO (Either String Int)
sendStored sn local k = runSendM (sendStored' (MsgHead sn local) k)
