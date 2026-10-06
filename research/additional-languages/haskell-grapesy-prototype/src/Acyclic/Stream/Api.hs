{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Acyclic.Stream.Api
  ( Append
  , Read
  , Follow
  , appendRequest
  , append
  ) where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Network.GRPC.Common
import Prelude hiding (Read)
import Network.GRPC.Common.Protobuf
import qualified Network.GRPC.Client as Client
import qualified Network.GRPC.Client.StreamType.IO as Typed
import Proto.Stream.V2.Stream
import Proto.Stream.V2.Stream_Fields hiding (append)

type Append = Protobuf StreamService "append"
type Read = Protobuf StreamService "read"
type Follow = Protobuf StreamService "follow"

type instance RequestMetadata          (Protobuf StreamService meth) = NoMetadata
type instance ResponseInitialMetadata  (Protobuf StreamService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf StreamService meth) = NoMetadata

-- | Build an Append request using the Rust-owned field types.  Consumers do
-- not construct an untyped record or translate the wire fields themselves.
appendRequest :: Text -> [ByteString] -> ByteString -> Proto AppendRequest
appendRequest pathValue recordsValue idempotencyKeyValue = Proto $
  defMessage
    & path .~ pathValue
    & records .~ recordsValue
    & idempotencyKey .~ idempotencyKeyValue

-- | Execute the unary Append RPC through the generated grapesy handler.
append :: Client.Connection -> Proto AppendRequest -> IO (Proto AppendResponse)
append conn value = Typed.nonStreaming conn (Client.rpc @Append) value
