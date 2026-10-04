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
  ) where

import Network.GRPC.Common
import Prelude hiding (Read)
import Network.GRPC.Common.Protobuf
import Proto.Stream.V2.Stream

type Append = Protobuf StreamService "append"
type Read = Protobuf StreamService "read"
type Follow = Protobuf StreamService "follow"

type instance RequestMetadata          (Protobuf StreamService meth) = NoMetadata
type instance ResponseInitialMetadata  (Protobuf StreamService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf StreamService meth) = NoMetadata
