{- This file was auto-generated from actors/v1/actors.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Actors.V1.Actors_Fields where
import qualified Data.ProtoLens.Runtime.Prelude as Prelude
import qualified Data.ProtoLens.Runtime.Data.Int as Data.Int
import qualified Data.ProtoLens.Runtime.Data.Monoid as Data.Monoid
import qualified Data.ProtoLens.Runtime.Data.Word as Data.Word
import qualified Data.ProtoLens.Runtime.Data.ProtoLens as Data.ProtoLens
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Encoding.Bytes as Data.ProtoLens.Encoding.Bytes
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Encoding.Growing as Data.ProtoLens.Encoding.Growing
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Encoding.Parser.Unsafe as Data.ProtoLens.Encoding.Parser.Unsafe
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Encoding.Wire as Data.ProtoLens.Encoding.Wire
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Field as Data.ProtoLens.Field
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Message.Enum as Data.ProtoLens.Message.Enum
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Service.Types as Data.ProtoLens.Service.Types
import qualified Data.ProtoLens.Runtime.Lens.Family2 as Lens.Family2
import qualified Data.ProtoLens.Runtime.Lens.Family2.Unchecked as Lens.Family2.Unchecked
import qualified Data.ProtoLens.Runtime.Data.Text as Data.Text
import qualified Data.ProtoLens.Runtime.Data.Map as Data.Map
import qualified Data.ProtoLens.Runtime.Data.ByteString as Data.ByteString
import qualified Data.ProtoLens.Runtime.Data.ByteString.Char8 as Data.ByteString.Char8
import qualified Data.ProtoLens.Runtime.Data.Text.Encoding as Data.Text.Encoding
import qualified Data.ProtoLens.Runtime.Data.Vector as Data.Vector
import qualified Data.ProtoLens.Runtime.Data.Vector.Generic as Data.Vector.Generic
import qualified Data.ProtoLens.Runtime.Data.Vector.Unboxed as Data.Vector.Unboxed
import qualified Data.ProtoLens.Runtime.Text.Read as Text.Read
actor ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "actor" a) =>
  Lens.Family2.LensLike' f s a
actor = Data.ProtoLens.Field.field @"actor"
actorId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "actorId" a) =>
  Lens.Family2.LensLike' f s a
actorId = Data.ProtoLens.Field.field @"actorId"
bindings ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bindings" a) =>
  Lens.Family2.LensLike' f s a
bindings = Data.ProtoLens.Field.field @"bindings"
body ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "body" a) =>
  Lens.Family2.LensLike' f s a
body = Data.ProtoLens.Field.field @"body"
capability ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "capability" a) =>
  Lens.Family2.LensLike' f s a
capability = Data.ProtoLens.Field.field @"capability"
checkpointBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "checkpointBytes" a) =>
  Lens.Family2.LensLike' f s a
checkpointBytes = Data.ProtoLens.Field.field @"checkpointBytes"
checkpointEpoch ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "checkpointEpoch" a) =>
  Lens.Family2.LensLike' f s a
checkpointEpoch = Data.ProtoLens.Field.field @"checkpointEpoch"
checkpointUnixMillis ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "checkpointUnixMillis" a) =>
  Lens.Family2.LensLike' f s a
checkpointUnixMillis
  = Data.ProtoLens.Field.field @"checkpointUnixMillis"
code ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "code" a) =>
  Lens.Family2.LensLike' f s a
code = Data.ProtoLens.Field.field @"code"
codeSha256 ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "codeSha256" a) =>
  Lens.Family2.LensLike' f s a
codeSha256 = Data.ProtoLens.Field.field @"codeSha256"
completedCursor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "completedCursor" a) =>
  Lens.Family2.LensLike' f s a
completedCursor = Data.ProtoLens.Field.field @"completedCursor"
configurationRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "configurationRevision" a) =>
  Lens.Family2.LensLike' f s a
configurationRevision
  = Data.ProtoLens.Field.field @"configurationRevision"
currentHead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "currentHead" a) =>
  Lens.Family2.LensLike' f s a
currentHead = Data.ProtoLens.Field.field @"currentHead"
cursor ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "cursor" a) =>
  Lens.Family2.LensLike' f s a
cursor = Data.ProtoLens.Field.field @"cursor"
deliveredCursor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "deliveredCursor" a) =>
  Lens.Family2.LensLike' f s a
deliveredCursor = Data.ProtoLens.Field.field @"deliveredCursor"
expectedConfigurationRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expectedConfigurationRevision" a) =>
  Lens.Family2.LensLike' f s a
expectedConfigurationRevision
  = Data.ProtoLens.Field.field @"expectedConfigurationRevision"
failedCursor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "failedCursor" a) =>
  Lens.Family2.LensLike' f s a
failedCursor = Data.ProtoLens.Field.field @"failedCursor"
failureCode ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "failureCode" a) =>
  Lens.Family2.LensLike' f s a
failureCode = Data.ProtoLens.Field.field @"failureCode"
handlerTimeoutMillis ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "handlerTimeoutMillis" a) =>
  Lens.Family2.LensLike' f s a
handlerTimeoutMillis
  = Data.ProtoLens.Field.field @"handlerTimeoutMillis"
headers ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "headers" a) =>
  Lens.Family2.LensLike' f s a
headers = Data.ProtoLens.Field.field @"headers"
homeRegion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "homeRegion" a) =>
  Lens.Family2.LensLike' f s a
homeRegion = Data.ProtoLens.Field.field @"homeRegion"
idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
idempotencyKey = Data.ProtoLens.Field.field @"idempotencyKey"
limits ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "limits" a) =>
  Lens.Family2.LensLike' f s a
limits = Data.ProtoLens.Field.field @"limits"
maybe'actor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'actor" a) =>
  Lens.Family2.LensLike' f s a
maybe'actor = Data.ProtoLens.Field.field @"maybe'actor"
maybe'checkpointUnixMillis ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'checkpointUnixMillis" a) =>
  Lens.Family2.LensLike' f s a
maybe'checkpointUnixMillis
  = Data.ProtoLens.Field.field @"maybe'checkpointUnixMillis"
maybe'currentHead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'currentHead" a) =>
  Lens.Family2.LensLike' f s a
maybe'currentHead = Data.ProtoLens.Field.field @"maybe'currentHead"
maybe'cursor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'cursor" a) =>
  Lens.Family2.LensLike' f s a
maybe'cursor = Data.ProtoLens.Field.field @"maybe'cursor"
maybe'failedCursor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'failedCursor" a) =>
  Lens.Family2.LensLike' f s a
maybe'failedCursor
  = Data.ProtoLens.Field.field @"maybe'failedCursor"
maybe'limits ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'limits" a) =>
  Lens.Family2.LensLike' f s a
maybe'limits = Data.ProtoLens.Field.field @"maybe'limits"
maybe'start ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'start" a) =>
  Lens.Family2.LensLike' f s a
maybe'start = Data.ProtoLens.Field.field @"maybe'start"
maybe'subscription ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'subscription" a) =>
  Lens.Family2.LensLike' f s a
maybe'subscription
  = Data.ProtoLens.Field.field @"maybe'subscription"
memoryBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "memoryBytes" a) =>
  Lens.Family2.LensLike' f s a
memoryBytes = Data.ProtoLens.Field.field @"memoryBytes"
message ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "message" a) =>
  Lens.Family2.LensLike' f s a
message = Data.ProtoLens.Field.field @"message"
method ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "method" a) =>
  Lens.Family2.LensLike' f s a
method = Data.ProtoLens.Field.field @"method"
name ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "name" a) =>
  Lens.Family2.LensLike' f s a
name = Data.ProtoLens.Field.field @"name"
placementAnchor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "placementAnchor" a) =>
  Lens.Family2.LensLike' f s a
placementAnchor = Data.ProtoLens.Field.field @"placementAnchor"
recoverableCursor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "recoverableCursor" a) =>
  Lens.Family2.LensLike' f s a
recoverableCursor = Data.ProtoLens.Field.field @"recoverableCursor"
resource ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "resource" a) =>
  Lens.Family2.LensLike' f s a
resource = Data.ProtoLens.Field.field @"resource"
retryCount ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "retryCount" a) =>
  Lens.Family2.LensLike' f s a
retryCount = Data.ProtoLens.Field.field @"retryCount"
start ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "start" a) =>
  Lens.Family2.LensLike' f s a
start = Data.ProtoLens.Field.field @"start"
state ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "state" a) =>
  Lens.Family2.LensLike' f s a
state = Data.ProtoLens.Field.field @"state"
status ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "status" a) =>
  Lens.Family2.LensLike' f s a
status = Data.ProtoLens.Field.field @"status"
streamPath ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "streamPath" a) =>
  Lens.Family2.LensLike' f s a
streamPath = Data.ProtoLens.Field.field @"streamPath"
subscription ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "subscription" a) =>
  Lens.Family2.LensLike' f s a
subscription = Data.ProtoLens.Field.field @"subscription"
subscriptionId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "subscriptionId" a) =>
  Lens.Family2.LensLike' f s a
subscriptionId = Data.ProtoLens.Field.field @"subscriptionId"
subscriptions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "subscriptions" a) =>
  Lens.Family2.LensLike' f s a
subscriptions = Data.ProtoLens.Field.field @"subscriptions"
url ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "url" a) =>
  Lens.Family2.LensLike' f s a
url = Data.ProtoLens.Field.field @"url"
value ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "value" a) =>
  Lens.Family2.LensLike' f s a
value = Data.ProtoLens.Field.field @"value"
vec'bindings ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'bindings" a) =>
  Lens.Family2.LensLike' f s a
vec'bindings = Data.ProtoLens.Field.field @"vec'bindings"
vec'headers ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'headers" a) =>
  Lens.Family2.LensLike' f s a
vec'headers = Data.ProtoLens.Field.field @"vec'headers"
vec'subscriptions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'subscriptions" a) =>
  Lens.Family2.LensLike' f s a
vec'subscriptions = Data.ProtoLens.Field.field @"vec'subscriptions"