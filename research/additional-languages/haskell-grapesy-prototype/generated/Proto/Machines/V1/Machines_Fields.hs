{- This file was auto-generated from machines/v1/machines.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Machines.V1.Machines_Fields where
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
after ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "after" a) =>
  Lens.Family2.LensLike' f s a
after = Data.ProtoLens.Field.field @"after"
afterIdleMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "afterIdleMs" a) =>
  Lens.Family2.LensLike' f s a
afterIdleMs = Data.ProtoLens.Field.field @"afterIdleMs"
afterSequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "afterSequence" a) =>
  Lens.Family2.LensLike' f s a
afterSequence = Data.ProtoLens.Field.field @"afterSequence"
budgets ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "budgets" a) =>
  Lens.Family2.LensLike' f s a
budgets = Data.ProtoLens.Field.field @"budgets"
capabilities ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "capabilities" a) =>
  Lens.Family2.LensLike' f s a
capabilities = Data.ProtoLens.Field.field @"capabilities"
changedAtUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "changedAtUnixMs" a) =>
  Lens.Family2.LensLike' f s a
changedAtUnixMs = Data.ProtoLens.Field.field @"changedAtUnixMs"
checkpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "checkpoint" a) =>
  Lens.Family2.LensLike' f s a
checkpoint = Data.ProtoLens.Field.field @"checkpoint"
checkpointDestroyed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "checkpointDestroyed" a) =>
  Lens.Family2.LensLike' f s a
checkpointDestroyed
  = Data.ProtoLens.Field.field @"checkpointDestroyed"
checkpointed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "checkpointed" a) =>
  Lens.Family2.LensLike' f s a
checkpointed = Data.ProtoLens.Field.field @"checkpointed"
children ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "children" a) =>
  Lens.Family2.LensLike' f s a
children = Data.ProtoLens.Field.field @"children"
compatibility ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "compatibility" a) =>
  Lens.Family2.LensLike' f s a
compatibility = Data.ProtoLens.Field.field @"compatibility"
compatibilityRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "compatibilityRevision" a) =>
  Lens.Family2.LensLike' f s a
compatibilityRevision
  = Data.ProtoLens.Field.field @"compatibilityRevision"
concurrency ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "concurrency" a) =>
  Lens.Family2.LensLike' f s a
concurrency = Data.ProtoLens.Field.field @"concurrency"
contract ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contract" a) =>
  Lens.Family2.LensLike' f s a
contract = Data.ProtoLens.Field.field @"contract"
count ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "count" a) =>
  Lens.Family2.LensLike' f s a
count = Data.ProtoLens.Field.field @"count"
create ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "create" a) =>
  Lens.Family2.LensLike' f s a
create = Data.ProtoLens.Field.field @"create"
created ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "created" a) =>
  Lens.Family2.LensLike' f s a
created = Data.ProtoLens.Field.field @"created"
createdAtUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createdAtUnixMs" a) =>
  Lens.Family2.LensLike' f s a
createdAtUnixMs = Data.ProtoLens.Field.field @"createdAtUnixMs"
customDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "customDigest" a) =>
  Lens.Family2.LensLike' f s a
customDigest = Data.ProtoLens.Field.field @"customDigest"
dedicatedCpuNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "dedicatedCpuNs" a) =>
  Lens.Family2.LensLike' f s a
dedicatedCpuNs = Data.ProtoLens.Field.field @"dedicatedCpuNs"
destroyCheckpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "destroyCheckpoint" a) =>
  Lens.Family2.LensLike' f s a
destroyCheckpoint = Data.ProtoLens.Field.field @"destroyCheckpoint"
destroyMachine ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "destroyMachine" a) =>
  Lens.Family2.LensLike' f s a
destroyMachine = Data.ProtoLens.Field.field @"destroyMachine"
durablePrivateBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "durablePrivateBytes" a) =>
  Lens.Family2.LensLike' f s a
durablePrivateBytes
  = Data.ProtoLens.Field.field @"durablePrivateBytes"
egressBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "egressBytes" a) =>
  Lens.Family2.LensLike' f s a
egressBytes = Data.ProtoLens.Field.field @"egressBytes"
elasticCpuNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "elasticCpuNs" a) =>
  Lens.Family2.LensLike' f s a
elasticCpuNs = Data.ProtoLens.Field.field @"elasticCpuNs"
endUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "endUnixMs" a) =>
  Lens.Family2.LensLike' f s a
endUnixMs = Data.ProtoLens.Field.field @"endUnixMs"
endpoints ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "endpoints" a) =>
  Lens.Family2.LensLike' f s a
endpoints = Data.ProtoLens.Field.field @"endpoints"
events ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "events" a) =>
  Lens.Family2.LensLike' f s a
events = Data.ProtoLens.Field.field @"events"
expiration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expiration" a) =>
  Lens.Family2.LensLike' f s a
expiration = Data.ProtoLens.Field.field @"expiration"
fidelity ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fidelity" a) =>
  Lens.Family2.LensLike' f s a
fidelity = Data.ProtoLens.Field.field @"fidelity"
fork ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "fork" a) =>
  Lens.Family2.LensLike' f s a
fork = Data.ProtoLens.Field.field @"fork"
forkMachine ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "forkMachine" a) =>
  Lens.Family2.LensLike' f s a
forkMachine = Data.ProtoLens.Field.field @"forkMachine"
forkable ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "forkable" a) =>
  Lens.Family2.LensLike' f s a
forkable = Data.ProtoLens.Field.field @"forkable"
forked ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "forked" a) =>
  Lens.Family2.LensLike' f s a
forked = Data.ProtoLens.Field.field @"forked"
idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
idempotencyKey = Data.ProtoLens.Field.field @"idempotencyKey"
image ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "image" a) =>
  Lens.Family2.LensLike' f s a
image = Data.ProtoLens.Field.field @"image"
kind ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "kind" a) =>
  Lens.Family2.LensLike' f s a
kind = Data.ProtoLens.Field.field @"kind"
lastCheckpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "lastCheckpoint" a) =>
  Lens.Family2.LensLike' f s a
lastCheckpoint = Data.ProtoLens.Field.field @"lastCheckpoint"
limit ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "limit" a) =>
  Lens.Family2.LensLike' f s a
limit = Data.ProtoLens.Field.field @"limit"
lineageReceiptSha256 ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "lineageReceiptSha256" a) =>
  Lens.Family2.LensLike' f s a
lineageReceiptSha256
  = Data.ProtoLens.Field.field @"lineageReceiptSha256"
machine ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "machine" a) =>
  Lens.Family2.LensLike' f s a
machine = Data.ProtoLens.Field.field @"machine"
machineDestroyed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "machineDestroyed" a) =>
  Lens.Family2.LensLike' f s a
machineDestroyed = Data.ProtoLens.Field.field @"machineDestroyed"
machineForked ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "machineForked" a) =>
  Lens.Family2.LensLike' f s a
machineForked = Data.ProtoLens.Field.field @"machineForked"
machines ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "machines" a) =>
  Lens.Family2.LensLike' f s a
machines = Data.ProtoLens.Field.field @"machines"
major ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "major" a) =>
  Lens.Family2.LensLike' f s a
major = Data.ProtoLens.Field.field @"major"
managedDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "managedDigest" a) =>
  Lens.Family2.LensLike' f s a
managedDigest = Data.ProtoLens.Field.field @"managedDigest"
manual ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "manual" a) =>
  Lens.Family2.LensLike' f s a
manual = Data.ProtoLens.Field.field @"manual"
maybe'after ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'after" a) =>
  Lens.Family2.LensLike' f s a
maybe'after = Data.ProtoLens.Field.field @"maybe'after"
maybe'afterIdleMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'afterIdleMs" a) =>
  Lens.Family2.LensLike' f s a
maybe'afterIdleMs = Data.ProtoLens.Field.field @"maybe'afterIdleMs"
maybe'budgets ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'budgets" a) =>
  Lens.Family2.LensLike' f s a
maybe'budgets = Data.ProtoLens.Field.field @"maybe'budgets"
maybe'checkpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'checkpoint" a) =>
  Lens.Family2.LensLike' f s a
maybe'checkpoint = Data.ProtoLens.Field.field @"maybe'checkpoint"
maybe'checkpointDestroyed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'checkpointDestroyed" a) =>
  Lens.Family2.LensLike' f s a
maybe'checkpointDestroyed
  = Data.ProtoLens.Field.field @"maybe'checkpointDestroyed"
maybe'checkpointed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'checkpointed" a) =>
  Lens.Family2.LensLike' f s a
maybe'checkpointed
  = Data.ProtoLens.Field.field @"maybe'checkpointed"
maybe'compatibility ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'compatibility" a) =>
  Lens.Family2.LensLike' f s a
maybe'compatibility
  = Data.ProtoLens.Field.field @"maybe'compatibility"
maybe'contract ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'contract" a) =>
  Lens.Family2.LensLike' f s a
maybe'contract = Data.ProtoLens.Field.field @"maybe'contract"
maybe'create ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'create" a) =>
  Lens.Family2.LensLike' f s a
maybe'create = Data.ProtoLens.Field.field @"maybe'create"
maybe'created ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'created" a) =>
  Lens.Family2.LensLike' f s a
maybe'created = Data.ProtoLens.Field.field @"maybe'created"
maybe'customDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'customDigest" a) =>
  Lens.Family2.LensLike' f s a
maybe'customDigest
  = Data.ProtoLens.Field.field @"maybe'customDigest"
maybe'destroyCheckpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'destroyCheckpoint" a) =>
  Lens.Family2.LensLike' f s a
maybe'destroyCheckpoint
  = Data.ProtoLens.Field.field @"maybe'destroyCheckpoint"
maybe'destroyMachine ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'destroyMachine" a) =>
  Lens.Family2.LensLike' f s a
maybe'destroyMachine
  = Data.ProtoLens.Field.field @"maybe'destroyMachine"
maybe'expiration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'expiration" a) =>
  Lens.Family2.LensLike' f s a
maybe'expiration = Data.ProtoLens.Field.field @"maybe'expiration"
maybe'fork ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'fork" a) =>
  Lens.Family2.LensLike' f s a
maybe'fork = Data.ProtoLens.Field.field @"maybe'fork"
maybe'forkMachine ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'forkMachine" a) =>
  Lens.Family2.LensLike' f s a
maybe'forkMachine = Data.ProtoLens.Field.field @"maybe'forkMachine"
maybe'forked ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'forked" a) =>
  Lens.Family2.LensLike' f s a
maybe'forked = Data.ProtoLens.Field.field @"maybe'forked"
maybe'idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
maybe'idempotencyKey
  = Data.ProtoLens.Field.field @"maybe'idempotencyKey"
maybe'image ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'image" a) =>
  Lens.Family2.LensLike' f s a
maybe'image = Data.ProtoLens.Field.field @"maybe'image"
maybe'immutableReference ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'immutableReference" a) =>
  Lens.Family2.LensLike' f s a
maybe'immutableReference
  = Data.ProtoLens.Field.field @"maybe'immutableReference"
maybe'lastCheckpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'lastCheckpoint" a) =>
  Lens.Family2.LensLike' f s a
maybe'lastCheckpoint
  = Data.ProtoLens.Field.field @"maybe'lastCheckpoint"
maybe'machine ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'machine" a) =>
  Lens.Family2.LensLike' f s a
maybe'machine = Data.ProtoLens.Field.field @"maybe'machine"
maybe'machineDestroyed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'machineDestroyed" a) =>
  Lens.Family2.LensLike' f s a
maybe'machineDestroyed
  = Data.ProtoLens.Field.field @"maybe'machineDestroyed"
maybe'machineForked ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'machineForked" a) =>
  Lens.Family2.LensLike' f s a
maybe'machineForked
  = Data.ProtoLens.Field.field @"maybe'machineForked"
maybe'managedDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'managedDigest" a) =>
  Lens.Family2.LensLike' f s a
maybe'managedDigest
  = Data.ProtoLens.Field.field @"maybe'managedDigest"
maybe'manual ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'manual" a) =>
  Lens.Family2.LensLike' f s a
maybe'manual = Data.ProtoLens.Field.field @"maybe'manual"
maybe'next ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'next" a) =>
  Lens.Family2.LensLike' f s a
maybe'next = Data.ProtoLens.Field.field @"maybe'next"
maybe'operation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'operation" a) =>
  Lens.Family2.LensLike' f s a
maybe'operation = Data.ProtoLens.Field.field @"maybe'operation"
maybe'policy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'policy" a) =>
  Lens.Family2.LensLike' f s a
maybe'policy = Data.ProtoLens.Field.field @"maybe'policy"
maybe'protocol ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'protocol" a) =>
  Lens.Family2.LensLike' f s a
maybe'protocol = Data.ProtoLens.Field.field @"maybe'protocol"
maybe'result ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'result" a) =>
  Lens.Family2.LensLike' f s a
maybe'result = Data.ProtoLens.Field.field @"maybe'result"
maybe'setSuspensionPolicy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'setSuspensionPolicy" a) =>
  Lens.Family2.LensLike' f s a
maybe'setSuspensionPolicy
  = Data.ProtoLens.Field.field @"maybe'setSuspensionPolicy"
maybe'source ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'source" a) =>
  Lens.Family2.LensLike' f s a
maybe'source = Data.ProtoLens.Field.field @"maybe'source"
maybe'suspend ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suspend" a) =>
  Lens.Family2.LensLike' f s a
maybe'suspend = Data.ProtoLens.Field.field @"maybe'suspend"
maybe'suspended ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suspended" a) =>
  Lens.Family2.LensLike' f s a
maybe'suspended = Data.ProtoLens.Field.field @"maybe'suspended"
maybe'suspension ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suspension" a) =>
  Lens.Family2.LensLike' f s a
maybe'suspension = Data.ProtoLens.Field.field @"maybe'suspension"
maybe'suspensionPolicySet ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suspensionPolicySet" a) =>
  Lens.Family2.LensLike' f s a
maybe'suspensionPolicySet
  = Data.ProtoLens.Field.field @"maybe'suspensionPolicySet"
maybe'wake ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'wake" a) =>
  Lens.Family2.LensLike' f s a
maybe'wake = Data.ProtoLens.Field.field @"maybe'wake"
maybe'woken ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'woken" a) =>
  Lens.Family2.LensLike' f s a
maybe'woken = Data.ProtoLens.Field.field @"maybe'woken"
minor ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "minor" a) =>
  Lens.Family2.LensLike' f s a
minor = Data.ProtoLens.Field.field @"minor"
mode ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "mode" a) =>
  Lens.Family2.LensLike' f s a
mode = Data.ProtoLens.Field.field @"mode"
name ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "name" a) =>
  Lens.Family2.LensLike' f s a
name = Data.ProtoLens.Field.field @"name"
networkPolicyDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "networkPolicyDigest" a) =>
  Lens.Family2.LensLike' f s a
networkPolicyDigest
  = Data.ProtoLens.Field.field @"networkPolicyDigest"
next ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "next" a) =>
  Lens.Family2.LensLike' f s a
next = Data.ProtoLens.Field.field @"next"
nextSequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "nextSequence" a) =>
  Lens.Family2.LensLike' f s a
nextSequence = Data.ProtoLens.Field.field @"nextSequence"
observedAtUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "observedAtUnixMs" a) =>
  Lens.Family2.LensLike' f s a
observedAtUnixMs = Data.ProtoLens.Field.field @"observedAtUnixMs"
operation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "operation" a) =>
  Lens.Family2.LensLike' f s a
operation = Data.ProtoLens.Field.field @"operation"
operations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "operations" a) =>
  Lens.Family2.LensLike' f s a
operations = Data.ProtoLens.Field.field @"operations"
policy ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "policy" a) =>
  Lens.Family2.LensLike' f s a
policy = Data.ProtoLens.Field.field @"policy"
pressure ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "pressure" a) =>
  Lens.Family2.LensLike' f s a
pressure = Data.ProtoLens.Field.field @"pressure"
privateResidentByteSeconds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "privateResidentByteSeconds" a) =>
  Lens.Family2.LensLike' f s a
privateResidentByteSeconds
  = Data.ProtoLens.Field.field @"privateResidentByteSeconds"
protocol ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "protocol" a) =>
  Lens.Family2.LensLike' f s a
protocol = Data.ProtoLens.Field.field @"protocol"
receipt ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "receipt" a) =>
  Lens.Family2.LensLike' f s a
receipt = Data.ProtoLens.Field.field @"receipt"
required ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "required" a) =>
  Lens.Family2.LensLike' f s a
required = Data.ProtoLens.Field.field @"required"
sequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sequence" a) =>
  Lens.Family2.LensLike' f s a
sequence = Data.ProtoLens.Field.field @"sequence"
setSuspensionPolicy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "setSuspensionPolicy" a) =>
  Lens.Family2.LensLike' f s a
setSuspensionPolicy
  = Data.ProtoLens.Field.field @"setSuspensionPolicy"
source ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "source" a) =>
  Lens.Family2.LensLike' f s a
source = Data.ProtoLens.Field.field @"source"
spendMicros ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "spendMicros" a) =>
  Lens.Family2.LensLike' f s a
spendMicros = Data.ProtoLens.Field.field @"spendMicros"
startUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "startUnixMs" a) =>
  Lens.Family2.LensLike' f s a
startUnixMs = Data.ProtoLens.Field.field @"startUnixMs"
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
suspend ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "suspend" a) =>
  Lens.Family2.LensLike' f s a
suspend = Data.ProtoLens.Field.field @"suspend"
suspended ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "suspended" a) =>
  Lens.Family2.LensLike' f s a
suspended = Data.ProtoLens.Field.field @"suspended"
suspension ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "suspension" a) =>
  Lens.Family2.LensLike' f s a
suspension = Data.ProtoLens.Field.field @"suspension"
suspensionPolicySet ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "suspensionPolicySet" a) =>
  Lens.Family2.LensLike' f s a
suspensionPolicySet
  = Data.ProtoLens.Field.field @"suspensionPolicySet"
uri ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "uri" a) =>
  Lens.Family2.LensLike' f s a
uri = Data.ProtoLens.Field.field @"uri"
value ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "value" a) =>
  Lens.Family2.LensLike' f s a
value = Data.ProtoLens.Field.field @"value"
valueMs ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "valueMs" a) =>
  Lens.Family2.LensLike' f s a
valueMs = Data.ProtoLens.Field.field @"valueMs"
vec'capabilities ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'capabilities" a) =>
  Lens.Family2.LensLike' f s a
vec'capabilities = Data.ProtoLens.Field.field @"vec'capabilities"
vec'children ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'children" a) =>
  Lens.Family2.LensLike' f s a
vec'children = Data.ProtoLens.Field.field @"vec'children"
vec'endpoints ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'endpoints" a) =>
  Lens.Family2.LensLike' f s a
vec'endpoints = Data.ProtoLens.Field.field @"vec'endpoints"
vec'events ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'events" a) =>
  Lens.Family2.LensLike' f s a
vec'events = Data.ProtoLens.Field.field @"vec'events"
vec'machines ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'machines" a) =>
  Lens.Family2.LensLike' f s a
vec'machines = Data.ProtoLens.Field.field @"vec'machines"
vec'operations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'operations" a) =>
  Lens.Family2.LensLike' f s a
vec'operations = Data.ProtoLens.Field.field @"vec'operations"
vec'required ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'required" a) =>
  Lens.Family2.LensLike' f s a
vec'required = Data.ProtoLens.Field.field @"vec'required"
wake ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "wake" a) =>
  Lens.Family2.LensLike' f s a
wake = Data.ProtoLens.Field.field @"wake"
woken ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "woken" a) =>
  Lens.Family2.LensLike' f s a
woken = Data.ProtoLens.Field.field @"woken"