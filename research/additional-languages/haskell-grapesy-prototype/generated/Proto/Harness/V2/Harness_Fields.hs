{- This file was auto-generated from harness/v2/harness.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Harness.V2.Harness_Fields where
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
import qualified Proto.Protocol.V1.Protocol
acknowledge ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "acknowledge" a) =>
  Lens.Family2.LensLike' f s a
acknowledge = Data.ProtoLens.Field.field @"acknowledge"
actionDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "actionDigest" a) =>
  Lens.Family2.LensLike' f s a
actionDigest = Data.ProtoLens.Field.field @"actionDigest"
actionType ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "actionType" a) =>
  Lens.Family2.LensLike' f s a
actionType = Data.ProtoLens.Field.field @"actionType"
admission ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "admission" a) =>
  Lens.Family2.LensLike' f s a
admission = Data.ProtoLens.Field.field @"admission"
agentId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "agentId" a) =>
  Lens.Family2.LensLike' f s a
agentId = Data.ProtoLens.Field.field @"agentId"
answered ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "answered" a) =>
  Lens.Family2.LensLike' f s a
answered = Data.ProtoLens.Field.field @"answered"
approval ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "approval" a) =>
  Lens.Family2.LensLike' f s a
approval = Data.ProtoLens.Field.field @"approval"
approved ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "approved" a) =>
  Lens.Family2.LensLike' f s a
approved = Data.ProtoLens.Field.field @"approved"
artifact ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "artifact" a) =>
  Lens.Family2.LensLike' f s a
artifact = Data.ProtoLens.Field.field @"artifact"
attachedAgentIds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "attachedAgentIds" a) =>
  Lens.Family2.LensLike' f s a
attachedAgentIds = Data.ProtoLens.Field.field @"attachedAgentIds"
attachmentManifest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "attachmentManifest" a) =>
  Lens.Family2.LensLike' f s a
attachmentManifest
  = Data.ProtoLens.Field.field @"attachmentManifest"
attachmentManifests ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "attachmentManifests" a) =>
  Lens.Family2.LensLike' f s a
attachmentManifests
  = Data.ProtoLens.Field.field @"attachmentManifests"
attachments ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "attachments" a) =>
  Lens.Family2.LensLike' f s a
attachments = Data.ProtoLens.Field.field @"attachments"
attestation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "attestation" a) =>
  Lens.Family2.LensLike' f s a
attestation = Data.ProtoLens.Field.field @"attestation"
authority ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "authority" a) =>
  Lens.Family2.LensLike' f s a
authority = Data.ProtoLens.Field.field @"authority"
batchId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "batchId" a) =>
  Lens.Family2.LensLike' f s a
batchId = Data.ProtoLens.Field.field @"batchId"
boundary ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "boundary" a) =>
  Lens.Family2.LensLike' f s a
boundary = Data.ProtoLens.Field.field @"boundary"
build ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "build" a) =>
  Lens.Family2.LensLike' f s a
build = Data.ProtoLens.Field.field @"build"
byteLength ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "byteLength" a) =>
  Lens.Family2.LensLike' f s a
byteLength = Data.ProtoLens.Field.field @"byteLength"
cancel ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "cancel" a) =>
  Lens.Family2.LensLike' f s a
cancel = Data.ProtoLens.Field.field @"cancel"
cancellation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "cancellation" a) =>
  Lens.Family2.LensLike' f s a
cancellation = Data.ProtoLens.Field.field @"cancellation"
cancellationRequested ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "cancellationRequested" a) =>
  Lens.Family2.LensLike' f s a
cancellationRequested
  = Data.ProtoLens.Field.field @"cancellationRequested"
cancelled ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "cancelled" a) =>
  Lens.Family2.LensLike' f s a
cancelled = Data.ProtoLens.Field.field @"cancelled"
canonicalActionJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalActionJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalActionJson
  = Data.ProtoLens.Field.field @"canonicalActionJson"
canonicalCompletedValueJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalCompletedValueJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalCompletedValueJson
  = Data.ProtoLens.Field.field @"canonicalCompletedValueJson"
canonicalEventJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalEventJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalEventJson
  = Data.ProtoLens.Field.field @"canonicalEventJson"
canonicalInputJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalInputJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalInputJson
  = Data.ProtoLens.Field.field @"canonicalInputJson"
canonicalInputSchemaJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalInputSchemaJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalInputSchemaJson
  = Data.ProtoLens.Field.field @"canonicalInputSchemaJson"
canonicalJsonStatement ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalJsonStatement" a) =>
  Lens.Family2.LensLike' f s a
canonicalJsonStatement
  = Data.ProtoLens.Field.field @"canonicalJsonStatement"
canonicalOutputSchemaJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalOutputSchemaJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalOutputSchemaJson
  = Data.ProtoLens.Field.field @"canonicalOutputSchemaJson"
canonicalPayloadJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalPayloadJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalPayloadJson
  = Data.ProtoLens.Field.field @"canonicalPayloadJson"
canonicalStateJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "canonicalStateJson" a) =>
  Lens.Family2.LensLike' f s a
canonicalStateJson
  = Data.ProtoLens.Field.field @"canonicalStateJson"
capabilities ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "capabilities" a) =>
  Lens.Family2.LensLike' f s a
capabilities = Data.ProtoLens.Field.field @"capabilities"
captured ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "captured" a) =>
  Lens.Family2.LensLike' f s a
captured = Data.ProtoLens.Field.field @"captured"
captures ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "captures" a) =>
  Lens.Family2.LensLike' f s a
captures = Data.ProtoLens.Field.field @"captures"
causalParent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "causalParent" a) =>
  Lens.Family2.LensLike' f s a
causalParent = Data.ProtoLens.Field.field @"causalParent"
child ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "child" a) =>
  Lens.Family2.LensLike' f s a
child = Data.ProtoLens.Field.field @"child"
childAgentId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "childAgentId" a) =>
  Lens.Family2.LensLike' f s a
childAgentId = Data.ProtoLens.Field.field @"childAgentId"
childPrivateGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "childPrivateGeneration" a) =>
  Lens.Family2.LensLike' f s a
childPrivateGeneration
  = Data.ProtoLens.Field.field @"childPrivateGeneration"
childPrivateVolume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "childPrivateVolume" a) =>
  Lens.Family2.LensLike' f s a
childPrivateVolume
  = Data.ProtoLens.Field.field @"childPrivateVolume"
childProjectVolume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "childProjectVolume" a) =>
  Lens.Family2.LensLike' f s a
childProjectVolume
  = Data.ProtoLens.Field.field @"childProjectVolume"
code ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "code" a) =>
  Lens.Family2.LensLike' f s a
code = Data.ProtoLens.Field.field @"code"
command ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "command" a) =>
  Lens.Family2.LensLike' f s a
command = Data.ProtoLens.Field.field @"command"
commands ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "commands" a) =>
  Lens.Family2.LensLike' f s a
commands = Data.ProtoLens.Field.field @"commands"
committedAtMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "committedAtMs" a) =>
  Lens.Family2.LensLike' f s a
committedAtMs = Data.ProtoLens.Field.field @"committedAtMs"
concurrency ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "concurrency" a) =>
  Lens.Family2.LensLike' f s a
concurrency = Data.ProtoLens.Field.field @"concurrency"
configurations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "configurations" a) =>
  Lens.Family2.LensLike' f s a
configurations = Data.ProtoLens.Field.field @"configurations"
content ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "content" a) =>
  Lens.Family2.LensLike' f s a
content = Data.ProtoLens.Field.field @"content"
context ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "context" a) =>
  Lens.Family2.LensLike' f s a
context = Data.ProtoLens.Field.field @"context"
contextMessages ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contextMessages" a) =>
  Lens.Family2.LensLike' f s a
contextMessages = Data.ProtoLens.Field.field @"contextMessages"
conversationRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "conversationRevision" a) =>
  Lens.Family2.LensLike' f s a
conversationRevision
  = Data.ProtoLens.Field.field @"conversationRevision"
cursors ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "cursors" a) =>
  Lens.Family2.LensLike' f s a
cursors = Data.ProtoLens.Field.field @"cursors"
deadlineEpochMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "deadlineEpochMs" a) =>
  Lens.Family2.LensLike' f s a
deadlineEpochMs = Data.ProtoLens.Field.field @"deadlineEpochMs"
deadlineUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "deadlineUnixMs" a) =>
  Lens.Family2.LensLike' f s a
deadlineUnixMs = Data.ProtoLens.Field.field @"deadlineUnixMs"
declined ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "declined" a) =>
  Lens.Family2.LensLike' f s a
declined = Data.ProtoLens.Field.field @"declined"
delivery ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "delivery" a) =>
  Lens.Family2.LensLike' f s a
delivery = Data.ProtoLens.Field.field @"delivery"
denied ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "denied" a) =>
  Lens.Family2.LensLike' f s a
denied = Data.ProtoLens.Field.field @"denied"
descriptor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "descriptor" a) =>
  Lens.Family2.LensLike' f s a
descriptor = Data.ProtoLens.Field.field @"descriptor"
detail ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "detail" a) =>
  Lens.Family2.LensLike' f s a
detail = Data.ProtoLens.Field.field @"detail"
digest ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "digest" a) =>
  Lens.Family2.LensLike' f s a
digest = Data.ProtoLens.Field.field @"digest"
displayName ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "displayName" a) =>
  Lens.Family2.LensLike' f s a
displayName = Data.ProtoLens.Field.field @"displayName"
entries ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "entries" a) =>
  Lens.Family2.LensLike' f s a
entries = Data.ProtoLens.Field.field @"entries"
environment ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "environment" a) =>
  Lens.Family2.LensLike' f s a
environment = Data.ProtoLens.Field.field @"environment"
error ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "error" a) =>
  Lens.Family2.LensLike' f s a
error = Data.ProtoLens.Field.field @"error"
event ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "event" a) =>
  Lens.Family2.LensLike' f s a
event = Data.ProtoLens.Field.field @"event"
eventDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "eventDigest" a) =>
  Lens.Family2.LensLike' f s a
eventDigest = Data.ProtoLens.Field.field @"eventDigest"
eventType ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "eventType" a) =>
  Lens.Family2.LensLike' f s a
eventType = Data.ProtoLens.Field.field @"eventType"
events ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "events" a) =>
  Lens.Family2.LensLike' f s a
events = Data.ProtoLens.Field.field @"events"
evidence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "evidence" a) =>
  Lens.Family2.LensLike' f s a
evidence = Data.ProtoLens.Field.field @"evidence"
execution ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "execution" a) =>
  Lens.Family2.LensLike' f s a
execution = Data.ProtoLens.Field.field @"execution"
expectedRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expectedRevision" a) =>
  Lens.Family2.LensLike' f s a
expectedRevision = Data.ProtoLens.Field.field @"expectedRevision"
expectedTargetGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expectedTargetGeneration" a) =>
  Lens.Family2.LensLike' f s a
expectedTargetGeneration
  = Data.ProtoLens.Field.field @"expectedTargetGeneration"
expectedVersion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expectedVersion" a) =>
  Lens.Family2.LensLike' f s a
expectedVersion = Data.ProtoLens.Field.field @"expectedVersion"
expired ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "expired" a) =>
  Lens.Family2.LensLike' f s a
expired = Data.ProtoLens.Field.field @"expired"
extension ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "extension" a) =>
  Lens.Family2.LensLike' f s a
extension = Data.ProtoLens.Field.field @"extension"
extensions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "extensions" a) =>
  Lens.Family2.LensLike' f s a
extensions = Data.ProtoLens.Field.field @"extensions"
failedMessage ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "failedMessage" a) =>
  Lens.Family2.LensLike' f s a
failedMessage = Data.ProtoLens.Field.field @"failedMessage"
failureMessage ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "failureMessage" a) =>
  Lens.Family2.LensLike' f s a
failureMessage = Data.ProtoLens.Field.field @"failureMessage"
family ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "family" a) =>
  Lens.Family2.LensLike' f s a
family = Data.ProtoLens.Field.field @"family"
file ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "file" a) =>
  Lens.Family2.LensLike' f s a
file = Data.ProtoLens.Field.field @"file"
fileBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fileBytes" a) =>
  Lens.Family2.LensLike' f s a
fileBytes = Data.ProtoLens.Field.field @"fileBytes"
forkPolicy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "forkPolicy" a) =>
  Lens.Family2.LensLike' f s a
forkPolicy = Data.ProtoLens.Field.field @"forkPolicy"
format ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "format" a) =>
  Lens.Family2.LensLike' f s a
format = Data.ProtoLens.Field.field @"format"
formatVersion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "formatVersion" a) =>
  Lens.Family2.LensLike' f s a
formatVersion = Data.ProtoLens.Field.field @"formatVersion"
fromRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fromRevision" a) =>
  Lens.Family2.LensLike' f s a
fromRevision = Data.ProtoLens.Field.field @"fromRevision"
generation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "generation" a) =>
  Lens.Family2.LensLike' f s a
generation = Data.ProtoLens.Field.field @"generation"
grants ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "grants" a) =>
  Lens.Family2.LensLike' f s a
grants = Data.ProtoLens.Field.field @"grants"
groupId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "groupId" a) =>
  Lens.Family2.LensLike' f s a
groupId = Data.ProtoLens.Field.field @"groupId"
groupPolicy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "groupPolicy" a) =>
  Lens.Family2.LensLike' f s a
groupPolicy = Data.ProtoLens.Field.field @"groupPolicy"
handshake ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "handshake" a) =>
  Lens.Family2.LensLike' f s a
handshake = Data.ProtoLens.Field.field @"handshake"
hasMore ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "hasMore" a) =>
  Lens.Family2.LensLike' f s a
hasMore = Data.ProtoLens.Field.field @"hasMore"
history ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "history" a) =>
  Lens.Family2.LensLike' f s a
history = Data.ProtoLens.Field.field @"history"
id ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "id" a) =>
  Lens.Family2.LensLike' f s a
id = Data.ProtoLens.Field.field @"id"
idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
idempotencyKey = Data.ProtoLens.Field.field @"idempotencyKey"
immutableVersion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "immutableVersion" a) =>
  Lens.Family2.LensLike' f s a
immutableVersion = Data.ProtoLens.Field.field @"immutableVersion"
implementationDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "implementationDigest" a) =>
  Lens.Family2.LensLike' f s a
implementationDigest
  = Data.ProtoLens.Field.field @"implementationDigest"
inFlightOperationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inFlightOperationId" a) =>
  Lens.Family2.LensLike' f s a
inFlightOperationId
  = Data.ProtoLens.Field.field @"inFlightOperationId"
indeterminateOperationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "indeterminateOperationId" a) =>
  Lens.Family2.LensLike' f s a
indeterminateOperationId
  = Data.ProtoLens.Field.field @"indeterminateOperationId"
inheritedContext ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inheritedContext" a) =>
  Lens.Family2.LensLike' f s a
inheritedContext = Data.ProtoLens.Field.field @"inheritedContext"
inheritedThroughSequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inheritedThroughSequence" a) =>
  Lens.Family2.LensLike' f s a
inheritedThroughSequence
  = Data.ProtoLens.Field.field @"inheritedThroughSequence"
initial ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "initial" a) =>
  Lens.Family2.LensLike' f s a
initial = Data.ProtoLens.Field.field @"initial"
inlineItems ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inlineItems" a) =>
  Lens.Family2.LensLike' f s a
inlineItems = Data.ProtoLens.Field.field @"inlineItems"
inputDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inputDigest" a) =>
  Lens.Family2.LensLike' f s a
inputDigest = Data.ProtoLens.Field.field @"inputDigest"
intentDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "intentDigest" a) =>
  Lens.Family2.LensLike' f s a
intentDigest = Data.ProtoLens.Field.field @"intentDigest"
issuer ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "issuer" a) =>
  Lens.Family2.LensLike' f s a
issuer = Data.ProtoLens.Field.field @"issuer"
itemCount ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "itemCount" a) =>
  Lens.Family2.LensLike' f s a
itemCount = Data.ProtoLens.Field.field @"itemCount"
items ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "items" a) =>
  Lens.Family2.LensLike' f s a
items = Data.ProtoLens.Field.field @"items"
key ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "key" a) =>
  Lens.Family2.LensLike' f s a
key = Data.ProtoLens.Field.field @"key"
kind ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "kind" a) =>
  Lens.Family2.LensLike' f s a
kind = Data.ProtoLens.Field.field @"kind"
label ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "label" a) =>
  Lens.Family2.LensLike' f s a
label = Data.ProtoLens.Field.field @"label"
limits ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "limits" a) =>
  Lens.Family2.LensLike' f s a
limits = Data.ProtoLens.Field.field @"limits"
live ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "live" a) =>
  Lens.Family2.LensLike' f s a
live = Data.ProtoLens.Field.field @"live"
machine ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "machine" a) =>
  Lens.Family2.LensLike' f s a
machine = Data.ProtoLens.Field.field @"machine"
manifest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "manifest" a) =>
  Lens.Family2.LensLike' f s a
manifest = Data.ProtoLens.Field.field @"manifest"
maxSteps ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maxSteps" a) =>
  Lens.Family2.LensLike' f s a
maxSteps = Data.ProtoLens.Field.field @"maxSteps"
maximumInheritedBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumInheritedBytes" a) =>
  Lens.Family2.LensLike' f s a
maximumInheritedBytes
  = Data.ProtoLens.Field.field @"maximumInheritedBytes"
maximumInheritedMessages ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumInheritedMessages" a) =>
  Lens.Family2.LensLike' f s a
maximumInheritedMessages
  = Data.ProtoLens.Field.field @"maximumInheritedMessages"
maximumInheritedReferences ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumInheritedReferences" a) =>
  Lens.Family2.LensLike' f s a
maximumInheritedReferences
  = Data.ProtoLens.Field.field @"maximumInheritedReferences"
maybe'acknowledge ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'acknowledge" a) =>
  Lens.Family2.LensLike' f s a
maybe'acknowledge = Data.ProtoLens.Field.field @"maybe'acknowledge"
maybe'admission ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'admission" a) =>
  Lens.Family2.LensLike' f s a
maybe'admission = Data.ProtoLens.Field.field @"maybe'admission"
maybe'agentId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'agentId" a) =>
  Lens.Family2.LensLike' f s a
maybe'agentId = Data.ProtoLens.Field.field @"maybe'agentId"
maybe'answered ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'answered" a) =>
  Lens.Family2.LensLike' f s a
maybe'answered = Data.ProtoLens.Field.field @"maybe'answered"
maybe'approval ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'approval" a) =>
  Lens.Family2.LensLike' f s a
maybe'approval = Data.ProtoLens.Field.field @"maybe'approval"
maybe'approved ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'approved" a) =>
  Lens.Family2.LensLike' f s a
maybe'approved = Data.ProtoLens.Field.field @"maybe'approved"
maybe'artifact ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'artifact" a) =>
  Lens.Family2.LensLike' f s a
maybe'artifact = Data.ProtoLens.Field.field @"maybe'artifact"
maybe'attachmentManifest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'attachmentManifest" a) =>
  Lens.Family2.LensLike' f s a
maybe'attachmentManifest
  = Data.ProtoLens.Field.field @"maybe'attachmentManifest"
maybe'attachments ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'attachments" a) =>
  Lens.Family2.LensLike' f s a
maybe'attachments = Data.ProtoLens.Field.field @"maybe'attachments"
maybe'authority ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'authority" a) =>
  Lens.Family2.LensLike' f s a
maybe'authority = Data.ProtoLens.Field.field @"maybe'authority"
maybe'boundary ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'boundary" a) =>
  Lens.Family2.LensLike' f s a
maybe'boundary = Data.ProtoLens.Field.field @"maybe'boundary"
maybe'build ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'build" a) =>
  Lens.Family2.LensLike' f s a
maybe'build = Data.ProtoLens.Field.field @"maybe'build"
maybe'cancel ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'cancel" a) =>
  Lens.Family2.LensLike' f s a
maybe'cancel = Data.ProtoLens.Field.field @"maybe'cancel"
maybe'cancellation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'cancellation" a) =>
  Lens.Family2.LensLike' f s a
maybe'cancellation
  = Data.ProtoLens.Field.field @"maybe'cancellation"
maybe'cancelled ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'cancelled" a) =>
  Lens.Family2.LensLike' f s a
maybe'cancelled = Data.ProtoLens.Field.field @"maybe'cancelled"
maybe'canonicalCompletedValueJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'canonicalCompletedValueJson" a) =>
  Lens.Family2.LensLike' f s a
maybe'canonicalCompletedValueJson
  = Data.ProtoLens.Field.field @"maybe'canonicalCompletedValueJson"
maybe'captured ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'captured" a) =>
  Lens.Family2.LensLike' f s a
maybe'captured = Data.ProtoLens.Field.field @"maybe'captured"
maybe'causalParent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'causalParent" a) =>
  Lens.Family2.LensLike' f s a
maybe'causalParent
  = Data.ProtoLens.Field.field @"maybe'causalParent"
maybe'child ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'child" a) =>
  Lens.Family2.LensLike' f s a
maybe'child = Data.ProtoLens.Field.field @"maybe'child"
maybe'childPrivateGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'childPrivateGeneration" a) =>
  Lens.Family2.LensLike' f s a
maybe'childPrivateGeneration
  = Data.ProtoLens.Field.field @"maybe'childPrivateGeneration"
maybe'childPrivateVolume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'childPrivateVolume" a) =>
  Lens.Family2.LensLike' f s a
maybe'childPrivateVolume
  = Data.ProtoLens.Field.field @"maybe'childPrivateVolume"
maybe'childProjectVolume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'childProjectVolume" a) =>
  Lens.Family2.LensLike' f s a
maybe'childProjectVolume
  = Data.ProtoLens.Field.field @"maybe'childProjectVolume"
maybe'command ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'command" a) =>
  Lens.Family2.LensLike' f s a
maybe'command = Data.ProtoLens.Field.field @"maybe'command"
maybe'committedAtMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'committedAtMs" a) =>
  Lens.Family2.LensLike' f s a
maybe'committedAtMs
  = Data.ProtoLens.Field.field @"maybe'committedAtMs"
maybe'concurrency ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'concurrency" a) =>
  Lens.Family2.LensLike' f s a
maybe'concurrency = Data.ProtoLens.Field.field @"maybe'concurrency"
maybe'content ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'content" a) =>
  Lens.Family2.LensLike' f s a
maybe'content = Data.ProtoLens.Field.field @"maybe'content"
maybe'context ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'context" a) =>
  Lens.Family2.LensLike' f s a
maybe'context = Data.ProtoLens.Field.field @"maybe'context"
maybe'deadlineEpochMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'deadlineEpochMs" a) =>
  Lens.Family2.LensLike' f s a
maybe'deadlineEpochMs
  = Data.ProtoLens.Field.field @"maybe'deadlineEpochMs"
maybe'deadlineUnixMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'deadlineUnixMs" a) =>
  Lens.Family2.LensLike' f s a
maybe'deadlineUnixMs
  = Data.ProtoLens.Field.field @"maybe'deadlineUnixMs"
maybe'declined ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'declined" a) =>
  Lens.Family2.LensLike' f s a
maybe'declined = Data.ProtoLens.Field.field @"maybe'declined"
maybe'delivery ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'delivery" a) =>
  Lens.Family2.LensLike' f s a
maybe'delivery = Data.ProtoLens.Field.field @"maybe'delivery"
maybe'denied ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'denied" a) =>
  Lens.Family2.LensLike' f s a
maybe'denied = Data.ProtoLens.Field.field @"maybe'denied"
maybe'descriptor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'descriptor" a) =>
  Lens.Family2.LensLike' f s a
maybe'descriptor = Data.ProtoLens.Field.field @"maybe'descriptor"
maybe'detail ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'detail" a) =>
  Lens.Family2.LensLike' f s a
maybe'detail = Data.ProtoLens.Field.field @"maybe'detail"
maybe'environment ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'environment" a) =>
  Lens.Family2.LensLike' f s a
maybe'environment = Data.ProtoLens.Field.field @"maybe'environment"
maybe'error ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'error" a) =>
  Lens.Family2.LensLike' f s a
maybe'error = Data.ProtoLens.Field.field @"maybe'error"
maybe'event ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'event" a) =>
  Lens.Family2.LensLike' f s a
maybe'event = Data.ProtoLens.Field.field @"maybe'event"
maybe'execution ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'execution" a) =>
  Lens.Family2.LensLike' f s a
maybe'execution = Data.ProtoLens.Field.field @"maybe'execution"
maybe'expectedTargetGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'expectedTargetGeneration" a) =>
  Lens.Family2.LensLike' f s a
maybe'expectedTargetGeneration
  = Data.ProtoLens.Field.field @"maybe'expectedTargetGeneration"
maybe'expired ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'expired" a) =>
  Lens.Family2.LensLike' f s a
maybe'expired = Data.ProtoLens.Field.field @"maybe'expired"
maybe'extension ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'extension" a) =>
  Lens.Family2.LensLike' f s a
maybe'extension = Data.ProtoLens.Field.field @"maybe'extension"
maybe'extensions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'extensions" a) =>
  Lens.Family2.LensLike' f s a
maybe'extensions = Data.ProtoLens.Field.field @"maybe'extensions"
maybe'failedMessage ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'failedMessage" a) =>
  Lens.Family2.LensLike' f s a
maybe'failedMessage
  = Data.ProtoLens.Field.field @"maybe'failedMessage"
maybe'failureMessage ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'failureMessage" a) =>
  Lens.Family2.LensLike' f s a
maybe'failureMessage
  = Data.ProtoLens.Field.field @"maybe'failureMessage"
maybe'file ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'file" a) =>
  Lens.Family2.LensLike' f s a
maybe'file = Data.ProtoLens.Field.field @"maybe'file"
maybe'frame ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'frame" a) =>
  Lens.Family2.LensLike' f s a
maybe'frame = Data.ProtoLens.Field.field @"maybe'frame"
maybe'generation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'generation" a) =>
  Lens.Family2.LensLike' f s a
maybe'generation = Data.ProtoLens.Field.field @"maybe'generation"
maybe'handshake ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'handshake" a) =>
  Lens.Family2.LensLike' f s a
maybe'handshake = Data.ProtoLens.Field.field @"maybe'handshake"
maybe'history ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'history" a) =>
  Lens.Family2.LensLike' f s a
maybe'history = Data.ProtoLens.Field.field @"maybe'history"
maybe'inFlightOperationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'inFlightOperationId" a) =>
  Lens.Family2.LensLike' f s a
maybe'inFlightOperationId
  = Data.ProtoLens.Field.field @"maybe'inFlightOperationId"
maybe'indeterminateOperationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'indeterminateOperationId" a) =>
  Lens.Family2.LensLike' f s a
maybe'indeterminateOperationId
  = Data.ProtoLens.Field.field @"maybe'indeterminateOperationId"
maybe'initial ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'initial" a) =>
  Lens.Family2.LensLike' f s a
maybe'initial = Data.ProtoLens.Field.field @"maybe'initial"
maybe'inlineItems ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'inlineItems" a) =>
  Lens.Family2.LensLike' f s a
maybe'inlineItems = Data.ProtoLens.Field.field @"maybe'inlineItems"
maybe'kind ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'kind" a) =>
  Lens.Family2.LensLike' f s a
maybe'kind = Data.ProtoLens.Field.field @"maybe'kind"
maybe'label ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'label" a) =>
  Lens.Family2.LensLike' f s a
maybe'label = Data.ProtoLens.Field.field @"maybe'label"
maybe'limits ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'limits" a) =>
  Lens.Family2.LensLike' f s a
maybe'limits = Data.ProtoLens.Field.field @"maybe'limits"
maybe'machine ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'machine" a) =>
  Lens.Family2.LensLike' f s a
maybe'machine = Data.ProtoLens.Field.field @"maybe'machine"
maybe'manifest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'manifest" a) =>
  Lens.Family2.LensLike' f s a
maybe'manifest = Data.ProtoLens.Field.field @"maybe'manifest"
maybe'maxSteps ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'maxSteps" a) =>
  Lens.Family2.LensLike' f s a
maybe'maxSteps = Data.ProtoLens.Field.field @"maybe'maxSteps"
maybe'next ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'next" a) =>
  Lens.Family2.LensLike' f s a
maybe'next = Data.ProtoLens.Field.field @"maybe'next"
maybe'notice ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'notice" a) =>
  Lens.Family2.LensLike' f s a
maybe'notice = Data.ProtoLens.Field.field @"maybe'notice"
maybe'observe ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'observe" a) =>
  Lens.Family2.LensLike' f s a
maybe'observe = Data.ProtoLens.Field.field @"maybe'observe"
maybe'operation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'operation" a) =>
  Lens.Family2.LensLike' f s a
maybe'operation = Data.ProtoLens.Field.field @"maybe'operation"
maybe'outcome ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'outcome" a) =>
  Lens.Family2.LensLike' f s a
maybe'outcome = Data.ProtoLens.Field.field @"maybe'outcome"
maybe'owner ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'owner" a) =>
  Lens.Family2.LensLike' f s a
maybe'owner = Data.ProtoLens.Field.field @"maybe'owner"
maybe'parent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'parent" a) =>
  Lens.Family2.LensLike' f s a
maybe'parent = Data.ProtoLens.Field.field @"maybe'parent"
maybe'parentTaskId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'parentTaskId" a) =>
  Lens.Family2.LensLike' f s a
maybe'parentTaskId
  = Data.ProtoLens.Field.field @"maybe'parentTaskId"
maybe'payload ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'payload" a) =>
  Lens.Family2.LensLike' f s a
maybe'payload = Data.ProtoLens.Field.field @"maybe'payload"
maybe'policy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'policy" a) =>
  Lens.Family2.LensLike' f s a
maybe'policy = Data.ProtoLens.Field.field @"maybe'policy"
maybe'preparation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'preparation" a) =>
  Lens.Family2.LensLike' f s a
maybe'preparation = Data.ProtoLens.Field.field @"maybe'preparation"
maybe'previous ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'previous" a) =>
  Lens.Family2.LensLike' f s a
maybe'previous = Data.ProtoLens.Field.field @"maybe'previous"
maybe'prior ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'prior" a) =>
  Lens.Family2.LensLike' f s a
maybe'prior = Data.ProtoLens.Field.field @"maybe'prior"
maybe'process ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'process" a) =>
  Lens.Family2.LensLike' f s a
maybe'process = Data.ProtoLens.Field.field @"maybe'process"
maybe'project ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'project" a) =>
  Lens.Family2.LensLike' f s a
maybe'project = Data.ProtoLens.Field.field @"maybe'project"
maybe'protocol ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'protocol" a) =>
  Lens.Family2.LensLike' f s a
maybe'protocol = Data.ProtoLens.Field.field @"maybe'protocol"
maybe'provider ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'provider" a) =>
  Lens.Family2.LensLike' f s a
maybe'provider = Data.ProtoLens.Field.field @"maybe'provider"
maybe'providerProof ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'providerProof" a) =>
  Lens.Family2.LensLike' f s a
maybe'providerProof
  = Data.ProtoLens.Field.field @"maybe'providerProof"
maybe'record ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'record" a) =>
  Lens.Family2.LensLike' f s a
maybe'record = Data.ProtoLens.Field.field @"maybe'record"
maybe'reference ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'reference" a) =>
  Lens.Family2.LensLike' f s a
maybe'reference = Data.ProtoLens.Field.field @"maybe'reference"
maybe'replyTo ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'replyTo" a) =>
  Lens.Family2.LensLike' f s a
maybe'replyTo = Data.ProtoLens.Field.field @"maybe'replyTo"
maybe'request ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'request" a) =>
  Lens.Family2.LensLike' f s a
maybe'request = Data.ProtoLens.Field.field @"maybe'request"
maybe'resource ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'resource" a) =>
  Lens.Family2.LensLike' f s a
maybe'resource = Data.ProtoLens.Field.field @"maybe'resource"
maybe'result ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'result" a) =>
  Lens.Family2.LensLike' f s a
maybe'result = Data.ProtoLens.Field.field @"maybe'result"
maybe'resultGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'resultGeneration" a) =>
  Lens.Family2.LensLike' f s a
maybe'resultGeneration
  = Data.ProtoLens.Field.field @"maybe'resultGeneration"
maybe'resume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'resume" a) =>
  Lens.Family2.LensLike' f s a
maybe'resume = Data.ProtoLens.Field.field @"maybe'resume"
maybe'revision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'revision" a) =>
  Lens.Family2.LensLike' f s a
maybe'revision = Data.ProtoLens.Field.field @"maybe'revision"
maybe'runLimits ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'runLimits" a) =>
  Lens.Family2.LensLike' f s a
maybe'runLimits = Data.ProtoLens.Field.field @"maybe'runLimits"
maybe'scope ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'scope" a) =>
  Lens.Family2.LensLike' f s a
maybe'scope = Data.ProtoLens.Field.field @"maybe'scope"
maybe'selection ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'selection" a) =>
  Lens.Family2.LensLike' f s a
maybe'selection = Data.ProtoLens.Field.field @"maybe'selection"
maybe'session ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'session" a) =>
  Lens.Family2.LensLike' f s a
maybe'session = Data.ProtoLens.Field.field @"maybe'session"
maybe'sharedVolume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'sharedVolume" a) =>
  Lens.Family2.LensLike' f s a
maybe'sharedVolume
  = Data.ProtoLens.Field.field @"maybe'sharedVolume"
maybe'source ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'source" a) =>
  Lens.Family2.LensLike' f s a
maybe'source = Data.ProtoLens.Field.field @"maybe'source"
maybe'sourceGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'sourceGeneration" a) =>
  Lens.Family2.LensLike' f s a
maybe'sourceGeneration
  = Data.ProtoLens.Field.field @"maybe'sourceGeneration"
maybe'sourceProject ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'sourceProject" a) =>
  Lens.Family2.LensLike' f s a
maybe'sourceProject
  = Data.ProtoLens.Field.field @"maybe'sourceProject"
maybe'status ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'status" a) =>
  Lens.Family2.LensLike' f s a
maybe'status = Data.ProtoLens.Field.field @"maybe'status"
maybe'succeeded ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'succeeded" a) =>
  Lens.Family2.LensLike' f s a
maybe'succeeded = Data.ProtoLens.Field.field @"maybe'succeeded"
maybe'suspended ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suspended" a) =>
  Lens.Family2.LensLike' f s a
maybe'suspended = Data.ProtoLens.Field.field @"maybe'suspended"
maybe'targetProject ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'targetProject" a) =>
  Lens.Family2.LensLike' f s a
maybe'targetProject
  = Data.ProtoLens.Field.field @"maybe'targetProject"
maybe'task ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'task" a) =>
  Lens.Family2.LensLike' f s a
maybe'task = Data.ProtoLens.Field.field @"maybe'task"
maybe'toolCallId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'toolCallId" a) =>
  Lens.Family2.LensLike' f s a
maybe'toolCallId = Data.ProtoLens.Field.field @"maybe'toolCallId"
maybe'transition ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'transition" a) =>
  Lens.Family2.LensLike' f s a
maybe'transition = Data.ProtoLens.Field.field @"maybe'transition"
maybe'unsupportedReason ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'unsupportedReason" a) =>
  Lens.Family2.LensLike' f s a
maybe'unsupportedReason
  = Data.ProtoLens.Field.field @"maybe'unsupportedReason"
maybe'value ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'value" a) =>
  Lens.Family2.LensLike' f s a
maybe'value = Data.ProtoLens.Field.field @"maybe'value"
maybe'version ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'version" a) =>
  Lens.Family2.LensLike' f s a
maybe'version = Data.ProtoLens.Field.field @"maybe'version"
maybe'volume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'volume" a) =>
  Lens.Family2.LensLike' f s a
maybe'volume = Data.ProtoLens.Field.field @"maybe'volume"
mediaType ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "mediaType" a) =>
  Lens.Family2.LensLike' f s a
mediaType = Data.ProtoLens.Field.field @"mediaType"
message ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "message" a) =>
  Lens.Family2.LensLike' f s a
message = Data.ProtoLens.Field.field @"message"
messageIds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "messageIds" a) =>
  Lens.Family2.LensLike' f s a
messageIds = Data.ProtoLens.Field.field @"messageIds"
modelEventsPerStep ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "modelEventsPerStep" a) =>
  Lens.Family2.LensLike' f s a
modelEventsPerStep
  = Data.ProtoLens.Field.field @"modelEventsPerStep"
modelSteps ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "modelSteps" a) =>
  Lens.Family2.LensLike' f s a
modelSteps = Data.ProtoLens.Field.field @"modelSteps"
name ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "name" a) =>
  Lens.Family2.LensLike' f s a
name = Data.ProtoLens.Field.field @"name"
namespace ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "namespace" a) =>
  Lens.Family2.LensLike' f s a
namespace = Data.ProtoLens.Field.field @"namespace"
next ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "next" a) =>
  Lens.Family2.LensLike' f s a
next = Data.ProtoLens.Field.field @"next"
normalizedPath ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "normalizedPath" a) =>
  Lens.Family2.LensLike' f s a
normalizedPath = Data.ProtoLens.Field.field @"normalizedPath"
notice ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "notice" a) =>
  Lens.Family2.LensLike' f s a
notice = Data.ProtoLens.Field.field @"notice"
observe ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "observe" a) =>
  Lens.Family2.LensLike' f s a
observe = Data.ProtoLens.Field.field @"observe"
omissions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "omissions" a) =>
  Lens.Family2.LensLike' f s a
omissions = Data.ProtoLens.Field.field @"omissions"
opaque ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "opaque" a) =>
  Lens.Family2.LensLike' f s a
opaque = Data.ProtoLens.Field.field @"opaque"
operation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "operation" a) =>
  Lens.Family2.LensLike' f s a
operation = Data.ProtoLens.Field.field @"operation"
operationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "operationId" a) =>
  Lens.Family2.LensLike' f s a
operationId = Data.ProtoLens.Field.field @"operationId"
operations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "operations" a) =>
  Lens.Family2.LensLike' f s a
operations = Data.ProtoLens.Field.field @"operations"
outcome ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "outcome" a) =>
  Lens.Family2.LensLike' f s a
outcome = Data.ProtoLens.Field.field @"outcome"
owner ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "owner" a) =>
  Lens.Family2.LensLike' f s a
owner = Data.ProtoLens.Field.field @"owner"
parent ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "parent" a) =>
  Lens.Family2.LensLike' f s a
parent = Data.ProtoLens.Field.field @"parent"
parentProof ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "parentProof" a) =>
  Lens.Family2.LensLike' f s a
parentProof = Data.ProtoLens.Field.field @"parentProof"
parentRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "parentRevision" a) =>
  Lens.Family2.LensLike' f s a
parentRevision = Data.ProtoLens.Field.field @"parentRevision"
parentTaskId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "parentTaskId" a) =>
  Lens.Family2.LensLike' f s a
parentTaskId = Data.ProtoLens.Field.field @"parentTaskId"
pathBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "pathBytes" a) =>
  Lens.Family2.LensLike' f s a
pathBytes = Data.ProtoLens.Field.field @"pathBytes"
payload ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "payload" a) =>
  Lens.Family2.LensLike' f s a
payload = Data.ProtoLens.Field.field @"payload"
policy ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "policy" a) =>
  Lens.Family2.LensLike' f s a
policy = Data.ProtoLens.Field.field @"policy"
preparation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "preparation" a) =>
  Lens.Family2.LensLike' f s a
preparation = Data.ProtoLens.Field.field @"preparation"
previous ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "previous" a) =>
  Lens.Family2.LensLike' f s a
previous = Data.ProtoLens.Field.field @"previous"
prior ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "prior" a) =>
  Lens.Family2.LensLike' f s a
prior = Data.ProtoLens.Field.field @"prior"
process ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "process" a) =>
  Lens.Family2.LensLike' f s a
process = Data.ProtoLens.Field.field @"process"
project ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "project" a) =>
  Lens.Family2.LensLike' f s a
project = Data.ProtoLens.Field.field @"project"
proof ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "proof" a) =>
  Lens.Family2.LensLike' f s a
proof = Data.ProtoLens.Field.field @"proof"
protocol ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "protocol" a) =>
  Lens.Family2.LensLike' f s a
protocol = Data.ProtoLens.Field.field @"protocol"
provider ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "provider" a) =>
  Lens.Family2.LensLike' f s a
provider = Data.ProtoLens.Field.field @"provider"
providerOperationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "providerOperationId" a) =>
  Lens.Family2.LensLike' f s a
providerOperationId
  = Data.ProtoLens.Field.field @"providerOperationId"
providerProof ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "providerProof" a) =>
  Lens.Family2.LensLike' f s a
providerProof = Data.ProtoLens.Field.field @"providerProof"
readerAgentId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "readerAgentId" a) =>
  Lens.Family2.LensLike' f s a
readerAgentId = Data.ProtoLens.Field.field @"readerAgentId"
readinessRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "readinessRevision" a) =>
  Lens.Family2.LensLike' f s a
readinessRevision = Data.ProtoLens.Field.field @"readinessRevision"
record ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "record" a) =>
  Lens.Family2.LensLike' f s a
record = Data.ProtoLens.Field.field @"record"
recursive ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "recursive" a) =>
  Lens.Family2.LensLike' f s a
recursive = Data.ProtoLens.Field.field @"recursive"
reference ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "reference" a) =>
  Lens.Family2.LensLike' f s a
reference = Data.ProtoLens.Field.field @"reference"
referenceGrants ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "referenceGrants" a) =>
  Lens.Family2.LensLike' f s a
referenceGrants = Data.ProtoLens.Field.field @"referenceGrants"
renderBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "renderBytes" a) =>
  Lens.Family2.LensLike' f s a
renderBytes = Data.ProtoLens.Field.field @"renderBytes"
replayed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "replayed" a) =>
  Lens.Family2.LensLike' f s a
replayed = Data.ProtoLens.Field.field @"replayed"
replyTo ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "replyTo" a) =>
  Lens.Family2.LensLike' f s a
replyTo = Data.ProtoLens.Field.field @"replyTo"
request ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "request" a) =>
  Lens.Family2.LensLike' f s a
request = Data.ProtoLens.Field.field @"request"
requestDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "requestDigest" a) =>
  Lens.Family2.LensLike' f s a
requestDigest = Data.ProtoLens.Field.field @"requestDigest"
required ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "required" a) =>
  Lens.Family2.LensLike' f s a
required = Data.ProtoLens.Field.field @"required"
resource ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "resource" a) =>
  Lens.Family2.LensLike' f s a
resource = Data.ProtoLens.Field.field @"resource"
resources ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "resources" a) =>
  Lens.Family2.LensLike' f s a
resources = Data.ProtoLens.Field.field @"resources"
resultGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "resultGeneration" a) =>
  Lens.Family2.LensLike' f s a
resultGeneration = Data.ProtoLens.Field.field @"resultGeneration"
resume ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "resume" a) =>
  Lens.Family2.LensLike' f s a
resume = Data.ProtoLens.Field.field @"resume"
revision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "revision" a) =>
  Lens.Family2.LensLike' f s a
revision = Data.ProtoLens.Field.field @"revision"
runLimits ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "runLimits" a) =>
  Lens.Family2.LensLike' f s a
runLimits = Data.ProtoLens.Field.field @"runLimits"
schemaDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "schemaDigest" a) =>
  Lens.Family2.LensLike' f s a
schemaDigest = Data.ProtoLens.Field.field @"schemaDigest"
scope ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "scope" a) =>
  Lens.Family2.LensLike' f s a
scope = Data.ProtoLens.Field.field @"scope"
selected ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "selected" a) =>
  Lens.Family2.LensLike' f s a
selected = Data.ProtoLens.Field.field @"selected"
selection ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "selection" a) =>
  Lens.Family2.LensLike' f s a
selection = Data.ProtoLens.Field.field @"selection"
selections ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "selections" a) =>
  Lens.Family2.LensLike' f s a
selections = Data.ProtoLens.Field.field @"selections"
sequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sequence" a) =>
  Lens.Family2.LensLike' f s a
sequence = Data.ProtoLens.Field.field @"sequence"
session ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "session" a) =>
  Lens.Family2.LensLike' f s a
session = Data.ProtoLens.Field.field @"session"
sha256 ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "sha256" a) =>
  Lens.Family2.LensLike' f s a
sha256 = Data.ProtoLens.Field.field @"sha256"
sharedGrants ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sharedGrants" a) =>
  Lens.Family2.LensLike' f s a
sharedGrants = Data.ProtoLens.Field.field @"sharedGrants"
sharedVolume ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sharedVolume" a) =>
  Lens.Family2.LensLike' f s a
sharedVolume = Data.ProtoLens.Field.field @"sharedVolume"
source ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "source" a) =>
  Lens.Family2.LensLike' f s a
source = Data.ProtoLens.Field.field @"source"
sourceGeneration ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourceGeneration" a) =>
  Lens.Family2.LensLike' f s a
sourceGeneration = Data.ProtoLens.Field.field @"sourceGeneration"
sourceProject ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourceProject" a) =>
  Lens.Family2.LensLike' f s a
sourceProject = Data.ProtoLens.Field.field @"sourceProject"
state ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "state" a) =>
  Lens.Family2.LensLike' f s a
state = Data.ProtoLens.Field.field @"state"
stateDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "stateDigest" a) =>
  Lens.Family2.LensLike' f s a
stateDigest = Data.ProtoLens.Field.field @"stateDigest"
status ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "status" a) =>
  Lens.Family2.LensLike' f s a
status = Data.ProtoLens.Field.field @"status"
succeeded ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "succeeded" a) =>
  Lens.Family2.LensLike' f s a
succeeded = Data.ProtoLens.Field.field @"succeeded"
suspended ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "suspended" a) =>
  Lens.Family2.LensLike' f s a
suspended = Data.ProtoLens.Field.field @"suspended"
targetProject ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "targetProject" a) =>
  Lens.Family2.LensLike' f s a
targetProject = Data.ProtoLens.Field.field @"targetProject"
task ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "task" a) =>
  Lens.Family2.LensLike' f s a
task = Data.ProtoLens.Field.field @"task"
throughRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "throughRevision" a) =>
  Lens.Family2.LensLike' f s a
throughRevision = Data.ProtoLens.Field.field @"throughRevision"
toolCallId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "toolCallId" a) =>
  Lens.Family2.LensLike' f s a
toolCallId = Data.ProtoLens.Field.field @"toolCallId"
toolCallsPerStep ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "toolCallsPerStep" a) =>
  Lens.Family2.LensLike' f s a
toolCallsPerStep = Data.ProtoLens.Field.field @"toolCallsPerStep"
transition ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "transition" a) =>
  Lens.Family2.LensLike' f s a
transition = Data.ProtoLens.Field.field @"transition"
unsupportedReason ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "unsupportedReason" a) =>
  Lens.Family2.LensLike' f s a
unsupportedReason = Data.ProtoLens.Field.field @"unsupportedReason"
value ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "value" a) =>
  Lens.Family2.LensLike' f s a
value = Data.ProtoLens.Field.field @"value"
vec'attachedAgentIds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'attachedAgentIds" a) =>
  Lens.Family2.LensLike' f s a
vec'attachedAgentIds
  = Data.ProtoLens.Field.field @"vec'attachedAgentIds"
vec'attachmentManifests ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'attachmentManifests" a) =>
  Lens.Family2.LensLike' f s a
vec'attachmentManifests
  = Data.ProtoLens.Field.field @"vec'attachmentManifests"
vec'canonicalInputJson ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'canonicalInputJson" a) =>
  Lens.Family2.LensLike' f s a
vec'canonicalInputJson
  = Data.ProtoLens.Field.field @"vec'canonicalInputJson"
vec'capabilities ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'capabilities" a) =>
  Lens.Family2.LensLike' f s a
vec'capabilities = Data.ProtoLens.Field.field @"vec'capabilities"
vec'captures ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'captures" a) =>
  Lens.Family2.LensLike' f s a
vec'captures = Data.ProtoLens.Field.field @"vec'captures"
vec'commands ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'commands" a) =>
  Lens.Family2.LensLike' f s a
vec'commands = Data.ProtoLens.Field.field @"vec'commands"
vec'configurations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'configurations" a) =>
  Lens.Family2.LensLike' f s a
vec'configurations
  = Data.ProtoLens.Field.field @"vec'configurations"
vec'cursors ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'cursors" a) =>
  Lens.Family2.LensLike' f s a
vec'cursors = Data.ProtoLens.Field.field @"vec'cursors"
vec'entries ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'entries" a) =>
  Lens.Family2.LensLike' f s a
vec'entries = Data.ProtoLens.Field.field @"vec'entries"
vec'events ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'events" a) =>
  Lens.Family2.LensLike' f s a
vec'events = Data.ProtoLens.Field.field @"vec'events"
vec'grants ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'grants" a) =>
  Lens.Family2.LensLike' f s a
vec'grants = Data.ProtoLens.Field.field @"vec'grants"
vec'inheritedContext ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'inheritedContext" a) =>
  Lens.Family2.LensLike' f s a
vec'inheritedContext
  = Data.ProtoLens.Field.field @"vec'inheritedContext"
vec'items ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'items" a) =>
  Lens.Family2.LensLike' f s a
vec'items = Data.ProtoLens.Field.field @"vec'items"
vec'messageIds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'messageIds" a) =>
  Lens.Family2.LensLike' f s a
vec'messageIds = Data.ProtoLens.Field.field @"vec'messageIds"
vec'omissions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'omissions" a) =>
  Lens.Family2.LensLike' f s a
vec'omissions = Data.ProtoLens.Field.field @"vec'omissions"
vec'operations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'operations" a) =>
  Lens.Family2.LensLike' f s a
vec'operations = Data.ProtoLens.Field.field @"vec'operations"
vec'previous ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'previous" a) =>
  Lens.Family2.LensLike' f s a
vec'previous = Data.ProtoLens.Field.field @"vec'previous"
vec'referenceGrants ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'referenceGrants" a) =>
  Lens.Family2.LensLike' f s a
vec'referenceGrants
  = Data.ProtoLens.Field.field @"vec'referenceGrants"
vec'resources ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'resources" a) =>
  Lens.Family2.LensLike' f s a
vec'resources = Data.ProtoLens.Field.field @"vec'resources"
vec'selected ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'selected" a) =>
  Lens.Family2.LensLike' f s a
vec'selected = Data.ProtoLens.Field.field @"vec'selected"
vec'selections ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'selections" a) =>
  Lens.Family2.LensLike' f s a
vec'selections = Data.ProtoLens.Field.field @"vec'selections"
vec'sharedGrants ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'sharedGrants" a) =>
  Lens.Family2.LensLike' f s a
vec'sharedGrants = Data.ProtoLens.Field.field @"vec'sharedGrants"
version ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "version" a) =>
  Lens.Family2.LensLike' f s a
version = Data.ProtoLens.Field.field @"version"
volume ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "volume" a) =>
  Lens.Family2.LensLike' f s a
volume = Data.ProtoLens.Field.field @"volume"
volumeClass ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "volumeClass" a) =>
  Lens.Family2.LensLike' f s a
volumeClass = Data.ProtoLens.Field.field @"volumeClass"