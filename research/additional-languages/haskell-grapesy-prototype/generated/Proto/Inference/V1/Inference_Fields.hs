{- This file was auto-generated from inference/v1/inference.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Inference.V1.Inference_Fields where
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
import qualified Proto.Validation.V1.Options
admissionReceiptId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "admissionReceiptId" a) =>
  Lens.Family2.LensLike' f s a
admissionReceiptId
  = Data.ProtoLens.Field.field @"admissionReceiptId"
aggregates ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "aggregates" a) =>
  Lens.Family2.LensLike' f s a
aggregates = Data.ProtoLens.Field.field @"aggregates"
aggregation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "aggregation" a) =>
  Lens.Family2.LensLike' f s a
aggregation = Data.ProtoLens.Field.field @"aggregation"
append ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "append" a) =>
  Lens.Family2.LensLike' f s a
append = Data.ProtoLens.Field.field @"append"
artifactDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "artifactDigest" a) =>
  Lens.Family2.LensLike' f s a
artifactDigest = Data.ProtoLens.Field.field @"artifactDigest"
bindingDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bindingDigest" a) =>
  Lens.Family2.LensLike' f s a
bindingDigest = Data.ProtoLens.Field.field @"bindingDigest"
cancellationRequested ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "cancellationRequested" a) =>
  Lens.Family2.LensLike' f s a
cancellationRequested
  = Data.ProtoLens.Field.field @"cancellationRequested"
candidateDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "candidateDigest" a) =>
  Lens.Family2.LensLike' f s a
candidateDigest = Data.ProtoLens.Field.field @"candidateDigest"
candidates ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "candidates" a) =>
  Lens.Family2.LensLike' f s a
candidates = Data.ProtoLens.Field.field @"candidates"
caseId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "caseId" a) =>
  Lens.Family2.LensLike' f s a
caseId = Data.ProtoLens.Field.field @"caseId"
caseResults ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "caseResults" a) =>
  Lens.Family2.LensLike' f s a
caseResults = Data.ProtoLens.Field.field @"caseResults"
cases ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "cases" a) =>
  Lens.Family2.LensLike' f s a
cases = Data.ProtoLens.Field.field @"cases"
clientInstance ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "clientInstance" a) =>
  Lens.Family2.LensLike' f s a
clientInstance = Data.ProtoLens.Field.field @"clientInstance"
commandDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "commandDigest" a) =>
  Lens.Family2.LensLike' f s a
commandDigest = Data.ProtoLens.Field.field @"commandDigest"
commitment ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "commitment" a) =>
  Lens.Family2.LensLike' f s a
commitment = Data.ProtoLens.Field.field @"commitment"
compact ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "compact" a) =>
  Lens.Family2.LensLike' f s a
compact = Data.ProtoLens.Field.field @"compact"
contentDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentDigest" a) =>
  Lens.Family2.LensLike' f s a
contentDigest = Data.ProtoLens.Field.field @"contentDigest"
context ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "context" a) =>
  Lens.Family2.LensLike' f s a
context = Data.ProtoLens.Field.field @"context"
continuationProfile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "continuationProfile" a) =>
  Lens.Family2.LensLike' f s a
continuationProfile
  = Data.ProtoLens.Field.field @"continuationProfile"
created ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "created" a) =>
  Lens.Family2.LensLike' f s a
created = Data.ProtoLens.Field.field @"created"
delete ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "delete" a) =>
  Lens.Family2.LensLike' f s a
delete = Data.ProtoLens.Field.field @"delete"
denominator ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "denominator" a) =>
  Lens.Family2.LensLike' f s a
denominator = Data.ProtoLens.Field.field @"denominator"
derived ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "derived" a) =>
  Lens.Family2.LensLike' f s a
derived = Data.ProtoLens.Field.field @"derived"
digest ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "digest" a) =>
  Lens.Family2.LensLike' f s a
digest = Data.ProtoLens.Field.field @"digest"
edit ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "edit" a) =>
  Lens.Family2.LensLike' f s a
edit = Data.ProtoLens.Field.field @"edit"
edits ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "edits" a) =>
  Lens.Family2.LensLike' f s a
edits = Data.ProtoLens.Field.field @"edits"
effectiveContextReads ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "effectiveContextReads" a) =>
  Lens.Family2.LensLike' f s a
effectiveContextReads
  = Data.ProtoLens.Field.field @"effectiveContextReads"
evaluationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "evaluationId" a) =>
  Lens.Family2.LensLike' f s a
evaluationId = Data.ProtoLens.Field.field @"evaluationId"
evidenceDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "evidenceDigest" a) =>
  Lens.Family2.LensLike' f s a
evidenceDigest = Data.ProtoLens.Field.field @"evidenceDigest"
executionProfile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "executionProfile" a) =>
  Lens.Family2.LensLike' f s a
executionProfile = Data.ProtoLens.Field.field @"executionProfile"
expiresAtMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expiresAtMs" a) =>
  Lens.Family2.LensLike' f s a
expiresAtMs = Data.ProtoLens.Field.field @"expiresAtMs"
features ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "features" a) =>
  Lens.Family2.LensLike' f s a
features = Data.ProtoLens.Field.field @"features"
fork ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "fork" a) =>
  Lens.Family2.LensLike' f s a
fork = Data.ProtoLens.Field.field @"fork"
forked ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "forked" a) =>
  Lens.Family2.LensLike' f s a
forked = Data.ProtoLens.Field.field @"forked"
fromSequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fromSequence" a) =>
  Lens.Family2.LensLike' f s a
fromSequence = Data.ProtoLens.Field.field @"fromSequence"
generated ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "generated" a) =>
  Lens.Family2.LensLike' f s a
generated = Data.ProtoLens.Field.field @"generated"
generatedOutput ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "generatedOutput" a) =>
  Lens.Family2.LensLike' f s a
generatedOutput = Data.ProtoLens.Field.field @"generatedOutput"
grader ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "grader" a) =>
  Lens.Family2.LensLike' f s a
grader = Data.ProtoLens.Field.field @"grader"
handle ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "handle" a) =>
  Lens.Family2.LensLike' f s a
handle = Data.ProtoLens.Field.field @"handle"
id ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "id" a) =>
  Lens.Family2.LensLike' f s a
id = Data.ProtoLens.Field.field @"id"
identity ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "identity" a) =>
  Lens.Family2.LensLike' f s a
identity = Data.ProtoLens.Field.field @"identity"
idleKv ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "idleKv" a) =>
  Lens.Family2.LensLike' f s a
idleKv = Data.ProtoLens.Field.field @"idleKv"
idleKvProfiles ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idleKvProfiles" a) =>
  Lens.Family2.LensLike' f s a
idleKvProfiles = Data.ProtoLens.Field.field @"idleKvProfiles"
idleTimeoutMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idleTimeoutMs" a) =>
  Lens.Family2.LensLike' f s a
idleTimeoutMs = Data.ProtoLens.Field.field @"idleTimeoutMs"
input ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "input" a) =>
  Lens.Family2.LensLike' f s a
input = Data.ProtoLens.Field.field @"input"
inputArtifactDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inputArtifactDigest" a) =>
  Lens.Family2.LensLike' f s a
inputArtifactDigest
  = Data.ProtoLens.Field.field @"inputArtifactDigest"
insertAfter ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "insertAfter" a) =>
  Lens.Family2.LensLike' f s a
insertAfter = Data.ProtoLens.Field.field @"insertAfter"
insertBefore ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "insertBefore" a) =>
  Lens.Family2.LensLike' f s a
insertBefore = Data.ProtoLens.Field.field @"insertBefore"
item ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "item" a) =>
  Lens.Family2.LensLike' f s a
item = Data.ProtoLens.Field.field @"item"
items ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "items" a) =>
  Lens.Family2.LensLike' f s a
items = Data.ProtoLens.Field.field @"items"
kind ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "kind" a) =>
  Lens.Family2.LensLike' f s a
kind = Data.ProtoLens.Field.field @"kind"
lastRunId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "lastRunId" a) =>
  Lens.Family2.LensLike' f s a
lastRunId = Data.ProtoLens.Field.field @"lastRunId"
lastSequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "lastSequence" a) =>
  Lens.Family2.LensLike' f s a
lastSequence = Data.ProtoLens.Field.field @"lastSequence"
lastUsedAtMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "lastUsedAtMs" a) =>
  Lens.Family2.LensLike' f s a
lastUsedAtMs = Data.ProtoLens.Field.field @"lastUsedAtMs"
latencyProfile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "latencyProfile" a) =>
  Lens.Family2.LensLike' f s a
latencyProfile = Data.ProtoLens.Field.field @"latencyProfile"
lineage ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "lineage" a) =>
  Lens.Family2.LensLike' f s a
lineage = Data.ProtoLens.Field.field @"lineage"
link ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "link" a) =>
  Lens.Family2.LensLike' f s a
link = Data.ProtoLens.Field.field @"link"
logicalSize ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "logicalSize" a) =>
  Lens.Family2.LensLike' f s a
logicalSize = Data.ProtoLens.Field.field @"logicalSize"
maximumCaseResults ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumCaseResults" a) =>
  Lens.Family2.LensLike' f s a
maximumCaseResults
  = Data.ProtoLens.Field.field @"maximumCaseResults"
maximumContext ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumContext" a) =>
  Lens.Family2.LensLike' f s a
maximumContext = Data.ProtoLens.Field.field @"maximumContext"
maximumDurationMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumDurationMs" a) =>
  Lens.Family2.LensLike' f s a
maximumDurationMs = Data.ProtoLens.Field.field @"maximumDurationMs"
maximumOutput ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumOutput" a) =>
  Lens.Family2.LensLike' f s a
maximumOutput = Data.ProtoLens.Field.field @"maximumOutput"
maybe'action ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'action" a) =>
  Lens.Family2.LensLike' f s a
maybe'action = Data.ProtoLens.Field.field @"maybe'action"
maybe'append ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'append" a) =>
  Lens.Family2.LensLike' f s a
maybe'append = Data.ProtoLens.Field.field @"maybe'append"
maybe'compact ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'compact" a) =>
  Lens.Family2.LensLike' f s a
maybe'compact = Data.ProtoLens.Field.field @"maybe'compact"
maybe'context ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'context" a) =>
  Lens.Family2.LensLike' f s a
maybe'context = Data.ProtoLens.Field.field @"maybe'context"
maybe'created ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'created" a) =>
  Lens.Family2.LensLike' f s a
maybe'created = Data.ProtoLens.Field.field @"maybe'created"
maybe'delete ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'delete" a) =>
  Lens.Family2.LensLike' f s a
maybe'delete = Data.ProtoLens.Field.field @"maybe'delete"
maybe'derived ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'derived" a) =>
  Lens.Family2.LensLike' f s a
maybe'derived = Data.ProtoLens.Field.field @"maybe'derived"
maybe'edit ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'edit" a) =>
  Lens.Family2.LensLike' f s a
maybe'edit = Data.ProtoLens.Field.field @"maybe'edit"
maybe'event ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'event" a) =>
  Lens.Family2.LensLike' f s a
maybe'event = Data.ProtoLens.Field.field @"maybe'event"
maybe'fork ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'fork" a) =>
  Lens.Family2.LensLike' f s a
maybe'fork = Data.ProtoLens.Field.field @"maybe'fork"
maybe'forked ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'forked" a) =>
  Lens.Family2.LensLike' f s a
maybe'forked = Data.ProtoLens.Field.field @"maybe'forked"
maybe'generated ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'generated" a) =>
  Lens.Family2.LensLike' f s a
maybe'generated = Data.ProtoLens.Field.field @"maybe'generated"
maybe'grader ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'grader" a) =>
  Lens.Family2.LensLike' f s a
maybe'grader = Data.ProtoLens.Field.field @"maybe'grader"
maybe'identity ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'identity" a) =>
  Lens.Family2.LensLike' f s a
maybe'identity = Data.ProtoLens.Field.field @"maybe'identity"
maybe'idleKv ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'idleKv" a) =>
  Lens.Family2.LensLike' f s a
maybe'idleKv = Data.ProtoLens.Field.field @"maybe'idleKv"
maybe'idleTimeoutMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'idleTimeoutMs" a) =>
  Lens.Family2.LensLike' f s a
maybe'idleTimeoutMs
  = Data.ProtoLens.Field.field @"maybe'idleTimeoutMs"
maybe'input ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'input" a) =>
  Lens.Family2.LensLike' f s a
maybe'input = Data.ProtoLens.Field.field @"maybe'input"
maybe'inputArtifactDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'inputArtifactDigest" a) =>
  Lens.Family2.LensLike' f s a
maybe'inputArtifactDigest
  = Data.ProtoLens.Field.field @"maybe'inputArtifactDigest"
maybe'insertAfter ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'insertAfter" a) =>
  Lens.Family2.LensLike' f s a
maybe'insertAfter = Data.ProtoLens.Field.field @"maybe'insertAfter"
maybe'insertBefore ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'insertBefore" a) =>
  Lens.Family2.LensLike' f s a
maybe'insertBefore
  = Data.ProtoLens.Field.field @"maybe'insertBefore"
maybe'item ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'item" a) =>
  Lens.Family2.LensLike' f s a
maybe'item = Data.ProtoLens.Field.field @"maybe'item"
maybe'lastRunId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'lastRunId" a) =>
  Lens.Family2.LensLike' f s a
maybe'lastRunId = Data.ProtoLens.Field.field @"maybe'lastRunId"
maybe'lastUsedAtMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'lastUsedAtMs" a) =>
  Lens.Family2.LensLike' f s a
maybe'lastUsedAtMs
  = Data.ProtoLens.Field.field @"maybe'lastUsedAtMs"
maybe'observation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'observation" a) =>
  Lens.Family2.LensLike' f s a
maybe'observation = Data.ProtoLens.Field.field @"maybe'observation"
maybe'origin ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'origin" a) =>
  Lens.Family2.LensLike' f s a
maybe'origin = Data.ProtoLens.Field.field @"maybe'origin"
maybe'output ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'output" a) =>
  Lens.Family2.LensLike' f s a
maybe'output = Data.ProtoLens.Field.field @"maybe'output"
maybe'parent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'parent" a) =>
  Lens.Family2.LensLike' f s a
maybe'parent = Data.ProtoLens.Field.field @"maybe'parent"
maybe'policy ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'policy" a) =>
  Lens.Family2.LensLike' f s a
maybe'policy = Data.ProtoLens.Field.field @"maybe'policy"
maybe'progress ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'progress" a) =>
  Lens.Family2.LensLike' f s a
maybe'progress = Data.ProtoLens.Field.field @"maybe'progress"
maybe'provenance ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'provenance" a) =>
  Lens.Family2.LensLike' f s a
maybe'provenance = Data.ProtoLens.Field.field @"maybe'provenance"
maybe'receipt ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'receipt" a) =>
  Lens.Family2.LensLike' f s a
maybe'receipt = Data.ProtoLens.Field.field @"maybe'receipt"
maybe'release ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'release" a) =>
  Lens.Family2.LensLike' f s a
maybe'release = Data.ProtoLens.Field.field @"maybe'release"
maybe'replace ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'replace" a) =>
  Lens.Family2.LensLike' f s a
maybe'replace = Data.ProtoLens.Field.field @"maybe'replace"
maybe'result ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'result" a) =>
  Lens.Family2.LensLike' f s a
maybe'result = Data.ProtoLens.Field.field @"maybe'result"
maybe'run ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'run" a) =>
  Lens.Family2.LensLike' f s a
maybe'run = Data.ProtoLens.Field.field @"maybe'run"
maybe'runInput ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'runInput" a) =>
  Lens.Family2.LensLike' f s a
maybe'runInput = Data.ProtoLens.Field.field @"maybe'runInput"
maybe'seed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'seed" a) =>
  Lens.Family2.LensLike' f s a
maybe'seed = Data.ProtoLens.Field.field @"maybe'seed"
maybe'spec ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'spec" a) =>
  Lens.Family2.LensLike' f s a
maybe'spec = Data.ProtoLens.Field.field @"maybe'spec"
maybe'suite ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suite" a) =>
  Lens.Family2.LensLike' f s a
maybe'suite = Data.ProtoLens.Field.field @"maybe'suite"
maybe'terminal ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'terminal" a) =>
  Lens.Family2.LensLike' f s a
maybe'terminal = Data.ProtoLens.Field.field @"maybe'terminal"
maybe'through ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'through" a) =>
  Lens.Family2.LensLike' f s a
maybe'through = Data.ProtoLens.Field.field @"maybe'through"
maybe'transfer ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'transfer" a) =>
  Lens.Family2.LensLike' f s a
maybe'transfer = Data.ProtoLens.Field.field @"maybe'transfer"
maybe'transferred ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'transferred" a) =>
  Lens.Family2.LensLike' f s a
maybe'transferred = Data.ProtoLens.Field.field @"maybe'transferred"
maybe'truncate ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'truncate" a) =>
  Lens.Family2.LensLike' f s a
maybe'truncate = Data.ProtoLens.Field.field @"maybe'truncate"
maybe'usage ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'usage" a) =>
  Lens.Family2.LensLike' f s a
maybe'usage = Data.ProtoLens.Field.field @"maybe'usage"
maybe'value ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'value" a) =>
  Lens.Family2.LensLike' f s a
maybe'value = Data.ProtoLens.Field.field @"maybe'value"
mediaType ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "mediaType" a) =>
  Lens.Family2.LensLike' f s a
mediaType = Data.ProtoLens.Field.field @"mediaType"
meterRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "meterRevision" a) =>
  Lens.Family2.LensLike' f s a
meterRevision = Data.ProtoLens.Field.field @"meterRevision"
metricIdentity ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "metricIdentity" a) =>
  Lens.Family2.LensLike' f s a
metricIdentity = Data.ProtoLens.Field.field @"metricIdentity"
metrics ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "metrics" a) =>
  Lens.Family2.LensLike' f s a
metrics = Data.ProtoLens.Field.field @"metrics"
minimumDurationMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "minimumDurationMs" a) =>
  Lens.Family2.LensLike' f s a
minimumDurationMs = Data.ProtoLens.Field.field @"minimumDurationMs"
model ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "model" a) =>
  Lens.Family2.LensLike' f s a
model = Data.ProtoLens.Field.field @"model"
modelProfile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "modelProfile" a) =>
  Lens.Family2.LensLike' f s a
modelProfile = Data.ProtoLens.Field.field @"modelProfile"
models ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "models" a) =>
  Lens.Family2.LensLike' f s a
models = Data.ProtoLens.Field.field @"models"
nativeOutputDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "nativeOutputDigest" a) =>
  Lens.Family2.LensLike' f s a
nativeOutputDigest
  = Data.ProtoLens.Field.field @"nativeOutputDigest"
newPrefill ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "newPrefill" a) =>
  Lens.Family2.LensLike' f s a
newPrefill = Data.ProtoLens.Field.field @"newPrefill"
numerator ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "numerator" a) =>
  Lens.Family2.LensLike' f s a
numerator = Data.ProtoLens.Field.field @"numerator"
observation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "observation" a) =>
  Lens.Family2.LensLike' f s a
observation = Data.ProtoLens.Field.field @"observation"
observationDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "observationDigest" a) =>
  Lens.Family2.LensLike' f s a
observationDigest = Data.ProtoLens.Field.field @"observationDigest"
outcome ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "outcome" a) =>
  Lens.Family2.LensLike' f s a
outcome = Data.ProtoLens.Field.field @"outcome"
output ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "output" a) =>
  Lens.Family2.LensLike' f s a
output = Data.ProtoLens.Field.field @"output"
parent ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "parent" a) =>
  Lens.Family2.LensLike' f s a
parent = Data.ProtoLens.Field.field @"parent"
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
profile ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "profile" a) =>
  Lens.Family2.LensLike' f s a
profile = Data.ProtoLens.Field.field @"profile"
progress ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "progress" a) =>
  Lens.Family2.LensLike' f s a
progress = Data.ProtoLens.Field.field @"progress"
provenance ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "provenance" a) =>
  Lens.Family2.LensLike' f s a
provenance = Data.ProtoLens.Field.field @"provenance"
rateCardRevision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "rateCardRevision" a) =>
  Lens.Family2.LensLike' f s a
rateCardRevision = Data.ProtoLens.Field.field @"rateCardRevision"
receipt ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "receipt" a) =>
  Lens.Family2.LensLike' f s a
receipt = Data.ProtoLens.Field.field @"receipt"
receiptId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "receiptId" a) =>
  Lens.Family2.LensLike' f s a
receiptId = Data.ProtoLens.Field.field @"receiptId"
release ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "release" a) =>
  Lens.Family2.LensLike' f s a
release = Data.ProtoLens.Field.field @"release"
replace ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "replace" a) =>
  Lens.Family2.LensLike' f s a
replace = Data.ProtoLens.Field.field @"replace"
replacement ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "replacement" a) =>
  Lens.Family2.LensLike' f s a
replacement = Data.ProtoLens.Field.field @"replacement"
requestId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "requestId" a) =>
  Lens.Family2.LensLike' f s a
requestId = Data.ProtoLens.Field.field @"requestId"
result ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "result" a) =>
  Lens.Family2.LensLike' f s a
result = Data.ProtoLens.Field.field @"result"
resultDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "resultDigest" a) =>
  Lens.Family2.LensLike' f s a
resultDigest = Data.ProtoLens.Field.field @"resultDigest"
retained ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "retained" a) =>
  Lens.Family2.LensLike' f s a
retained = Data.ProtoLens.Field.field @"retained"
retainedAtMs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "retainedAtMs" a) =>
  Lens.Family2.LensLike' f s a
retainedAtMs = Data.ProtoLens.Field.field @"retainedAtMs"
retainedByteMillis ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "retainedByteMillis" a) =>
  Lens.Family2.LensLike' f s a
retainedByteMillis
  = Data.ProtoLens.Field.field @"retainedByteMillis"
retentionProfiles ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "retentionProfiles" a) =>
  Lens.Family2.LensLike' f s a
retentionProfiles = Data.ProtoLens.Field.field @"retentionProfiles"
reusedCompatibleState ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "reusedCompatibleState" a) =>
  Lens.Family2.LensLike' f s a
reusedCompatibleState
  = Data.ProtoLens.Field.field @"reusedCompatibleState"
revision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "revision" a) =>
  Lens.Family2.LensLike' f s a
revision = Data.ProtoLens.Field.field @"revision"
run ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "run" a) =>
  Lens.Family2.LensLike' f s a
run = Data.ProtoLens.Field.field @"run"
runId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "runId" a) =>
  Lens.Family2.LensLike' f s a
runId = Data.ProtoLens.Field.field @"runId"
runInput ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "runInput" a) =>
  Lens.Family2.LensLike' f s a
runInput = Data.ProtoLens.Field.field @"runInput"
seed ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "seed" a) =>
  Lens.Family2.LensLike' f s a
seed = Data.ProtoLens.Field.field @"seed"
selected ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "selected" a) =>
  Lens.Family2.LensLike' f s a
selected = Data.ProtoLens.Field.field @"selected"
sequence ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sequence" a) =>
  Lens.Family2.LensLike' f s a
sequence = Data.ProtoLens.Field.field @"sequence"
source ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "source" a) =>
  Lens.Family2.LensLike' f s a
source = Data.ProtoLens.Field.field @"source"
spec ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "spec" a) =>
  Lens.Family2.LensLike' f s a
spec = Data.ProtoLens.Field.field @"spec"
specDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "specDigest" a) =>
  Lens.Family2.LensLike' f s a
specDigest = Data.ProtoLens.Field.field @"specDigest"
state ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "state" a) =>
  Lens.Family2.LensLike' f s a
state = Data.ProtoLens.Field.field @"state"
suite ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "suite" a) =>
  Lens.Family2.LensLike' f s a
suite = Data.ProtoLens.Field.field @"suite"
target ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "target" a) =>
  Lens.Family2.LensLike' f s a
target = Data.ProtoLens.Field.field @"target"
terminal ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "terminal" a) =>
  Lens.Family2.LensLike' f s a
terminal = Data.ProtoLens.Field.field @"terminal"
terminalReceiptDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "terminalReceiptDigest" a) =>
  Lens.Family2.LensLike' f s a
terminalReceiptDigest
  = Data.ProtoLens.Field.field @"terminalReceiptDigest"
through ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "through" a) =>
  Lens.Family2.LensLike' f s a
through = Data.ProtoLens.Field.field @"through"
transfer ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "transfer" a) =>
  Lens.Family2.LensLike' f s a
transfer = Data.ProtoLens.Field.field @"transfer"
transferred ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "transferred" a) =>
  Lens.Family2.LensLike' f s a
transferred = Data.ProtoLens.Field.field @"transferred"
truncate ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "truncate" a) =>
  Lens.Family2.LensLike' f s a
truncate = Data.ProtoLens.Field.field @"truncate"
usage ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "usage" a) =>
  Lens.Family2.LensLike' f s a
usage = Data.ProtoLens.Field.field @"usage"
value ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "value" a) =>
  Lens.Family2.LensLike' f s a
value = Data.ProtoLens.Field.field @"value"
vec'aggregates ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'aggregates" a) =>
  Lens.Family2.LensLike' f s a
vec'aggregates = Data.ProtoLens.Field.field @"vec'aggregates"
vec'candidates ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'candidates" a) =>
  Lens.Family2.LensLike' f s a
vec'candidates = Data.ProtoLens.Field.field @"vec'candidates"
vec'caseResults ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'caseResults" a) =>
  Lens.Family2.LensLike' f s a
vec'caseResults = Data.ProtoLens.Field.field @"vec'caseResults"
vec'cases ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'cases" a) =>
  Lens.Family2.LensLike' f s a
vec'cases = Data.ProtoLens.Field.field @"vec'cases"
vec'edits ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'edits" a) =>
  Lens.Family2.LensLike' f s a
vec'edits = Data.ProtoLens.Field.field @"vec'edits"
vec'features ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'features" a) =>
  Lens.Family2.LensLike' f s a
vec'features = Data.ProtoLens.Field.field @"vec'features"
vec'idleKvProfiles ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'idleKvProfiles" a) =>
  Lens.Family2.LensLike' f s a
vec'idleKvProfiles
  = Data.ProtoLens.Field.field @"vec'idleKvProfiles"
vec'items ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'items" a) =>
  Lens.Family2.LensLike' f s a
vec'items = Data.ProtoLens.Field.field @"vec'items"
vec'metrics ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'metrics" a) =>
  Lens.Family2.LensLike' f s a
vec'metrics = Data.ProtoLens.Field.field @"vec'metrics"
vec'models ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'models" a) =>
  Lens.Family2.LensLike' f s a
vec'models = Data.ProtoLens.Field.field @"vec'models"
vec'replacement ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'replacement" a) =>
  Lens.Family2.LensLike' f s a
vec'replacement = Data.ProtoLens.Field.field @"vec'replacement"
vec'retentionProfiles ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'retentionProfiles" a) =>
  Lens.Family2.LensLike' f s a
vec'retentionProfiles
  = Data.ProtoLens.Field.field @"vec'retentionProfiles"
vec'selected ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'selected" a) =>
  Lens.Family2.LensLike' f s a
vec'selected = Data.ProtoLens.Field.field @"vec'selected"