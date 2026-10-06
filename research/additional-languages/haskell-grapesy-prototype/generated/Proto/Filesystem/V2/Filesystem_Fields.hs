{- This file was auto-generated from filesystem/v2/filesystem.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Filesystem.V2.Filesystem_Fields where
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
accessKeyId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "accessKeyId" a) =>
  Lens.Family2.LensLike' f s a
accessKeyId = Data.ProtoLens.Field.field @"accessKeyId"
accessedNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "accessedNs" a) =>
  Lens.Family2.LensLike' f s a
accessedNs = Data.ProtoLens.Field.field @"accessedNs"
actualDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "actualDigest" a) =>
  Lens.Family2.LensLike' f s a
actualDigest = Data.ProtoLens.Field.field @"actualDigest"
actualHead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "actualHead" a) =>
  Lens.Family2.LensLike' f s a
actualHead = Data.ProtoLens.Field.field @"actualHead"
after ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "after" a) =>
  Lens.Family2.LensLike' f s a
after = Data.ProtoLens.Field.field @"after"
allocated ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "allocated" a) =>
  Lens.Family2.LensLike' f s a
allocated = Data.ProtoLens.Field.field @"allocated"
allocationOperations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "allocationOperations" a) =>
  Lens.Family2.LensLike' f s a
allocationOperations
  = Data.ProtoLens.Field.field @"allocationOperations"
authorityBytesRead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "authorityBytesRead" a) =>
  Lens.Family2.LensLike' f s a
authorityBytesRead
  = Data.ProtoLens.Field.field @"authorityBytesRead"
authorityBytesWritten ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "authorityBytesWritten" a) =>
  Lens.Family2.LensLike' f s a
authorityBytesWritten
  = Data.ProtoLens.Field.field @"authorityBytesWritten"
authorityRecordsAppended ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "authorityRecordsAppended" a) =>
  Lens.Family2.LensLike' f s a
authorityRecordsAppended
  = Data.ProtoLens.Field.field @"authorityRecordsAppended"
authorityRecordsRead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "authorityRecordsRead" a) =>
  Lens.Family2.LensLike' f s a
authorityRecordsRead
  = Data.ProtoLens.Field.field @"authorityRecordsRead"
backendReadOperations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "backendReadOperations" a) =>
  Lens.Family2.LensLike' f s a
backendReadOperations
  = Data.ProtoLens.Field.field @"backendReadOperations"
backendWriteOperations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "backendWriteOperations" a) =>
  Lens.Family2.LensLike' f s a
backendWriteOperations
  = Data.ProtoLens.Field.field @"backendWriteOperations"
base ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "base" a) =>
  Lens.Family2.LensLike' f s a
base = Data.ProtoLens.Field.field @"base"
bearerToken ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bearerToken" a) =>
  Lens.Family2.LensLike' f s a
bearerToken = Data.ProtoLens.Field.field @"bearerToken"
before ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "before" a) =>
  Lens.Family2.LensLike' f s a
before = Data.ProtoLens.Field.field @"before"
bindingChanges ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bindingChanges" a) =>
  Lens.Family2.LensLike' f s a
bindingChanges = Data.ProtoLens.Field.field @"bindingChanges"
bindings ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bindings" a) =>
  Lens.Family2.LensLike' f s a
bindings = Data.ProtoLens.Field.field @"bindings"
bucket ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "bucket" a) =>
  Lens.Family2.LensLike' f s a
bucket = Data.ProtoLens.Field.field @"bucket"
bytes ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "bytes" a) =>
  Lens.Family2.LensLike' f s a
bytes = Data.ProtoLens.Field.field @"bytes"
bytesCopied ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bytesCopied" a) =>
  Lens.Family2.LensLike' f s a
bytesCopied = Data.ProtoLens.Field.field @"bytesCopied"
bytesEncoded ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bytesEncoded" a) =>
  Lens.Family2.LensLike' f s a
bytesEncoded = Data.ProtoLens.Field.field @"bytesEncoded"
bytesHashed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "bytesHashed" a) =>
  Lens.Family2.LensLike' f s a
bytesHashed = Data.ProtoLens.Field.field @"bytesHashed"
capabilities ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "capabilities" a) =>
  Lens.Family2.LensLike' f s a
capabilities = Data.ProtoLens.Field.field @"capabilities"
changedNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "changedNs" a) =>
  Lens.Family2.LensLike' f s a
changedNs = Data.ProtoLens.Field.field @"changedNs"
cloneRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "cloneRange" a) =>
  Lens.Family2.LensLike' f s a
cloneRange = Data.ProtoLens.Field.field @"cloneRange"
commonAncestor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "commonAncestor" a) =>
  Lens.Family2.LensLike' f s a
commonAncestor = Data.ProtoLens.Field.field @"commonAncestor"
conflicts ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "conflicts" a) =>
  Lens.Family2.LensLike' f s a
conflicts = Data.ProtoLens.Field.field @"conflicts"
contentRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentRange" a) =>
  Lens.Family2.LensLike' f s a
contentRange = Data.ProtoLens.Field.field @"contentRange"
contents ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contents" a) =>
  Lens.Family2.LensLike' f s a
contents = Data.ProtoLens.Field.field @"contents"
contextId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contextId" a) =>
  Lens.Family2.LensLike' f s a
contextId = Data.ProtoLens.Field.field @"contextId"
contextIds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contextIds" a) =>
  Lens.Family2.LensLike' f s a
contextIds = Data.ProtoLens.Field.field @"contextIds"
contractVersion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contractVersion" a) =>
  Lens.Family2.LensLike' f s a
contractVersion = Data.ProtoLens.Field.field @"contractVersion"
copyFile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "copyFile" a) =>
  Lens.Family2.LensLike' f s a
copyFile = Data.ProtoLens.Field.field @"copyFile"
createDirectories ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createDirectories" a) =>
  Lens.Family2.LensLike' f s a
createDirectories = Data.ProtoLens.Field.field @"createDirectories"
createDirectory ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createDirectory" a) =>
  Lens.Family2.LensLike' f s a
createDirectory = Data.ProtoLens.Field.field @"createDirectory"
createFile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createFile" a) =>
  Lens.Family2.LensLike' f s a
createFile = Data.ProtoLens.Field.field @"createFile"
createSymbolicLink ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createSymbolicLink" a) =>
  Lens.Family2.LensLike' f s a
createSymbolicLink
  = Data.ProtoLens.Field.field @"createSymbolicLink"
createdNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createdNs" a) =>
  Lens.Family2.LensLike' f s a
createdNs = Data.ProtoLens.Field.field @"createdNs"
cursor ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "cursor" a) =>
  Lens.Family2.LensLike' f s a
cursor = Data.ProtoLens.Field.field @"cursor"
deleted ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "deleted" a) =>
  Lens.Family2.LensLike' f s a
deleted = Data.ProtoLens.Field.field @"deleted"
destination ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "destination" a) =>
  Lens.Family2.LensLike' f s a
destination = Data.ProtoLens.Field.field @"destination"
destinationName ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "destinationName" a) =>
  Lens.Family2.LensLike' f s a
destinationName = Data.ProtoLens.Field.field @"destinationName"
destinationOffset ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "destinationOffset" a) =>
  Lens.Family2.LensLike' f s a
destinationOffset = Data.ProtoLens.Field.field @"destinationOffset"
deviceMajor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "deviceMajor" a) =>
  Lens.Family2.LensLike' f s a
deviceMajor = Data.ProtoLens.Field.field @"deviceMajor"
deviceMinor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "deviceMinor" a) =>
  Lens.Family2.LensLike' f s a
deviceMinor = Data.ProtoLens.Field.field @"deviceMinor"
directoryId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "directoryId" a) =>
  Lens.Family2.LensLike' f s a
directoryId = Data.ProtoLens.Field.field @"directoryId"
directoryName ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "directoryName" a) =>
  Lens.Family2.LensLike' f s a
directoryName = Data.ProtoLens.Field.field @"directoryName"
directoryRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "directoryRange" a) =>
  Lens.Family2.LensLike' f s a
directoryRange = Data.ProtoLens.Field.field @"directoryRange"
durabilityOperations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "durabilityOperations" a) =>
  Lens.Family2.LensLike' f s a
durabilityOperations
  = Data.ProtoLens.Field.field @"durabilityOperations"
encoding ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "encoding" a) =>
  Lens.Family2.LensLike' f s a
encoding = Data.ProtoLens.Field.field @"encoding"
endpoint ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "endpoint" a) =>
  Lens.Family2.LensLike' f s a
endpoint = Data.ProtoLens.Field.field @"endpoint"
entries ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "entries" a) =>
  Lens.Family2.LensLike' f s a
entries = Data.ProtoLens.Field.field @"entries"
expectedDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expectedDigest" a) =>
  Lens.Family2.LensLike' f s a
expectedDigest = Data.ProtoLens.Field.field @"expectedDigest"
expectedTarget ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expectedTarget" a) =>
  Lens.Family2.LensLike' f s a
expectedTarget = Data.ProtoLens.Field.field @"expectedTarget"
expiresAfterSeconds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expiresAfterSeconds" a) =>
  Lens.Family2.LensLike' f s a
expiresAfterSeconds
  = Data.ProtoLens.Field.field @"expiresAfterSeconds"
expiresAtUnixSeconds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expiresAtUnixSeconds" a) =>
  Lens.Family2.LensLike' f s a
expiresAtUnixSeconds
  = Data.ProtoLens.Field.field @"expiresAtUnixSeconds"
extend ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "extend" a) =>
  Lens.Family2.LensLike' f s a
extend = Data.ProtoLens.Field.field @"extend"
extents ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "extents" a) =>
  Lens.Family2.LensLike' f s a
extents = Data.ProtoLens.Field.field @"extents"
fileChanges ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fileChanges" a) =>
  Lens.Family2.LensLike' f s a
fileChanges = Data.ProtoLens.Field.field @"fileChanges"
fileId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "fileId" a) =>
  Lens.Family2.LensLike' f s a
fileId = Data.ProtoLens.Field.field @"fileId"
fileKind ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fileKind" a) =>
  Lens.Family2.LensLike' f s a
fileKind = Data.ProtoLens.Field.field @"fileKind"
fileLength ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fileLength" a) =>
  Lens.Family2.LensLike' f s a
fileLength = Data.ProtoLens.Field.field @"fileLength"
fileRecord ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "fileRecord" a) =>
  Lens.Family2.LensLike' f s a
fileRecord = Data.ProtoLens.Field.field @"fileRecord"
files ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "files" a) =>
  Lens.Family2.LensLike' f s a
files = Data.ProtoLens.Field.field @"files"
from ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "from" a) =>
  Lens.Family2.LensLike' f s a
from = Data.ProtoLens.Field.field @"from"
generation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "generation" a) =>
  Lens.Family2.LensLike' f s a
generation = Data.ProtoLens.Field.field @"generation"
generationId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "generationId" a) =>
  Lens.Family2.LensLike' f s a
generationId = Data.ProtoLens.Field.field @"generationId"
hardLink ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "hardLink" a) =>
  Lens.Family2.LensLike' f s a
hardLink = Data.ProtoLens.Field.field @"hardLink"
hasAcl ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "hasAcl" a) =>
  Lens.Family2.LensLike' f s a
hasAcl = Data.ProtoLens.Field.field @"hasAcl"
hasNamedAttributes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "hasNamedAttributes" a) =>
  Lens.Family2.LensLike' f s a
hasNamedAttributes
  = Data.ProtoLens.Field.field @"hasNamedAttributes"
hasSecurityDescriptor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "hasSecurityDescriptor" a) =>
  Lens.Family2.LensLike' f s a
hasSecurityDescriptor
  = Data.ProtoLens.Field.field @"hasSecurityDescriptor"
head ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "head" a) =>
  Lens.Family2.LensLike' f s a
head = Data.ProtoLens.Field.field @"head"
history ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "history" a) =>
  Lens.Family2.LensLike' f s a
history = Data.ProtoLens.Field.field @"history"
idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
idempotencyKey = Data.ProtoLens.Field.field @"idempotencyKey"
identity ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "identity" a) =>
  Lens.Family2.LensLike' f s a
identity = Data.ProtoLens.Field.field @"identity"
inlineBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "inlineBytes" a) =>
  Lens.Family2.LensLike' f s a
inlineBytes = Data.ProtoLens.Field.field @"inlineBytes"
itemsExamined ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "itemsExamined" a) =>
  Lens.Family2.LensLike' f s a
itemsExamined = Data.ProtoLens.Field.field @"itemsExamined"
itemsReturned ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "itemsReturned" a) =>
  Lens.Family2.LensLike' f s a
itemsReturned = Data.ProtoLens.Field.field @"itemsReturned"
keepSize ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "keepSize" a) =>
  Lens.Family2.LensLike' f s a
keepSize = Data.ProtoLens.Field.field @"keepSize"
kind ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "kind" a) =>
  Lens.Family2.LensLike' f s a
kind = Data.ProtoLens.Field.field @"kind"
length ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "length" a) =>
  Lens.Family2.LensLike' f s a
length = Data.ProtoLens.Field.field @"length"
linkCount ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "linkCount" a) =>
  Lens.Family2.LensLike' f s a
linkCount = Data.ProtoLens.Field.field @"linkCount"
logicalBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "logicalBytes" a) =>
  Lens.Family2.LensLike' f s a
logicalBytes = Data.ProtoLens.Field.field @"logicalBytes"
materializations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "materializations" a) =>
  Lens.Family2.LensLike' f s a
materializations = Data.ProtoLens.Field.field @"materializations"
maximumBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumBytes" a) =>
  Lens.Family2.LensLike' f s a
maximumBytes = Data.ProtoLens.Field.field @"maximumBytes"
maximumChanges ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumChanges" a) =>
  Lens.Family2.LensLike' f s a
maximumChanges = Data.ProtoLens.Field.field @"maximumChanges"
maximumConflicts ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumConflicts" a) =>
  Lens.Family2.LensLike' f s a
maximumConflicts = Data.ProtoLens.Field.field @"maximumConflicts"
maximumEntries ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumEntries" a) =>
  Lens.Family2.LensLike' f s a
maximumEntries = Data.ProtoLens.Field.field @"maximumEntries"
maximumExtents ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumExtents" a) =>
  Lens.Family2.LensLike' f s a
maximumExtents = Data.ProtoLens.Field.field @"maximumExtents"
maximumGenerations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumGenerations" a) =>
  Lens.Family2.LensLike' f s a
maximumGenerations
  = Data.ProtoLens.Field.field @"maximumGenerations"
maximumItems ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumItems" a) =>
  Lens.Family2.LensLike' f s a
maximumItems = Data.ProtoLens.Field.field @"maximumItems"
maximumObjects ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumObjects" a) =>
  Lens.Family2.LensLike' f s a
maximumObjects = Data.ProtoLens.Field.field @"maximumObjects"
maximumPageItems ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumPageItems" a) =>
  Lens.Family2.LensLike' f s a
maximumPageItems = Data.ProtoLens.Field.field @"maximumPageItems"
maximumRequestBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumRequestBytes" a) =>
  Lens.Family2.LensLike' f s a
maximumRequestBytes
  = Data.ProtoLens.Field.field @"maximumRequestBytes"
maximumResponseBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumResponseBytes" a) =>
  Lens.Family2.LensLike' f s a
maximumResponseBytes
  = Data.ProtoLens.Field.field @"maximumResponseBytes"
maximumTransactionMutations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maximumTransactionMutations" a) =>
  Lens.Family2.LensLike' f s a
maximumTransactionMutations
  = Data.ProtoLens.Field.field @"maximumTransactionMutations"
maybe'accessedNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'accessedNs" a) =>
  Lens.Family2.LensLike' f s a
maybe'accessedNs = Data.ProtoLens.Field.field @"maybe'accessedNs"
maybe'actualHead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'actualHead" a) =>
  Lens.Family2.LensLike' f s a
maybe'actualHead = Data.ProtoLens.Field.field @"maybe'actualHead"
maybe'after ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'after" a) =>
  Lens.Family2.LensLike' f s a
maybe'after = Data.ProtoLens.Field.field @"maybe'after"
maybe'base ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'base" a) =>
  Lens.Family2.LensLike' f s a
maybe'base = Data.ProtoLens.Field.field @"maybe'base"
maybe'bearerToken ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'bearerToken" a) =>
  Lens.Family2.LensLike' f s a
maybe'bearerToken = Data.ProtoLens.Field.field @"maybe'bearerToken"
maybe'before ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'before" a) =>
  Lens.Family2.LensLike' f s a
maybe'before = Data.ProtoLens.Field.field @"maybe'before"
maybe'capabilities ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'capabilities" a) =>
  Lens.Family2.LensLike' f s a
maybe'capabilities
  = Data.ProtoLens.Field.field @"maybe'capabilities"
maybe'changedNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'changedNs" a) =>
  Lens.Family2.LensLike' f s a
maybe'changedNs = Data.ProtoLens.Field.field @"maybe'changedNs"
maybe'cloneRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'cloneRange" a) =>
  Lens.Family2.LensLike' f s a
maybe'cloneRange = Data.ProtoLens.Field.field @"maybe'cloneRange"
maybe'commonAncestor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'commonAncestor" a) =>
  Lens.Family2.LensLike' f s a
maybe'commonAncestor
  = Data.ProtoLens.Field.field @"maybe'commonAncestor"
maybe'contentRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'contentRange" a) =>
  Lens.Family2.LensLike' f s a
maybe'contentRange
  = Data.ProtoLens.Field.field @"maybe'contentRange"
maybe'copyFile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'copyFile" a) =>
  Lens.Family2.LensLike' f s a
maybe'copyFile = Data.ProtoLens.Field.field @"maybe'copyFile"
maybe'createDirectories ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'createDirectories" a) =>
  Lens.Family2.LensLike' f s a
maybe'createDirectories
  = Data.ProtoLens.Field.field @"maybe'createDirectories"
maybe'createDirectory ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'createDirectory" a) =>
  Lens.Family2.LensLike' f s a
maybe'createDirectory
  = Data.ProtoLens.Field.field @"maybe'createDirectory"
maybe'createFile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'createFile" a) =>
  Lens.Family2.LensLike' f s a
maybe'createFile = Data.ProtoLens.Field.field @"maybe'createFile"
maybe'createSymbolicLink ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'createSymbolicLink" a) =>
  Lens.Family2.LensLike' f s a
maybe'createSymbolicLink
  = Data.ProtoLens.Field.field @"maybe'createSymbolicLink"
maybe'createdNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'createdNs" a) =>
  Lens.Family2.LensLike' f s a
maybe'createdNs = Data.ProtoLens.Field.field @"maybe'createdNs"
maybe'credential ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'credential" a) =>
  Lens.Family2.LensLike' f s a
maybe'credential = Data.ProtoLens.Field.field @"maybe'credential"
maybe'deviceMajor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'deviceMajor" a) =>
  Lens.Family2.LensLike' f s a
maybe'deviceMajor = Data.ProtoLens.Field.field @"maybe'deviceMajor"
maybe'deviceMinor ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'deviceMinor" a) =>
  Lens.Family2.LensLike' f s a
maybe'deviceMinor = Data.ProtoLens.Field.field @"maybe'deviceMinor"
maybe'directoryName ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'directoryName" a) =>
  Lens.Family2.LensLike' f s a
maybe'directoryName
  = Data.ProtoLens.Field.field @"maybe'directoryName"
maybe'directoryRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'directoryRange" a) =>
  Lens.Family2.LensLike' f s a
maybe'directoryRange
  = Data.ProtoLens.Field.field @"maybe'directoryRange"
maybe'expectedTarget ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'expectedTarget" a) =>
  Lens.Family2.LensLike' f s a
maybe'expectedTarget
  = Data.ProtoLens.Field.field @"maybe'expectedTarget"
maybe'fileLength ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'fileLength" a) =>
  Lens.Family2.LensLike' f s a
maybe'fileLength = Data.ProtoLens.Field.field @"maybe'fileLength"
maybe'fileRecord ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'fileRecord" a) =>
  Lens.Family2.LensLike' f s a
maybe'fileRecord = Data.ProtoLens.Field.field @"maybe'fileRecord"
maybe'from ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'from" a) =>
  Lens.Family2.LensLike' f s a
maybe'from = Data.ProtoLens.Field.field @"maybe'from"
maybe'generation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'generation" a) =>
  Lens.Family2.LensLike' f s a
maybe'generation = Data.ProtoLens.Field.field @"maybe'generation"
maybe'hardLink ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'hardLink" a) =>
  Lens.Family2.LensLike' f s a
maybe'hardLink = Data.ProtoLens.Field.field @"maybe'hardLink"
maybe'head ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'head" a) =>
  Lens.Family2.LensLike' f s a
maybe'head = Data.ProtoLens.Field.field @"maybe'head"
maybe'logicalBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'logicalBytes" a) =>
  Lens.Family2.LensLike' f s a
maybe'logicalBytes
  = Data.ProtoLens.Field.field @"maybe'logicalBytes"
maybe'metadata ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'metadata" a) =>
  Lens.Family2.LensLike' f s a
maybe'metadata = Data.ProtoLens.Field.field @"maybe'metadata"
maybe'modifiedNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'modifiedNs" a) =>
  Lens.Family2.LensLike' f s a
maybe'modifiedNs = Data.ProtoLens.Field.field @"maybe'modifiedNs"
maybe'mountPath ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'mountPath" a) =>
  Lens.Family2.LensLike' f s a
maybe'mountPath = Data.ProtoLens.Field.field @"maybe'mountPath"
maybe'mutation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'mutation" a) =>
  Lens.Family2.LensLike' f s a
maybe'mutation = Data.ProtoLens.Field.field @"maybe'mutation"
maybe'name ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'name" a) =>
  Lens.Family2.LensLike' f s a
maybe'name = Data.ProtoLens.Field.field @"maybe'name"
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
maybe'outcome ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'outcome" a) =>
  Lens.Family2.LensLike' f s a
maybe'outcome = Data.ProtoLens.Field.field @"maybe'outcome"
maybe'page ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'page" a) =>
  Lens.Family2.LensLike' f s a
maybe'page = Data.ProtoLens.Field.field @"maybe'page"
maybe'parentContextId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'parentContextId" a) =>
  Lens.Family2.LensLike' f s a
maybe'parentContextId
  = Data.ProtoLens.Field.field @"maybe'parentContextId"
maybe'parentWorkspaceId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'parentWorkspaceId" a) =>
  Lens.Family2.LensLike' f s a
maybe'parentWorkspaceId
  = Data.ProtoLens.Field.field @"maybe'parentWorkspaceId"
maybe'plan ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'plan" a) =>
  Lens.Family2.LensLike' f s a
maybe'plan = Data.ProtoLens.Field.field @"maybe'plan"
maybe'posixFlags ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'posixFlags" a) =>
  Lens.Family2.LensLike' f s a
maybe'posixFlags = Data.ProtoLens.Field.field @"maybe'posixFlags"
maybe'posixGid ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'posixGid" a) =>
  Lens.Family2.LensLike' f s a
maybe'posixGid = Data.ProtoLens.Field.field @"maybe'posixGid"
maybe'posixMode ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'posixMode" a) =>
  Lens.Family2.LensLike' f s a
maybe'posixMode = Data.ProtoLens.Field.field @"maybe'posixMode"
maybe'posixUid ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'posixUid" a) =>
  Lens.Family2.LensLike' f s a
maybe'posixUid = Data.ProtoLens.Field.field @"maybe'posixUid"
maybe'preallocate ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'preallocate" a) =>
  Lens.Family2.LensLike' f s a
maybe'preallocate = Data.ProtoLens.Field.field @"maybe'preallocate"
maybe'present ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'present" a) =>
  Lens.Family2.LensLike' f s a
maybe'present = Data.ProtoLens.Field.field @"maybe'present"
maybe'protocol ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'protocol" a) =>
  Lens.Family2.LensLike' f s a
maybe'protocol = Data.ProtoLens.Field.field @"maybe'protocol"
maybe'putFile ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'putFile" a) =>
  Lens.Family2.LensLike' f s a
maybe'putFile = Data.ProtoLens.Field.field @"maybe'putFile"
maybe'range ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'range" a) =>
  Lens.Family2.LensLike' f s a
maybe'range = Data.ProtoLens.Field.field @"maybe'range"
maybe'region ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'region" a) =>
  Lens.Family2.LensLike' f s a
maybe'region = Data.ProtoLens.Field.field @"maybe'region"
maybe'remove ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'remove" a) =>
  Lens.Family2.LensLike' f s a
maybe'remove = Data.ProtoLens.Field.field @"maybe'remove"
maybe'rename ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'rename" a) =>
  Lens.Family2.LensLike' f s a
maybe'rename = Data.ProtoLens.Field.field @"maybe'rename"
maybe'resize ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'resize" a) =>
  Lens.Family2.LensLike' f s a
maybe'resize = Data.ProtoLens.Field.field @"maybe'resize"
maybe's3 ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe's3" a) =>
  Lens.Family2.LensLike' f s a
maybe's3 = Data.ProtoLens.Field.field @"maybe's3"
maybe'selector ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'selector" a) =>
  Lens.Family2.LensLike' f s a
maybe'selector = Data.ProtoLens.Field.field @"maybe'selector"
maybe'setMetadata ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'setMetadata" a) =>
  Lens.Family2.LensLike' f s a
maybe'setMetadata = Data.ProtoLens.Field.field @"maybe'setMetadata"
maybe'source ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'source" a) =>
  Lens.Family2.LensLike' f s a
maybe'source = Data.ProtoLens.Field.field @"maybe'source"
maybe'sparseSeek ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'sparseSeek" a) =>
  Lens.Family2.LensLike' f s a
maybe'sparseSeek = Data.ProtoLens.Field.field @"maybe'sparseSeek"
maybe'stat ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'stat" a) =>
  Lens.Family2.LensLike' f s a
maybe'stat = Data.ProtoLens.Field.field @"maybe'stat"
maybe'target ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'target" a) =>
  Lens.Family2.LensLike' f s a
maybe'target = Data.ProtoLens.Field.field @"maybe'target"
maybe'to ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'to" a) =>
  Lens.Family2.LensLike' f s a
maybe'to = Data.ProtoLens.Field.field @"maybe'to"
maybe'unavailable ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'unavailable" a) =>
  Lens.Family2.LensLike' f s a
maybe'unavailable = Data.ProtoLens.Field.field @"maybe'unavailable"
maybe'value ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'value" a) =>
  Lens.Family2.LensLike' f s a
maybe'value = Data.ProtoLens.Field.field @"maybe'value"
maybe'windowsAttributes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'windowsAttributes" a) =>
  Lens.Family2.LensLike' f s a
maybe'windowsAttributes
  = Data.ProtoLens.Field.field @"maybe'windowsAttributes"
maybe'work ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'work" a) =>
  Lens.Family2.LensLike' f s a
maybe'work = Data.ProtoLens.Field.field @"maybe'work"
maybe'workspace ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'workspace" a) =>
  Lens.Family2.LensLike' f s a
maybe'workspace = Data.ProtoLens.Field.field @"maybe'workspace"
maybe'write ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'write" a) =>
  Lens.Family2.LensLike' f s a
maybe'write = Data.ProtoLens.Field.field @"maybe'write"
maybe'zeroRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'zeroRange" a) =>
  Lens.Family2.LensLike' f s a
maybe'zeroRange = Data.ProtoLens.Field.field @"maybe'zeroRange"
metadata ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "metadata" a) =>
  Lens.Family2.LensLike' f s a
metadata = Data.ProtoLens.Field.field @"metadata"
metadataObject ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "metadataObject" a) =>
  Lens.Family2.LensLike' f s a
metadataObject = Data.ProtoLens.Field.field @"metadataObject"
modifiedNs ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "modifiedNs" a) =>
  Lens.Family2.LensLike' f s a
modifiedNs = Data.ProtoLens.Field.field @"modifiedNs"
mountPath ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "mountPath" a) =>
  Lens.Family2.LensLike' f s a
mountPath = Data.ProtoLens.Field.field @"mountPath"
mutations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "mutations" a) =>
  Lens.Family2.LensLike' f s a
mutations = Data.ProtoLens.Field.field @"mutations"
name ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "name" a) =>
  Lens.Family2.LensLike' f s a
name = Data.ProtoLens.Field.field @"name"
nativeMountCredentials ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "nativeMountCredentials" a) =>
  Lens.Family2.LensLike' f s a
nativeMountCredentials
  = Data.ProtoLens.Field.field @"nativeMountCredentials"
next ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "next" a) =>
  Lens.Family2.LensLike' f s a
next = Data.ProtoLens.Field.field @"next"
objectBytesRead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "objectBytesRead" a) =>
  Lens.Family2.LensLike' f s a
objectBytesRead = Data.ProtoLens.Field.field @"objectBytesRead"
objectBytesWritten ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "objectBytesWritten" a) =>
  Lens.Family2.LensLike' f s a
objectBytesWritten
  = Data.ProtoLens.Field.field @"objectBytesWritten"
objectId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "objectId" a) =>
  Lens.Family2.LensLike' f s a
objectId = Data.ProtoLens.Field.field @"objectId"
objectProbes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "objectProbes" a) =>
  Lens.Family2.LensLike' f s a
objectProbes = Data.ProtoLens.Field.field @"objectProbes"
offset ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "offset" a) =>
  Lens.Family2.LensLike' f s a
offset = Data.ProtoLens.Field.field @"offset"
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
outcome ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "outcome" a) =>
  Lens.Family2.LensLike' f s a
outcome = Data.ProtoLens.Field.field @"outcome"
outputBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "outputBytes" a) =>
  Lens.Family2.LensLike' f s a
outputBytes = Data.ProtoLens.Field.field @"outputBytes"
page ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "page" a) =>
  Lens.Family2.LensLike' f s a
page = Data.ProtoLens.Field.field @"page"
pageReads ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "pageReads" a) =>
  Lens.Family2.LensLike' f s a
pageReads = Data.ProtoLens.Field.field @"pageReads"
pageWrites ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "pageWrites" a) =>
  Lens.Family2.LensLike' f s a
pageWrites = Data.ProtoLens.Field.field @"pageWrites"
parentContextId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "parentContextId" a) =>
  Lens.Family2.LensLike' f s a
parentContextId = Data.ProtoLens.Field.field @"parentContextId"
parentWorkspaceId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "parentWorkspaceId" a) =>
  Lens.Family2.LensLike' f s a
parentWorkspaceId = Data.ProtoLens.Field.field @"parentWorkspaceId"
parents ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "parents" a) =>
  Lens.Family2.LensLike' f s a
parents = Data.ProtoLens.Field.field @"parents"
path ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "path" a) =>
  Lens.Family2.LensLike' f s a
path = Data.ProtoLens.Field.field @"path"
payloadKind ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "payloadKind" a) =>
  Lens.Family2.LensLike' f s a
payloadKind = Data.ProtoLens.Field.field @"payloadKind"
payloadObject ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "payloadObject" a) =>
  Lens.Family2.LensLike' f s a
payloadObject = Data.ProtoLens.Field.field @"payloadObject"
peakAllocationBytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "peakAllocationBytes" a) =>
  Lens.Family2.LensLike' f s a
peakAllocationBytes
  = Data.ProtoLens.Field.field @"peakAllocationBytes"
plan ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "plan" a) =>
  Lens.Family2.LensLike' f s a
plan = Data.ProtoLens.Field.field @"plan"
planId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "planId" a) =>
  Lens.Family2.LensLike' f s a
planId = Data.ProtoLens.Field.field @"planId"
posixFlags ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "posixFlags" a) =>
  Lens.Family2.LensLike' f s a
posixFlags = Data.ProtoLens.Field.field @"posixFlags"
posixGid ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "posixGid" a) =>
  Lens.Family2.LensLike' f s a
posixGid = Data.ProtoLens.Field.field @"posixGid"
posixMode ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "posixMode" a) =>
  Lens.Family2.LensLike' f s a
posixMode = Data.ProtoLens.Field.field @"posixMode"
posixUid ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "posixUid" a) =>
  Lens.Family2.LensLike' f s a
posixUid = Data.ProtoLens.Field.field @"posixUid"
preallocate ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "preallocate" a) =>
  Lens.Family2.LensLike' f s a
preallocate = Data.ProtoLens.Field.field @"preallocate"
present ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "present" a) =>
  Lens.Family2.LensLike' f s a
present = Data.ProtoLens.Field.field @"present"
profile ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "profile" a) =>
  Lens.Family2.LensLike' f s a
profile = Data.ProtoLens.Field.field @"profile"
profiles ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "profiles" a) =>
  Lens.Family2.LensLike' f s a
profiles = Data.ProtoLens.Field.field @"profiles"
protocol ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "protocol" a) =>
  Lens.Family2.LensLike' f s a
protocol = Data.ProtoLens.Field.field @"protocol"
putFile ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "putFile" a) =>
  Lens.Family2.LensLike' f s a
putFile = Data.ProtoLens.Field.field @"putFile"
range ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "range" a) =>
  Lens.Family2.LensLike' f s a
range = Data.ProtoLens.Field.field @"range"
reason ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "reason" a) =>
  Lens.Family2.LensLike' f s a
reason = Data.ProtoLens.Field.field @"reason"
region ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "region" a) =>
  Lens.Family2.LensLike' f s a
region = Data.ProtoLens.Field.field @"region"
remove ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "remove" a) =>
  Lens.Family2.LensLike' f s a
remove = Data.ProtoLens.Field.field @"remove"
rename ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "rename" a) =>
  Lens.Family2.LensLike' f s a
rename = Data.ProtoLens.Field.field @"rename"
replace ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "replace" a) =>
  Lens.Family2.LensLike' f s a
replace = Data.ProtoLens.Field.field @"replace"
resize ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "resize" a) =>
  Lens.Family2.LensLike' f s a
resize = Data.ProtoLens.Field.field @"resize"
revision ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "revision" a) =>
  Lens.Family2.LensLike' f s a
revision = Data.ProtoLens.Field.field @"revision"
rootId ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "rootId" a) =>
  Lens.Family2.LensLike' f s a
rootId = Data.ProtoLens.Field.field @"rootId"
roots ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "roots" a) =>
  Lens.Family2.LensLike' f s a
roots = Data.ProtoLens.Field.field @"roots"
s3 ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "s3" a) =>
  Lens.Family2.LensLike' f s a
s3 = Data.ProtoLens.Field.field @"s3"
s3Credentials ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "s3Credentials" a) =>
  Lens.Family2.LensLike' f s a
s3Credentials = Data.ProtoLens.Field.field @"s3Credentials"
secretAccessKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "secretAccessKey" a) =>
  Lens.Family2.LensLike' f s a
secretAccessKey = Data.ProtoLens.Field.field @"secretAccessKey"
sessionToken ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sessionToken" a) =>
  Lens.Family2.LensLike' f s a
sessionToken = Data.ProtoLens.Field.field @"sessionToken"
setMetadata ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "setMetadata" a) =>
  Lens.Family2.LensLike' f s a
setMetadata = Data.ProtoLens.Field.field @"setMetadata"
source ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "source" a) =>
  Lens.Family2.LensLike' f s a
source = Data.ProtoLens.Field.field @"source"
sourceBytesRead ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourceBytesRead" a) =>
  Lens.Family2.LensLike' f s a
sourceBytesRead = Data.ProtoLens.Field.field @"sourceBytesRead"
sourceEntriesVisited ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourceEntriesVisited" a) =>
  Lens.Family2.LensLike' f s a
sourceEntriesVisited
  = Data.ProtoLens.Field.field @"sourceEntriesVisited"
sourceOffset ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourceOffset" a) =>
  Lens.Family2.LensLike' f s a
sourceOffset = Data.ProtoLens.Field.field @"sourceOffset"
sourcePath ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourcePath" a) =>
  Lens.Family2.LensLike' f s a
sourcePath = Data.ProtoLens.Field.field @"sourcePath"
sourcePathComponents ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourcePathComponents" a) =>
  Lens.Family2.LensLike' f s a
sourcePathComponents
  = Data.ProtoLens.Field.field @"sourcePathComponents"
sourceReconciliation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sourceReconciliation" a) =>
  Lens.Family2.LensLike' f s a
sourceReconciliation
  = Data.ProtoLens.Field.field @"sourceReconciliation"
sparseSeek ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "sparseSeek" a) =>
  Lens.Family2.LensLike' f s a
sparseSeek = Data.ProtoLens.Field.field @"sparseSeek"
stat ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "stat" a) =>
  Lens.Family2.LensLike' f s a
stat = Data.ProtoLens.Field.field @"stat"
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
to ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "to" a) =>
  Lens.Family2.LensLike' f s a
to = Data.ProtoLens.Field.field @"to"
truncated ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "truncated" a) =>
  Lens.Family2.LensLike' f s a
truncated = Data.ProtoLens.Field.field @"truncated"
unavailable ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "unavailable" a) =>
  Lens.Family2.LensLike' f s a
unavailable = Data.ProtoLens.Field.field @"unavailable"
use ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "use" a) =>
  Lens.Family2.LensLike' f s a
use = Data.ProtoLens.Field.field @"use"
vec'bindingChanges ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'bindingChanges" a) =>
  Lens.Family2.LensLike' f s a
vec'bindingChanges
  = Data.ProtoLens.Field.field @"vec'bindingChanges"
vec'bindings ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'bindings" a) =>
  Lens.Family2.LensLike' f s a
vec'bindings = Data.ProtoLens.Field.field @"vec'bindings"
vec'conflicts ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'conflicts" a) =>
  Lens.Family2.LensLike' f s a
vec'conflicts = Data.ProtoLens.Field.field @"vec'conflicts"
vec'contextIds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'contextIds" a) =>
  Lens.Family2.LensLike' f s a
vec'contextIds = Data.ProtoLens.Field.field @"vec'contextIds"
vec'entries ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'entries" a) =>
  Lens.Family2.LensLike' f s a
vec'entries = Data.ProtoLens.Field.field @"vec'entries"
vec'extents ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'extents" a) =>
  Lens.Family2.LensLike' f s a
vec'extents = Data.ProtoLens.Field.field @"vec'extents"
vec'fileChanges ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'fileChanges" a) =>
  Lens.Family2.LensLike' f s a
vec'fileChanges = Data.ProtoLens.Field.field @"vec'fileChanges"
vec'files ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'files" a) =>
  Lens.Family2.LensLike' f s a
vec'files = Data.ProtoLens.Field.field @"vec'files"
vec'mutations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'mutations" a) =>
  Lens.Family2.LensLike' f s a
vec'mutations = Data.ProtoLens.Field.field @"vec'mutations"
vec'parents ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'parents" a) =>
  Lens.Family2.LensLike' f s a
vec'parents = Data.ProtoLens.Field.field @"vec'parents"
vec'profiles ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'profiles" a) =>
  Lens.Family2.LensLike' f s a
vec'profiles = Data.ProtoLens.Field.field @"vec'profiles"
vec'roots ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'roots" a) =>
  Lens.Family2.LensLike' f s a
vec'roots = Data.ProtoLens.Field.field @"vec'roots"
version ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "version" a) =>
  Lens.Family2.LensLike' f s a
version = Data.ProtoLens.Field.field @"version"
windowsAttributes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "windowsAttributes" a) =>
  Lens.Family2.LensLike' f s a
windowsAttributes = Data.ProtoLens.Field.field @"windowsAttributes"
work ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "work" a) =>
  Lens.Family2.LensLike' f s a
work = Data.ProtoLens.Field.field @"work"
workspace ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "workspace" a) =>
  Lens.Family2.LensLike' f s a
workspace = Data.ProtoLens.Field.field @"workspace"
workspaceId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "workspaceId" a) =>
  Lens.Family2.LensLike' f s a
workspaceId = Data.ProtoLens.Field.field @"workspaceId"
workspaceName ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "workspaceName" a) =>
  Lens.Family2.LensLike' f s a
workspaceName = Data.ProtoLens.Field.field @"workspaceName"
writable ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "writable" a) =>
  Lens.Family2.LensLike' f s a
writable = Data.ProtoLens.Field.field @"writable"
write ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "write" a) =>
  Lens.Family2.LensLike' f s a
write = Data.ProtoLens.Field.field @"write"
zeroRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "zeroRange" a) =>
  Lens.Family2.LensLike' f s a
zeroRange = Data.ProtoLens.Field.field @"zeroRange"