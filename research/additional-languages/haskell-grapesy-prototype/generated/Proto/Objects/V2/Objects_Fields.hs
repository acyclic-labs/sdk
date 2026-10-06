{- This file was auto-generated from objects/v2/objects.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Objects.V2.Objects_Fields where
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
import qualified Proto.Google.Protobuf.Timestamp
afterPartNumber ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "afterPartNumber" a) =>
  Lens.Family2.LensLike' f s a
afterPartNumber = Data.ProtoLens.Field.field @"afterPartNumber"
body ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "body" a) =>
  Lens.Family2.LensLike' f s a
body = Data.ProtoLens.Field.field @"body"
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
cacheControl ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "cacheControl" a) =>
  Lens.Family2.LensLike' f s a
cacheControl = Data.ProtoLens.Field.field @"cacheControl"
code ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "code" a) =>
  Lens.Family2.LensLike' f s a
code = Data.ProtoLens.Field.field @"code"
commonPrefixes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "commonPrefixes" a) =>
  Lens.Family2.LensLike' f s a
commonPrefixes = Data.ProtoLens.Field.field @"commonPrefixes"
complete ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "complete" a) =>
  Lens.Family2.LensLike' f s a
complete = Data.ProtoLens.Field.field @"complete"
contentDisposition ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentDisposition" a) =>
  Lens.Family2.LensLike' f s a
contentDisposition
  = Data.ProtoLens.Field.field @"contentDisposition"
contentEncoding ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentEncoding" a) =>
  Lens.Family2.LensLike' f s a
contentEncoding = Data.ProtoLens.Field.field @"contentEncoding"
contentLanguage ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentLanguage" a) =>
  Lens.Family2.LensLike' f s a
contentLanguage = Data.ProtoLens.Field.field @"contentLanguage"
contentRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentRange" a) =>
  Lens.Family2.LensLike' f s a
contentRange = Data.ProtoLens.Field.field @"contentRange"
contentType ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "contentType" a) =>
  Lens.Family2.LensLike' f s a
contentType = Data.ProtoLens.Field.field @"contentType"
continuationToken ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "continuationToken" a) =>
  Lens.Family2.LensLike' f s a
continuationToken = Data.ProtoLens.Field.field @"continuationToken"
createdAt ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "createdAt" a) =>
  Lens.Family2.LensLike' f s a
createdAt = Data.ProtoLens.Field.field @"createdAt"
delimiter ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "delimiter" a) =>
  Lens.Family2.LensLike' f s a
delimiter = Data.ProtoLens.Field.field @"delimiter"
end ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "end" a) =>
  Lens.Family2.LensLike' f s a
end = Data.ProtoLens.Field.field @"end"
entries ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "entries" a) =>
  Lens.Family2.LensLike' f s a
entries = Data.ProtoLens.Field.field @"entries"
error ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "error" a) =>
  Lens.Family2.LensLike' f s a
error = Data.ProtoLens.Field.field @"error"
etag ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "etag" a) =>
  Lens.Family2.LensLike' f s a
etag = Data.ProtoLens.Field.field @"etag"
existed ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "existed" a) =>
  Lens.Family2.LensLike' f s a
existed = Data.ProtoLens.Field.field @"existed"
expiresUnixSeconds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expiresUnixSeconds" a) =>
  Lens.Family2.LensLike' f s a
expiresUnixSeconds
  = Data.ProtoLens.Field.field @"expiresUnixSeconds"
header ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "header" a) =>
  Lens.Family2.LensLike' f s a
header = Data.ProtoLens.Field.field @"header"
idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
idempotencyKey = Data.ProtoLens.Field.field @"idempotencyKey"
ifAbsent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "ifAbsent" a) =>
  Lens.Family2.LensLike' f s a
ifAbsent = Data.ProtoLens.Field.field @"ifAbsent"
ifMatch ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "ifMatch" a) =>
  Lens.Family2.LensLike' f s a
ifMatch = Data.ProtoLens.Field.field @"ifMatch"
ifNoneMatch ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "ifNoneMatch" a) =>
  Lens.Family2.LensLike' f s a
ifNoneMatch = Data.ProtoLens.Field.field @"ifNoneMatch"
isTruncated ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "isTruncated" a) =>
  Lens.Family2.LensLike' f s a
isTruncated = Data.ProtoLens.Field.field @"isTruncated"
key ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "key" a) =>
  Lens.Family2.LensLike' f s a
key = Data.ProtoLens.Field.field @"key"
lastModified ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "lastModified" a) =>
  Lens.Family2.LensLike' f s a
lastModified = Data.ProtoLens.Field.field @"lastModified"
maybe'body ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'body" a) =>
  Lens.Family2.LensLike' f s a
maybe'body = Data.ProtoLens.Field.field @"maybe'body"
maybe'bucket ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'bucket" a) =>
  Lens.Family2.LensLike' f s a
maybe'bucket = Data.ProtoLens.Field.field @"maybe'bucket"
maybe'bytes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'bytes" a) =>
  Lens.Family2.LensLike' f s a
maybe'bytes = Data.ProtoLens.Field.field @"maybe'bytes"
maybe'complete ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'complete" a) =>
  Lens.Family2.LensLike' f s a
maybe'complete = Data.ProtoLens.Field.field @"maybe'complete"
maybe'condition ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'condition" a) =>
  Lens.Family2.LensLike' f s a
maybe'condition = Data.ProtoLens.Field.field @"maybe'condition"
maybe'contentRange ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'contentRange" a) =>
  Lens.Family2.LensLike' f s a
maybe'contentRange
  = Data.ProtoLens.Field.field @"maybe'contentRange"
maybe'createdAt ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'createdAt" a) =>
  Lens.Family2.LensLike' f s a
maybe'createdAt = Data.ProtoLens.Field.field @"maybe'createdAt"
maybe'end ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'end" a) =>
  Lens.Family2.LensLike' f s a
maybe'end = Data.ProtoLens.Field.field @"maybe'end"
maybe'error ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'error" a) =>
  Lens.Family2.LensLike' f s a
maybe'error = Data.ProtoLens.Field.field @"maybe'error"
maybe'expiresUnixSeconds ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'expiresUnixSeconds" a) =>
  Lens.Family2.LensLike' f s a
maybe'expiresUnixSeconds
  = Data.ProtoLens.Field.field @"maybe'expiresUnixSeconds"
maybe'frame ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'frame" a) =>
  Lens.Family2.LensLike' f s a
maybe'frame = Data.ProtoLens.Field.field @"maybe'frame"
maybe'header ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'header" a) =>
  Lens.Family2.LensLike' f s a
maybe'header = Data.ProtoLens.Field.field @"maybe'header"
maybe'ifAbsent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'ifAbsent" a) =>
  Lens.Family2.LensLike' f s a
maybe'ifAbsent = Data.ProtoLens.Field.field @"maybe'ifAbsent"
maybe'ifMatch ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'ifMatch" a) =>
  Lens.Family2.LensLike' f s a
maybe'ifMatch = Data.ProtoLens.Field.field @"maybe'ifMatch"
maybe'lastModified ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'lastModified" a) =>
  Lens.Family2.LensLike' f s a
maybe'lastModified
  = Data.ProtoLens.Field.field @"maybe'lastModified"
maybe'metadata ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'metadata" a) =>
  Lens.Family2.LensLike' f s a
maybe'metadata = Data.ProtoLens.Field.field @"maybe'metadata"
maybe'mutation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'mutation" a) =>
  Lens.Family2.LensLike' f s a
maybe'mutation = Data.ProtoLens.Field.field @"maybe'mutation"
maybe'object ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'object" a) =>
  Lens.Family2.LensLike' f s a
maybe'object = Data.ProtoLens.Field.field @"maybe'object"
maybe'preconditions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'preconditions" a) =>
  Lens.Family2.LensLike' f s a
maybe'preconditions
  = Data.ProtoLens.Field.field @"maybe'preconditions"
maybe'range ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'range" a) =>
  Lens.Family2.LensLike' f s a
maybe'range = Data.ProtoLens.Field.field @"maybe'range"
maybe'selection ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'selection" a) =>
  Lens.Family2.LensLike' f s a
maybe'selection = Data.ProtoLens.Field.field @"maybe'selection"
maybe'suffixLength ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'suffixLength" a) =>
  Lens.Family2.LensLike' f s a
maybe'suffixLength
  = Data.ProtoLens.Field.field @"maybe'suffixLength"
metadata ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "metadata" a) =>
  Lens.Family2.LensLike' f s a
metadata = Data.ProtoLens.Field.field @"metadata"
mutation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "mutation" a) =>
  Lens.Family2.LensLike' f s a
mutation = Data.ProtoLens.Field.field @"mutation"
name ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "name" a) =>
  Lens.Family2.LensLike' f s a
name = Data.ProtoLens.Field.field @"name"
nextPartNumber ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "nextPartNumber" a) =>
  Lens.Family2.LensLike' f s a
nextPartNumber = Data.ProtoLens.Field.field @"nextPartNumber"
object ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "object" a) =>
  Lens.Family2.LensLike' f s a
object = Data.ProtoLens.Field.field @"object"
objectKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "objectKey" a) =>
  Lens.Family2.LensLike' f s a
objectKey = Data.ProtoLens.Field.field @"objectKey"
pageSize ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "pageSize" a) =>
  Lens.Family2.LensLike' f s a
pageSize = Data.ProtoLens.Field.field @"pageSize"
partNumber ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "partNumber" a) =>
  Lens.Family2.LensLike' f s a
partNumber = Data.ProtoLens.Field.field @"partNumber"
parts ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "parts" a) =>
  Lens.Family2.LensLike' f s a
parts = Data.ProtoLens.Field.field @"parts"
preconditions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "preconditions" a) =>
  Lens.Family2.LensLike' f s a
preconditions = Data.ProtoLens.Field.field @"preconditions"
prefix ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "prefix" a) =>
  Lens.Family2.LensLike' f s a
prefix = Data.ProtoLens.Field.field @"prefix"
range ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "range" a) =>
  Lens.Family2.LensLike' f s a
range = Data.ProtoLens.Field.field @"range"
requestId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "requestId" a) =>
  Lens.Family2.LensLike' f s a
requestId = Data.ProtoLens.Field.field @"requestId"
size ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "size" a) =>
  Lens.Family2.LensLike' f s a
size = Data.ProtoLens.Field.field @"size"
start ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "start" a) =>
  Lens.Family2.LensLike' f s a
start = Data.ProtoLens.Field.field @"start"
suffixLength ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "suffixLength" a) =>
  Lens.Family2.LensLike' f s a
suffixLength = Data.ProtoLens.Field.field @"suffixLength"
total ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "total" a) =>
  Lens.Family2.LensLike' f s a
total = Data.ProtoLens.Field.field @"total"
uploadId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "uploadId" a) =>
  Lens.Family2.LensLike' f s a
uploadId = Data.ProtoLens.Field.field @"uploadId"
user ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "user" a) =>
  Lens.Family2.LensLike' f s a
user = Data.ProtoLens.Field.field @"user"
value ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "value" a) =>
  Lens.Family2.LensLike' f s a
value = Data.ProtoLens.Field.field @"value"
vec'commonPrefixes ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'commonPrefixes" a) =>
  Lens.Family2.LensLike' f s a
vec'commonPrefixes
  = Data.ProtoLens.Field.field @"vec'commonPrefixes"
vec'entries ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'entries" a) =>
  Lens.Family2.LensLike' f s a
vec'entries = Data.ProtoLens.Field.field @"vec'entries"
vec'parts ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'parts" a) =>
  Lens.Family2.LensLike' f s a
vec'parts = Data.ProtoLens.Field.field @"vec'parts"