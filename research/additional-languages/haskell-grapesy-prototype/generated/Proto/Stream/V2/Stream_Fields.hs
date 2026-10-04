{- This file was auto-generated from stream/v2/stream.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Stream.V2.Stream_Fields where
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
absent ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "absent" a) =>
  Lens.Family2.LensLike' f s a
absent = Data.ProtoLens.Field.field @"absent"
actual ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "actual" a) =>
  Lens.Family2.LensLike' f s a
actual = Data.ProtoLens.Field.field @"actual"
actualTail ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "actualTail" a) =>
  Lens.Family2.LensLike' f s a
actualTail = Data.ProtoLens.Field.field @"actualTail"
after ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "after" a) =>
  Lens.Family2.LensLike' f s a
after = Data.ProtoLens.Field.field @"after"
allow ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "allow" a) =>
  Lens.Family2.LensLike' f s a
allow = Data.ProtoLens.Field.field @"allow"
append ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "append" a) =>
  Lens.Family2.LensLike' f s a
append = Data.ProtoLens.Field.field @"append"
atTail ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "atTail" a) =>
  Lens.Family2.LensLike' f s a
atTail = Data.ProtoLens.Field.field @"atTail"
child ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "child" a) =>
  Lens.Family2.LensLike' f s a
child = Data.ProtoLens.Field.field @"child"
children ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "children" a) =>
  Lens.Family2.LensLike' f s a
children = Data.ProtoLens.Field.field @"children"
commit ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "commit" a) =>
  Lens.Family2.LensLike' f s a
commit = Data.ProtoLens.Field.field @"commit"
commitId ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "commitId" a) =>
  Lens.Family2.LensLike' f s a
commitId = Data.ProtoLens.Field.field @"commitId"
committed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "committed" a) =>
  Lens.Family2.LensLike' f s a
committed = Data.ProtoLens.Field.field @"committed"
committedAtMicros ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "committedAtMicros" a) =>
  Lens.Family2.LensLike' f s a
committedAtMicros = Data.ProtoLens.Field.field @"committedAtMicros"
conditions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "conditions" a) =>
  Lens.Family2.LensLike' f s a
conditions = Data.ProtoLens.Field.field @"conditions"
conflict ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "conflict" a) =>
  Lens.Family2.LensLike' f s a
conflict = Data.ProtoLens.Field.field @"conflict"
conflicts ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "conflicts" a) =>
  Lens.Family2.LensLike' f s a
conflicts = Data.ProtoLens.Field.field @"conflicts"
deadlineUnixMillis ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "deadlineUnixMillis" a) =>
  Lens.Family2.LensLike' f s a
deadlineUnixMillis
  = Data.ProtoLens.Field.field @"deadlineUnixMillis"
destination ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "destination" a) =>
  Lens.Family2.LensLike' f s a
destination = Data.ProtoLens.Field.field @"destination"
end ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "end" a) =>
  Lens.Family2.LensLike' f s a
end = Data.ProtoLens.Field.field @"end"
exists ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "exists" a) =>
  Lens.Family2.LensLike' f s a
exists = Data.ProtoLens.Field.field @"exists"
expected ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expected" a) =>
  Lens.Family2.LensLike' f s a
expected = Data.ProtoLens.Field.field @"expected"
expiresIn ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "expiresIn" a) =>
  Lens.Family2.LensLike' f s a
expiresIn = Data.ProtoLens.Field.field @"expiresIn"
fork ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "fork" a) =>
  Lens.Family2.LensLike' f s a
fork = Data.ProtoLens.Field.field @"fork"
forkedAt ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "forkedAt" a) =>
  Lens.Family2.LensLike' f s a
forkedAt = Data.ProtoLens.Field.field @"forkedAt"
from ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "from" a) =>
  Lens.Family2.LensLike' f s a
from = Data.ProtoLens.Field.field @"from"
hierarchyVersion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "hierarchyVersion" a) =>
  Lens.Family2.LensLike' f s a
hierarchyVersion = Data.ProtoLens.Field.field @"hierarchyVersion"
idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
idempotencyKey = Data.ProtoLens.Field.field @"idempotencyKey"
ifTail ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "ifTail" a) =>
  Lens.Family2.LensLike' f s a
ifTail = Data.ProtoLens.Field.field @"ifTail"
limit ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "limit" a) =>
  Lens.Family2.LensLike' f s a
limit = Data.ProtoLens.Field.field @"limit"
maybe'absent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'absent" a) =>
  Lens.Family2.LensLike' f s a
maybe'absent = Data.ProtoLens.Field.field @"maybe'absent"
maybe'actual ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'actual" a) =>
  Lens.Family2.LensLike' f s a
maybe'actual = Data.ProtoLens.Field.field @"maybe'actual"
maybe'after ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'after" a) =>
  Lens.Family2.LensLike' f s a
maybe'after = Data.ProtoLens.Field.field @"maybe'after"
maybe'append ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'append" a) =>
  Lens.Family2.LensLike' f s a
maybe'append = Data.ProtoLens.Field.field @"maybe'append"
maybe'atTail ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'atTail" a) =>
  Lens.Family2.LensLike' f s a
maybe'atTail = Data.ProtoLens.Field.field @"maybe'atTail"
maybe'child ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'child" a) =>
  Lens.Family2.LensLike' f s a
maybe'child = Data.ProtoLens.Field.field @"maybe'child"
maybe'commit ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'commit" a) =>
  Lens.Family2.LensLike' f s a
maybe'commit = Data.ProtoLens.Field.field @"maybe'commit"
maybe'committed ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'committed" a) =>
  Lens.Family2.LensLike' f s a
maybe'committed = Data.ProtoLens.Field.field @"maybe'committed"
maybe'condition ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'condition" a) =>
  Lens.Family2.LensLike' f s a
maybe'condition = Data.ProtoLens.Field.field @"maybe'condition"
maybe'conflict ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'conflict" a) =>
  Lens.Family2.LensLike' f s a
maybe'conflict = Data.ProtoLens.Field.field @"maybe'conflict"
maybe'deadlineUnixMillis ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'deadlineUnixMillis" a) =>
  Lens.Family2.LensLike' f s a
maybe'deadlineUnixMillis
  = Data.ProtoLens.Field.field @"maybe'deadlineUnixMillis"
maybe'exists ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'exists" a) =>
  Lens.Family2.LensLike' f s a
maybe'exists = Data.ProtoLens.Field.field @"maybe'exists"
maybe'fork ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'fork" a) =>
  Lens.Family2.LensLike' f s a
maybe'fork = Data.ProtoLens.Field.field @"maybe'fork"
maybe'hierarchyVersion ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'hierarchyVersion" a) =>
  Lens.Family2.LensLike' f s a
maybe'hierarchyVersion
  = Data.ProtoLens.Field.field @"maybe'hierarchyVersion"
maybe'idempotencyKey ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'idempotencyKey" a) =>
  Lens.Family2.LensLike' f s a
maybe'idempotencyKey
  = Data.ProtoLens.Field.field @"maybe'idempotencyKey"
maybe'ifTail ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'ifTail" a) =>
  Lens.Family2.LensLike' f s a
maybe'ifTail = Data.ProtoLens.Field.field @"maybe'ifTail"
maybe'mutation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'mutation" a) =>
  Lens.Family2.LensLike' f s a
maybe'mutation = Data.ProtoLens.Field.field @"maybe'mutation"
maybe'nextAfter ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'nextAfter" a) =>
  Lens.Family2.LensLike' f s a
maybe'nextAfter = Data.ProtoLens.Field.field @"maybe'nextAfter"
maybe'observation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'observation" a) =>
  Lens.Family2.LensLike' f s a
maybe'observation = Data.ProtoLens.Field.field @"maybe'observation"
maybe'outcome ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'outcome" a) =>
  Lens.Family2.LensLike' f s a
maybe'outcome = Data.ProtoLens.Field.field @"maybe'outcome"
maybe'parent ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'parent" a) =>
  Lens.Family2.LensLike' f s a
maybe'parent = Data.ProtoLens.Field.field @"maybe'parent"
maybe'record ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'record" a) =>
  Lens.Family2.LensLike' f s a
maybe'record = Data.ProtoLens.Field.field @"maybe'record"
maybe'subtree ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'subtree" a) =>
  Lens.Family2.LensLike' f s a
maybe'subtree = Data.ProtoLens.Field.field @"maybe'subtree"
maybe'tail ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "maybe'tail" a) =>
  Lens.Family2.LensLike' f s a
maybe'tail = Data.ProtoLens.Field.field @"maybe'tail"
mutations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "mutations" a) =>
  Lens.Family2.LensLike' f s a
mutations = Data.ProtoLens.Field.field @"mutations"
nextAfter ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "nextAfter" a) =>
  Lens.Family2.LensLike' f s a
nextAfter = Data.ProtoLens.Field.field @"nextAfter"
observation ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "observation" a) =>
  Lens.Family2.LensLike' f s a
observation = Data.ProtoLens.Field.field @"observation"
operations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "operations" a) =>
  Lens.Family2.LensLike' f s a
operations = Data.ProtoLens.Field.field @"operations"
parent ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "parent" a) =>
  Lens.Family2.LensLike' f s a
parent = Data.ProtoLens.Field.field @"parent"
path ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "path" a) =>
  Lens.Family2.LensLike' f s a
path = Data.ProtoLens.Field.field @"path"
record ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "record" a) =>
  Lens.Family2.LensLike' f s a
record = Data.ProtoLens.Field.field @"record"
records ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "records" a) =>
  Lens.Family2.LensLike' f s a
records = Data.ProtoLens.Field.field @"records"
requestDigest ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "requestDigest" a) =>
  Lens.Family2.LensLike' f s a
requestDigest = Data.ProtoLens.Field.field @"requestDigest"
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
start ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "start" a) =>
  Lens.Family2.LensLike' f s a
start = Data.ProtoLens.Field.field @"start"
subtree ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "subtree" a) =>
  Lens.Family2.LensLike' f s a
subtree = Data.ProtoLens.Field.field @"subtree"
tail ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "tail" a) =>
  Lens.Family2.LensLike' f s a
tail = Data.ProtoLens.Field.field @"tail"
value ::
  forall f s a.
  (Prelude.Functor f, Data.ProtoLens.Field.HasField s "value" a) =>
  Lens.Family2.LensLike' f s a
value = Data.ProtoLens.Field.field @"value"
vec'allow ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'allow" a) =>
  Lens.Family2.LensLike' f s a
vec'allow = Data.ProtoLens.Field.field @"vec'allow"
vec'children ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'children" a) =>
  Lens.Family2.LensLike' f s a
vec'children = Data.ProtoLens.Field.field @"vec'children"
vec'conditions ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'conditions" a) =>
  Lens.Family2.LensLike' f s a
vec'conditions = Data.ProtoLens.Field.field @"vec'conditions"
vec'conflicts ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'conflicts" a) =>
  Lens.Family2.LensLike' f s a
vec'conflicts = Data.ProtoLens.Field.field @"vec'conflicts"
vec'mutations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'mutations" a) =>
  Lens.Family2.LensLike' f s a
vec'mutations = Data.ProtoLens.Field.field @"vec'mutations"
vec'operations ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'operations" a) =>
  Lens.Family2.LensLike' f s a
vec'operations = Data.ProtoLens.Field.field @"vec'operations"
vec'records ::
  forall f s a.
  (Prelude.Functor f,
   Data.ProtoLens.Field.HasField s "vec'records" a) =>
  Lens.Family2.LensLike' f s a
vec'records = Data.ProtoLens.Field.field @"vec'records"