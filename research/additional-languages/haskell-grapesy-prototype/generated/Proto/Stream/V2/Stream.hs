{- This file was auto-generated from stream/v2/stream.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Stream.V2.Stream (
        StreamService(..), AbsentCondition(), AppendMutation(),
        AppendReceipt(), AppendRequest(), AppendResponse(),
        AppendResponse'Outcome(..), _AppendResponse'Committed,
        _AppendResponse'Conflict, Child(), ChildrenPageRequest(),
        ChildrenPageResponse(), ChildrenRequest(), ChildrenResponse(),
        CommitCondition(), CommitCondition'Condition(..),
        _CommitCondition'Tail, _CommitCondition'Absent, CommitConflict(),
        CommitConflict'Conflict(..), _CommitConflict'Tail,
        _CommitConflict'Exists, CommitConflicts(), CommitMutation(),
        CommitMutation'Mutation(..), _CommitMutation'Append,
        _CommitMutation'Fork, CommitRequest(), CommitResponse(),
        CommitResponse'Outcome(..), _CommitResponse'Committed,
        _CommitResponse'Conflict, CommittedAppend(), CommittedEnvelope(),
        CommittedFork(), CommittedMutation(),
        CommittedMutation'Mutation(..), _CommittedMutation'Append,
        _CommittedMutation'Fork, CreateTokenRequest(),
        ExistsCommitConflict(), FollowRequest(), ForkMutation(),
        ForkReceipt(), ForkRequest(), IdempotencyObservation(),
        IdempotencyObservation'Outcome(..), _IdempotencyObservation'Append,
        _IdempotencyObservation'Fork, _IdempotencyObservation'Commit,
        InspectIdempotencyRequest(), InspectIdempotencyResponse(),
        ReadCommitRequest(), ReadRequest(), ReadResponse(), Record(),
        StreamLimit(..), StreamLimit(), StreamLimit'UnrecognizedValue,
        TailCommitConflict(), TailCondition(), TailConflict(),
        TailRequest(), TailResponse(), TokenGrant()
    ) where
import qualified Data.ProtoLens.Runtime.Control.DeepSeq as Control.DeepSeq
import qualified Data.ProtoLens.Runtime.Data.ProtoLens.Prism as Data.ProtoLens.Prism
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
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' AbsentCondition Data.Text.Text@ -}
data AbsentCondition
  = AbsentCondition'_constructor {_AbsentCondition'path :: !Data.Text.Text,
                                  _AbsentCondition'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AbsentCondition where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AbsentCondition "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbsentCondition'path
           (\ x__ y__ -> x__ {_AbsentCondition'path = y__}))
        Prelude.id
instance Data.ProtoLens.Message AbsentCondition where
  messageName _ = Data.Text.pack "acyclic.stream.v2.AbsentCondition"
  packedMessageDescriptor _
    = "\n\
      \\SIAbsentCondition\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor AbsentCondition
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, path__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AbsentCondition'_unknownFields
        (\ x__ y__ -> x__ {_AbsentCondition'_unknownFields = y__})
  defMessage
    = AbsentCondition'_constructor
        {_AbsentCondition'path = Data.ProtoLens.fieldDefault,
         _AbsentCondition'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AbsentCondition
          -> Data.ProtoLens.Encoding.Bytes.Parser AbsentCondition
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "AbsentCondition"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData AbsentCondition where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AbsentCondition'_unknownFields x__)
             (Control.DeepSeq.deepseq (_AbsentCondition'path x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' AppendMutation Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.records' @:: Lens' AppendMutation [Data.ByteString.ByteString]@
         * 'Proto.Stream.V2.Stream_Fields.vec'records' @:: Lens' AppendMutation (Data.Vector.Vector Data.ByteString.ByteString)@ -}
data AppendMutation
  = AppendMutation'_constructor {_AppendMutation'path :: !Data.Text.Text,
                                 _AppendMutation'records :: !(Data.Vector.Vector Data.ByteString.ByteString),
                                 _AppendMutation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AppendMutation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AppendMutation "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendMutation'path
           (\ x__ y__ -> x__ {_AppendMutation'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendMutation "records" [Data.ByteString.ByteString] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendMutation'records
           (\ x__ y__ -> x__ {_AppendMutation'records = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField AppendMutation "vec'records" (Data.Vector.Vector Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendMutation'records
           (\ x__ y__ -> x__ {_AppendMutation'records = y__}))
        Prelude.id
instance Data.ProtoLens.Message AppendMutation where
  messageName _ = Data.Text.pack "acyclic.stream.v2.AppendMutation"
  packedMessageDescriptor _
    = "\n\
      \\SOAppendMutation\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\CAN\n\
      \\arecords\CAN\STX \ETX(\fR\arecords"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor AppendMutation
        records__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "records"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"records")) ::
              Data.ProtoLens.FieldDescriptor AppendMutation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, records__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AppendMutation'_unknownFields
        (\ x__ y__ -> x__ {_AppendMutation'_unknownFields = y__})
  defMessage
    = AppendMutation'_constructor
        {_AppendMutation'path = Data.ProtoLens.fieldDefault,
         _AppendMutation'records = Data.Vector.Generic.empty,
         _AppendMutation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AppendMutation
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.ByteString.ByteString
             -> Data.ProtoLens.Encoding.Bytes.Parser AppendMutation
        loop x mutable'records
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'records)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'records") frozen'records x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                                  mutable'records
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getBytes
                                              (Prelude.fromIntegral len))
                                        "records"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'records y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'records
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'records)
          "AppendMutation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                   (\ _v
                      -> (Data.Monoid.<>)
                           (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                           ((\ bs
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                       (Prelude.fromIntegral (Data.ByteString.length bs)))
                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                              _v))
                   (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'records") _x))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData AppendMutation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AppendMutation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_AppendMutation'path x__)
                (Control.DeepSeq.deepseq (_AppendMutation'records x__) ()))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.start' @:: Lens' AppendReceipt Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.end' @:: Lens' AppendReceipt Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' AppendReceipt Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.commitId' @:: Lens' AppendReceipt Data.ByteString.ByteString@ -}
data AppendReceipt
  = AppendReceipt'_constructor {_AppendReceipt'start :: !Data.Word.Word64,
                                _AppendReceipt'end :: !Data.Word.Word64,
                                _AppendReceipt'tail :: !Data.Word.Word64,
                                _AppendReceipt'commitId :: !Data.ByteString.ByteString,
                                _AppendReceipt'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AppendReceipt where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AppendReceipt "start" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendReceipt'start
           (\ x__ y__ -> x__ {_AppendReceipt'start = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendReceipt "end" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendReceipt'end (\ x__ y__ -> x__ {_AppendReceipt'end = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendReceipt "tail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendReceipt'tail (\ x__ y__ -> x__ {_AppendReceipt'tail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendReceipt "commitId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendReceipt'commitId
           (\ x__ y__ -> x__ {_AppendReceipt'commitId = y__}))
        Prelude.id
instance Data.ProtoLens.Message AppendReceipt where
  messageName _ = Data.Text.pack "acyclic.stream.v2.AppendReceipt"
  packedMessageDescriptor _
    = "\n\
      \\rAppendReceipt\DC2\DC4\n\
      \\ENQstart\CAN\SOH \SOH(\EOTR\ENQstart\DC2\DLE\n\
      \\ETXend\CAN\STX \SOH(\EOTR\ETXend\DC2\DC2\n\
      \\EOTtail\CAN\ETX \SOH(\EOTR\EOTtail\DC2\ESC\n\
      \\tcommit_id\CAN\EOT \SOH(\fR\bcommitId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        start__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "start"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"start")) ::
              Data.ProtoLens.FieldDescriptor AppendReceipt
        end__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "end"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"end")) ::
              Data.ProtoLens.FieldDescriptor AppendReceipt
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"tail")) ::
              Data.ProtoLens.FieldDescriptor AppendReceipt
        commitId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commit_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitId")) ::
              Data.ProtoLens.FieldDescriptor AppendReceipt
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, start__field_descriptor),
           (Data.ProtoLens.Tag 2, end__field_descriptor),
           (Data.ProtoLens.Tag 3, tail__field_descriptor),
           (Data.ProtoLens.Tag 4, commitId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AppendReceipt'_unknownFields
        (\ x__ y__ -> x__ {_AppendReceipt'_unknownFields = y__})
  defMessage
    = AppendReceipt'_constructor
        {_AppendReceipt'start = Data.ProtoLens.fieldDefault,
         _AppendReceipt'end = Data.ProtoLens.fieldDefault,
         _AppendReceipt'tail = Data.ProtoLens.fieldDefault,
         _AppendReceipt'commitId = Data.ProtoLens.fieldDefault,
         _AppendReceipt'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AppendReceipt -> Data.ProtoLens.Encoding.Bytes.Parser AppendReceipt
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        8 -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "start"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"start") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "end"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"end") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "tail"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commit_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "AppendReceipt"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"start") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
             ((Data.Monoid.<>)
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"end") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"tail") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"commitId") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                               ((\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                  _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData AppendReceipt where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AppendReceipt'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_AppendReceipt'start x__)
                (Control.DeepSeq.deepseq
                   (_AppendReceipt'end x__)
                   (Control.DeepSeq.deepseq
                      (_AppendReceipt'tail x__)
                      (Control.DeepSeq.deepseq (_AppendReceipt'commitId x__) ()))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' AppendRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.records' @:: Lens' AppendRequest [Data.ByteString.ByteString]@
         * 'Proto.Stream.V2.Stream_Fields.vec'records' @:: Lens' AppendRequest (Data.Vector.Vector Data.ByteString.ByteString)@
         * 'Proto.Stream.V2.Stream_Fields.ifTail' @:: Lens' AppendRequest Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.maybe'ifTail' @:: Lens' AppendRequest (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Stream.V2.Stream_Fields.idempotencyKey' @:: Lens' AppendRequest Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.maybe'idempotencyKey' @:: Lens' AppendRequest (Prelude.Maybe Data.ByteString.ByteString)@ -}
data AppendRequest
  = AppendRequest'_constructor {_AppendRequest'path :: !Data.Text.Text,
                                _AppendRequest'records :: !(Data.Vector.Vector Data.ByteString.ByteString),
                                _AppendRequest'ifTail :: !(Prelude.Maybe Data.Word.Word64),
                                _AppendRequest'idempotencyKey :: !(Prelude.Maybe Data.ByteString.ByteString),
                                _AppendRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AppendRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AppendRequest "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'path (\ x__ y__ -> x__ {_AppendRequest'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendRequest "records" [Data.ByteString.ByteString] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'records
           (\ x__ y__ -> x__ {_AppendRequest'records = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField AppendRequest "vec'records" (Data.Vector.Vector Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'records
           (\ x__ y__ -> x__ {_AppendRequest'records = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendRequest "ifTail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'ifTail
           (\ x__ y__ -> x__ {_AppendRequest'ifTail = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField AppendRequest "maybe'ifTail" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'ifTail
           (\ x__ y__ -> x__ {_AppendRequest'ifTail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendRequest "idempotencyKey" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'idempotencyKey
           (\ x__ y__ -> x__ {_AppendRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField AppendRequest "maybe'idempotencyKey" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendRequest'idempotencyKey
           (\ x__ y__ -> x__ {_AppendRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message AppendRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.AppendRequest"
  packedMessageDescriptor _
    = "\n\
      \\rAppendRequest\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\CAN\n\
      \\arecords\CAN\STX \ETX(\fR\arecords\DC2\FS\n\
      \\aif_tail\CAN\ETX \SOH(\EOTH\NULR\ACKifTail\136\SOH\SOH\DC2,\n\
      \\SIidempotency_key\CAN\EOT \SOH(\fH\SOHR\SOidempotencyKey\136\SOH\SOHB\n\
      \\n\
      \\b_if_tailB\DC2\n\
      \\DLE_idempotency_key"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor AppendRequest
        records__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "records"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"records")) ::
              Data.ProtoLens.FieldDescriptor AppendRequest
        ifTail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'ifTail")) ::
              Data.ProtoLens.FieldDescriptor AppendRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor AppendRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, records__field_descriptor),
           (Data.ProtoLens.Tag 3, ifTail__field_descriptor),
           (Data.ProtoLens.Tag 4, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AppendRequest'_unknownFields
        (\ x__ y__ -> x__ {_AppendRequest'_unknownFields = y__})
  defMessage
    = AppendRequest'_constructor
        {_AppendRequest'path = Data.ProtoLens.fieldDefault,
         _AppendRequest'records = Data.Vector.Generic.empty,
         _AppendRequest'ifTail = Prelude.Nothing,
         _AppendRequest'idempotencyKey = Prelude.Nothing,
         _AppendRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AppendRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.ByteString.ByteString
             -> Data.ProtoLens.Encoding.Bytes.Parser AppendRequest
        loop x mutable'records
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'records)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'records") frozen'records x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                                  mutable'records
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getBytes
                                              (Prelude.fromIntegral len))
                                        "records"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'records y)
                                loop x v
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "if_tail"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"ifTail") y x)
                                  mutable'records
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                                  mutable'records
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'records
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'records)
          "AppendRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                   (\ _v
                      -> (Data.Monoid.<>)
                           (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                           ((\ bs
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                       (Prelude.fromIntegral (Data.ByteString.length bs)))
                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                              _v))
                   (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'records") _x))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'ifTail") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                ((\ bs
                                    -> (Data.Monoid.<>)
                                         (Data.ProtoLens.Encoding.Bytes.putVarInt
                                            (Prelude.fromIntegral (Data.ByteString.length bs)))
                                         (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                   _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData AppendRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AppendRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_AppendRequest'path x__)
                (Control.DeepSeq.deepseq
                   (_AppendRequest'records x__)
                   (Control.DeepSeq.deepseq
                      (_AppendRequest'ifTail x__)
                      (Control.DeepSeq.deepseq (_AppendRequest'idempotencyKey x__) ()))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.maybe'outcome' @:: Lens' AppendResponse (Prelude.Maybe AppendResponse'Outcome)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'committed' @:: Lens' AppendResponse (Prelude.Maybe AppendReceipt)@
         * 'Proto.Stream.V2.Stream_Fields.committed' @:: Lens' AppendResponse AppendReceipt@
         * 'Proto.Stream.V2.Stream_Fields.maybe'conflict' @:: Lens' AppendResponse (Prelude.Maybe TailConflict)@
         * 'Proto.Stream.V2.Stream_Fields.conflict' @:: Lens' AppendResponse TailConflict@ -}
data AppendResponse
  = AppendResponse'_constructor {_AppendResponse'outcome :: !(Prelude.Maybe AppendResponse'Outcome),
                                 _AppendResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AppendResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data AppendResponse'Outcome
  = AppendResponse'Committed !AppendReceipt |
    AppendResponse'Conflict !TailConflict
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField AppendResponse "maybe'outcome" (Prelude.Maybe AppendResponse'Outcome) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendResponse'outcome
           (\ x__ y__ -> x__ {_AppendResponse'outcome = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AppendResponse "maybe'committed" (Prelude.Maybe AppendReceipt) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendResponse'outcome
           (\ x__ y__ -> x__ {_AppendResponse'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (AppendResponse'Committed x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap AppendResponse'Committed y__))
instance Data.ProtoLens.Field.HasField AppendResponse "committed" AppendReceipt where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendResponse'outcome
           (\ x__ y__ -> x__ {_AppendResponse'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (AppendResponse'Committed x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap AppendResponse'Committed y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField AppendResponse "maybe'conflict" (Prelude.Maybe TailConflict) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendResponse'outcome
           (\ x__ y__ -> x__ {_AppendResponse'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (AppendResponse'Conflict x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap AppendResponse'Conflict y__))
instance Data.ProtoLens.Field.HasField AppendResponse "conflict" TailConflict where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AppendResponse'outcome
           (\ x__ y__ -> x__ {_AppendResponse'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (AppendResponse'Conflict x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap AppendResponse'Conflict y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message AppendResponse where
  messageName _ = Data.Text.pack "acyclic.stream.v2.AppendResponse"
  packedMessageDescriptor _
    = "\n\
      \\SOAppendResponse\DC2@\n\
      \\tcommitted\CAN\SOH \SOH(\v2 .acyclic.stream.v2.AppendReceiptH\NULR\tcommitted\DC2=\n\
      \\bconflict\CAN\STX \SOH(\v2\US.acyclic.stream.v2.TailConflictH\NULR\bconflictB\t\n\
      \\aoutcome"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        committed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "committed"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor AppendReceipt)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'committed")) ::
              Data.ProtoLens.FieldDescriptor AppendResponse
        conflict__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "conflict"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor TailConflict)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'conflict")) ::
              Data.ProtoLens.FieldDescriptor AppendResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, committed__field_descriptor),
           (Data.ProtoLens.Tag 2, conflict__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AppendResponse'_unknownFields
        (\ x__ y__ -> x__ {_AppendResponse'_unknownFields = y__})
  defMessage
    = AppendResponse'_constructor
        {_AppendResponse'outcome = Prelude.Nothing,
         _AppendResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AppendResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser AppendResponse
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "committed"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"committed") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "conflict"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"conflict") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "AppendResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'outcome") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (AppendResponse'Committed v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (AppendResponse'Conflict v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData AppendResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AppendResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_AppendResponse'outcome x__) ())
instance Control.DeepSeq.NFData AppendResponse'Outcome where
  rnf (AppendResponse'Committed x__) = Control.DeepSeq.rnf x__
  rnf (AppendResponse'Conflict x__) = Control.DeepSeq.rnf x__
_AppendResponse'Committed ::
  Data.ProtoLens.Prism.Prism' AppendResponse'Outcome AppendReceipt
_AppendResponse'Committed
  = Data.ProtoLens.Prism.prism'
      AppendResponse'Committed
      (\ p__
         -> case p__ of
              (AppendResponse'Committed p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_AppendResponse'Conflict ::
  Data.ProtoLens.Prism.Prism' AppendResponse'Outcome TailConflict
_AppendResponse'Conflict
  = Data.ProtoLens.Prism.prism'
      AppendResponse'Conflict
      (\ p__
         -> case p__ of
              (AppendResponse'Conflict p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' Child Data.Text.Text@ -}
data Child
  = Child'_constructor {_Child'path :: !Data.Text.Text,
                        _Child'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Child where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Child "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Child'path (\ x__ y__ -> x__ {_Child'path = y__}))
        Prelude.id
instance Data.ProtoLens.Message Child where
  messageName _ = Data.Text.pack "acyclic.stream.v2.Child"
  packedMessageDescriptor _
    = "\n\
      \\ENQChild\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor Child
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, path__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Child'_unknownFields
        (\ x__ y__ -> x__ {_Child'_unknownFields = y__})
  defMessage
    = Child'_constructor
        {_Child'path = Data.ProtoLens.fieldDefault,
         _Child'_unknownFields = []}
  parseMessage
    = let
        loop :: Child -> Data.ProtoLens.Encoding.Bytes.Parser Child
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Child"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData Child where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Child'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Child'path x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.parent' @:: Lens' ChildrenPageRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.maybe'parent' @:: Lens' ChildrenPageRequest (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Stream.V2.Stream_Fields.after' @:: Lens' ChildrenPageRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.maybe'after' @:: Lens' ChildrenPageRequest (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Stream.V2.Stream_Fields.hierarchyVersion' @:: Lens' ChildrenPageRequest Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.maybe'hierarchyVersion' @:: Lens' ChildrenPageRequest (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Stream.V2.Stream_Fields.limit' @:: Lens' ChildrenPageRequest Data.Word.Word32@ -}
data ChildrenPageRequest
  = ChildrenPageRequest'_constructor {_ChildrenPageRequest'parent :: !(Prelude.Maybe Data.Text.Text),
                                      _ChildrenPageRequest'after :: !(Prelude.Maybe Data.Text.Text),
                                      _ChildrenPageRequest'hierarchyVersion :: !(Prelude.Maybe Data.ByteString.ByteString),
                                      _ChildrenPageRequest'limit :: !Data.Word.Word32,
                                      _ChildrenPageRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ChildrenPageRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "parent" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'parent
           (\ x__ y__ -> x__ {_ChildrenPageRequest'parent = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "maybe'parent" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'parent
           (\ x__ y__ -> x__ {_ChildrenPageRequest'parent = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "after" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'after
           (\ x__ y__ -> x__ {_ChildrenPageRequest'after = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "maybe'after" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'after
           (\ x__ y__ -> x__ {_ChildrenPageRequest'after = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "hierarchyVersion" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'hierarchyVersion
           (\ x__ y__ -> x__ {_ChildrenPageRequest'hierarchyVersion = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "maybe'hierarchyVersion" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'hierarchyVersion
           (\ x__ y__ -> x__ {_ChildrenPageRequest'hierarchyVersion = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ChildrenPageRequest "limit" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageRequest'limit
           (\ x__ y__ -> x__ {_ChildrenPageRequest'limit = y__}))
        Prelude.id
instance Data.ProtoLens.Message ChildrenPageRequest where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.ChildrenPageRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3ChildrenPageRequest\DC2\ESC\n\
      \\ACKparent\CAN\SOH \SOH(\tH\NULR\ACKparent\136\SOH\SOH\DC2\EM\n\
      \\ENQafter\CAN\STX \SOH(\tH\SOHR\ENQafter\136\SOH\SOH\DC20\n\
      \\DC1hierarchy_version\CAN\ETX \SOH(\fH\STXR\DLEhierarchyVersion\136\SOH\SOH\DC2\DC4\n\
      \\ENQlimit\CAN\EOT \SOH(\rR\ENQlimitB\t\n\
      \\a_parentB\b\n\
      \\ACK_afterB\DC4\n\
      \\DC2_hierarchy_version"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        parent__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "parent"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'parent")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageRequest
        after__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "after"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'after")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageRequest
        hierarchyVersion__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "hierarchy_version"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'hierarchyVersion")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageRequest
        limit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limit"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"limit")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, parent__field_descriptor),
           (Data.ProtoLens.Tag 2, after__field_descriptor),
           (Data.ProtoLens.Tag 3, hierarchyVersion__field_descriptor),
           (Data.ProtoLens.Tag 4, limit__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ChildrenPageRequest'_unknownFields
        (\ x__ y__ -> x__ {_ChildrenPageRequest'_unknownFields = y__})
  defMessage
    = ChildrenPageRequest'_constructor
        {_ChildrenPageRequest'parent = Prelude.Nothing,
         _ChildrenPageRequest'after = Prelude.Nothing,
         _ChildrenPageRequest'hierarchyVersion = Prelude.Nothing,
         _ChildrenPageRequest'limit = Data.ProtoLens.fieldDefault,
         _ChildrenPageRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ChildrenPageRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ChildrenPageRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "parent"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"parent") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "after"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"after") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "hierarchy_version"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"hierarchyVersion") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "limit"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"limit") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ChildrenPageRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'parent") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just _v)
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'after") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just _v)
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.Text.Encoding.encodeUtf8 _v))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'hierarchyVersion") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                             ((\ bs
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt
                                         (Prelude.fromIntegral (Data.ByteString.length bs)))
                                      (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                _v))
                   ((Data.Monoid.<>)
                      (let
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"limit") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData ChildrenPageRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ChildrenPageRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ChildrenPageRequest'parent x__)
                (Control.DeepSeq.deepseq
                   (_ChildrenPageRequest'after x__)
                   (Control.DeepSeq.deepseq
                      (_ChildrenPageRequest'hierarchyVersion x__)
                      (Control.DeepSeq.deepseq (_ChildrenPageRequest'limit x__) ()))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.hierarchyVersion' @:: Lens' ChildrenPageResponse Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.children' @:: Lens' ChildrenPageResponse [Child]@
         * 'Proto.Stream.V2.Stream_Fields.vec'children' @:: Lens' ChildrenPageResponse (Data.Vector.Vector Child)@
         * 'Proto.Stream.V2.Stream_Fields.nextAfter' @:: Lens' ChildrenPageResponse Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.maybe'nextAfter' @:: Lens' ChildrenPageResponse (Prelude.Maybe Data.Text.Text)@ -}
data ChildrenPageResponse
  = ChildrenPageResponse'_constructor {_ChildrenPageResponse'hierarchyVersion :: !Data.ByteString.ByteString,
                                       _ChildrenPageResponse'children :: !(Data.Vector.Vector Child),
                                       _ChildrenPageResponse'nextAfter :: !(Prelude.Maybe Data.Text.Text),
                                       _ChildrenPageResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ChildrenPageResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ChildrenPageResponse "hierarchyVersion" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageResponse'hierarchyVersion
           (\ x__ y__ -> x__ {_ChildrenPageResponse'hierarchyVersion = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ChildrenPageResponse "children" [Child] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageResponse'children
           (\ x__ y__ -> x__ {_ChildrenPageResponse'children = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ChildrenPageResponse "vec'children" (Data.Vector.Vector Child) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageResponse'children
           (\ x__ y__ -> x__ {_ChildrenPageResponse'children = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ChildrenPageResponse "nextAfter" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageResponse'nextAfter
           (\ x__ y__ -> x__ {_ChildrenPageResponse'nextAfter = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ChildrenPageResponse "maybe'nextAfter" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenPageResponse'nextAfter
           (\ x__ y__ -> x__ {_ChildrenPageResponse'nextAfter = y__}))
        Prelude.id
instance Data.ProtoLens.Message ChildrenPageResponse where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.ChildrenPageResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC4ChildrenPageResponse\DC2+\n\
      \\DC1hierarchy_version\CAN\SOH \SOH(\fR\DLEhierarchyVersion\DC24\n\
      \\bchildren\CAN\STX \ETX(\v2\CAN.acyclic.stream.v2.ChildR\bchildren\DC2\"\n\
      \\n\
      \next_after\CAN\ETX \SOH(\tH\NULR\tnextAfter\136\SOH\SOHB\r\n\
      \\v_next_after"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        hierarchyVersion__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "hierarchy_version"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"hierarchyVersion")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageResponse
        children__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "children"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Child)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"children")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageResponse
        nextAfter__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "next_after"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'nextAfter")) ::
              Data.ProtoLens.FieldDescriptor ChildrenPageResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, hierarchyVersion__field_descriptor),
           (Data.ProtoLens.Tag 2, children__field_descriptor),
           (Data.ProtoLens.Tag 3, nextAfter__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ChildrenPageResponse'_unknownFields
        (\ x__ y__ -> x__ {_ChildrenPageResponse'_unknownFields = y__})
  defMessage
    = ChildrenPageResponse'_constructor
        {_ChildrenPageResponse'hierarchyVersion = Data.ProtoLens.fieldDefault,
         _ChildrenPageResponse'children = Data.Vector.Generic.empty,
         _ChildrenPageResponse'nextAfter = Prelude.Nothing,
         _ChildrenPageResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ChildrenPageResponse
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Child
             -> Data.ProtoLens.Encoding.Bytes.Parser ChildrenPageResponse
        loop x mutable'children
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'children <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'children)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'children") frozen'children x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "hierarchy_version"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"hierarchyVersion") y x)
                                  mutable'children
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "children"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'children y)
                                loop x v
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "next_after"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"nextAfter") y x)
                                  mutable'children
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'children
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'children <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'children)
          "ChildrenPageResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"hierarchyVersion") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             ((Data.Monoid.<>)
                (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                   (\ _v
                      -> (Data.Monoid.<>)
                           (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                           ((Prelude..)
                              (\ bs
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt
                                         (Prelude.fromIntegral (Data.ByteString.length bs)))
                                      (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                              Data.ProtoLens.encodeMessage _v))
                   (Lens.Family2.view
                      (Data.ProtoLens.Field.field @"vec'children") _x))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'nextAfter") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.Text.Encoding.encodeUtf8 _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ChildrenPageResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ChildrenPageResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ChildrenPageResponse'hierarchyVersion x__)
                (Control.DeepSeq.deepseq
                   (_ChildrenPageResponse'children x__)
                   (Control.DeepSeq.deepseq
                      (_ChildrenPageResponse'nextAfter x__) ())))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.parent' @:: Lens' ChildrenRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.maybe'parent' @:: Lens' ChildrenRequest (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Stream.V2.Stream_Fields.limit' @:: Lens' ChildrenRequest Data.Word.Word32@ -}
data ChildrenRequest
  = ChildrenRequest'_constructor {_ChildrenRequest'parent :: !(Prelude.Maybe Data.Text.Text),
                                  _ChildrenRequest'limit :: !Data.Word.Word32,
                                  _ChildrenRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ChildrenRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ChildrenRequest "parent" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenRequest'parent
           (\ x__ y__ -> x__ {_ChildrenRequest'parent = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ChildrenRequest "maybe'parent" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenRequest'parent
           (\ x__ y__ -> x__ {_ChildrenRequest'parent = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ChildrenRequest "limit" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenRequest'limit
           (\ x__ y__ -> x__ {_ChildrenRequest'limit = y__}))
        Prelude.id
instance Data.ProtoLens.Message ChildrenRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ChildrenRequest"
  packedMessageDescriptor _
    = "\n\
      \\SIChildrenRequest\DC2\ESC\n\
      \\ACKparent\CAN\SOH \SOH(\tH\NULR\ACKparent\136\SOH\SOH\DC2\DC4\n\
      \\ENQlimit\CAN\STX \SOH(\rR\ENQlimitB\t\n\
      \\a_parent"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        parent__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "parent"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'parent")) ::
              Data.ProtoLens.FieldDescriptor ChildrenRequest
        limit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limit"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"limit")) ::
              Data.ProtoLens.FieldDescriptor ChildrenRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, parent__field_descriptor),
           (Data.ProtoLens.Tag 2, limit__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ChildrenRequest'_unknownFields
        (\ x__ y__ -> x__ {_ChildrenRequest'_unknownFields = y__})
  defMessage
    = ChildrenRequest'_constructor
        {_ChildrenRequest'parent = Prelude.Nothing,
         _ChildrenRequest'limit = Data.ProtoLens.fieldDefault,
         _ChildrenRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ChildrenRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ChildrenRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "parent"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"parent") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "limit"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"limit") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ChildrenRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'parent") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just _v)
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"limit") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         ((Prelude..)
                            Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ChildrenRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ChildrenRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ChildrenRequest'parent x__)
                (Control.DeepSeq.deepseq (_ChildrenRequest'limit x__) ()))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.child' @:: Lens' ChildrenResponse Child@
         * 'Proto.Stream.V2.Stream_Fields.maybe'child' @:: Lens' ChildrenResponse (Prelude.Maybe Child)@ -}
data ChildrenResponse
  = ChildrenResponse'_constructor {_ChildrenResponse'child :: !(Prelude.Maybe Child),
                                   _ChildrenResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ChildrenResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ChildrenResponse "child" Child where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenResponse'child
           (\ x__ y__ -> x__ {_ChildrenResponse'child = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ChildrenResponse "maybe'child" (Prelude.Maybe Child) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ChildrenResponse'child
           (\ x__ y__ -> x__ {_ChildrenResponse'child = y__}))
        Prelude.id
instance Data.ProtoLens.Message ChildrenResponse where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ChildrenResponse"
  packedMessageDescriptor _
    = "\n\
      \\DLEChildrenResponse\DC2.\n\
      \\ENQchild\CAN\SOH \SOH(\v2\CAN.acyclic.stream.v2.ChildR\ENQchild"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        child__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "child"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Child)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'child")) ::
              Data.ProtoLens.FieldDescriptor ChildrenResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, child__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ChildrenResponse'_unknownFields
        (\ x__ y__ -> x__ {_ChildrenResponse'_unknownFields = y__})
  defMessage
    = ChildrenResponse'_constructor
        {_ChildrenResponse'child = Prelude.Nothing,
         _ChildrenResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ChildrenResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser ChildrenResponse
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "child"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"child") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ChildrenResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'child") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just _v)
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ChildrenResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ChildrenResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ChildrenResponse'child x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.maybe'condition' @:: Lens' CommitCondition (Prelude.Maybe CommitCondition'Condition)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'tail' @:: Lens' CommitCondition (Prelude.Maybe TailCondition)@
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' CommitCondition TailCondition@
         * 'Proto.Stream.V2.Stream_Fields.maybe'absent' @:: Lens' CommitCondition (Prelude.Maybe AbsentCondition)@
         * 'Proto.Stream.V2.Stream_Fields.absent' @:: Lens' CommitCondition AbsentCondition@ -}
data CommitCondition
  = CommitCondition'_constructor {_CommitCondition'condition :: !(Prelude.Maybe CommitCondition'Condition),
                                  _CommitCondition'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommitCondition where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data CommitCondition'Condition
  = CommitCondition'Tail !TailCondition |
    CommitCondition'Absent !AbsentCondition
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField CommitCondition "maybe'condition" (Prelude.Maybe CommitCondition'Condition) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitCondition'condition
           (\ x__ y__ -> x__ {_CommitCondition'condition = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitCondition "maybe'tail" (Prelude.Maybe TailCondition) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitCondition'condition
           (\ x__ y__ -> x__ {_CommitCondition'condition = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitCondition'Tail x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitCondition'Tail y__))
instance Data.ProtoLens.Field.HasField CommitCondition "tail" TailCondition where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitCondition'condition
           (\ x__ y__ -> x__ {_CommitCondition'condition = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitCondition'Tail x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitCondition'Tail y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField CommitCondition "maybe'absent" (Prelude.Maybe AbsentCondition) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitCondition'condition
           (\ x__ y__ -> x__ {_CommitCondition'condition = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitCondition'Absent x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitCondition'Absent y__))
instance Data.ProtoLens.Field.HasField CommitCondition "absent" AbsentCondition where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitCondition'condition
           (\ x__ y__ -> x__ {_CommitCondition'condition = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitCondition'Absent x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitCondition'Absent y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message CommitCondition where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommitCondition"
  packedMessageDescriptor _
    = "\n\
      \\SICommitCondition\DC26\n\
      \\EOTtail\CAN\SOH \SOH(\v2 .acyclic.stream.v2.TailConditionH\NULR\EOTtail\DC2<\n\
      \\ACKabsent\CAN\STX \SOH(\v2\".acyclic.stream.v2.AbsentConditionH\NULR\ACKabsentB\v\n\
      \\tcondition"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor TailCondition)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'tail")) ::
              Data.ProtoLens.FieldDescriptor CommitCondition
        absent__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "absent"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor AbsentCondition)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'absent")) ::
              Data.ProtoLens.FieldDescriptor CommitCondition
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, tail__field_descriptor),
           (Data.ProtoLens.Tag 2, absent__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommitCondition'_unknownFields
        (\ x__ y__ -> x__ {_CommitCondition'_unknownFields = y__})
  defMessage
    = CommitCondition'_constructor
        {_CommitCondition'condition = Prelude.Nothing,
         _CommitCondition'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommitCondition
          -> Data.ProtoLens.Encoding.Bytes.Parser CommitCondition
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "tail"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "absent"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"absent") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CommitCondition"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'condition") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (CommitCondition'Tail v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (CommitCondition'Absent v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CommitCondition where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommitCondition'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CommitCondition'condition x__) ())
instance Control.DeepSeq.NFData CommitCondition'Condition where
  rnf (CommitCondition'Tail x__) = Control.DeepSeq.rnf x__
  rnf (CommitCondition'Absent x__) = Control.DeepSeq.rnf x__
_CommitCondition'Tail ::
  Data.ProtoLens.Prism.Prism' CommitCondition'Condition TailCondition
_CommitCondition'Tail
  = Data.ProtoLens.Prism.prism'
      CommitCondition'Tail
      (\ p__
         -> case p__ of
              (CommitCondition'Tail p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_CommitCondition'Absent ::
  Data.ProtoLens.Prism.Prism' CommitCondition'Condition AbsentCondition
_CommitCondition'Absent
  = Data.ProtoLens.Prism.prism'
      CommitCondition'Absent
      (\ p__
         -> case p__ of
              (CommitCondition'Absent p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.maybe'conflict' @:: Lens' CommitConflict (Prelude.Maybe CommitConflict'Conflict)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'tail' @:: Lens' CommitConflict (Prelude.Maybe TailCommitConflict)@
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' CommitConflict TailCommitConflict@
         * 'Proto.Stream.V2.Stream_Fields.maybe'exists' @:: Lens' CommitConflict (Prelude.Maybe ExistsCommitConflict)@
         * 'Proto.Stream.V2.Stream_Fields.exists' @:: Lens' CommitConflict ExistsCommitConflict@ -}
data CommitConflict
  = CommitConflict'_constructor {_CommitConflict'conflict :: !(Prelude.Maybe CommitConflict'Conflict),
                                 _CommitConflict'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommitConflict where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data CommitConflict'Conflict
  = CommitConflict'Tail !TailCommitConflict |
    CommitConflict'Exists !ExistsCommitConflict
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField CommitConflict "maybe'conflict" (Prelude.Maybe CommitConflict'Conflict) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflict'conflict
           (\ x__ y__ -> x__ {_CommitConflict'conflict = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitConflict "maybe'tail" (Prelude.Maybe TailCommitConflict) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflict'conflict
           (\ x__ y__ -> x__ {_CommitConflict'conflict = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitConflict'Tail x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitConflict'Tail y__))
instance Data.ProtoLens.Field.HasField CommitConflict "tail" TailCommitConflict where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflict'conflict
           (\ x__ y__ -> x__ {_CommitConflict'conflict = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitConflict'Tail x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitConflict'Tail y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField CommitConflict "maybe'exists" (Prelude.Maybe ExistsCommitConflict) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflict'conflict
           (\ x__ y__ -> x__ {_CommitConflict'conflict = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitConflict'Exists x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitConflict'Exists y__))
instance Data.ProtoLens.Field.HasField CommitConflict "exists" ExistsCommitConflict where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflict'conflict
           (\ x__ y__ -> x__ {_CommitConflict'conflict = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitConflict'Exists x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitConflict'Exists y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message CommitConflict where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommitConflict"
  packedMessageDescriptor _
    = "\n\
      \\SOCommitConflict\DC2;\n\
      \\EOTtail\CAN\SOH \SOH(\v2%.acyclic.stream.v2.TailCommitConflictH\NULR\EOTtail\DC2A\n\
      \\ACKexists\CAN\STX \SOH(\v2'.acyclic.stream.v2.ExistsCommitConflictH\NULR\ACKexistsB\n\
      \\n\
      \\bconflictJ\EOT\b\ETX\DLE\EOTR\aretired"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor TailCommitConflict)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'tail")) ::
              Data.ProtoLens.FieldDescriptor CommitConflict
        exists__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "exists"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ExistsCommitConflict)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'exists")) ::
              Data.ProtoLens.FieldDescriptor CommitConflict
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, tail__field_descriptor),
           (Data.ProtoLens.Tag 2, exists__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommitConflict'_unknownFields
        (\ x__ y__ -> x__ {_CommitConflict'_unknownFields = y__})
  defMessage
    = CommitConflict'_constructor
        {_CommitConflict'conflict = Prelude.Nothing,
         _CommitConflict'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommitConflict
          -> Data.ProtoLens.Encoding.Bytes.Parser CommitConflict
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "tail"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "exists"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"exists") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CommitConflict"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'conflict") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (CommitConflict'Tail v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (CommitConflict'Exists v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CommitConflict where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommitConflict'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CommitConflict'conflict x__) ())
instance Control.DeepSeq.NFData CommitConflict'Conflict where
  rnf (CommitConflict'Tail x__) = Control.DeepSeq.rnf x__
  rnf (CommitConflict'Exists x__) = Control.DeepSeq.rnf x__
_CommitConflict'Tail ::
  Data.ProtoLens.Prism.Prism' CommitConflict'Conflict TailCommitConflict
_CommitConflict'Tail
  = Data.ProtoLens.Prism.prism'
      CommitConflict'Tail
      (\ p__
         -> case p__ of
              (CommitConflict'Tail p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_CommitConflict'Exists ::
  Data.ProtoLens.Prism.Prism' CommitConflict'Conflict ExistsCommitConflict
_CommitConflict'Exists
  = Data.ProtoLens.Prism.prism'
      CommitConflict'Exists
      (\ p__
         -> case p__ of
              (CommitConflict'Exists p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.conflicts' @:: Lens' CommitConflicts [CommitConflict]@
         * 'Proto.Stream.V2.Stream_Fields.vec'conflicts' @:: Lens' CommitConflicts (Data.Vector.Vector CommitConflict)@ -}
data CommitConflicts
  = CommitConflicts'_constructor {_CommitConflicts'conflicts :: !(Data.Vector.Vector CommitConflict),
                                  _CommitConflicts'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommitConflicts where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CommitConflicts "conflicts" [CommitConflict] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflicts'conflicts
           (\ x__ y__ -> x__ {_CommitConflicts'conflicts = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CommitConflicts "vec'conflicts" (Data.Vector.Vector CommitConflict) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitConflicts'conflicts
           (\ x__ y__ -> x__ {_CommitConflicts'conflicts = y__}))
        Prelude.id
instance Data.ProtoLens.Message CommitConflicts where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommitConflicts"
  packedMessageDescriptor _
    = "\n\
      \\SICommitConflicts\DC2?\n\
      \\tconflicts\CAN\SOH \ETX(\v2!.acyclic.stream.v2.CommitConflictR\tconflicts"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        conflicts__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "conflicts"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommitConflict)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"conflicts")) ::
              Data.ProtoLens.FieldDescriptor CommitConflicts
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, conflicts__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommitConflicts'_unknownFields
        (\ x__ y__ -> x__ {_CommitConflicts'_unknownFields = y__})
  defMessage
    = CommitConflicts'_constructor
        {_CommitConflicts'conflicts = Data.Vector.Generic.empty,
         _CommitConflicts'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommitConflicts
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld CommitConflict
             -> Data.ProtoLens.Encoding.Bytes.Parser CommitConflicts
        loop x mutable'conflicts
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'conflicts <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                            (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                               mutable'conflicts)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'conflicts") frozen'conflicts x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "conflicts"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'conflicts y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'conflicts
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'conflicts <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                     Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'conflicts)
          "CommitConflicts"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                (\ _v
                   -> (Data.Monoid.<>)
                        (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                        ((Prelude..)
                           (\ bs
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                      (Prelude.fromIntegral (Data.ByteString.length bs)))
                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                           Data.ProtoLens.encodeMessage _v))
                (Lens.Family2.view
                   (Data.ProtoLens.Field.field @"vec'conflicts") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CommitConflicts where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommitConflicts'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CommitConflicts'conflicts x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.maybe'mutation' @:: Lens' CommitMutation (Prelude.Maybe CommitMutation'Mutation)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'append' @:: Lens' CommitMutation (Prelude.Maybe AppendMutation)@
         * 'Proto.Stream.V2.Stream_Fields.append' @:: Lens' CommitMutation AppendMutation@
         * 'Proto.Stream.V2.Stream_Fields.maybe'fork' @:: Lens' CommitMutation (Prelude.Maybe ForkMutation)@
         * 'Proto.Stream.V2.Stream_Fields.fork' @:: Lens' CommitMutation ForkMutation@ -}
data CommitMutation
  = CommitMutation'_constructor {_CommitMutation'mutation :: !(Prelude.Maybe CommitMutation'Mutation),
                                 _CommitMutation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommitMutation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data CommitMutation'Mutation
  = CommitMutation'Append !AppendMutation |
    CommitMutation'Fork !ForkMutation
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField CommitMutation "maybe'mutation" (Prelude.Maybe CommitMutation'Mutation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitMutation'mutation
           (\ x__ y__ -> x__ {_CommitMutation'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitMutation "maybe'append" (Prelude.Maybe AppendMutation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitMutation'mutation
           (\ x__ y__ -> x__ {_CommitMutation'mutation = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitMutation'Append x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitMutation'Append y__))
instance Data.ProtoLens.Field.HasField CommitMutation "append" AppendMutation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitMutation'mutation
           (\ x__ y__ -> x__ {_CommitMutation'mutation = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitMutation'Append x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitMutation'Append y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField CommitMutation "maybe'fork" (Prelude.Maybe ForkMutation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitMutation'mutation
           (\ x__ y__ -> x__ {_CommitMutation'mutation = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitMutation'Fork x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitMutation'Fork y__))
instance Data.ProtoLens.Field.HasField CommitMutation "fork" ForkMutation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitMutation'mutation
           (\ x__ y__ -> x__ {_CommitMutation'mutation = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitMutation'Fork x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitMutation'Fork y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message CommitMutation where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommitMutation"
  packedMessageDescriptor _
    = "\n\
      \\SOCommitMutation\DC2;\n\
      \\ACKappend\CAN\SOH \SOH(\v2!.acyclic.stream.v2.AppendMutationH\NULR\ACKappend\DC25\n\
      \\EOTfork\CAN\STX \SOH(\v2\US.acyclic.stream.v2.ForkMutationH\NULR\EOTforkB\n\
      \\n\
      \\bmutationJ\EOT\b\ETX\DLE\EOTJ\EOT\b\EOT\DLE\ENQR\EOTtrimR\ACKdelete"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        append__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "append"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor AppendMutation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'append")) ::
              Data.ProtoLens.FieldDescriptor CommitMutation
        fork__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fork"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ForkMutation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'fork")) ::
              Data.ProtoLens.FieldDescriptor CommitMutation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, append__field_descriptor),
           (Data.ProtoLens.Tag 2, fork__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommitMutation'_unknownFields
        (\ x__ y__ -> x__ {_CommitMutation'_unknownFields = y__})
  defMessage
    = CommitMutation'_constructor
        {_CommitMutation'mutation = Prelude.Nothing,
         _CommitMutation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommitMutation
          -> Data.ProtoLens.Encoding.Bytes.Parser CommitMutation
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "append"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"append") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "fork"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"fork") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CommitMutation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (CommitMutation'Append v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (CommitMutation'Fork v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CommitMutation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommitMutation'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CommitMutation'mutation x__) ())
instance Control.DeepSeq.NFData CommitMutation'Mutation where
  rnf (CommitMutation'Append x__) = Control.DeepSeq.rnf x__
  rnf (CommitMutation'Fork x__) = Control.DeepSeq.rnf x__
_CommitMutation'Append ::
  Data.ProtoLens.Prism.Prism' CommitMutation'Mutation AppendMutation
_CommitMutation'Append
  = Data.ProtoLens.Prism.prism'
      CommitMutation'Append
      (\ p__
         -> case p__ of
              (CommitMutation'Append p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_CommitMutation'Fork ::
  Data.ProtoLens.Prism.Prism' CommitMutation'Mutation ForkMutation
_CommitMutation'Fork
  = Data.ProtoLens.Prism.prism'
      CommitMutation'Fork
      (\ p__
         -> case p__ of
              (CommitMutation'Fork p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.conditions' @:: Lens' CommitRequest [CommitCondition]@
         * 'Proto.Stream.V2.Stream_Fields.vec'conditions' @:: Lens' CommitRequest (Data.Vector.Vector CommitCondition)@
         * 'Proto.Stream.V2.Stream_Fields.mutations' @:: Lens' CommitRequest [CommitMutation]@
         * 'Proto.Stream.V2.Stream_Fields.vec'mutations' @:: Lens' CommitRequest (Data.Vector.Vector CommitMutation)@
         * 'Proto.Stream.V2.Stream_Fields.idempotencyKey' @:: Lens' CommitRequest Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.deadlineUnixMillis' @:: Lens' CommitRequest Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.maybe'deadlineUnixMillis' @:: Lens' CommitRequest (Prelude.Maybe Data.Word.Word64)@ -}
data CommitRequest
  = CommitRequest'_constructor {_CommitRequest'conditions :: !(Data.Vector.Vector CommitCondition),
                                _CommitRequest'mutations :: !(Data.Vector.Vector CommitMutation),
                                _CommitRequest'idempotencyKey :: !Data.ByteString.ByteString,
                                _CommitRequest'deadlineUnixMillis :: !(Prelude.Maybe Data.Word.Word64),
                                _CommitRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommitRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CommitRequest "conditions" [CommitCondition] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'conditions
           (\ x__ y__ -> x__ {_CommitRequest'conditions = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CommitRequest "vec'conditions" (Data.Vector.Vector CommitCondition) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'conditions
           (\ x__ y__ -> x__ {_CommitRequest'conditions = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitRequest "mutations" [CommitMutation] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'mutations
           (\ x__ y__ -> x__ {_CommitRequest'mutations = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CommitRequest "vec'mutations" (Data.Vector.Vector CommitMutation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'mutations
           (\ x__ y__ -> x__ {_CommitRequest'mutations = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitRequest "idempotencyKey" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'idempotencyKey
           (\ x__ y__ -> x__ {_CommitRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitRequest "deadlineUnixMillis" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'deadlineUnixMillis
           (\ x__ y__ -> x__ {_CommitRequest'deadlineUnixMillis = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField CommitRequest "maybe'deadlineUnixMillis" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitRequest'deadlineUnixMillis
           (\ x__ y__ -> x__ {_CommitRequest'deadlineUnixMillis = y__}))
        Prelude.id
instance Data.ProtoLens.Message CommitRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommitRequest"
  packedMessageDescriptor _
    = "\n\
      \\rCommitRequest\DC2B\n\
      \\n\
      \conditions\CAN\SOH \ETX(\v2\".acyclic.stream.v2.CommitConditionR\n\
      \conditions\DC2?\n\
      \\tmutations\CAN\STX \ETX(\v2!.acyclic.stream.v2.CommitMutationR\tmutations\DC2'\n\
      \\SIidempotency_key\CAN\ETX \SOH(\fR\SOidempotencyKey\DC25\n\
      \\DC4deadline_unix_millis\CAN\EOT \SOH(\EOTH\NULR\DC2deadlineUnixMillis\136\SOH\SOHB\ETB\n\
      \\NAK_deadline_unix_millis"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        conditions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "conditions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommitCondition)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"conditions")) ::
              Data.ProtoLens.FieldDescriptor CommitRequest
        mutations__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutations"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommitMutation)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"mutations")) ::
              Data.ProtoLens.FieldDescriptor CommitRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CommitRequest
        deadlineUnixMillis__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "deadline_unix_millis"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'deadlineUnixMillis")) ::
              Data.ProtoLens.FieldDescriptor CommitRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, conditions__field_descriptor),
           (Data.ProtoLens.Tag 2, mutations__field_descriptor),
           (Data.ProtoLens.Tag 3, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 4, deadlineUnixMillis__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommitRequest'_unknownFields
        (\ x__ y__ -> x__ {_CommitRequest'_unknownFields = y__})
  defMessage
    = CommitRequest'_constructor
        {_CommitRequest'conditions = Data.Vector.Generic.empty,
         _CommitRequest'mutations = Data.Vector.Generic.empty,
         _CommitRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _CommitRequest'deadlineUnixMillis = Prelude.Nothing,
         _CommitRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommitRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld CommitCondition
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld CommitMutation
                -> Data.ProtoLens.Encoding.Bytes.Parser CommitRequest
        loop x mutable'conditions mutable'mutations
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'conditions <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                             (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                mutable'conditions)
                      frozen'mutations <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                            (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                               mutable'mutations)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'conditions") frozen'conditions
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'mutations") frozen'mutations x)))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "conditions"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'conditions y)
                                loop x v mutable'mutations
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "mutations"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'mutations y)
                                loop x mutable'conditions v
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                                  mutable'conditions mutable'mutations
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "deadline_unix_millis"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"deadlineUnixMillis") y x)
                                  mutable'conditions mutable'mutations
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'conditions mutable'mutations
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'conditions <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                      Data.ProtoLens.Encoding.Growing.new
              mutable'mutations <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                     Data.ProtoLens.Encoding.Growing.new
              loop
                Data.ProtoLens.defMessage mutable'conditions mutable'mutations)
          "CommitRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                (\ _v
                   -> (Data.Monoid.<>)
                        (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                        ((Prelude..)
                           (\ bs
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                      (Prelude.fromIntegral (Data.ByteString.length bs)))
                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                           Data.ProtoLens.encodeMessage _v))
                (Lens.Family2.view
                   (Data.ProtoLens.Field.field @"vec'conditions") _x))
             ((Data.Monoid.<>)
                (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                   (\ _v
                      -> (Data.Monoid.<>)
                           (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                           ((Prelude..)
                              (\ bs
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt
                                         (Prelude.fromIntegral (Data.ByteString.length bs)))
                                      (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                              Data.ProtoLens.encodeMessage _v))
                   (Lens.Family2.view
                      (Data.ProtoLens.Field.field @"vec'mutations") _x))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"idempotencyKey") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                            ((\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                               _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'deadlineUnixMillis") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData CommitRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommitRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CommitRequest'conditions x__)
                (Control.DeepSeq.deepseq
                   (_CommitRequest'mutations x__)
                   (Control.DeepSeq.deepseq
                      (_CommitRequest'idempotencyKey x__)
                      (Control.DeepSeq.deepseq
                         (_CommitRequest'deadlineUnixMillis x__) ()))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.maybe'outcome' @:: Lens' CommitResponse (Prelude.Maybe CommitResponse'Outcome)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'committed' @:: Lens' CommitResponse (Prelude.Maybe CommittedEnvelope)@
         * 'Proto.Stream.V2.Stream_Fields.committed' @:: Lens' CommitResponse CommittedEnvelope@
         * 'Proto.Stream.V2.Stream_Fields.maybe'conflict' @:: Lens' CommitResponse (Prelude.Maybe CommitConflicts)@
         * 'Proto.Stream.V2.Stream_Fields.conflict' @:: Lens' CommitResponse CommitConflicts@ -}
data CommitResponse
  = CommitResponse'_constructor {_CommitResponse'outcome :: !(Prelude.Maybe CommitResponse'Outcome),
                                 _CommitResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommitResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data CommitResponse'Outcome
  = CommitResponse'Committed !CommittedEnvelope |
    CommitResponse'Conflict !CommitConflicts
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField CommitResponse "maybe'outcome" (Prelude.Maybe CommitResponse'Outcome) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitResponse'outcome
           (\ x__ y__ -> x__ {_CommitResponse'outcome = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommitResponse "maybe'committed" (Prelude.Maybe CommittedEnvelope) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitResponse'outcome
           (\ x__ y__ -> x__ {_CommitResponse'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitResponse'Committed x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitResponse'Committed y__))
instance Data.ProtoLens.Field.HasField CommitResponse "committed" CommittedEnvelope where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitResponse'outcome
           (\ x__ y__ -> x__ {_CommitResponse'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitResponse'Committed x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitResponse'Committed y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField CommitResponse "maybe'conflict" (Prelude.Maybe CommitConflicts) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitResponse'outcome
           (\ x__ y__ -> x__ {_CommitResponse'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommitResponse'Conflict x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommitResponse'Conflict y__))
instance Data.ProtoLens.Field.HasField CommitResponse "conflict" CommitConflicts where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommitResponse'outcome
           (\ x__ y__ -> x__ {_CommitResponse'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommitResponse'Conflict x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommitResponse'Conflict y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message CommitResponse where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommitResponse"
  packedMessageDescriptor _
    = "\n\
      \\SOCommitResponse\DC2D\n\
      \\tcommitted\CAN\SOH \SOH(\v2$.acyclic.stream.v2.CommittedEnvelopeH\NULR\tcommitted\DC2@\n\
      \\bconflict\CAN\STX \SOH(\v2\".acyclic.stream.v2.CommitConflictsH\NULR\bconflictB\t\n\
      \\aoutcome"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        committed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "committed"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommittedEnvelope)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'committed")) ::
              Data.ProtoLens.FieldDescriptor CommitResponse
        conflict__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "conflict"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommitConflicts)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'conflict")) ::
              Data.ProtoLens.FieldDescriptor CommitResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, committed__field_descriptor),
           (Data.ProtoLens.Tag 2, conflict__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommitResponse'_unknownFields
        (\ x__ y__ -> x__ {_CommitResponse'_unknownFields = y__})
  defMessage
    = CommitResponse'_constructor
        {_CommitResponse'outcome = Prelude.Nothing,
         _CommitResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommitResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser CommitResponse
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "committed"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"committed") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "conflict"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"conflict") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CommitResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'outcome") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (CommitResponse'Committed v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (CommitResponse'Conflict v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CommitResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommitResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CommitResponse'outcome x__) ())
instance Control.DeepSeq.NFData CommitResponse'Outcome where
  rnf (CommitResponse'Committed x__) = Control.DeepSeq.rnf x__
  rnf (CommitResponse'Conflict x__) = Control.DeepSeq.rnf x__
_CommitResponse'Committed ::
  Data.ProtoLens.Prism.Prism' CommitResponse'Outcome CommittedEnvelope
_CommitResponse'Committed
  = Data.ProtoLens.Prism.prism'
      CommitResponse'Committed
      (\ p__
         -> case p__ of
              (CommitResponse'Committed p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_CommitResponse'Conflict ::
  Data.ProtoLens.Prism.Prism' CommitResponse'Outcome CommitConflicts
_CommitResponse'Conflict
  = Data.ProtoLens.Prism.prism'
      CommitResponse'Conflict
      (\ p__
         -> case p__ of
              (CommitResponse'Conflict p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' CommittedAppend Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.start' @:: Lens' CommittedAppend Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.end' @:: Lens' CommittedAppend Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' CommittedAppend Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.records' @:: Lens' CommittedAppend [Record]@
         * 'Proto.Stream.V2.Stream_Fields.vec'records' @:: Lens' CommittedAppend (Data.Vector.Vector Record)@ -}
data CommittedAppend
  = CommittedAppend'_constructor {_CommittedAppend'path :: !Data.Text.Text,
                                  _CommittedAppend'start :: !Data.Word.Word64,
                                  _CommittedAppend'end :: !Data.Word.Word64,
                                  _CommittedAppend'tail :: !Data.Word.Word64,
                                  _CommittedAppend'records :: !(Data.Vector.Vector Record),
                                  _CommittedAppend'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommittedAppend where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CommittedAppend "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedAppend'path
           (\ x__ y__ -> x__ {_CommittedAppend'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedAppend "start" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedAppend'start
           (\ x__ y__ -> x__ {_CommittedAppend'start = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedAppend "end" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedAppend'end
           (\ x__ y__ -> x__ {_CommittedAppend'end = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedAppend "tail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedAppend'tail
           (\ x__ y__ -> x__ {_CommittedAppend'tail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedAppend "records" [Record] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedAppend'records
           (\ x__ y__ -> x__ {_CommittedAppend'records = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CommittedAppend "vec'records" (Data.Vector.Vector Record) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedAppend'records
           (\ x__ y__ -> x__ {_CommittedAppend'records = y__}))
        Prelude.id
instance Data.ProtoLens.Message CommittedAppend where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommittedAppend"
  packedMessageDescriptor _
    = "\n\
      \\SICommittedAppend\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\DC4\n\
      \\ENQstart\CAN\STX \SOH(\EOTR\ENQstart\DC2\DLE\n\
      \\ETXend\CAN\ETX \SOH(\EOTR\ETXend\DC2\DC2\n\
      \\EOTtail\CAN\EOT \SOH(\EOTR\EOTtail\DC23\n\
      \\arecords\CAN\ENQ \ETX(\v2\EM.acyclic.stream.v2.RecordR\arecords"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor CommittedAppend
        start__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "start"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"start")) ::
              Data.ProtoLens.FieldDescriptor CommittedAppend
        end__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "end"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"end")) ::
              Data.ProtoLens.FieldDescriptor CommittedAppend
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"tail")) ::
              Data.ProtoLens.FieldDescriptor CommittedAppend
        records__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "records"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Record)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"records")) ::
              Data.ProtoLens.FieldDescriptor CommittedAppend
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, start__field_descriptor),
           (Data.ProtoLens.Tag 3, end__field_descriptor),
           (Data.ProtoLens.Tag 4, tail__field_descriptor),
           (Data.ProtoLens.Tag 5, records__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommittedAppend'_unknownFields
        (\ x__ y__ -> x__ {_CommittedAppend'_unknownFields = y__})
  defMessage
    = CommittedAppend'_constructor
        {_CommittedAppend'path = Data.ProtoLens.fieldDefault,
         _CommittedAppend'start = Data.ProtoLens.fieldDefault,
         _CommittedAppend'end = Data.ProtoLens.fieldDefault,
         _CommittedAppend'tail = Data.ProtoLens.fieldDefault,
         _CommittedAppend'records = Data.Vector.Generic.empty,
         _CommittedAppend'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommittedAppend
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Record
             -> Data.ProtoLens.Encoding.Bytes.Parser CommittedAppend
        loop x mutable'records
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'records)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'records") frozen'records x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                                  mutable'records
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "start"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"start") y x)
                                  mutable'records
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "end"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"end") y x)
                                  mutable'records
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "tail"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                                  mutable'records
                        42
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "records"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'records y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'records
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'records)
          "CommittedAppend"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"start") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"end") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"tail") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                            (\ _v
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                    ((Prelude..)
                                       (\ bs
                                          -> (Data.Monoid.<>)
                                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                  (Prelude.fromIntegral
                                                     (Data.ByteString.length bs)))
                                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                       Data.ProtoLens.encodeMessage _v))
                            (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'records") _x))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData CommittedAppend where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommittedAppend'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CommittedAppend'path x__)
                (Control.DeepSeq.deepseq
                   (_CommittedAppend'start x__)
                   (Control.DeepSeq.deepseq
                      (_CommittedAppend'end x__)
                      (Control.DeepSeq.deepseq
                         (_CommittedAppend'tail x__)
                         (Control.DeepSeq.deepseq (_CommittedAppend'records x__) ())))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.commitId' @:: Lens' CommittedEnvelope Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.mutations' @:: Lens' CommittedEnvelope [CommittedMutation]@
         * 'Proto.Stream.V2.Stream_Fields.vec'mutations' @:: Lens' CommittedEnvelope (Data.Vector.Vector CommittedMutation)@ -}
data CommittedEnvelope
  = CommittedEnvelope'_constructor {_CommittedEnvelope'commitId :: !Data.ByteString.ByteString,
                                    _CommittedEnvelope'mutations :: !(Data.Vector.Vector CommittedMutation),
                                    _CommittedEnvelope'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommittedEnvelope where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CommittedEnvelope "commitId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedEnvelope'commitId
           (\ x__ y__ -> x__ {_CommittedEnvelope'commitId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedEnvelope "mutations" [CommittedMutation] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedEnvelope'mutations
           (\ x__ y__ -> x__ {_CommittedEnvelope'mutations = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CommittedEnvelope "vec'mutations" (Data.Vector.Vector CommittedMutation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedEnvelope'mutations
           (\ x__ y__ -> x__ {_CommittedEnvelope'mutations = y__}))
        Prelude.id
instance Data.ProtoLens.Message CommittedEnvelope where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.CommittedEnvelope"
  packedMessageDescriptor _
    = "\n\
      \\DC1CommittedEnvelope\DC2\ESC\n\
      \\tcommit_id\CAN\SOH \SOH(\fR\bcommitId\DC2B\n\
      \\tmutations\CAN\STX \ETX(\v2$.acyclic.stream.v2.CommittedMutationR\tmutations"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        commitId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commit_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitId")) ::
              Data.ProtoLens.FieldDescriptor CommittedEnvelope
        mutations__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutations"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommittedMutation)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"mutations")) ::
              Data.ProtoLens.FieldDescriptor CommittedEnvelope
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, commitId__field_descriptor),
           (Data.ProtoLens.Tag 2, mutations__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommittedEnvelope'_unknownFields
        (\ x__ y__ -> x__ {_CommittedEnvelope'_unknownFields = y__})
  defMessage
    = CommittedEnvelope'_constructor
        {_CommittedEnvelope'commitId = Data.ProtoLens.fieldDefault,
         _CommittedEnvelope'mutations = Data.Vector.Generic.empty,
         _CommittedEnvelope'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommittedEnvelope
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld CommittedMutation
             -> Data.ProtoLens.Encoding.Bytes.Parser CommittedEnvelope
        loop x mutable'mutations
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'mutations <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                            (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                               mutable'mutations)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'mutations") frozen'mutations x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commit_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitId") y x)
                                  mutable'mutations
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "mutations"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'mutations y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'mutations
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'mutations <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                     Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'mutations)
          "CommittedEnvelope"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"commitId") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             ((Data.Monoid.<>)
                (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                   (\ _v
                      -> (Data.Monoid.<>)
                           (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                           ((Prelude..)
                              (\ bs
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt
                                         (Prelude.fromIntegral (Data.ByteString.length bs)))
                                      (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                              Data.ProtoLens.encodeMessage _v))
                   (Lens.Family2.view
                      (Data.ProtoLens.Field.field @"vec'mutations") _x))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData CommittedEnvelope where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommittedEnvelope'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CommittedEnvelope'commitId x__)
                (Control.DeepSeq.deepseq (_CommittedEnvelope'mutations x__) ()))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.source' @:: Lens' CommittedFork Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.destination' @:: Lens' CommittedFork Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.forkedAt' @:: Lens' CommittedFork Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' CommittedFork Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.records' @:: Lens' CommittedFork [Record]@
         * 'Proto.Stream.V2.Stream_Fields.vec'records' @:: Lens' CommittedFork (Data.Vector.Vector Record)@ -}
data CommittedFork
  = CommittedFork'_constructor {_CommittedFork'source :: !Data.Text.Text,
                                _CommittedFork'destination :: !Data.Text.Text,
                                _CommittedFork'forkedAt :: !Data.Word.Word64,
                                _CommittedFork'tail :: !Data.Word.Word64,
                                _CommittedFork'records :: !(Data.Vector.Vector Record),
                                _CommittedFork'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommittedFork where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CommittedFork "source" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedFork'source
           (\ x__ y__ -> x__ {_CommittedFork'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedFork "destination" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedFork'destination
           (\ x__ y__ -> x__ {_CommittedFork'destination = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedFork "forkedAt" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedFork'forkedAt
           (\ x__ y__ -> x__ {_CommittedFork'forkedAt = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedFork "tail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedFork'tail (\ x__ y__ -> x__ {_CommittedFork'tail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedFork "records" [Record] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedFork'records
           (\ x__ y__ -> x__ {_CommittedFork'records = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CommittedFork "vec'records" (Data.Vector.Vector Record) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedFork'records
           (\ x__ y__ -> x__ {_CommittedFork'records = y__}))
        Prelude.id
instance Data.ProtoLens.Message CommittedFork where
  messageName _ = Data.Text.pack "acyclic.stream.v2.CommittedFork"
  packedMessageDescriptor _
    = "\n\
      \\rCommittedFork\DC2\SYN\n\
      \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
      \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\ESC\n\
      \\tforked_at\CAN\ETX \SOH(\EOTR\bforkedAt\DC2\DC2\n\
      \\EOTtail\CAN\EOT \SOH(\EOTR\EOTtail\DC23\n\
      \\arecords\CAN\ENQ \ETX(\v2\EM.acyclic.stream.v2.RecordR\arecords"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor CommittedFork
        destination__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destination"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"destination")) ::
              Data.ProtoLens.FieldDescriptor CommittedFork
        forkedAt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "forked_at"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"forkedAt")) ::
              Data.ProtoLens.FieldDescriptor CommittedFork
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"tail")) ::
              Data.ProtoLens.FieldDescriptor CommittedFork
        records__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "records"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Record)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"records")) ::
              Data.ProtoLens.FieldDescriptor CommittedFork
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, destination__field_descriptor),
           (Data.ProtoLens.Tag 3, forkedAt__field_descriptor),
           (Data.ProtoLens.Tag 4, tail__field_descriptor),
           (Data.ProtoLens.Tag 5, records__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommittedFork'_unknownFields
        (\ x__ y__ -> x__ {_CommittedFork'_unknownFields = y__})
  defMessage
    = CommittedFork'_constructor
        {_CommittedFork'source = Data.ProtoLens.fieldDefault,
         _CommittedFork'destination = Data.ProtoLens.fieldDefault,
         _CommittedFork'forkedAt = Data.ProtoLens.fieldDefault,
         _CommittedFork'tail = Data.ProtoLens.fieldDefault,
         _CommittedFork'records = Data.Vector.Generic.empty,
         _CommittedFork'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommittedFork
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Record
             -> Data.ProtoLens.Encoding.Bytes.Parser CommittedFork
        loop x mutable'records
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'records)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'records") frozen'records x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "source"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                                  mutable'records
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "destination"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"destination") y x)
                                  mutable'records
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "forked_at"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"forkedAt") y x)
                                  mutable'records
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "tail"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                                  mutable'records
                        42
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "records"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'records y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'records
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'records)
          "CommittedFork"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"source") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"destination") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((Prelude..)
                            (\ bs
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                       (Prelude.fromIntegral (Data.ByteString.length bs)))
                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            Data.Text.Encoding.encodeUtf8 _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"forkedAt") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"tail") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                            (\ _v
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                    ((Prelude..)
                                       (\ bs
                                          -> (Data.Monoid.<>)
                                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                  (Prelude.fromIntegral
                                                     (Data.ByteString.length bs)))
                                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                       Data.ProtoLens.encodeMessage _v))
                            (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'records") _x))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData CommittedFork where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommittedFork'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CommittedFork'source x__)
                (Control.DeepSeq.deepseq
                   (_CommittedFork'destination x__)
                   (Control.DeepSeq.deepseq
                      (_CommittedFork'forkedAt x__)
                      (Control.DeepSeq.deepseq
                         (_CommittedFork'tail x__)
                         (Control.DeepSeq.deepseq (_CommittedFork'records x__) ())))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.maybe'mutation' @:: Lens' CommittedMutation (Prelude.Maybe CommittedMutation'Mutation)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'append' @:: Lens' CommittedMutation (Prelude.Maybe CommittedAppend)@
         * 'Proto.Stream.V2.Stream_Fields.append' @:: Lens' CommittedMutation CommittedAppend@
         * 'Proto.Stream.V2.Stream_Fields.maybe'fork' @:: Lens' CommittedMutation (Prelude.Maybe CommittedFork)@
         * 'Proto.Stream.V2.Stream_Fields.fork' @:: Lens' CommittedMutation CommittedFork@ -}
data CommittedMutation
  = CommittedMutation'_constructor {_CommittedMutation'mutation :: !(Prelude.Maybe CommittedMutation'Mutation),
                                    _CommittedMutation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CommittedMutation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data CommittedMutation'Mutation
  = CommittedMutation'Append !CommittedAppend |
    CommittedMutation'Fork !CommittedFork
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField CommittedMutation "maybe'mutation" (Prelude.Maybe CommittedMutation'Mutation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedMutation'mutation
           (\ x__ y__ -> x__ {_CommittedMutation'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CommittedMutation "maybe'append" (Prelude.Maybe CommittedAppend) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedMutation'mutation
           (\ x__ y__ -> x__ {_CommittedMutation'mutation = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommittedMutation'Append x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommittedMutation'Append y__))
instance Data.ProtoLens.Field.HasField CommittedMutation "append" CommittedAppend where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedMutation'mutation
           (\ x__ y__ -> x__ {_CommittedMutation'mutation = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommittedMutation'Append x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommittedMutation'Append y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField CommittedMutation "maybe'fork" (Prelude.Maybe CommittedFork) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedMutation'mutation
           (\ x__ y__ -> x__ {_CommittedMutation'mutation = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (CommittedMutation'Fork x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap CommittedMutation'Fork y__))
instance Data.ProtoLens.Field.HasField CommittedMutation "fork" CommittedFork where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CommittedMutation'mutation
           (\ x__ y__ -> x__ {_CommittedMutation'mutation = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (CommittedMutation'Fork x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap CommittedMutation'Fork y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message CommittedMutation where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.CommittedMutation"
  packedMessageDescriptor _
    = "\n\
      \\DC1CommittedMutation\DC2<\n\
      \\ACKappend\CAN\SOH \SOH(\v2\".acyclic.stream.v2.CommittedAppendH\NULR\ACKappend\DC26\n\
      \\EOTfork\CAN\STX \SOH(\v2 .acyclic.stream.v2.CommittedForkH\NULR\EOTforkB\n\
      \\n\
      \\bmutationJ\EOT\b\ETX\DLE\EOTJ\EOT\b\EOT\DLE\ENQR\EOTtrimR\ACKdelete"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        append__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "append"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommittedAppend)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'append")) ::
              Data.ProtoLens.FieldDescriptor CommittedMutation
        fork__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fork"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommittedFork)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'fork")) ::
              Data.ProtoLens.FieldDescriptor CommittedMutation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, append__field_descriptor),
           (Data.ProtoLens.Tag 2, fork__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CommittedMutation'_unknownFields
        (\ x__ y__ -> x__ {_CommittedMutation'_unknownFields = y__})
  defMessage
    = CommittedMutation'_constructor
        {_CommittedMutation'mutation = Prelude.Nothing,
         _CommittedMutation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CommittedMutation
          -> Data.ProtoLens.Encoding.Bytes.Parser CommittedMutation
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "append"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"append") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "fork"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"fork") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CommittedMutation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (CommittedMutation'Append v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (CommittedMutation'Fork v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CommittedMutation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CommittedMutation'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CommittedMutation'mutation x__) ())
instance Control.DeepSeq.NFData CommittedMutation'Mutation where
  rnf (CommittedMutation'Append x__) = Control.DeepSeq.rnf x__
  rnf (CommittedMutation'Fork x__) = Control.DeepSeq.rnf x__
_CommittedMutation'Append ::
  Data.ProtoLens.Prism.Prism' CommittedMutation'Mutation CommittedAppend
_CommittedMutation'Append
  = Data.ProtoLens.Prism.prism'
      CommittedMutation'Append
      (\ p__
         -> case p__ of
              (CommittedMutation'Append p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_CommittedMutation'Fork ::
  Data.ProtoLens.Prism.Prism' CommittedMutation'Mutation CommittedFork
_CommittedMutation'Fork
  = Data.ProtoLens.Prism.prism'
      CommittedMutation'Fork
      (\ p__
         -> case p__ of
              (CommittedMutation'Fork p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.expiresIn' @:: Lens' CreateTokenRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.allow' @:: Lens' CreateTokenRequest [TokenGrant]@
         * 'Proto.Stream.V2.Stream_Fields.vec'allow' @:: Lens' CreateTokenRequest (Data.Vector.Vector TokenGrant)@ -}
data CreateTokenRequest
  = CreateTokenRequest'_constructor {_CreateTokenRequest'expiresIn :: !Data.Text.Text,
                                     _CreateTokenRequest'allow :: !(Data.Vector.Vector TokenGrant),
                                     _CreateTokenRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateTokenRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateTokenRequest "expiresIn" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateTokenRequest'expiresIn
           (\ x__ y__ -> x__ {_CreateTokenRequest'expiresIn = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateTokenRequest "allow" [TokenGrant] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateTokenRequest'allow
           (\ x__ y__ -> x__ {_CreateTokenRequest'allow = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CreateTokenRequest "vec'allow" (Data.Vector.Vector TokenGrant) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateTokenRequest'allow
           (\ x__ y__ -> x__ {_CreateTokenRequest'allow = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateTokenRequest where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.CreateTokenRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2CreateTokenRequest\DC2\GS\n\
      \\n\
      \expires_in\CAN\SOH \SOH(\tR\texpiresIn\DC23\n\
      \\ENQallow\CAN\STX \ETX(\v2\GS.acyclic.stream.v2.TokenGrantR\ENQallow"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        expiresIn__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expires_in"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expiresIn")) ::
              Data.ProtoLens.FieldDescriptor CreateTokenRequest
        allow__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "allow"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor TokenGrant)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"allow")) ::
              Data.ProtoLens.FieldDescriptor CreateTokenRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, expiresIn__field_descriptor),
           (Data.ProtoLens.Tag 2, allow__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateTokenRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateTokenRequest'_unknownFields = y__})
  defMessage
    = CreateTokenRequest'_constructor
        {_CreateTokenRequest'expiresIn = Data.ProtoLens.fieldDefault,
         _CreateTokenRequest'allow = Data.Vector.Generic.empty,
         _CreateTokenRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateTokenRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld TokenGrant
             -> Data.ProtoLens.Encoding.Bytes.Parser CreateTokenRequest
        loop x mutable'allow
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'allow <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'allow)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'allow") frozen'allow x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "expires_in"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expiresIn") y x)
                                  mutable'allow
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "allow"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'allow y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'allow
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'allow <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'allow)
          "CreateTokenRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"expiresIn") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                   (\ _v
                      -> (Data.Monoid.<>)
                           (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                           ((Prelude..)
                              (\ bs
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt
                                         (Prelude.fromIntegral (Data.ByteString.length bs)))
                                      (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                              Data.ProtoLens.encodeMessage _v))
                   (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'allow") _x))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData CreateTokenRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateTokenRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateTokenRequest'expiresIn x__)
                (Control.DeepSeq.deepseq (_CreateTokenRequest'allow x__) ()))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' ExistsCommitConflict Data.Text.Text@ -}
data ExistsCommitConflict
  = ExistsCommitConflict'_constructor {_ExistsCommitConflict'path :: !Data.Text.Text,
                                       _ExistsCommitConflict'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ExistsCommitConflict where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ExistsCommitConflict "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ExistsCommitConflict'path
           (\ x__ y__ -> x__ {_ExistsCommitConflict'path = y__}))
        Prelude.id
instance Data.ProtoLens.Message ExistsCommitConflict where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.ExistsCommitConflict"
  packedMessageDescriptor _
    = "\n\
      \\DC4ExistsCommitConflict\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor ExistsCommitConflict
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, path__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ExistsCommitConflict'_unknownFields
        (\ x__ y__ -> x__ {_ExistsCommitConflict'_unknownFields = y__})
  defMessage
    = ExistsCommitConflict'_constructor
        {_ExistsCommitConflict'path = Data.ProtoLens.fieldDefault,
         _ExistsCommitConflict'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ExistsCommitConflict
          -> Data.ProtoLens.Encoding.Bytes.Parser ExistsCommitConflict
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ExistsCommitConflict"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ExistsCommitConflict where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ExistsCommitConflict'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ExistsCommitConflict'path x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' FollowRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.from' @:: Lens' FollowRequest Data.Word.Word64@ -}
data FollowRequest
  = FollowRequest'_constructor {_FollowRequest'path :: !Data.Text.Text,
                                _FollowRequest'from :: !Data.Word.Word64,
                                _FollowRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show FollowRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField FollowRequest "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _FollowRequest'path (\ x__ y__ -> x__ {_FollowRequest'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField FollowRequest "from" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _FollowRequest'from (\ x__ y__ -> x__ {_FollowRequest'from = y__}))
        Prelude.id
instance Data.ProtoLens.Message FollowRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.FollowRequest"
  packedMessageDescriptor _
    = "\n\
      \\rFollowRequest\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\DC2\n\
      \\EOTfrom\CAN\STX \SOH(\EOTR\EOTfrom"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor FollowRequest
        from__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "from"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"from")) ::
              Data.ProtoLens.FieldDescriptor FollowRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, from__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _FollowRequest'_unknownFields
        (\ x__ y__ -> x__ {_FollowRequest'_unknownFields = y__})
  defMessage
    = FollowRequest'_constructor
        {_FollowRequest'path = Data.ProtoLens.fieldDefault,
         _FollowRequest'from = Data.ProtoLens.fieldDefault,
         _FollowRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          FollowRequest -> Data.ProtoLens.Encoding.Bytes.Parser FollowRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "from"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"from") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "FollowRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"from") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData FollowRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_FollowRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_FollowRequest'path x__)
                (Control.DeepSeq.deepseq (_FollowRequest'from x__) ()))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.source' @:: Lens' ForkMutation Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.destination' @:: Lens' ForkMutation Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.atTail' @:: Lens' ForkMutation Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.records' @:: Lens' ForkMutation [Data.ByteString.ByteString]@
         * 'Proto.Stream.V2.Stream_Fields.vec'records' @:: Lens' ForkMutation (Data.Vector.Vector Data.ByteString.ByteString)@ -}
data ForkMutation
  = ForkMutation'_constructor {_ForkMutation'source :: !Data.Text.Text,
                               _ForkMutation'destination :: !Data.Text.Text,
                               _ForkMutation'atTail :: !Data.Word.Word64,
                               _ForkMutation'records :: !(Data.Vector.Vector Data.ByteString.ByteString),
                               _ForkMutation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkMutation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkMutation "source" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMutation'source
           (\ x__ y__ -> x__ {_ForkMutation'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMutation "destination" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMutation'destination
           (\ x__ y__ -> x__ {_ForkMutation'destination = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMutation "atTail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMutation'atTail
           (\ x__ y__ -> x__ {_ForkMutation'atTail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMutation "records" [Data.ByteString.ByteString] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMutation'records
           (\ x__ y__ -> x__ {_ForkMutation'records = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ForkMutation "vec'records" (Data.Vector.Vector Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMutation'records
           (\ x__ y__ -> x__ {_ForkMutation'records = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkMutation where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ForkMutation"
  packedMessageDescriptor _
    = "\n\
      \\fForkMutation\DC2\SYN\n\
      \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
      \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\ETB\n\
      \\aat_tail\CAN\ETX \SOH(\EOTR\ACKatTail\DC2\CAN\n\
      \\arecords\CAN\EOT \ETX(\fR\arecords"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor ForkMutation
        destination__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destination"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"destination")) ::
              Data.ProtoLens.FieldDescriptor ForkMutation
        atTail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "at_tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"atTail")) ::
              Data.ProtoLens.FieldDescriptor ForkMutation
        records__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "records"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"records")) ::
              Data.ProtoLens.FieldDescriptor ForkMutation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, destination__field_descriptor),
           (Data.ProtoLens.Tag 3, atTail__field_descriptor),
           (Data.ProtoLens.Tag 4, records__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkMutation'_unknownFields
        (\ x__ y__ -> x__ {_ForkMutation'_unknownFields = y__})
  defMessage
    = ForkMutation'_constructor
        {_ForkMutation'source = Data.ProtoLens.fieldDefault,
         _ForkMutation'destination = Data.ProtoLens.fieldDefault,
         _ForkMutation'atTail = Data.ProtoLens.fieldDefault,
         _ForkMutation'records = Data.Vector.Generic.empty,
         _ForkMutation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkMutation
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.ByteString.ByteString
             -> Data.ProtoLens.Encoding.Bytes.Parser ForkMutation
        loop x mutable'records
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'records)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'records") frozen'records x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "source"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                                  mutable'records
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "destination"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"destination") y x)
                                  mutable'records
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "at_tail"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"atTail") y x)
                                  mutable'records
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getBytes
                                              (Prelude.fromIntegral len))
                                        "records"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'records y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'records
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'records <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'records)
          "ForkMutation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"source") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"destination") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((Prelude..)
                            (\ bs
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                       (Prelude.fromIntegral (Data.ByteString.length bs)))
                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            Data.Text.Encoding.encodeUtf8 _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"atTail") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                         (\ _v
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                 ((\ bs
                                     -> (Data.Monoid.<>)
                                          (Data.ProtoLens.Encoding.Bytes.putVarInt
                                             (Prelude.fromIntegral (Data.ByteString.length bs)))
                                          (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                    _v))
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'records") _x))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData ForkMutation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkMutation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkMutation'source x__)
                (Control.DeepSeq.deepseq
                   (_ForkMutation'destination x__)
                   (Control.DeepSeq.deepseq
                      (_ForkMutation'atTail x__)
                      (Control.DeepSeq.deepseq (_ForkMutation'records x__) ()))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.source' @:: Lens' ForkReceipt Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.destination' @:: Lens' ForkReceipt Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.forkedAt' @:: Lens' ForkReceipt Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' ForkReceipt Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.commitId' @:: Lens' ForkReceipt Data.ByteString.ByteString@ -}
data ForkReceipt
  = ForkReceipt'_constructor {_ForkReceipt'source :: !Data.Text.Text,
                              _ForkReceipt'destination :: !Data.Text.Text,
                              _ForkReceipt'forkedAt :: !Data.Word.Word64,
                              _ForkReceipt'tail :: !Data.Word.Word64,
                              _ForkReceipt'commitId :: !Data.ByteString.ByteString,
                              _ForkReceipt'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkReceipt where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkReceipt "source" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkReceipt'source (\ x__ y__ -> x__ {_ForkReceipt'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkReceipt "destination" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkReceipt'destination
           (\ x__ y__ -> x__ {_ForkReceipt'destination = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkReceipt "forkedAt" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkReceipt'forkedAt
           (\ x__ y__ -> x__ {_ForkReceipt'forkedAt = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkReceipt "tail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkReceipt'tail (\ x__ y__ -> x__ {_ForkReceipt'tail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkReceipt "commitId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkReceipt'commitId
           (\ x__ y__ -> x__ {_ForkReceipt'commitId = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkReceipt where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ForkReceipt"
  packedMessageDescriptor _
    = "\n\
      \\vForkReceipt\DC2\SYN\n\
      \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
      \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\ESC\n\
      \\tforked_at\CAN\ETX \SOH(\EOTR\bforkedAt\DC2\DC2\n\
      \\EOTtail\CAN\EOT \SOH(\EOTR\EOTtail\DC2\ESC\n\
      \\tcommit_id\CAN\ENQ \SOH(\fR\bcommitId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor ForkReceipt
        destination__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destination"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"destination")) ::
              Data.ProtoLens.FieldDescriptor ForkReceipt
        forkedAt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "forked_at"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"forkedAt")) ::
              Data.ProtoLens.FieldDescriptor ForkReceipt
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"tail")) ::
              Data.ProtoLens.FieldDescriptor ForkReceipt
        commitId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commit_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitId")) ::
              Data.ProtoLens.FieldDescriptor ForkReceipt
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, destination__field_descriptor),
           (Data.ProtoLens.Tag 3, forkedAt__field_descriptor),
           (Data.ProtoLens.Tag 4, tail__field_descriptor),
           (Data.ProtoLens.Tag 5, commitId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkReceipt'_unknownFields
        (\ x__ y__ -> x__ {_ForkReceipt'_unknownFields = y__})
  defMessage
    = ForkReceipt'_constructor
        {_ForkReceipt'source = Data.ProtoLens.fieldDefault,
         _ForkReceipt'destination = Data.ProtoLens.fieldDefault,
         _ForkReceipt'forkedAt = Data.ProtoLens.fieldDefault,
         _ForkReceipt'tail = Data.ProtoLens.fieldDefault,
         _ForkReceipt'commitId = Data.ProtoLens.fieldDefault,
         _ForkReceipt'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkReceipt -> Data.ProtoLens.Encoding.Bytes.Parser ForkReceipt
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "destination"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"destination") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "forked_at"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"forkedAt") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "tail"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commit_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ForkReceipt"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"source") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"destination") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((Prelude..)
                            (\ bs
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                       (Prelude.fromIntegral (Data.ByteString.length bs)))
                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            Data.Text.Encoding.encodeUtf8 _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"forkedAt") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"tail") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (let
                            _v = Lens.Family2.view (Data.ProtoLens.Field.field @"commitId") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                  ((\ bs
                                      -> (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt
                                              (Prelude.fromIntegral (Data.ByteString.length bs)))
                                           (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                     _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData ForkReceipt where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkReceipt'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkReceipt'source x__)
                (Control.DeepSeq.deepseq
                   (_ForkReceipt'destination x__)
                   (Control.DeepSeq.deepseq
                      (_ForkReceipt'forkedAt x__)
                      (Control.DeepSeq.deepseq
                         (_ForkReceipt'tail x__)
                         (Control.DeepSeq.deepseq (_ForkReceipt'commitId x__) ())))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.source' @:: Lens' ForkRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.destination' @:: Lens' ForkRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.atTail' @:: Lens' ForkRequest Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.maybe'atTail' @:: Lens' ForkRequest (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Stream.V2.Stream_Fields.idempotencyKey' @:: Lens' ForkRequest Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.maybe'idempotencyKey' @:: Lens' ForkRequest (Prelude.Maybe Data.ByteString.ByteString)@ -}
data ForkRequest
  = ForkRequest'_constructor {_ForkRequest'source :: !Data.Text.Text,
                              _ForkRequest'destination :: !Data.Text.Text,
                              _ForkRequest'atTail :: !(Prelude.Maybe Data.Word.Word64),
                              _ForkRequest'idempotencyKey :: !(Prelude.Maybe Data.ByteString.ByteString),
                              _ForkRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkRequest "source" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkRequest'source (\ x__ y__ -> x__ {_ForkRequest'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkRequest "destination" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkRequest'destination
           (\ x__ y__ -> x__ {_ForkRequest'destination = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkRequest "atTail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkRequest'atTail (\ x__ y__ -> x__ {_ForkRequest'atTail = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ForkRequest "maybe'atTail" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkRequest'atTail (\ x__ y__ -> x__ {_ForkRequest'atTail = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkRequest "idempotencyKey" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkRequest'idempotencyKey
           (\ x__ y__ -> x__ {_ForkRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ForkRequest "maybe'idempotencyKey" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkRequest'idempotencyKey
           (\ x__ y__ -> x__ {_ForkRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ForkRequest"
  packedMessageDescriptor _
    = "\n\
      \\vForkRequest\DC2\SYN\n\
      \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
      \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\FS\n\
      \\aat_tail\CAN\ETX \SOH(\EOTH\NULR\ACKatTail\136\SOH\SOH\DC2,\n\
      \\SIidempotency_key\CAN\EOT \SOH(\fH\SOHR\SOidempotencyKey\136\SOH\SOHB\n\
      \\n\
      \\b_at_tailB\DC2\n\
      \\DLE_idempotency_key"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor ForkRequest
        destination__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destination"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"destination")) ::
              Data.ProtoLens.FieldDescriptor ForkRequest
        atTail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "at_tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'atTail")) ::
              Data.ProtoLens.FieldDescriptor ForkRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor ForkRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, destination__field_descriptor),
           (Data.ProtoLens.Tag 3, atTail__field_descriptor),
           (Data.ProtoLens.Tag 4, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkRequest'_unknownFields
        (\ x__ y__ -> x__ {_ForkRequest'_unknownFields = y__})
  defMessage
    = ForkRequest'_constructor
        {_ForkRequest'source = Data.ProtoLens.fieldDefault,
         _ForkRequest'destination = Data.ProtoLens.fieldDefault,
         _ForkRequest'atTail = Prelude.Nothing,
         _ForkRequest'idempotencyKey = Prelude.Nothing,
         _ForkRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkRequest -> Data.ProtoLens.Encoding.Bytes.Parser ForkRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "destination"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"destination") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "at_tail"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"atTail") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ForkRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"source") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"destination") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((Prelude..)
                            (\ bs
                               -> (Data.Monoid.<>)
                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                       (Prelude.fromIntegral (Data.ByteString.length bs)))
                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            Data.Text.Encoding.encodeUtf8 _v))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'atTail") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                ((\ bs
                                    -> (Data.Monoid.<>)
                                         (Data.ProtoLens.Encoding.Bytes.putVarInt
                                            (Prelude.fromIntegral (Data.ByteString.length bs)))
                                         (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                   _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData ForkRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkRequest'source x__)
                (Control.DeepSeq.deepseq
                   (_ForkRequest'destination x__)
                   (Control.DeepSeq.deepseq
                      (_ForkRequest'atTail x__)
                      (Control.DeepSeq.deepseq (_ForkRequest'idempotencyKey x__) ()))))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.idempotencyKey' @:: Lens' IdempotencyObservation Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.requestDigest' @:: Lens' IdempotencyObservation Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.maybe'outcome' @:: Lens' IdempotencyObservation (Prelude.Maybe IdempotencyObservation'Outcome)@
         * 'Proto.Stream.V2.Stream_Fields.maybe'append' @:: Lens' IdempotencyObservation (Prelude.Maybe AppendResponse)@
         * 'Proto.Stream.V2.Stream_Fields.append' @:: Lens' IdempotencyObservation AppendResponse@
         * 'Proto.Stream.V2.Stream_Fields.maybe'fork' @:: Lens' IdempotencyObservation (Prelude.Maybe ForkReceipt)@
         * 'Proto.Stream.V2.Stream_Fields.fork' @:: Lens' IdempotencyObservation ForkReceipt@
         * 'Proto.Stream.V2.Stream_Fields.maybe'commit' @:: Lens' IdempotencyObservation (Prelude.Maybe CommitResponse)@
         * 'Proto.Stream.V2.Stream_Fields.commit' @:: Lens' IdempotencyObservation CommitResponse@ -}
data IdempotencyObservation
  = IdempotencyObservation'_constructor {_IdempotencyObservation'idempotencyKey :: !Data.ByteString.ByteString,
                                         _IdempotencyObservation'requestDigest :: !Data.ByteString.ByteString,
                                         _IdempotencyObservation'outcome :: !(Prelude.Maybe IdempotencyObservation'Outcome),
                                         _IdempotencyObservation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show IdempotencyObservation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data IdempotencyObservation'Outcome
  = IdempotencyObservation'Append !AppendResponse |
    IdempotencyObservation'Fork !ForkReceipt |
    IdempotencyObservation'Commit !CommitResponse
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField IdempotencyObservation "idempotencyKey" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'idempotencyKey
           (\ x__ y__ -> x__ {_IdempotencyObservation'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdempotencyObservation "requestDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'requestDigest
           (\ x__ y__ -> x__ {_IdempotencyObservation'requestDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdempotencyObservation "maybe'outcome" (Prelude.Maybe IdempotencyObservation'Outcome) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdempotencyObservation "maybe'append" (Prelude.Maybe AppendResponse) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (IdempotencyObservation'Append x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap IdempotencyObservation'Append y__))
instance Data.ProtoLens.Field.HasField IdempotencyObservation "append" AppendResponse where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (IdempotencyObservation'Append x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap IdempotencyObservation'Append y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField IdempotencyObservation "maybe'fork" (Prelude.Maybe ForkReceipt) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (IdempotencyObservation'Fork x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap IdempotencyObservation'Fork y__))
instance Data.ProtoLens.Field.HasField IdempotencyObservation "fork" ForkReceipt where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (IdempotencyObservation'Fork x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap IdempotencyObservation'Fork y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField IdempotencyObservation "maybe'commit" (Prelude.Maybe CommitResponse) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (IdempotencyObservation'Commit x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap IdempotencyObservation'Commit y__))
instance Data.ProtoLens.Field.HasField IdempotencyObservation "commit" CommitResponse where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyObservation'outcome
           (\ x__ y__ -> x__ {_IdempotencyObservation'outcome = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (IdempotencyObservation'Commit x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap IdempotencyObservation'Commit y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message IdempotencyObservation where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.IdempotencyObservation"
  packedMessageDescriptor _
    = "\n\
      \\SYNIdempotencyObservation\DC2'\n\
      \\SIidempotency_key\CAN\SOH \SOH(\fR\SOidempotencyKey\DC2%\n\
      \\SOrequest_digest\CAN\STX \SOH(\fR\rrequestDigest\DC2;\n\
      \\ACKappend\CAN\ETX \SOH(\v2!.acyclic.stream.v2.AppendResponseH\NULR\ACKappend\DC24\n\
      \\EOTfork\CAN\EOT \SOH(\v2\RS.acyclic.stream.v2.ForkReceiptH\NULR\EOTfork\DC2;\n\
      \\ACKcommit\CAN\a \SOH(\v2!.acyclic.stream.v2.CommitResponseH\NULR\ACKcommitB\t\n\
      \\aoutcomeJ\EOT\b\ENQ\DLE\ACKJ\EOT\b\ACK\DLE\aR\EOTtrimR\ACKdelete"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor IdempotencyObservation
        requestDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "request_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"requestDigest")) ::
              Data.ProtoLens.FieldDescriptor IdempotencyObservation
        append__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "append"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor AppendResponse)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'append")) ::
              Data.ProtoLens.FieldDescriptor IdempotencyObservation
        fork__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fork"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ForkReceipt)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'fork")) ::
              Data.ProtoLens.FieldDescriptor IdempotencyObservation
        commit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commit"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CommitResponse)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'commit")) ::
              Data.ProtoLens.FieldDescriptor IdempotencyObservation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 2, requestDigest__field_descriptor),
           (Data.ProtoLens.Tag 3, append__field_descriptor),
           (Data.ProtoLens.Tag 4, fork__field_descriptor),
           (Data.ProtoLens.Tag 7, commit__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _IdempotencyObservation'_unknownFields
        (\ x__ y__ -> x__ {_IdempotencyObservation'_unknownFields = y__})
  defMessage
    = IdempotencyObservation'_constructor
        {_IdempotencyObservation'idempotencyKey = Data.ProtoLens.fieldDefault,
         _IdempotencyObservation'requestDigest = Data.ProtoLens.fieldDefault,
         _IdempotencyObservation'outcome = Prelude.Nothing,
         _IdempotencyObservation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          IdempotencyObservation
          -> Data.ProtoLens.Encoding.Bytes.Parser IdempotencyObservation
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "request_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"requestDigest") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "append"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"append") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "fork"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"fork") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "commit"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"commit") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "IdempotencyObservation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"idempotencyKey") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"requestDigest") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            _v))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'outcome") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just (IdempotencyObservation'Append v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (IdempotencyObservation'Fork v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (IdempotencyObservation'Commit v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData IdempotencyObservation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_IdempotencyObservation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_IdempotencyObservation'idempotencyKey x__)
                (Control.DeepSeq.deepseq
                   (_IdempotencyObservation'requestDigest x__)
                   (Control.DeepSeq.deepseq
                      (_IdempotencyObservation'outcome x__) ())))
instance Control.DeepSeq.NFData IdempotencyObservation'Outcome where
  rnf (IdempotencyObservation'Append x__) = Control.DeepSeq.rnf x__
  rnf (IdempotencyObservation'Fork x__) = Control.DeepSeq.rnf x__
  rnf (IdempotencyObservation'Commit x__) = Control.DeepSeq.rnf x__
_IdempotencyObservation'Append ::
  Data.ProtoLens.Prism.Prism' IdempotencyObservation'Outcome AppendResponse
_IdempotencyObservation'Append
  = Data.ProtoLens.Prism.prism'
      IdempotencyObservation'Append
      (\ p__
         -> case p__ of
              (IdempotencyObservation'Append p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_IdempotencyObservation'Fork ::
  Data.ProtoLens.Prism.Prism' IdempotencyObservation'Outcome ForkReceipt
_IdempotencyObservation'Fork
  = Data.ProtoLens.Prism.prism'
      IdempotencyObservation'Fork
      (\ p__
         -> case p__ of
              (IdempotencyObservation'Fork p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_IdempotencyObservation'Commit ::
  Data.ProtoLens.Prism.Prism' IdempotencyObservation'Outcome CommitResponse
_IdempotencyObservation'Commit
  = Data.ProtoLens.Prism.prism'
      IdempotencyObservation'Commit
      (\ p__
         -> case p__ of
              (IdempotencyObservation'Commit p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.idempotencyKey' @:: Lens' InspectIdempotencyRequest Data.ByteString.ByteString@ -}
data InspectIdempotencyRequest
  = InspectIdempotencyRequest'_constructor {_InspectIdempotencyRequest'idempotencyKey :: !Data.ByteString.ByteString,
                                            _InspectIdempotencyRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectIdempotencyRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectIdempotencyRequest "idempotencyKey" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectIdempotencyRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_InspectIdempotencyRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectIdempotencyRequest where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.InspectIdempotencyRequest"
  packedMessageDescriptor _
    = "\n\
      \\EMInspectIdempotencyRequest\DC2'\n\
      \\SIidempotency_key\CAN\SOH \SOH(\fR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor InspectIdempotencyRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectIdempotencyRequest'_unknownFields
        (\ x__ y__
           -> x__ {_InspectIdempotencyRequest'_unknownFields = y__})
  defMessage
    = InspectIdempotencyRequest'_constructor
        {_InspectIdempotencyRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _InspectIdempotencyRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectIdempotencyRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectIdempotencyRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectIdempotencyRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"idempotencyKey") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData InspectIdempotencyRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectIdempotencyRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InspectIdempotencyRequest'idempotencyKey x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.observation' @:: Lens' InspectIdempotencyResponse IdempotencyObservation@
         * 'Proto.Stream.V2.Stream_Fields.maybe'observation' @:: Lens' InspectIdempotencyResponse (Prelude.Maybe IdempotencyObservation)@ -}
data InspectIdempotencyResponse
  = InspectIdempotencyResponse'_constructor {_InspectIdempotencyResponse'observation :: !(Prelude.Maybe IdempotencyObservation),
                                             _InspectIdempotencyResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectIdempotencyResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectIdempotencyResponse "observation" IdempotencyObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectIdempotencyResponse'observation
           (\ x__ y__ -> x__ {_InspectIdempotencyResponse'observation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectIdempotencyResponse "maybe'observation" (Prelude.Maybe IdempotencyObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectIdempotencyResponse'observation
           (\ x__ y__ -> x__ {_InspectIdempotencyResponse'observation = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectIdempotencyResponse where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.InspectIdempotencyResponse"
  packedMessageDescriptor _
    = "\n\
      \\SUBInspectIdempotencyResponse\DC2P\n\
      \\vobservation\CAN\SOH \SOH(\v2).acyclic.stream.v2.IdempotencyObservationH\NULR\vobservation\136\SOH\SOHB\SO\n\
      \\f_observation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        observation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "observation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'observation")) ::
              Data.ProtoLens.FieldDescriptor InspectIdempotencyResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, observation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectIdempotencyResponse'_unknownFields
        (\ x__ y__
           -> x__ {_InspectIdempotencyResponse'_unknownFields = y__})
  defMessage
    = InspectIdempotencyResponse'_constructor
        {_InspectIdempotencyResponse'observation = Prelude.Nothing,
         _InspectIdempotencyResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectIdempotencyResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectIdempotencyResponse
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "observation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"observation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectIdempotencyResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'observation") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just _v)
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData InspectIdempotencyResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectIdempotencyResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InspectIdempotencyResponse'observation x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.commitId' @:: Lens' ReadCommitRequest Data.ByteString.ByteString@ -}
data ReadCommitRequest
  = ReadCommitRequest'_constructor {_ReadCommitRequest'commitId :: !Data.ByteString.ByteString,
                                    _ReadCommitRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ReadCommitRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ReadCommitRequest "commitId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadCommitRequest'commitId
           (\ x__ y__ -> x__ {_ReadCommitRequest'commitId = y__}))
        Prelude.id
instance Data.ProtoLens.Message ReadCommitRequest where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.ReadCommitRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1ReadCommitRequest\DC2\ESC\n\
      \\tcommit_id\CAN\SOH \SOH(\fR\bcommitId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        commitId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commit_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitId")) ::
              Data.ProtoLens.FieldDescriptor ReadCommitRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, commitId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ReadCommitRequest'_unknownFields
        (\ x__ y__ -> x__ {_ReadCommitRequest'_unknownFields = y__})
  defMessage
    = ReadCommitRequest'_constructor
        {_ReadCommitRequest'commitId = Data.ProtoLens.fieldDefault,
         _ReadCommitRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ReadCommitRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ReadCommitRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commit_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ReadCommitRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"commitId") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ReadCommitRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ReadCommitRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ReadCommitRequest'commitId x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' ReadRequest Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.from' @:: Lens' ReadRequest Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.limit' @:: Lens' ReadRequest Data.Word.Word32@ -}
data ReadRequest
  = ReadRequest'_constructor {_ReadRequest'path :: !Data.Text.Text,
                              _ReadRequest'from :: !Data.Word.Word64,
                              _ReadRequest'limit :: !Data.Word.Word32,
                              _ReadRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ReadRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ReadRequest "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadRequest'path (\ x__ y__ -> x__ {_ReadRequest'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ReadRequest "from" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadRequest'from (\ x__ y__ -> x__ {_ReadRequest'from = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ReadRequest "limit" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadRequest'limit (\ x__ y__ -> x__ {_ReadRequest'limit = y__}))
        Prelude.id
instance Data.ProtoLens.Message ReadRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ReadRequest"
  packedMessageDescriptor _
    = "\n\
      \\vReadRequest\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\DC2\n\
      \\EOTfrom\CAN\STX \SOH(\EOTR\EOTfrom\DC2\DC4\n\
      \\ENQlimit\CAN\ETX \SOH(\rR\ENQlimit"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor ReadRequest
        from__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "from"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"from")) ::
              Data.ProtoLens.FieldDescriptor ReadRequest
        limit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limit"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"limit")) ::
              Data.ProtoLens.FieldDescriptor ReadRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, from__field_descriptor),
           (Data.ProtoLens.Tag 3, limit__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ReadRequest'_unknownFields
        (\ x__ y__ -> x__ {_ReadRequest'_unknownFields = y__})
  defMessage
    = ReadRequest'_constructor
        {_ReadRequest'path = Data.ProtoLens.fieldDefault,
         _ReadRequest'from = Data.ProtoLens.fieldDefault,
         _ReadRequest'limit = Data.ProtoLens.fieldDefault,
         _ReadRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ReadRequest -> Data.ProtoLens.Encoding.Bytes.Parser ReadRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "from"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"from") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "limit"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"limit") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ReadRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"from") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"limit") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            ((Prelude..)
                               Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ReadRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ReadRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ReadRequest'path x__)
                (Control.DeepSeq.deepseq
                   (_ReadRequest'from x__)
                   (Control.DeepSeq.deepseq (_ReadRequest'limit x__) ())))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.record' @:: Lens' ReadResponse Record@
         * 'Proto.Stream.V2.Stream_Fields.maybe'record' @:: Lens' ReadResponse (Prelude.Maybe Record)@ -}
data ReadResponse
  = ReadResponse'_constructor {_ReadResponse'record :: !(Prelude.Maybe Record),
                               _ReadResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ReadResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ReadResponse "record" Record where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadResponse'record
           (\ x__ y__ -> x__ {_ReadResponse'record = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ReadResponse "maybe'record" (Prelude.Maybe Record) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadResponse'record
           (\ x__ y__ -> x__ {_ReadResponse'record = y__}))
        Prelude.id
instance Data.ProtoLens.Message ReadResponse where
  messageName _ = Data.Text.pack "acyclic.stream.v2.ReadResponse"
  packedMessageDescriptor _
    = "\n\
      \\fReadResponse\DC21\n\
      \\ACKrecord\CAN\SOH \SOH(\v2\EM.acyclic.stream.v2.RecordR\ACKrecord"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        record__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "record"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Record)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'record")) ::
              Data.ProtoLens.FieldDescriptor ReadResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, record__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ReadResponse'_unknownFields
        (\ x__ y__ -> x__ {_ReadResponse'_unknownFields = y__})
  defMessage
    = ReadResponse'_constructor
        {_ReadResponse'record = Prelude.Nothing,
         _ReadResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ReadResponse -> Data.ProtoLens.Encoding.Bytes.Parser ReadResponse
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "record"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"record") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ReadResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'record") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just _v)
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ReadResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ReadResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ReadResponse'record x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.sequence' @:: Lens' Record Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.value' @:: Lens' Record Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.commitId' @:: Lens' Record Data.ByteString.ByteString@
         * 'Proto.Stream.V2.Stream_Fields.committedAtMicros' @:: Lens' Record Data.Word.Word64@ -}
data Record
  = Record'_constructor {_Record'sequence :: !Data.Word.Word64,
                         _Record'value :: !Data.ByteString.ByteString,
                         _Record'commitId :: !Data.ByteString.ByteString,
                         _Record'committedAtMicros :: !Data.Word.Word64,
                         _Record'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Record where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Record "sequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Record'sequence (\ x__ y__ -> x__ {_Record'sequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Record "value" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Record'value (\ x__ y__ -> x__ {_Record'value = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Record "commitId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Record'commitId (\ x__ y__ -> x__ {_Record'commitId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Record "committedAtMicros" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Record'committedAtMicros
           (\ x__ y__ -> x__ {_Record'committedAtMicros = y__}))
        Prelude.id
instance Data.ProtoLens.Message Record where
  messageName _ = Data.Text.pack "acyclic.stream.v2.Record"
  packedMessageDescriptor _
    = "\n\
      \\ACKRecord\DC2\SUB\n\
      \\bsequence\CAN\SOH \SOH(\EOTR\bsequence\DC2\DC4\n\
      \\ENQvalue\CAN\STX \SOH(\fR\ENQvalue\DC2\ESC\n\
      \\tcommit_id\CAN\ETX \SOH(\fR\bcommitId\DC2.\n\
      \\DC3committed_at_micros\CAN\EOT \SOH(\EOTR\DC1committedAtMicros"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        sequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sequence")) ::
              Data.ProtoLens.FieldDescriptor Record
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor Record
        commitId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commit_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitId")) ::
              Data.ProtoLens.FieldDescriptor Record
        committedAtMicros__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "committed_at_micros"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"committedAtMicros")) ::
              Data.ProtoLens.FieldDescriptor Record
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, sequence__field_descriptor),
           (Data.ProtoLens.Tag 2, value__field_descriptor),
           (Data.ProtoLens.Tag 3, commitId__field_descriptor),
           (Data.ProtoLens.Tag 4, committedAtMicros__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Record'_unknownFields
        (\ x__ y__ -> x__ {_Record'_unknownFields = y__})
  defMessage
    = Record'_constructor
        {_Record'sequence = Data.ProtoLens.fieldDefault,
         _Record'value = Data.ProtoLens.fieldDefault,
         _Record'commitId = Data.ProtoLens.fieldDefault,
         _Record'committedAtMicros = Data.ProtoLens.fieldDefault,
         _Record'_unknownFields = []}
  parseMessage
    = let
        loop :: Record -> Data.ProtoLens.Encoding.Bytes.Parser Record
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        8 -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "sequence"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"sequence") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "value"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"value") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commit_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitId") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "committed_at_micros"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"committedAtMicros") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Record"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"sequence") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"value") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"commitId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                            ((\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                               _v))
                   ((Data.Monoid.<>)
                      (let
                         _v
                           = Lens.Family2.view
                               (Data.ProtoLens.Field.field @"committedAtMicros") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData Record where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Record'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Record'sequence x__)
                (Control.DeepSeq.deepseq
                   (_Record'value x__)
                   (Control.DeepSeq.deepseq
                      (_Record'commitId x__)
                      (Control.DeepSeq.deepseq (_Record'committedAtMicros x__) ()))))
newtype StreamLimit'UnrecognizedValue
  = StreamLimit'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data StreamLimit
  = STREAM_LIMIT_UNSPECIFIED |
    STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES |
    STREAM_LIMIT_MAX_ITEMS |
    STREAM_LIMIT_MAX_PATH_BYTES |
    STREAM_LIMIT_MAX_RECORD_BYTES |
    STREAM_LIMIT_MAX_COMMAND_BYTES |
    StreamLimit'Unrecognized !StreamLimit'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum StreamLimit where
  maybeToEnum 0 = Prelude.Just STREAM_LIMIT_UNSPECIFIED
  maybeToEnum 256
    = Prelude.Just STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  maybeToEnum 1024 = Prelude.Just STREAM_LIMIT_MAX_ITEMS
  maybeToEnum 65535 = Prelude.Just STREAM_LIMIT_MAX_PATH_BYTES
  maybeToEnum 65536 = Prelude.Just STREAM_LIMIT_MAX_RECORD_BYTES
  maybeToEnum 1056768 = Prelude.Just STREAM_LIMIT_MAX_COMMAND_BYTES
  maybeToEnum k
    = Prelude.Just
        (StreamLimit'Unrecognized
           (StreamLimit'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum STREAM_LIMIT_UNSPECIFIED = "STREAM_LIMIT_UNSPECIFIED"
  showEnum STREAM_LIMIT_MAX_RECORD_BYTES
    = "STREAM_LIMIT_MAX_RECORD_BYTES"
  showEnum STREAM_LIMIT_MAX_ITEMS = "STREAM_LIMIT_MAX_ITEMS"
  showEnum STREAM_LIMIT_MAX_COMMAND_BYTES
    = "STREAM_LIMIT_MAX_COMMAND_BYTES"
  showEnum STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = "STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES"
  showEnum STREAM_LIMIT_MAX_PATH_BYTES
    = "STREAM_LIMIT_MAX_PATH_BYTES"
  showEnum
    (StreamLimit'Unrecognized (StreamLimit'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "STREAM_LIMIT_UNSPECIFIED"
    = Prelude.Just STREAM_LIMIT_UNSPECIFIED
    | (Prelude.==) k "STREAM_LIMIT_MAX_RECORD_BYTES"
    = Prelude.Just STREAM_LIMIT_MAX_RECORD_BYTES
    | (Prelude.==) k "STREAM_LIMIT_MAX_ITEMS"
    = Prelude.Just STREAM_LIMIT_MAX_ITEMS
    | (Prelude.==) k "STREAM_LIMIT_MAX_COMMAND_BYTES"
    = Prelude.Just STREAM_LIMIT_MAX_COMMAND_BYTES
    | (Prelude.==) k "STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES"
    = Prelude.Just STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    | (Prelude.==) k "STREAM_LIMIT_MAX_PATH_BYTES"
    = Prelude.Just STREAM_LIMIT_MAX_PATH_BYTES
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded StreamLimit where
  minBound = STREAM_LIMIT_UNSPECIFIED
  maxBound = STREAM_LIMIT_MAX_COMMAND_BYTES
instance Prelude.Enum StreamLimit where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum StreamLimit: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum STREAM_LIMIT_UNSPECIFIED = 0
  fromEnum STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES = 256
  fromEnum STREAM_LIMIT_MAX_ITEMS = 1024
  fromEnum STREAM_LIMIT_MAX_PATH_BYTES = 65535
  fromEnum STREAM_LIMIT_MAX_RECORD_BYTES = 65536
  fromEnum STREAM_LIMIT_MAX_COMMAND_BYTES = 1056768
  fromEnum
    (StreamLimit'Unrecognized (StreamLimit'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ STREAM_LIMIT_MAX_COMMAND_BYTES
    = Prelude.error
        "StreamLimit.succ: bad argument STREAM_LIMIT_MAX_COMMAND_BYTES. This value would be out of bounds."
  succ STREAM_LIMIT_UNSPECIFIED
    = STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  succ STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = STREAM_LIMIT_MAX_ITEMS
  succ STREAM_LIMIT_MAX_ITEMS = STREAM_LIMIT_MAX_PATH_BYTES
  succ STREAM_LIMIT_MAX_PATH_BYTES = STREAM_LIMIT_MAX_RECORD_BYTES
  succ STREAM_LIMIT_MAX_RECORD_BYTES = STREAM_LIMIT_MAX_COMMAND_BYTES
  succ (StreamLimit'Unrecognized _)
    = Prelude.error
        "StreamLimit.succ: bad argument: unrecognized value"
  pred STREAM_LIMIT_UNSPECIFIED
    = Prelude.error
        "StreamLimit.pred: bad argument STREAM_LIMIT_UNSPECIFIED. This value would be out of bounds."
  pred STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = STREAM_LIMIT_UNSPECIFIED
  pred STREAM_LIMIT_MAX_ITEMS
    = STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  pred STREAM_LIMIT_MAX_PATH_BYTES = STREAM_LIMIT_MAX_ITEMS
  pred STREAM_LIMIT_MAX_RECORD_BYTES = STREAM_LIMIT_MAX_PATH_BYTES
  pred STREAM_LIMIT_MAX_COMMAND_BYTES = STREAM_LIMIT_MAX_RECORD_BYTES
  pred (StreamLimit'Unrecognized _)
    = Prelude.error
        "StreamLimit.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault StreamLimit where
  fieldDefault = STREAM_LIMIT_UNSPECIFIED
instance Control.DeepSeq.NFData StreamLimit where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' TailCommitConflict Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.expected' @:: Lens' TailCommitConflict Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.actual' @:: Lens' TailCommitConflict Data.Word.Word64@
         * 'Proto.Stream.V2.Stream_Fields.maybe'actual' @:: Lens' TailCommitConflict (Prelude.Maybe Data.Word.Word64)@ -}
data TailCommitConflict
  = TailCommitConflict'_constructor {_TailCommitConflict'path :: !Data.Text.Text,
                                     _TailCommitConflict'expected :: !Data.Word.Word64,
                                     _TailCommitConflict'actual :: !(Prelude.Maybe Data.Word.Word64),
                                     _TailCommitConflict'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TailCommitConflict where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TailCommitConflict "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailCommitConflict'path
           (\ x__ y__ -> x__ {_TailCommitConflict'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField TailCommitConflict "expected" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailCommitConflict'expected
           (\ x__ y__ -> x__ {_TailCommitConflict'expected = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField TailCommitConflict "actual" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailCommitConflict'actual
           (\ x__ y__ -> x__ {_TailCommitConflict'actual = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField TailCommitConflict "maybe'actual" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailCommitConflict'actual
           (\ x__ y__ -> x__ {_TailCommitConflict'actual = y__}))
        Prelude.id
instance Data.ProtoLens.Message TailCommitConflict where
  messageName _
    = Data.Text.pack "acyclic.stream.v2.TailCommitConflict"
  packedMessageDescriptor _
    = "\n\
      \\DC2TailCommitConflict\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\SUB\n\
      \\bexpected\CAN\STX \SOH(\EOTR\bexpected\DC2\ESC\n\
      \\ACKactual\CAN\ETX \SOH(\EOTH\NULR\ACKactual\136\SOH\SOHB\t\n\
      \\a_actual"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor TailCommitConflict
        expected__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expected"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expected")) ::
              Data.ProtoLens.FieldDescriptor TailCommitConflict
        actual__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actual"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actual")) ::
              Data.ProtoLens.FieldDescriptor TailCommitConflict
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, expected__field_descriptor),
           (Data.ProtoLens.Tag 3, actual__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TailCommitConflict'_unknownFields
        (\ x__ y__ -> x__ {_TailCommitConflict'_unknownFields = y__})
  defMessage
    = TailCommitConflict'_constructor
        {_TailCommitConflict'path = Data.ProtoLens.fieldDefault,
         _TailCommitConflict'expected = Data.ProtoLens.fieldDefault,
         _TailCommitConflict'actual = Prelude.Nothing,
         _TailCommitConflict'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TailCommitConflict
          -> Data.ProtoLens.Encoding.Bytes.Parser TailCommitConflict
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "expected"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expected") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "actual"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actual") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "TailCommitConflict"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"expected") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actual") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData TailCommitConflict where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TailCommitConflict'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_TailCommitConflict'path x__)
                (Control.DeepSeq.deepseq
                   (_TailCommitConflict'expected x__)
                   (Control.DeepSeq.deepseq (_TailCommitConflict'actual x__) ())))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' TailCondition Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.expected' @:: Lens' TailCondition Data.Word.Word64@ -}
data TailCondition
  = TailCondition'_constructor {_TailCondition'path :: !Data.Text.Text,
                                _TailCondition'expected :: !Data.Word.Word64,
                                _TailCondition'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TailCondition where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TailCondition "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailCondition'path (\ x__ y__ -> x__ {_TailCondition'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField TailCondition "expected" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailCondition'expected
           (\ x__ y__ -> x__ {_TailCondition'expected = y__}))
        Prelude.id
instance Data.ProtoLens.Message TailCondition where
  messageName _ = Data.Text.pack "acyclic.stream.v2.TailCondition"
  packedMessageDescriptor _
    = "\n\
      \\rTailCondition\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\SUB\n\
      \\bexpected\CAN\STX \SOH(\EOTR\bexpected"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor TailCondition
        expected__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expected"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expected")) ::
              Data.ProtoLens.FieldDescriptor TailCondition
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, expected__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TailCondition'_unknownFields
        (\ x__ y__ -> x__ {_TailCondition'_unknownFields = y__})
  defMessage
    = TailCondition'_constructor
        {_TailCondition'path = Data.ProtoLens.fieldDefault,
         _TailCondition'expected = Data.ProtoLens.fieldDefault,
         _TailCondition'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TailCondition -> Data.ProtoLens.Encoding.Bytes.Parser TailCondition
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "expected"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expected") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "TailCondition"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"expected") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData TailCondition where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TailCondition'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_TailCondition'path x__)
                (Control.DeepSeq.deepseq (_TailCondition'expected x__) ()))
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.actualTail' @:: Lens' TailConflict Data.Word.Word64@ -}
data TailConflict
  = TailConflict'_constructor {_TailConflict'actualTail :: !Data.Word.Word64,
                               _TailConflict'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TailConflict where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TailConflict "actualTail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailConflict'actualTail
           (\ x__ y__ -> x__ {_TailConflict'actualTail = y__}))
        Prelude.id
instance Data.ProtoLens.Message TailConflict where
  messageName _ = Data.Text.pack "acyclic.stream.v2.TailConflict"
  packedMessageDescriptor _
    = "\n\
      \\fTailConflict\DC2\US\n\
      \\vactual_tail\CAN\SOH \SOH(\EOTR\n\
      \actualTail"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actualTail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actual_tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"actualTail")) ::
              Data.ProtoLens.FieldDescriptor TailConflict
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actualTail__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TailConflict'_unknownFields
        (\ x__ y__ -> x__ {_TailConflict'_unknownFields = y__})
  defMessage
    = TailConflict'_constructor
        {_TailConflict'actualTail = Data.ProtoLens.fieldDefault,
         _TailConflict'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TailConflict -> Data.ProtoLens.Encoding.Bytes.Parser TailConflict
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        8 -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "actual_tail"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"actualTail") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "TailConflict"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"actualTail") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData TailConflict where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TailConflict'_unknownFields x__)
             (Control.DeepSeq.deepseq (_TailConflict'actualTail x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' TailRequest Data.Text.Text@ -}
data TailRequest
  = TailRequest'_constructor {_TailRequest'path :: !Data.Text.Text,
                              _TailRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TailRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TailRequest "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailRequest'path (\ x__ y__ -> x__ {_TailRequest'path = y__}))
        Prelude.id
instance Data.ProtoLens.Message TailRequest where
  messageName _ = Data.Text.pack "acyclic.stream.v2.TailRequest"
  packedMessageDescriptor _
    = "\n\
      \\vTailRequest\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor TailRequest
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, path__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TailRequest'_unknownFields
        (\ x__ y__ -> x__ {_TailRequest'_unknownFields = y__})
  defMessage
    = TailRequest'_constructor
        {_TailRequest'path = Data.ProtoLens.fieldDefault,
         _TailRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TailRequest -> Data.ProtoLens.Encoding.Bytes.Parser TailRequest
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "TailRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData TailRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TailRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_TailRequest'path x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.tail' @:: Lens' TailResponse Data.Word.Word64@ -}
data TailResponse
  = TailResponse'_constructor {_TailResponse'tail :: !Data.Word.Word64,
                               _TailResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TailResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TailResponse "tail" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TailResponse'tail (\ x__ y__ -> x__ {_TailResponse'tail = y__}))
        Prelude.id
instance Data.ProtoLens.Message TailResponse where
  messageName _ = Data.Text.pack "acyclic.stream.v2.TailResponse"
  packedMessageDescriptor _
    = "\n\
      \\fTailResponse\DC2\DC2\n\
      \\EOTtail\CAN\SOH \SOH(\EOTR\EOTtailJ\EOT\b\STX\DLE\ETXR\n\
      \trim_point"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        tail__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "tail"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"tail")) ::
              Data.ProtoLens.FieldDescriptor TailResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, tail__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TailResponse'_unknownFields
        (\ x__ y__ -> x__ {_TailResponse'_unknownFields = y__})
  defMessage
    = TailResponse'_constructor
        {_TailResponse'tail = Data.ProtoLens.fieldDefault,
         _TailResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TailResponse -> Data.ProtoLens.Encoding.Bytes.Parser TailResponse
        loop x
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t) x)
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        8 -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "tail"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"tail") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "TailResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"tail") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData TailResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TailResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_TailResponse'tail x__) ())
{- | Fields :
     
         * 'Proto.Stream.V2.Stream_Fields.path' @:: Lens' TokenGrant Data.Text.Text@
         * 'Proto.Stream.V2.Stream_Fields.subtree' @:: Lens' TokenGrant Prelude.Bool@
         * 'Proto.Stream.V2.Stream_Fields.maybe'subtree' @:: Lens' TokenGrant (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Stream.V2.Stream_Fields.operations' @:: Lens' TokenGrant [Data.Text.Text]@
         * 'Proto.Stream.V2.Stream_Fields.vec'operations' @:: Lens' TokenGrant (Data.Vector.Vector Data.Text.Text)@ -}
data TokenGrant
  = TokenGrant'_constructor {_TokenGrant'path :: !Data.Text.Text,
                             _TokenGrant'subtree :: !(Prelude.Maybe Prelude.Bool),
                             _TokenGrant'operations :: !(Data.Vector.Vector Data.Text.Text),
                             _TokenGrant'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TokenGrant where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TokenGrant "path" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TokenGrant'path (\ x__ y__ -> x__ {_TokenGrant'path = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField TokenGrant "subtree" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TokenGrant'subtree (\ x__ y__ -> x__ {_TokenGrant'subtree = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField TokenGrant "maybe'subtree" (Prelude.Maybe Prelude.Bool) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TokenGrant'subtree (\ x__ y__ -> x__ {_TokenGrant'subtree = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField TokenGrant "operations" [Data.Text.Text] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TokenGrant'operations
           (\ x__ y__ -> x__ {_TokenGrant'operations = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField TokenGrant "vec'operations" (Data.Vector.Vector Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TokenGrant'operations
           (\ x__ y__ -> x__ {_TokenGrant'operations = y__}))
        Prelude.id
instance Data.ProtoLens.Message TokenGrant where
  messageName _ = Data.Text.pack "acyclic.stream.v2.TokenGrant"
  packedMessageDescriptor _
    = "\n\
      \\n\
      \TokenGrant\DC2\DC2\n\
      \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\GS\n\
      \\asubtree\CAN\STX \SOH(\bH\NULR\asubtree\136\SOH\SOH\DC2\RS\n\
      \\n\
      \operations\CAN\ETX \ETX(\tR\n\
      \operationsB\n\
      \\n\
      \\b_subtree"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        path__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"path")) ::
              Data.ProtoLens.FieldDescriptor TokenGrant
        subtree__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subtree"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'subtree")) ::
              Data.ProtoLens.FieldDescriptor TokenGrant
        operations__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operations"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"operations")) ::
              Data.ProtoLens.FieldDescriptor TokenGrant
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, path__field_descriptor),
           (Data.ProtoLens.Tag 2, subtree__field_descriptor),
           (Data.ProtoLens.Tag 3, operations__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TokenGrant'_unknownFields
        (\ x__ y__ -> x__ {_TokenGrant'_unknownFields = y__})
  defMessage
    = TokenGrant'_constructor
        {_TokenGrant'path = Data.ProtoLens.fieldDefault,
         _TokenGrant'subtree = Prelude.Nothing,
         _TokenGrant'operations = Data.Vector.Generic.empty,
         _TokenGrant'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TokenGrant
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.Text.Text
             -> Data.ProtoLens.Encoding.Bytes.Parser TokenGrant
        loop x mutable'operations
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'operations <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                             (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                mutable'operations)
                      (let missing = []
                       in
                         if Prelude.null missing then
                             Prelude.return ()
                         else
                             Prelude.fail
                               ((Prelude.++)
                                  "Missing required fields: "
                                  (Prelude.show (missing :: [Prelude.String]))))
                      Prelude.return
                        (Lens.Family2.over
                           Data.ProtoLens.unknownFields (\ !t -> Prelude.reverse t)
                           (Lens.Family2.set
                              (Data.ProtoLens.Field.field @"vec'operations") frozen'operations
                              x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "path"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"path") y x)
                                  mutable'operations
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "subtree"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"subtree") y x)
                                  mutable'operations
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getText
                                              (Prelude.fromIntegral len))
                                        "operations"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'operations y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'operations
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'operations <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                      Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'operations)
          "TokenGrant"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"path") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'subtree") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just _v)
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                          ((Prelude..)
                             Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                             _v))
                ((Data.Monoid.<>)
                   (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                      (\ _v
                         -> (Data.Monoid.<>)
                              (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                              ((Prelude..)
                                 (\ bs
                                    -> (Data.Monoid.<>)
                                         (Data.ProtoLens.Encoding.Bytes.putVarInt
                                            (Prelude.fromIntegral (Data.ByteString.length bs)))
                                         (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                 Data.Text.Encoding.encodeUtf8 _v))
                      (Lens.Family2.view
                         (Data.ProtoLens.Field.field @"vec'operations") _x))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData TokenGrant where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TokenGrant'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_TokenGrant'path x__)
                (Control.DeepSeq.deepseq
                   (_TokenGrant'subtree x__)
                   (Control.DeepSeq.deepseq (_TokenGrant'operations x__) ())))
data StreamService = StreamService {}
instance Data.ProtoLens.Service.Types.Service StreamService where
  type ServiceName StreamService = "StreamService"
  type ServicePackage StreamService = "acyclic.stream.v2"
  type ServiceMethods StreamService = '["append",
                                        "children",
                                        "childrenPage",
                                        "commit",
                                        "follow",
                                        "fork",
                                        "inspectIdempotency",
                                        "read",
                                        "readCommit",
                                        "tail"]
  packedServiceDescriptor _
    = "\n\
      \\rStreamService\DC2q\n\
      \\DC2InspectIdempotency\DC2,.acyclic.stream.v2.InspectIdempotencyRequest\SUB-.acyclic.stream.v2.InspectIdempotencyResponse\DC2M\n\
      \\ACKAppend\DC2 .acyclic.stream.v2.AppendRequest\SUB!.acyclic.stream.v2.AppendResponse\DC2G\n\
      \\EOTTail\DC2\RS.acyclic.stream.v2.TailRequest\SUB\US.acyclic.stream.v2.TailResponse\DC2F\n\
      \\EOTFork\DC2\RS.acyclic.stream.v2.ForkRequest\SUB\RS.acyclic.stream.v2.ForkReceipt\DC2I\n\
      \\EOTRead\DC2\RS.acyclic.stream.v2.ReadRequest\SUB\US.acyclic.stream.v2.ReadResponse0\SOH\DC2M\n\
      \\ACKFollow\DC2 .acyclic.stream.v2.FollowRequest\SUB\US.acyclic.stream.v2.ReadResponse0\SOH\DC2U\n\
      \\bChildren\DC2\".acyclic.stream.v2.ChildrenRequest\SUB#.acyclic.stream.v2.ChildrenResponse0\SOH\DC2_\n\
      \\fChildrenPage\DC2&.acyclic.stream.v2.ChildrenPageRequest\SUB'.acyclic.stream.v2.ChildrenPageResponse\DC2M\n\
      \\ACKCommit\DC2 .acyclic.stream.v2.CommitRequest\SUB!.acyclic.stream.v2.CommitResponse\DC2X\n\
      \\n\
      \ReadCommit\DC2$.acyclic.stream.v2.ReadCommitRequest\SUB$.acyclic.stream.v2.CommittedEnvelope"
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "inspectIdempotency" where
  type MethodName StreamService "inspectIdempotency" = "InspectIdempotency"
  type MethodInput StreamService "inspectIdempotency" = InspectIdempotencyRequest
  type MethodOutput StreamService "inspectIdempotency" = InspectIdempotencyResponse
  type MethodStreamingType StreamService "inspectIdempotency" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "append" where
  type MethodName StreamService "append" = "Append"
  type MethodInput StreamService "append" = AppendRequest
  type MethodOutput StreamService "append" = AppendResponse
  type MethodStreamingType StreamService "append" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "tail" where
  type MethodName StreamService "tail" = "Tail"
  type MethodInput StreamService "tail" = TailRequest
  type MethodOutput StreamService "tail" = TailResponse
  type MethodStreamingType StreamService "tail" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "fork" where
  type MethodName StreamService "fork" = "Fork"
  type MethodInput StreamService "fork" = ForkRequest
  type MethodOutput StreamService "fork" = ForkReceipt
  type MethodStreamingType StreamService "fork" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "read" where
  type MethodName StreamService "read" = "Read"
  type MethodInput StreamService "read" = ReadRequest
  type MethodOutput StreamService "read" = ReadResponse
  type MethodStreamingType StreamService "read" = 'Data.ProtoLens.Service.Types.ServerStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "follow" where
  type MethodName StreamService "follow" = "Follow"
  type MethodInput StreamService "follow" = FollowRequest
  type MethodOutput StreamService "follow" = ReadResponse
  type MethodStreamingType StreamService "follow" = 'Data.ProtoLens.Service.Types.ServerStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "children" where
  type MethodName StreamService "children" = "Children"
  type MethodInput StreamService "children" = ChildrenRequest
  type MethodOutput StreamService "children" = ChildrenResponse
  type MethodStreamingType StreamService "children" = 'Data.ProtoLens.Service.Types.ServerStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "childrenPage" where
  type MethodName StreamService "childrenPage" = "ChildrenPage"
  type MethodInput StreamService "childrenPage" = ChildrenPageRequest
  type MethodOutput StreamService "childrenPage" = ChildrenPageResponse
  type MethodStreamingType StreamService "childrenPage" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "commit" where
  type MethodName StreamService "commit" = "Commit"
  type MethodInput StreamService "commit" = CommitRequest
  type MethodOutput StreamService "commit" = CommitResponse
  type MethodStreamingType StreamService "commit" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl StreamService "readCommit" where
  type MethodName StreamService "readCommit" = "ReadCommit"
  type MethodInput StreamService "readCommit" = ReadCommitRequest
  type MethodOutput StreamService "readCommit" = CommittedEnvelope
  type MethodStreamingType StreamService "readCommit" = 'Data.ProtoLens.Service.Types.NonStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\SYNstream/v2/stream.proto\DC2\DC1acyclic.stream.v2\"\135\SOH\n\
    \\ACKRecord\DC2\SUB\n\
    \\bsequence\CAN\SOH \SOH(\EOTR\bsequence\DC2\DC4\n\
    \\ENQvalue\CAN\STX \SOH(\fR\ENQvalue\DC2\ESC\n\
    \\tcommit_id\CAN\ETX \SOH(\fR\bcommitId\DC2.\n\
    \\DC3committed_at_micros\CAN\EOT \SOH(\EOTR\DC1committedAtMicros\"\169\SOH\n\
    \\rAppendRequest\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\CAN\n\
    \\arecords\CAN\STX \ETX(\fR\arecords\DC2\FS\n\
    \\aif_tail\CAN\ETX \SOH(\EOTH\NULR\ACKifTail\136\SOH\SOH\DC2,\n\
    \\SIidempotency_key\CAN\EOT \SOH(\fH\SOHR\SOidempotencyKey\136\SOH\SOHB\n\
    \\n\
    \\b_if_tailB\DC2\n\
    \\DLE_idempotency_key\"h\n\
    \\rAppendReceipt\DC2\DC4\n\
    \\ENQstart\CAN\SOH \SOH(\EOTR\ENQstart\DC2\DLE\n\
    \\ETXend\CAN\STX \SOH(\EOTR\ETXend\DC2\DC2\n\
    \\EOTtail\CAN\ETX \SOH(\EOTR\EOTtail\DC2\ESC\n\
    \\tcommit_id\CAN\EOT \SOH(\fR\bcommitId\"/\n\
    \\fTailConflict\DC2\US\n\
    \\vactual_tail\CAN\SOH \SOH(\EOTR\n\
    \actualTail\"\156\SOH\n\
    \\SOAppendResponse\DC2@\n\
    \\tcommitted\CAN\SOH \SOH(\v2 .acyclic.stream.v2.AppendReceiptH\NULR\tcommitted\DC2=\n\
    \\bconflict\CAN\STX \SOH(\v2\US.acyclic.stream.v2.TailConflictH\NULR\bconflictB\t\n\
    \\aoutcome\"!\n\
    \\vTailRequest\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\"4\n\
    \\fTailResponse\DC2\DC2\n\
    \\EOTtail\CAN\SOH \SOH(\EOTR\EOTtailJ\EOT\b\STX\DLE\ETXR\n\
    \trim_point\"\179\SOH\n\
    \\vForkRequest\DC2\SYN\n\
    \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
    \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\FS\n\
    \\aat_tail\CAN\ETX \SOH(\EOTH\NULR\ACKatTail\136\SOH\SOH\DC2,\n\
    \\SIidempotency_key\CAN\EOT \SOH(\fH\SOHR\SOidempotencyKey\136\SOH\SOHB\n\
    \\n\
    \\b_at_tailB\DC2\n\
    \\DLE_idempotency_key\"\149\SOH\n\
    \\vForkReceipt\DC2\SYN\n\
    \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
    \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\ESC\n\
    \\tforked_at\CAN\ETX \SOH(\EOTR\bforkedAt\DC2\DC2\n\
    \\EOTtail\CAN\EOT \SOH(\EOTR\EOTtail\DC2\ESC\n\
    \\tcommit_id\CAN\ENQ \SOH(\fR\bcommitId\"K\n\
    \\vReadRequest\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\DC2\n\
    \\EOTfrom\CAN\STX \SOH(\EOTR\EOTfrom\DC2\DC4\n\
    \\ENQlimit\CAN\ETX \SOH(\rR\ENQlimit\"7\n\
    \\rFollowRequest\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\DC2\n\
    \\EOTfrom\CAN\STX \SOH(\EOTR\EOTfrom\"A\n\
    \\fReadResponse\DC21\n\
    \\ACKrecord\CAN\SOH \SOH(\v2\EM.acyclic.stream.v2.RecordR\ACKrecord\"O\n\
    \\SIChildrenRequest\DC2\ESC\n\
    \\ACKparent\CAN\SOH \SOH(\tH\NULR\ACKparent\136\SOH\SOH\DC2\DC4\n\
    \\ENQlimit\CAN\STX \SOH(\rR\ENQlimitB\t\n\
    \\a_parent\"\ESC\n\
    \\ENQChild\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\"B\n\
    \\DLEChildrenResponse\DC2.\n\
    \\ENQchild\CAN\SOH \SOH(\v2\CAN.acyclic.stream.v2.ChildR\ENQchild\"\192\SOH\n\
    \\DC3ChildrenPageRequest\DC2\ESC\n\
    \\ACKparent\CAN\SOH \SOH(\tH\NULR\ACKparent\136\SOH\SOH\DC2\EM\n\
    \\ENQafter\CAN\STX \SOH(\tH\SOHR\ENQafter\136\SOH\SOH\DC20\n\
    \\DC1hierarchy_version\CAN\ETX \SOH(\fH\STXR\DLEhierarchyVersion\136\SOH\SOH\DC2\DC4\n\
    \\ENQlimit\CAN\EOT \SOH(\rR\ENQlimitB\t\n\
    \\a_parentB\b\n\
    \\ACK_afterB\DC4\n\
    \\DC2_hierarchy_version\"\172\SOH\n\
    \\DC4ChildrenPageResponse\DC2+\n\
    \\DC1hierarchy_version\CAN\SOH \SOH(\fR\DLEhierarchyVersion\DC24\n\
    \\bchildren\CAN\STX \ETX(\v2\CAN.acyclic.stream.v2.ChildR\bchildren\DC2\"\n\
    \\n\
    \next_after\CAN\ETX \SOH(\tH\NULR\tnextAfter\136\SOH\SOHB\r\n\
    \\v_next_after\"?\n\
    \\rTailCondition\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\SUB\n\
    \\bexpected\CAN\STX \SOH(\EOTR\bexpected\"%\n\
    \\SIAbsentCondition\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\"\148\SOH\n\
    \\SICommitCondition\DC26\n\
    \\EOTtail\CAN\SOH \SOH(\v2 .acyclic.stream.v2.TailConditionH\NULR\EOTtail\DC2<\n\
    \\ACKabsent\CAN\STX \SOH(\v2\".acyclic.stream.v2.AbsentConditionH\NULR\ACKabsentB\v\n\
    \\tcondition\">\n\
    \\SOAppendMutation\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\CAN\n\
    \\arecords\CAN\STX \ETX(\fR\arecords\"{\n\
    \\fForkMutation\DC2\SYN\n\
    \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
    \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\ETB\n\
    \\aat_tail\CAN\ETX \SOH(\EOTR\ACKatTail\DC2\CAN\n\
    \\arecords\CAN\EOT \ETX(\fR\arecords\"\170\SOH\n\
    \\SOCommitMutation\DC2;\n\
    \\ACKappend\CAN\SOH \SOH(\v2!.acyclic.stream.v2.AppendMutationH\NULR\ACKappend\DC25\n\
    \\EOTfork\CAN\STX \SOH(\v2\US.acyclic.stream.v2.ForkMutationH\NULR\EOTforkB\n\
    \\n\
    \\bmutationJ\EOT\b\ETX\DLE\EOTJ\EOT\b\EOT\DLE\ENQR\EOTtrimR\ACKdelete\"\141\STX\n\
    \\rCommitRequest\DC2B\n\
    \\n\
    \conditions\CAN\SOH \ETX(\v2\".acyclic.stream.v2.CommitConditionR\n\
    \conditions\DC2?\n\
    \\tmutations\CAN\STX \ETX(\v2!.acyclic.stream.v2.CommitMutationR\tmutations\DC2'\n\
    \\SIidempotency_key\CAN\ETX \SOH(\fR\SOidempotencyKey\DC25\n\
    \\DC4deadline_unix_millis\CAN\EOT \SOH(\EOTH\NULR\DC2deadlineUnixMillis\136\SOH\SOHB\ETB\n\
    \\NAK_deadline_unix_millis\"\150\SOH\n\
    \\SICommittedAppend\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\DC4\n\
    \\ENQstart\CAN\STX \SOH(\EOTR\ENQstart\DC2\DLE\n\
    \\ETXend\CAN\ETX \SOH(\EOTR\ETXend\DC2\DC2\n\
    \\EOTtail\CAN\EOT \SOH(\EOTR\EOTtail\DC23\n\
    \\arecords\CAN\ENQ \ETX(\v2\EM.acyclic.stream.v2.RecordR\arecords\"\175\SOH\n\
    \\rCommittedFork\DC2\SYN\n\
    \\ACKsource\CAN\SOH \SOH(\tR\ACKsource\DC2 \n\
    \\vdestination\CAN\STX \SOH(\tR\vdestination\DC2\ESC\n\
    \\tforked_at\CAN\ETX \SOH(\EOTR\bforkedAt\DC2\DC2\n\
    \\EOTtail\CAN\EOT \SOH(\EOTR\EOTtail\DC23\n\
    \\arecords\CAN\ENQ \ETX(\v2\EM.acyclic.stream.v2.RecordR\arecords\"\175\SOH\n\
    \\DC1CommittedMutation\DC2<\n\
    \\ACKappend\CAN\SOH \SOH(\v2\".acyclic.stream.v2.CommittedAppendH\NULR\ACKappend\DC26\n\
    \\EOTfork\CAN\STX \SOH(\v2 .acyclic.stream.v2.CommittedForkH\NULR\EOTforkB\n\
    \\n\
    \\bmutationJ\EOT\b\ETX\DLE\EOTJ\EOT\b\EOT\DLE\ENQR\EOTtrimR\ACKdelete\"t\n\
    \\DC1CommittedEnvelope\DC2\ESC\n\
    \\tcommit_id\CAN\SOH \SOH(\fR\bcommitId\DC2B\n\
    \\tmutations\CAN\STX \ETX(\v2$.acyclic.stream.v2.CommittedMutationR\tmutations\"l\n\
    \\DC2TailCommitConflict\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\SUB\n\
    \\bexpected\CAN\STX \SOH(\EOTR\bexpected\DC2\ESC\n\
    \\ACKactual\CAN\ETX \SOH(\EOTH\NULR\ACKactual\136\SOH\SOHB\t\n\
    \\a_actual\"*\n\
    \\DC4ExistsCommitConflict\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\"\171\SOH\n\
    \\SOCommitConflict\DC2;\n\
    \\EOTtail\CAN\SOH \SOH(\v2%.acyclic.stream.v2.TailCommitConflictH\NULR\EOTtail\DC2A\n\
    \\ACKexists\CAN\STX \SOH(\v2'.acyclic.stream.v2.ExistsCommitConflictH\NULR\ACKexistsB\n\
    \\n\
    \\bconflictJ\EOT\b\ETX\DLE\EOTR\aretired\"R\n\
    \\SICommitConflicts\DC2?\n\
    \\tconflicts\CAN\SOH \ETX(\v2!.acyclic.stream.v2.CommitConflictR\tconflicts\"\163\SOH\n\
    \\SOCommitResponse\DC2D\n\
    \\tcommitted\CAN\SOH \SOH(\v2$.acyclic.stream.v2.CommittedEnvelopeH\NULR\tcommitted\DC2@\n\
    \\bconflict\CAN\STX \SOH(\v2\".acyclic.stream.v2.CommitConflictsH\NULR\bconflictB\t\n\
    \\aoutcome\"0\n\
    \\DC1ReadCommitRequest\DC2\ESC\n\
    \\tcommit_id\CAN\SOH \SOH(\fR\bcommitId\"D\n\
    \\EMInspectIdempotencyRequest\DC2'\n\
    \\SIidempotency_key\CAN\SOH \SOH(\fR\SOidempotencyKey\"\189\STX\n\
    \\SYNIdempotencyObservation\DC2'\n\
    \\SIidempotency_key\CAN\SOH \SOH(\fR\SOidempotencyKey\DC2%\n\
    \\SOrequest_digest\CAN\STX \SOH(\fR\rrequestDigest\DC2;\n\
    \\ACKappend\CAN\ETX \SOH(\v2!.acyclic.stream.v2.AppendResponseH\NULR\ACKappend\DC24\n\
    \\EOTfork\CAN\EOT \SOH(\v2\RS.acyclic.stream.v2.ForkReceiptH\NULR\EOTfork\DC2;\n\
    \\ACKcommit\CAN\a \SOH(\v2!.acyclic.stream.v2.CommitResponseH\NULR\ACKcommitB\t\n\
    \\aoutcomeJ\EOT\b\ENQ\DLE\ACKJ\EOT\b\ACK\DLE\aR\EOTtrimR\ACKdelete\"~\n\
    \\SUBInspectIdempotencyResponse\DC2P\n\
    \\vobservation\CAN\SOH \SOH(\v2).acyclic.stream.v2.IdempotencyObservationH\NULR\vobservation\136\SOH\SOHB\SO\n\
    \\f_observation\"k\n\
    \\n\
    \TokenGrant\DC2\DC2\n\
    \\EOTpath\CAN\SOH \SOH(\tR\EOTpath\DC2\GS\n\
    \\asubtree\CAN\STX \SOH(\bH\NULR\asubtree\136\SOH\SOH\DC2\RS\n\
    \\n\
    \operations\CAN\ETX \ETX(\tR\n\
    \operationsB\n\
    \\n\
    \\b_subtree\"h\n\
    \\DC2CreateTokenRequest\DC2\GS\n\
    \\n\
    \expires_in\CAN\SOH \SOH(\tR\texpiresIn\DC23\n\
    \\ENQallow\CAN\STX \ETX(\v2\GS.acyclic.stream.v2.TokenGrantR\ENQallow*\227\SOH\n\
    \\vStreamLimit\DC2\FS\n\
    \\CANSTREAM_LIMIT_UNSPECIFIED\DLE\NUL\DC2#\n\
    \\GSSTREAM_LIMIT_MAX_RECORD_BYTES\DLE\128\128\EOT\DC2\ESC\n\
    \\SYNSTREAM_LIMIT_MAX_ITEMS\DLE\128\b\DC2$\n\
    \\RSSTREAM_LIMIT_MAX_COMMAND_BYTES\DLE\128\192@\DC2+\n\
    \&STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES\DLE\128\STX\DC2!\n\
    \\ESCSTREAM_LIMIT_MAX_PATH_BYTES\DLE\255\255\ETX2\221\ACK\n\
    \\rStreamService\DC2q\n\
    \\DC2InspectIdempotency\DC2,.acyclic.stream.v2.InspectIdempotencyRequest\SUB-.acyclic.stream.v2.InspectIdempotencyResponse\DC2M\n\
    \\ACKAppend\DC2 .acyclic.stream.v2.AppendRequest\SUB!.acyclic.stream.v2.AppendResponse\DC2G\n\
    \\EOTTail\DC2\RS.acyclic.stream.v2.TailRequest\SUB\US.acyclic.stream.v2.TailResponse\DC2F\n\
    \\EOTFork\DC2\RS.acyclic.stream.v2.ForkRequest\SUB\RS.acyclic.stream.v2.ForkReceipt\DC2I\n\
    \\EOTRead\DC2\RS.acyclic.stream.v2.ReadRequest\SUB\US.acyclic.stream.v2.ReadResponse0\SOH\DC2M\n\
    \\ACKFollow\DC2 .acyclic.stream.v2.FollowRequest\SUB\US.acyclic.stream.v2.ReadResponse0\SOH\DC2U\n\
    \\bChildren\DC2\".acyclic.stream.v2.ChildrenRequest\SUB#.acyclic.stream.v2.ChildrenResponse0\SOH\DC2_\n\
    \\fChildrenPage\DC2&.acyclic.stream.v2.ChildrenPageRequest\SUB'.acyclic.stream.v2.ChildrenPageResponse\DC2M\n\
    \\ACKCommit\DC2 .acyclic.stream.v2.CommitRequest\SUB!.acyclic.stream.v2.CommitResponse\DC2X\n\
    \\n\
    \ReadCommit\DC2$.acyclic.stream.v2.ReadCommitRequest\SUB$.acyclic.stream.v2.CommittedEnvelopeB7Z5github.com/acyclic-labs/sdk/go/gen/stream/v2;streamv2J\142G\n\
    \\a\DC2\ENQ\SOH\NUL\138\STX\SOH\n\
    \K\n\
    \\SOH\f\DC2\ETX\SOH\NUL\DC2\SUBA Canonical provider-independent hierarchical Stream v2 contract.\n\
    \\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\STX\NUL\SUB\n\
    \\b\n\
    \\SOH\b\DC2\ETX\EOT\NULL\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\EOT\NULL\n\
    \O\n\
    \\STX\ENQ\NUL\DC2\EOT\a\NUL\SO\SOH\SUBC Canonical bounds shared by Rust and generated TypeScript clients.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETX\a\ENQ\DLE\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETX\b\STX\US\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETX\b\STX\SUB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETX\b\GS\RS\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETX\t\STX(\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETX\t\STX\US\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETX\t\"'\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\STX\DC2\ETX\n\
    \\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\SOH\DC2\ETX\n\
    \\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\STX\DC2\ETX\n\
    \\ESC\US\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ETX\DC2\ETX\v\STX+\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\SOH\DC2\ETX\v\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\STX\DC2\ETX\v#*\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\EOT\DC2\ETX\f\STX/\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\SOH\DC2\ETX\f\STX(\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\STX\DC2\ETX\f+.\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ENQ\DC2\ETX\r\STX&\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ENQ\SOH\DC2\ETX\r\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ENQ\STX\DC2\ETX\r %\n\
    \\n\
    \\n\
    \\STX\EOT\NUL\DC2\EOT\DLE\NUL\NAK\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\DLE\b\SO\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\DC1\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\DC1\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\DC1\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\DC1\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\SOH\DC2\ETX\DC2\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ENQ\DC2\ETX\DC2\STX\a\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\SOH\DC2\ETX\DC2\b\r\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ETX\DC2\ETX\DC2\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\STX\DC2\ETX\DC3\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\STX\ENQ\DC2\ETX\DC3\STX\a\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\STX\SOH\DC2\ETX\DC3\b\DC1\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\STX\ETX\DC2\ETX\DC3\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\ETX\DC2\ETX\DC4\STX!\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\ETX\ENQ\DC2\ETX\DC4\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\ETX\SOH\DC2\ETX\DC4\t\FS\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\ETX\ETX\DC2\ETX\DC4\US \n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\ETB\NUL\FS\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\ETB\b\NAK\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\CAN\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ENQ\DC2\ETX\CAN\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\CAN\t\r\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\CAN\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\SOH\DC2\ETX\EM\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\EOT\DC2\ETX\EM\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ENQ\DC2\ETX\EM\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\SOH\DC2\ETX\EM\DC1\CAN\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ETX\DC2\ETX\EM\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\STX\DC2\ETX\SUB\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\EOT\DC2\ETX\SUB\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\ENQ\DC2\ETX\SUB\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\SOH\DC2\ETX\SUB\DC2\EM\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\ETX\DC2\ETX\SUB\FS\GS\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\ETX\DC2\ETX\ESC\STX%\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\ETX\EOT\DC2\ETX\ESC\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\ETX\ENQ\DC2\ETX\ESC\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\ETX\SOH\DC2\ETX\ESC\DC1 \n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\ETX\ETX\DC2\ETX\ESC#$\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\RS\NUL#\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\RS\b\NAK\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\US\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ENQ\DC2\ETX\US\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\US\t\SO\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\US\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\STX\STX\SOH\DC2\ETX \STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ENQ\DC2\ETX \STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\SOH\DC2\ETX \t\f\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ETX\DC2\ETX \SI\DLE\n\
    \\v\n\
    \\EOT\EOT\STX\STX\STX\DC2\ETX!\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ENQ\DC2\ETX!\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\SOH\DC2\ETX!\t\r\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ETX\DC2\ETX!\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\STX\STX\ETX\DC2\ETX\"\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\ENQ\DC2\ETX\"\STX\a\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\SOH\DC2\ETX\"\b\DC1\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\ETX\DC2\ETX\"\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT%\NUL'\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX%\b\DC4\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX&\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ENQ\DC2\ETX&\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX&\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX&\ETB\CAN\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT)\NUL.\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX)\b\SYN\n\
    \\f\n\
    \\EOT\EOT\EOT\b\NUL\DC2\EOT*\STX-\ETX\n\
    \\f\n\
    \\ENQ\EOT\EOT\b\NUL\SOH\DC2\ETX*\b\SI\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX+\EOT \n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ACK\DC2\ETX+\EOT\DC1\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX+\DC2\ESC\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX+\RS\US\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\SOH\DC2\ETX,\EOT\RS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ACK\DC2\ETX,\EOT\DLE\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\SOH\DC2\ETX,\DC1\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ETX\DC2\ETX,\FS\GS\n\
    \\n\
    \\n\
    \\STX\EOT\ENQ\DC2\EOT0\NUL2\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETX0\b\DC3\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETX1\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ENQ\DC2\ETX1\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETX1\t\r\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETX1\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\ACK\DC2\EOT3\NUL7\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETX3\b\DC4\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX4\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ENQ\DC2\ETX4\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX4\t\r\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX4\DLE\DC1\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\t\DC2\ETX5\STX\r\n\
    \\v\n\
    \\EOT\EOT\ACK\t\NUL\DC2\ETX5\v\f\n\
    \\f\n\
    \\ENQ\EOT\ACK\t\NUL\SOH\DC2\ETX5\v\f\n\
    \\f\n\
    \\ENQ\EOT\ACK\t\NUL\STX\DC2\ETX5\v\f\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\n\
    \\DC2\ETX6\STX\CAN\n\
    \\v\n\
    \\EOT\EOT\ACK\n\
    \\NUL\DC2\ETX6\v\ETB\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOT9\NUL>\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETX9\b\DC3\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETX:\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ENQ\DC2\ETX:\STX\b\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETX:\t\SI\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETX:\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\a\STX\SOH\DC2\ETX;\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ENQ\DC2\ETX;\STX\b\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\SOH\DC2\ETX;\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ETX\DC2\ETX;\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\a\STX\STX\DC2\ETX<\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\EOT\DC2\ETX<\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\ENQ\DC2\ETX<\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\SOH\DC2\ETX<\DC2\EM\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\ETX\DC2\ETX<\FS\GS\n\
    \\v\n\
    \\EOT\EOT\a\STX\ETX\DC2\ETX=\STX%\n\
    \\f\n\
    \\ENQ\EOT\a\STX\ETX\EOT\DC2\ETX=\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\a\STX\ETX\ENQ\DC2\ETX=\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\a\STX\ETX\SOH\DC2\ETX=\DC1 \n\
    \\f\n\
    \\ENQ\EOT\a\STX\ETX\ETX\DC2\ETX=#$\n\
    \\n\
    \\n\
    \\STX\EOT\b\DC2\EOT@\NULF\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETX@\b\DC3\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETXA\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ENQ\DC2\ETXA\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETXA\t\SI\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETXA\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\b\STX\SOH\DC2\ETXB\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ENQ\DC2\ETXB\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\SOH\DC2\ETXB\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ETX\DC2\ETXB\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\b\STX\STX\DC2\ETXC\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\ENQ\DC2\ETXC\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\SOH\DC2\ETXC\t\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\ETX\DC2\ETXC\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\b\STX\ETX\DC2\ETXD\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\ENQ\DC2\ETXD\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\SOH\DC2\ETXD\t\r\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\ETX\DC2\ETXD\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\b\STX\EOT\DC2\ETXE\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\EOT\ENQ\DC2\ETXE\STX\a\n\
    \\f\n\
    \\ENQ\EOT\b\STX\EOT\SOH\DC2\ETXE\b\DC1\n\
    \\f\n\
    \\ENQ\EOT\b\STX\EOT\ETX\DC2\ETXE\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOTH\NULL\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETXH\b\DC3\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETXI\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ENQ\DC2\ETXI\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETXI\t\r\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETXI\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\t\STX\SOH\DC2\ETXJ\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ENQ\DC2\ETXJ\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\SOH\DC2\ETXJ\t\r\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ETX\DC2\ETXJ\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\t\STX\STX\DC2\ETXK\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\t\STX\STX\ENQ\DC2\ETXK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\STX\SOH\DC2\ETXK\t\SO\n\
    \\f\n\
    \\ENQ\EOT\t\STX\STX\ETX\DC2\ETXK\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\EOT\n\
    \\DC2\EOTN\NULQ\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\n\
    \\SOH\DC2\ETXN\b\NAK\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\ETXO\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ENQ\DC2\ETXO\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\ETXO\t\r\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\ETXO\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\SOH\DC2\ETXP\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ENQ\DC2\ETXP\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\SOH\DC2\ETXP\t\r\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ETX\DC2\ETXP\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\v\DC2\EOTS\NULU\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\v\SOH\DC2\ETXS\b\DC4\n\
    \\v\n\
    \\EOT\EOT\v\STX\NUL\DC2\ETXT\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ACK\DC2\ETXT\STX\b\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\ETXT\t\SI\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\ETXT\DC2\DC3\n\
    \\n\
    \\n\
    \\STX\EOT\f\DC2\EOTW\NULZ\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\f\SOH\DC2\ETXW\b\ETB\n\
    \\v\n\
    \\EOT\EOT\f\STX\NUL\DC2\ETXX\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\EOT\DC2\ETXX\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ENQ\DC2\ETXX\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\ETXX\DC2\CAN\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\ETXX\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\f\STX\SOH\DC2\ETXY\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ENQ\DC2\ETXY\STX\b\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\ETXY\t\SO\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\ETXY\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\EOT\r\DC2\EOT\\\NUL^\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\r\SOH\DC2\ETX\\\b\r\n\
    \\v\n\
    \\EOT\EOT\r\STX\NUL\DC2\ETX]\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ENQ\DC2\ETX]\STX\b\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\ETX]\t\r\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\ETX]\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\SO\DC2\EOT_\NULa\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SO\SOH\DC2\ETX_\b\CAN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\NUL\DC2\ETX`\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ACK\DC2\ETX`\STX\a\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\ETX`\b\r\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\ETX`\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\SI\DC2\EOTc\NULh\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SI\SOH\DC2\ETXc\b\ESC\n\
    \\v\n\
    \\EOT\EOT\SI\STX\NUL\DC2\ETXd\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\EOT\DC2\ETXd\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ENQ\DC2\ETXd\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\ETXd\DC2\CAN\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\ETXd\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\SI\STX\SOH\DC2\ETXe\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\EOT\DC2\ETXe\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ENQ\DC2\ETXe\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\SOH\DC2\ETXe\DC2\ETB\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ETX\DC2\ETXe\SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\SI\STX\STX\DC2\ETXf\STX'\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\EOT\DC2\ETXf\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\ENQ\DC2\ETXf\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\SOH\DC2\ETXf\DC1\"\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\ETX\DC2\ETXf%&\n\
    \\v\n\
    \\EOT\EOT\SI\STX\ETX\DC2\ETXg\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\ETX\ENQ\DC2\ETXg\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\ETX\SOH\DC2\ETXg\t\SO\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\ETX\ETX\DC2\ETXg\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\EOT\DLE\DC2\EOTj\NULn\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DLE\SOH\DC2\ETXj\b\FS\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\ETXk\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ENQ\DC2\ETXk\STX\a\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\ETXk\b\EM\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\ETXk\FS\GS\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\SOH\DC2\ETXl\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\EOT\DC2\ETXl\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\ACK\DC2\ETXl\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\SOH\DC2\ETXl\DC1\EM\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\ETX\DC2\ETXl\FS\GS\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\STX\DC2\ETXm\STX!\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\EOT\DC2\ETXm\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\ENQ\DC2\ETXm\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\SOH\DC2\ETXm\DC2\FS\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\ETX\DC2\ETXm\US \n\
    \\n\
    \\n\
    \\STX\EOT\DC1\DC2\EOTp\NULs\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC1\SOH\DC2\ETXp\b\NAK\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\ETXq\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ENQ\DC2\ETXq\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\ETXq\t\r\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\ETXq\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\SOH\DC2\ETXr\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\ENQ\DC2\ETXr\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\SOH\DC2\ETXr\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\ETX\DC2\ETXr\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\DC2\DC2\EOTu\NULw\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC2\SOH\DC2\ETXu\b\ETB\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\ETXv\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ENQ\DC2\ETXv\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\ETXv\t\r\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\ETXv\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\DC3\DC2\EOTy\NUL~\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC3\SOH\DC2\ETXy\b\ETB\n\
    \\f\n\
    \\EOT\EOT\DC3\b\NUL\DC2\EOTz\STX}\ETX\n\
    \\f\n\
    \\ENQ\EOT\DC3\b\NUL\SOH\DC2\ETXz\b\DC1\n\
    \\v\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\ETX{\EOT\ESC\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\ACK\DC2\ETX{\EOT\DC1\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\ETX{\DC2\SYN\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\ETX{\EM\SUB\n\
    \\v\n\
    \\EOT\EOT\DC3\STX\SOH\DC2\ETX|\EOT\US\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\SOH\ACK\DC2\ETX|\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\SOH\SOH\DC2\ETX|\DC4\SUB\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\SOH\ETX\DC2\ETX|\GS\RS\n\
    \\f\n\
    \\STX\EOT\DC4\DC2\ACK\128\SOH\NUL\131\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC4\SOH\DC2\EOT\128\SOH\b\SYN\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\EOT\129\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ENQ\DC2\EOT\129\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\EOT\129\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\EOT\129\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\EOT\130\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\EOT\DC2\EOT\130\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ENQ\DC2\EOT\130\SOH\v\DLE\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\EOT\130\SOH\DC1\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\EOT\130\SOH\ESC\FS\n\
    \\164\SOH\n\
    \\STX\EOT\NAK\DC2\ACK\135\SOH\NUL\140\SOH\SOH\SUB\149\SOH Forks the source's prefix ending at `at_tail` into the new destination,\n\
    \ then appends `records` to the destination, all at one linearization point.\n\
    \\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\135\SOH\b\DC4\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\136\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ENQ\DC2\EOT\136\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\136\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\136\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\SOH\DC2\EOT\137\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ENQ\DC2\EOT\137\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\SOH\DC2\EOT\137\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ETX\DC2\EOT\137\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\STX\DC2\EOT\138\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ENQ\DC2\EOT\138\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\SOH\DC2\EOT\138\SOH\t\DLE\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ETX\DC2\EOT\138\SOH\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\ETX\DC2\EOT\139\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\EOT\DC2\EOT\139\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ENQ\DC2\EOT\139\SOH\v\DLE\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\SOH\DC2\EOT\139\SOH\DC1\CAN\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ETX\DC2\EOT\139\SOH\ESC\FS\n\
    \\f\n\
    \\STX\EOT\SYN\DC2\ACK\142\SOH\NUL\149\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\142\SOH\b\SYN\n\
    \\v\n\
    \\ETX\EOT\SYN\t\DC2\EOT\143\SOH\STX\DLE\n\
    \\f\n\
    \\EOT\EOT\SYN\t\NUL\DC2\EOT\143\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\SYN\t\NUL\SOH\DC2\EOT\143\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\SYN\t\NUL\STX\DC2\EOT\143\SOH\v\f\n\
    \\f\n\
    \\EOT\EOT\SYN\t\SOH\DC2\EOT\143\SOH\SO\SI\n\
    \\r\n\
    \\ENQ\EOT\SYN\t\SOH\SOH\DC2\EOT\143\SOH\SO\SI\n\
    \\r\n\
    \\ENQ\EOT\SYN\t\SOH\STX\DC2\EOT\143\SOH\SO\SI\n\
    \\v\n\
    \\ETX\EOT\SYN\n\
    \\DC2\EOT\144\SOH\STX\FS\n\
    \\f\n\
    \\EOT\EOT\SYN\n\
    \\NUL\DC2\EOT\144\SOH\v\DC1\n\
    \\f\n\
    \\EOT\EOT\SYN\n\
    \\SOH\DC2\EOT\144\SOH\DC3\ESC\n\
    \\SO\n\
    \\EOT\EOT\SYN\b\NUL\DC2\ACK\145\SOH\STX\148\SOH\ETX\n\
    \\r\n\
    \\ENQ\EOT\SYN\b\NUL\SOH\DC2\EOT\145\SOH\b\DLE\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\146\SOH\EOT\RS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ACK\DC2\EOT\146\SOH\EOT\DC2\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\146\SOH\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\146\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\147\SOH\EOT\SUB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ACK\DC2\EOT\147\SOH\EOT\DLE\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\147\SOH\DC1\NAK\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\147\SOH\CAN\EM\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\151\SOH\NUL\156\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\151\SOH\b\NAK\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\152\SOH\STX*\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\EOT\DC2\EOT\152\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ACK\DC2\EOT\152\SOH\v\SUB\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\152\SOH\ESC%\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\152\SOH()\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\153\SOH\STX(\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\EOT\DC2\EOT\153\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ACK\DC2\EOT\153\SOH\v\EM\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\153\SOH\SUB#\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\153\SOH&'\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\STX\DC2\EOT\154\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ENQ\DC2\EOT\154\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\SOH\DC2\EOT\154\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ETX\DC2\EOT\154\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\ETX\DC2\EOT\155\SOH\STX+\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\EOT\DC2\EOT\155\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\ENQ\DC2\EOT\155\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\SOH\DC2\EOT\155\SOH\DC2&\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\ETX\DC2\EOT\155\SOH)*\n\
    \\f\n\
    \\STX\EOT\CAN\DC2\ACK\158\SOH\NUL\164\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\CAN\SOH\DC2\EOT\158\SOH\b\ETB\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\NUL\DC2\EOT\159\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ENQ\DC2\EOT\159\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\SOH\DC2\EOT\159\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ETX\DC2\EOT\159\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\SOH\DC2\EOT\160\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ENQ\DC2\EOT\160\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\SOH\DC2\EOT\160\SOH\t\SO\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ETX\DC2\EOT\160\SOH\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\STX\DC2\EOT\161\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\ENQ\DC2\EOT\161\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\SOH\DC2\EOT\161\SOH\t\f\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\ETX\DC2\EOT\161\SOH\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\ETX\DC2\EOT\162\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\ENQ\DC2\EOT\162\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\SOH\DC2\EOT\162\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\ETX\DC2\EOT\162\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\EOT\DC2\EOT\163\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\EOT\EOT\DC2\EOT\163\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\EOT\ACK\DC2\EOT\163\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\EOT\SOH\DC2\EOT\163\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\EOT\ETX\DC2\EOT\163\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\EM\DC2\ACK\166\SOH\NUL\172\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\EM\SOH\DC2\EOT\166\SOH\b\NAK\n\
    \\f\n\
    \\EOT\EOT\EM\STX\NUL\DC2\EOT\167\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ENQ\DC2\EOT\167\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\SOH\DC2\EOT\167\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ETX\DC2\EOT\167\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\EM\STX\SOH\DC2\EOT\168\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ENQ\DC2\EOT\168\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\SOH\DC2\EOT\168\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ETX\DC2\EOT\168\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\STX\DC2\EOT\169\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ENQ\DC2\EOT\169\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\SOH\DC2\EOT\169\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ETX\DC2\EOT\169\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\ETX\DC2\EOT\170\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ENQ\DC2\EOT\170\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\SOH\DC2\EOT\170\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ETX\DC2\EOT\170\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\EM\STX\EOT\DC2\EOT\171\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\EOT\DC2\EOT\171\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\ACK\DC2\EOT\171\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\SOH\DC2\EOT\171\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\ETX\DC2\EOT\171\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\SUB\DC2\ACK\174\SOH\NUL\181\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SUB\SOH\DC2\EOT\174\SOH\b\EM\n\
    \\v\n\
    \\ETX\EOT\SUB\t\DC2\EOT\175\SOH\STX\DLE\n\
    \\f\n\
    \\EOT\EOT\SUB\t\NUL\DC2\EOT\175\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\SUB\t\NUL\SOH\DC2\EOT\175\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\SUB\t\NUL\STX\DC2\EOT\175\SOH\v\f\n\
    \\f\n\
    \\EOT\EOT\SUB\t\SOH\DC2\EOT\175\SOH\SO\SI\n\
    \\r\n\
    \\ENQ\EOT\SUB\t\SOH\SOH\DC2\EOT\175\SOH\SO\SI\n\
    \\r\n\
    \\ENQ\EOT\SUB\t\SOH\STX\DC2\EOT\175\SOH\SO\SI\n\
    \\v\n\
    \\ETX\EOT\SUB\n\
    \\DC2\EOT\176\SOH\STX\FS\n\
    \\f\n\
    \\EOT\EOT\SUB\n\
    \\NUL\DC2\EOT\176\SOH\v\DC1\n\
    \\f\n\
    \\EOT\EOT\SUB\n\
    \\SOH\DC2\EOT\176\SOH\DC3\ESC\n\
    \\SO\n\
    \\EOT\EOT\SUB\b\NUL\DC2\ACK\177\SOH\STX\180\SOH\ETX\n\
    \\r\n\
    \\ENQ\EOT\SUB\b\NUL\SOH\DC2\EOT\177\SOH\b\DLE\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\NUL\DC2\EOT\178\SOH\EOT\US\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ACK\DC2\EOT\178\SOH\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\SOH\DC2\EOT\178\SOH\DC4\SUB\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ETX\DC2\EOT\178\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\SOH\DC2\EOT\179\SOH\EOT\ESC\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ACK\DC2\EOT\179\SOH\EOT\DC1\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\SOH\DC2\EOT\179\SOH\DC2\SYN\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ETX\DC2\EOT\179\SOH\EM\SUB\n\
    \\f\n\
    \\STX\EOT\ESC\DC2\ACK\183\SOH\NUL\186\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ESC\SOH\DC2\EOT\183\SOH\b\EM\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\NUL\DC2\EOT\184\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ENQ\DC2\EOT\184\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\SOH\DC2\EOT\184\SOH\b\DC1\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ETX\DC2\EOT\184\SOH\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\SOH\DC2\EOT\185\SOH\STX+\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\EOT\DC2\EOT\185\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ACK\DC2\EOT\185\SOH\v\FS\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\SOH\DC2\EOT\185\SOH\GS&\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ETX\DC2\EOT\185\SOH)*\n\
    \\f\n\
    \\STX\EOT\FS\DC2\ACK\188\SOH\NUL\192\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\FS\SOH\DC2\EOT\188\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\FS\STX\NUL\DC2\EOT\189\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ENQ\DC2\EOT\189\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\SOH\DC2\EOT\189\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ETX\DC2\EOT\189\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\FS\STX\SOH\DC2\EOT\190\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ENQ\DC2\EOT\190\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\SOH\DC2\EOT\190\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ETX\DC2\EOT\190\SOH\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT\FS\STX\STX\DC2\EOT\191\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\EOT\DC2\EOT\191\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ENQ\DC2\EOT\191\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\SOH\DC2\EOT\191\SOH\DC2\CAN\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ETX\DC2\EOT\191\SOH\ESC\FS\n\
    \\f\n\
    \\STX\EOT\GS\DC2\ACK\194\SOH\NUL\196\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\GS\SOH\DC2\EOT\194\SOH\b\FS\n\
    \\f\n\
    \\EOT\EOT\GS\STX\NUL\DC2\EOT\195\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ENQ\DC2\EOT\195\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\SOH\DC2\EOT\195\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ETX\DC2\EOT\195\SOH\DLE\DC1\n\
    \\f\n\
    \\STX\EOT\RS\DC2\ACK\197\SOH\NUL\204\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\RS\SOH\DC2\EOT\197\SOH\b\SYN\n\
    \\v\n\
    \\ETX\EOT\RS\t\DC2\EOT\198\SOH\STX\r\n\
    \\f\n\
    \\EOT\EOT\RS\t\NUL\DC2\EOT\198\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\RS\t\NUL\SOH\DC2\EOT\198\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\RS\t\NUL\STX\DC2\EOT\198\SOH\v\f\n\
    \\v\n\
    \\ETX\EOT\RS\n\
    \\DC2\EOT\199\SOH\STX\NAK\n\
    \\f\n\
    \\EOT\EOT\RS\n\
    \\NUL\DC2\EOT\199\SOH\v\DC4\n\
    \\SO\n\
    \\EOT\EOT\RS\b\NUL\DC2\ACK\200\SOH\STX\203\SOH\ETX\n\
    \\r\n\
    \\ENQ\EOT\RS\b\NUL\SOH\DC2\EOT\200\SOH\b\DLE\n\
    \\f\n\
    \\EOT\EOT\RS\STX\NUL\DC2\EOT\201\SOH\EOT \n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ACK\DC2\EOT\201\SOH\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\SOH\DC2\EOT\201\SOH\ETB\ESC\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ETX\DC2\EOT\201\SOH\RS\US\n\
    \\f\n\
    \\EOT\EOT\RS\STX\SOH\DC2\EOT\202\SOH\EOT$\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ACK\DC2\EOT\202\SOH\EOT\CAN\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\SOH\DC2\EOT\202\SOH\EM\US\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ETX\DC2\EOT\202\SOH\"#\n\
    \\f\n\
    \\STX\EOT\US\DC2\ACK\206\SOH\NUL\208\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\US\SOH\DC2\EOT\206\SOH\b\ETB\n\
    \\f\n\
    \\EOT\EOT\US\STX\NUL\DC2\EOT\207\SOH\STX(\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\EOT\DC2\EOT\207\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ACK\DC2\EOT\207\SOH\v\EM\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\SOH\DC2\EOT\207\SOH\SUB#\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ETX\DC2\EOT\207\SOH&'\n\
    \\f\n\
    \\STX\EOT \DC2\ACK\210\SOH\NUL\215\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT \SOH\DC2\EOT\210\SOH\b\SYN\n\
    \\SO\n\
    \\EOT\EOT \b\NUL\DC2\ACK\211\SOH\STX\214\SOH\ETX\n\
    \\r\n\
    \\ENQ\EOT \b\NUL\SOH\DC2\EOT\211\SOH\b\SI\n\
    \\f\n\
    \\EOT\EOT \STX\NUL\DC2\EOT\212\SOH\EOT$\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ACK\DC2\EOT\212\SOH\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\SOH\DC2\EOT\212\SOH\SYN\US\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ETX\DC2\EOT\212\SOH\"#\n\
    \\f\n\
    \\EOT\EOT \STX\SOH\DC2\EOT\213\SOH\EOT!\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ACK\DC2\EOT\213\SOH\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\SOH\DC2\EOT\213\SOH\DC4\FS\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ETX\DC2\EOT\213\SOH\US \n\
    \\f\n\
    \\STX\EOT!\DC2\ACK\217\SOH\NUL\219\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT!\SOH\DC2\EOT\217\SOH\b\EM\n\
    \\f\n\
    \\EOT\EOT!\STX\NUL\DC2\EOT\218\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ENQ\DC2\EOT\218\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\SOH\DC2\EOT\218\SOH\b\DC1\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ETX\DC2\EOT\218\SOH\DC4\NAK\n\
    \\f\n\
    \\STX\EOT\"\DC2\ACK\221\SOH\NUL\223\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\"\SOH\DC2\EOT\221\SOH\b!\n\
    \\f\n\
    \\EOT\EOT\"\STX\NUL\DC2\EOT\222\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ENQ\DC2\EOT\222\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\SOH\DC2\EOT\222\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ETX\DC2\EOT\222\SOH\SUB\ESC\n\
    \\f\n\
    \\STX\EOT#\DC2\ACK\225\SOH\NUL\235\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT#\SOH\DC2\EOT\225\SOH\b\RS\n\
    \\f\n\
    \\EOT\EOT#\STX\NUL\DC2\EOT\226\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ENQ\DC2\EOT\226\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\SOH\DC2\EOT\226\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ETX\DC2\EOT\226\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT#\STX\SOH\DC2\EOT\227\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ENQ\DC2\EOT\227\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\SOH\DC2\EOT\227\SOH\b\SYN\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ETX\DC2\EOT\227\SOH\EM\SUB\n\
    \\v\n\
    \\ETX\EOT#\t\DC2\EOT\228\SOH\STX\DLE\n\
    \\f\n\
    \\EOT\EOT#\t\NUL\DC2\EOT\228\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT#\t\NUL\SOH\DC2\EOT\228\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT#\t\NUL\STX\DC2\EOT\228\SOH\v\f\n\
    \\f\n\
    \\EOT\EOT#\t\SOH\DC2\EOT\228\SOH\SO\SI\n\
    \\r\n\
    \\ENQ\EOT#\t\SOH\SOH\DC2\EOT\228\SOH\SO\SI\n\
    \\r\n\
    \\ENQ\EOT#\t\SOH\STX\DC2\EOT\228\SOH\SO\SI\n\
    \\v\n\
    \\ETX\EOT#\n\
    \\DC2\EOT\229\SOH\STX\FS\n\
    \\f\n\
    \\EOT\EOT#\n\
    \\NUL\DC2\EOT\229\SOH\v\DC1\n\
    \\f\n\
    \\EOT\EOT#\n\
    \\SOH\DC2\EOT\229\SOH\DC3\ESC\n\
    \\SO\n\
    \\EOT\EOT#\b\NUL\DC2\ACK\230\SOH\STX\234\SOH\ETX\n\
    \\r\n\
    \\ENQ\EOT#\b\NUL\SOH\DC2\EOT\230\SOH\b\SI\n\
    \\f\n\
    \\EOT\EOT#\STX\STX\DC2\EOT\231\SOH\EOT\RS\n\
    \\r\n\
    \\ENQ\EOT#\STX\STX\ACK\DC2\EOT\231\SOH\EOT\DC2\n\
    \\r\n\
    \\ENQ\EOT#\STX\STX\SOH\DC2\EOT\231\SOH\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT#\STX\STX\ETX\DC2\EOT\231\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT#\STX\ETX\DC2\EOT\232\SOH\EOT\EM\n\
    \\r\n\
    \\ENQ\EOT#\STX\ETX\ACK\DC2\EOT\232\SOH\EOT\SI\n\
    \\r\n\
    \\ENQ\EOT#\STX\ETX\SOH\DC2\EOT\232\SOH\DLE\DC4\n\
    \\r\n\
    \\ENQ\EOT#\STX\ETX\ETX\DC2\EOT\232\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT#\STX\EOT\DC2\EOT\233\SOH\EOT\RS\n\
    \\r\n\
    \\ENQ\EOT#\STX\EOT\ACK\DC2\EOT\233\SOH\EOT\DC2\n\
    \\r\n\
    \\ENQ\EOT#\STX\EOT\SOH\DC2\EOT\233\SOH\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT#\STX\EOT\ETX\DC2\EOT\233\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT$\DC2\ACK\237\SOH\NUL\239\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT$\SOH\DC2\EOT\237\SOH\b\"\n\
    \\f\n\
    \\EOT\EOT$\STX\NUL\DC2\EOT\238\SOH\STX2\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\EOT\DC2\EOT\238\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ACK\DC2\EOT\238\SOH\v!\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\SOH\DC2\EOT\238\SOH\"-\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ETX\DC2\EOT\238\SOH01\n\
    \\214\SOH\n\
    \\STX\EOT%\DC2\ACK\244\SOH\NUL\248\SOH\SOH\SUB\199\SOH Hosted account token creation request.  This uses the same canonical path\n\
    \ and operation vocabulary as the Stream client, while the hosted adapter\n\
    \ owns its JSON spelling at the Rust/WASM boundary.\n\
    \\n\
    \\v\n\
    \\ETX\EOT%\SOH\DC2\EOT\244\SOH\b\DC2\n\
    \\f\n\
    \\EOT\EOT%\STX\NUL\DC2\EOT\245\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ENQ\DC2\EOT\245\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\SOH\DC2\EOT\245\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ETX\DC2\EOT\245\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT%\STX\SOH\DC2\EOT\246\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\EOT\DC2\EOT\246\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\ENQ\DC2\EOT\246\SOH\v\SI\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\SOH\DC2\EOT\246\SOH\DLE\ETB\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\ETX\DC2\EOT\246\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT%\STX\STX\DC2\EOT\247\SOH\STX!\n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\EOT\DC2\EOT\247\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\ENQ\DC2\EOT\247\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\SOH\DC2\EOT\247\SOH\DC2\FS\n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\ETX\DC2\EOT\247\SOH\US \n\
    \\f\n\
    \\STX\EOT&\DC2\ACK\250\SOH\NUL\253\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT&\SOH\DC2\EOT\250\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT&\STX\NUL\DC2\EOT\251\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ENQ\DC2\EOT\251\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\SOH\DC2\EOT\251\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ETX\DC2\EOT\251\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT&\STX\SOH\DC2\EOT\252\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\EOT\DC2\EOT\252\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ACK\DC2\EOT\252\SOH\v\NAK\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\SOH\DC2\EOT\252\SOH\SYN\ESC\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ETX\DC2\EOT\252\SOH\RS\US\n\
    \\f\n\
    \\STX\ACK\NUL\DC2\ACK\255\SOH\NUL\138\STX\SOH\n\
    \\v\n\
    \\ETX\ACK\NUL\SOH\DC2\EOT\255\SOH\b\NAK\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\EOT\128\STX\STXY\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\EOT\128\STX\ACK\CAN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\EOT\128\STX\EM2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\EOT\128\STX=W\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\SOH\DC2\EOT\129\STX\STX5\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\SOH\DC2\EOT\129\STX\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\STX\DC2\EOT\129\STX\r\SUB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\ETX\DC2\EOT\129\STX%3\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\STX\DC2\EOT\130\STX\STX/\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\SOH\DC2\EOT\130\STX\ACK\n\
    \\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\STX\DC2\EOT\130\STX\v\SYN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\ETX\DC2\EOT\130\STX!-\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ETX\DC2\EOT\131\STX\STX.\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\SOH\DC2\EOT\131\STX\ACK\n\
    \\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\STX\DC2\EOT\131\STX\v\SYN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\ETX\DC2\EOT\131\STX!,\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\EOT\DC2\EOT\132\STX\STX6\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\SOH\DC2\EOT\132\STX\ACK\n\
    \\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\STX\DC2\EOT\132\STX\v\SYN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\ACK\DC2\EOT\132\STX!'\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\ETX\DC2\EOT\132\STX(4\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ENQ\DC2\EOT\133\STX\STX:\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\SOH\DC2\EOT\133\STX\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\STX\DC2\EOT\133\STX\r\SUB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\ACK\DC2\EOT\133\STX%+\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\ETX\DC2\EOT\133\STX,8\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ACK\DC2\EOT\134\STX\STXB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\SOH\DC2\EOT\134\STX\ACK\SO\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\STX\DC2\EOT\134\STX\SI\RS\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\ACK\DC2\EOT\134\STX)/\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\ETX\DC2\EOT\134\STX0@\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\a\DC2\EOT\135\STX\STXG\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\SOH\DC2\EOT\135\STX\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\STX\DC2\EOT\135\STX\DC3&\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\ETX\DC2\EOT\135\STX1E\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\b\DC2\EOT\136\STX\STX5\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\b\SOH\DC2\EOT\136\STX\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\b\STX\DC2\EOT\136\STX\r\SUB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\b\ETX\DC2\EOT\136\STX%3\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\t\DC2\EOT\137\STX\STX@\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\t\SOH\DC2\EOT\137\STX\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\t\STX\DC2\EOT\137\STX\DC1\"\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\t\ETX\DC2\EOT\137\STX->b\ACKproto3"