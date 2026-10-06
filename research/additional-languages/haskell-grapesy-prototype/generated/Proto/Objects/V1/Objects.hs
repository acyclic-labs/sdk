{- This file was auto-generated from objects/v1/objects.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Objects.V1.Objects (
        BucketsService(..), ObjectsService(..), MultipartService(..),
        SnapshotsService(..), AbortMultipartRequest(),
        AbortMultipartResponse(), Bucket(), BucketRef(),
        CompleteMultipartRequest(), CreateBucketRequest(),
        CreateMultipartRequest(), CreateSnapshotRequest(),
        DeleteBucketRequest(), DeleteBucketResponse(),
        DeleteObjectRequest(), DeleteObjectResponse(),
        DestroySnapshotRequest(), DestroySnapshotResponse(), ErrorCode(..),
        ErrorCode(), ErrorCode'UnrecognizedValue, ErrorDetail(),
        ForkBucketRequest(), ForkSnapshotRequest(), GetObjectRequest(),
        GetObjectResponse(), GetObjectResponse'Frame(..),
        _GetObjectResponse'Version, _GetObjectResponse'Body,
        HeadBucketRequest(), HeadObjectRequest(), HeadObjectResponse(),
        ListEntry(), ListObjectsRequest(), ListObjectsResponse(),
        ListPartsRequest(), ListPartsResponse(), ListingMode(..),
        ListingMode(), ListingMode'UnrecognizedValue, MultipartUpload(),
        MutationIdentity(), ObjectMetadata(), ObjectMetadata'UserEntry(),
        ObjectVersion(), ObjectsLimit(..), ObjectsLimit(),
        ObjectsLimit'UnrecognizedValue, Preconditions(),
        Preconditions'Condition(..), _Preconditions'IfAbsent,
        _Preconditions'IfMatch, _Preconditions'IfVersion,
        PutObjectHeader(), PutObjectRequest(), PutObjectRequest'Frame(..),
        _PutObjectRequest'Header, _PutObjectRequest'Body, ReadTarget(),
        ReadTarget'Target(..), _ReadTarget'Bucket, _ReadTarget'Snapshot,
        Snapshot(), SnapshotRef(), UploadPartHeader(), UploadPartRequest(),
        UploadPartRequest'Frame(..), _UploadPartRequest'Header,
        _UploadPartRequest'Body, UploadedPart()
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
import qualified Proto.Google.Protobuf.Timestamp
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' AbortMultipartRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' AbortMultipartRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' AbortMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.uploadId' @:: Lens' AbortMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' AbortMultipartRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' AbortMultipartRequest (Prelude.Maybe MutationIdentity)@ -}
data AbortMultipartRequest
  = AbortMultipartRequest'_constructor {_AbortMultipartRequest'bucket :: !(Prelude.Maybe BucketRef),
                                        _AbortMultipartRequest'objectKey :: !Data.Text.Text,
                                        _AbortMultipartRequest'uploadId :: !Data.Text.Text,
                                        _AbortMultipartRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                        _AbortMultipartRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AbortMultipartRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AbortMultipartRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartRequest'bucket
           (\ x__ y__ -> x__ {_AbortMultipartRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField AbortMultipartRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartRequest'bucket
           (\ x__ y__ -> x__ {_AbortMultipartRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AbortMultipartRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartRequest'objectKey
           (\ x__ y__ -> x__ {_AbortMultipartRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AbortMultipartRequest "uploadId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartRequest'uploadId
           (\ x__ y__ -> x__ {_AbortMultipartRequest'uploadId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AbortMultipartRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartRequest'mutation
           (\ x__ y__ -> x__ {_AbortMultipartRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField AbortMultipartRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartRequest'mutation
           (\ x__ y__ -> x__ {_AbortMultipartRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message AbortMultipartRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.AbortMultipartRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKAbortMultipartRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2@\n\
      \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor AbortMultipartRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor AbortMultipartRequest
        uploadId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "upload_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"uploadId")) ::
              Data.ProtoLens.FieldDescriptor AbortMultipartRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor AbortMultipartRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, uploadId__field_descriptor),
           (Data.ProtoLens.Tag 4, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AbortMultipartRequest'_unknownFields
        (\ x__ y__ -> x__ {_AbortMultipartRequest'_unknownFields = y__})
  defMessage
    = AbortMultipartRequest'_constructor
        {_AbortMultipartRequest'bucket = Prelude.Nothing,
         _AbortMultipartRequest'objectKey = Data.ProtoLens.fieldDefault,
         _AbortMultipartRequest'uploadId = Data.ProtoLens.fieldDefault,
         _AbortMultipartRequest'mutation = Prelude.Nothing,
         _AbortMultipartRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AbortMultipartRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser AbortMultipartRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "upload_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"uploadId") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "AbortMultipartRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"uploadId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                            ((Prelude..)
                               (\ bs
                                  -> (Data.Monoid.<>)
                                       (Data.ProtoLens.Encoding.Bytes.putVarInt
                                          (Prelude.fromIntegral (Data.ByteString.length bs)))
                                       (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                               Data.Text.Encoding.encodeUtf8 _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                ((Prelude..)
                                   (\ bs
                                      -> (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt
                                              (Prelude.fromIntegral (Data.ByteString.length bs)))
                                           (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                   Data.ProtoLens.encodeMessage _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData AbortMultipartRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AbortMultipartRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_AbortMultipartRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_AbortMultipartRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_AbortMultipartRequest'uploadId x__)
                      (Control.DeepSeq.deepseq
                         (_AbortMultipartRequest'mutation x__) ()))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.existed' @:: Lens' AbortMultipartResponse Prelude.Bool@ -}
data AbortMultipartResponse
  = AbortMultipartResponse'_constructor {_AbortMultipartResponse'existed :: !Prelude.Bool,
                                         _AbortMultipartResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AbortMultipartResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AbortMultipartResponse "existed" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AbortMultipartResponse'existed
           (\ x__ y__ -> x__ {_AbortMultipartResponse'existed = y__}))
        Prelude.id
instance Data.ProtoLens.Message AbortMultipartResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.AbortMultipartResponse"
  packedMessageDescriptor _
    = "\n\
      \\SYNAbortMultipartResponse\DC2\CAN\n\
      \\aexisted\CAN\SOH \SOH(\bR\aexisted"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        existed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "existed"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"existed")) ::
              Data.ProtoLens.FieldDescriptor AbortMultipartResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, existed__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AbortMultipartResponse'_unknownFields
        (\ x__ y__ -> x__ {_AbortMultipartResponse'_unknownFields = y__})
  defMessage
    = AbortMultipartResponse'_constructor
        {_AbortMultipartResponse'existed = Data.ProtoLens.fieldDefault,
         _AbortMultipartResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AbortMultipartResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser AbortMultipartResponse
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
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "existed"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"existed") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "AbortMultipartResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"existed") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                         _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData AbortMultipartResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AbortMultipartResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_AbortMultipartResponse'existed x__) ())
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' Bucket BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' Bucket (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.createdAt' @:: Lens' Bucket Proto.Google.Protobuf.Timestamp.Timestamp@
         * 'Proto.Objects.V1.Objects_Fields.maybe'createdAt' @:: Lens' Bucket (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp)@ -}
data Bucket
  = Bucket'_constructor {_Bucket'bucket :: !(Prelude.Maybe BucketRef),
                         _Bucket'createdAt :: !(Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp),
                         _Bucket'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Bucket where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Bucket "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Bucket'bucket (\ x__ y__ -> x__ {_Bucket'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField Bucket "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Bucket'bucket (\ x__ y__ -> x__ {_Bucket'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Bucket "createdAt" Proto.Google.Protobuf.Timestamp.Timestamp where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Bucket'createdAt (\ x__ y__ -> x__ {_Bucket'createdAt = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField Bucket "maybe'createdAt" (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Bucket'createdAt (\ x__ y__ -> x__ {_Bucket'createdAt = y__}))
        Prelude.id
instance Data.ProtoLens.Message Bucket where
  messageName _ = Data.Text.pack "acyclic.objects.v1.Bucket"
  packedMessageDescriptor _
    = "\n\
      \\ACKBucket\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC29\n\
      \\n\
      \created_at\CAN\STX \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor Bucket
        createdAt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created_at"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Proto.Google.Protobuf.Timestamp.Timestamp)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'createdAt")) ::
              Data.ProtoLens.FieldDescriptor Bucket
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, createdAt__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Bucket'_unknownFields
        (\ x__ y__ -> x__ {_Bucket'_unknownFields = y__})
  defMessage
    = Bucket'_constructor
        {_Bucket'bucket = Prelude.Nothing,
         _Bucket'createdAt = Prelude.Nothing, _Bucket'_unknownFields = []}
  parseMessage
    = let
        loop :: Bucket -> Data.ProtoLens.Encoding.Bytes.Parser Bucket
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "created_at"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"createdAt") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Bucket"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view
                       (Data.ProtoLens.Field.field @"maybe'createdAt") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData Bucket where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Bucket'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Bucket'bucket x__)
                (Control.DeepSeq.deepseq (_Bucket'createdAt x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucketId' @:: Lens' BucketRef Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.name' @:: Lens' BucketRef Data.Text.Text@ -}
data BucketRef
  = BucketRef'_constructor {_BucketRef'bucketId :: !Data.Text.Text,
                            _BucketRef'name :: !Data.Text.Text,
                            _BucketRef'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show BucketRef where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField BucketRef "bucketId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _BucketRef'bucketId (\ x__ y__ -> x__ {_BucketRef'bucketId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField BucketRef "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _BucketRef'name (\ x__ y__ -> x__ {_BucketRef'name = y__}))
        Prelude.id
instance Data.ProtoLens.Message BucketRef where
  messageName _ = Data.Text.pack "acyclic.objects.v1.BucketRef"
  packedMessageDescriptor _
    = "\n\
      \\tBucketRef\DC2\ESC\n\
      \\tbucket_id\CAN\SOH \SOH(\tR\bbucketId\DC2\DC2\n\
      \\EOTname\CAN\STX \SOH(\tR\EOTname"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucketId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"bucketId")) ::
              Data.ProtoLens.FieldDescriptor BucketRef
        name__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "name"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"name")) ::
              Data.ProtoLens.FieldDescriptor BucketRef
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucketId__field_descriptor),
           (Data.ProtoLens.Tag 2, name__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _BucketRef'_unknownFields
        (\ x__ y__ -> x__ {_BucketRef'_unknownFields = y__})
  defMessage
    = BucketRef'_constructor
        {_BucketRef'bucketId = Data.ProtoLens.fieldDefault,
         _BucketRef'name = Data.ProtoLens.fieldDefault,
         _BucketRef'_unknownFields = []}
  parseMessage
    = let
        loop :: BucketRef -> Data.ProtoLens.Encoding.Bytes.Parser BucketRef
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
                                       "bucket_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"bucketId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "name"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"name") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "BucketRef"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"bucketId") _x
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"name") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData BucketRef where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_BucketRef'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_BucketRef'bucketId x__)
                (Control.DeepSeq.deepseq (_BucketRef'name x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' CompleteMultipartRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' CompleteMultipartRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' CompleteMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.uploadId' @:: Lens' CompleteMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.parts' @:: Lens' CompleteMultipartRequest [UploadedPart]@
         * 'Proto.Objects.V1.Objects_Fields.vec'parts' @:: Lens' CompleteMultipartRequest (Data.Vector.Vector UploadedPart)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' CompleteMultipartRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' CompleteMultipartRequest (Prelude.Maybe MutationIdentity)@ -}
data CompleteMultipartRequest
  = CompleteMultipartRequest'_constructor {_CompleteMultipartRequest'bucket :: !(Prelude.Maybe BucketRef),
                                           _CompleteMultipartRequest'objectKey :: !Data.Text.Text,
                                           _CompleteMultipartRequest'uploadId :: !Data.Text.Text,
                                           _CompleteMultipartRequest'parts :: !(Data.Vector.Vector UploadedPart),
                                           _CompleteMultipartRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                           _CompleteMultipartRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CompleteMultipartRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'bucket
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'bucket
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'objectKey
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "uploadId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'uploadId
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'uploadId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "parts" [UploadedPart] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'parts
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'parts = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "vec'parts" (Data.Vector.Vector UploadedPart) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'parts
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'parts = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'mutation
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'mutation
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message CompleteMultipartRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.CompleteMultipartRequest"
  packedMessageDescriptor _
    = "\n\
      \\CANCompleteMultipartRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC26\n\
      \\ENQparts\CAN\EOT \ETX(\v2 .acyclic.objects.v1.UploadedPartR\ENQparts\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor CompleteMultipartRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor CompleteMultipartRequest
        uploadId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "upload_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"uploadId")) ::
              Data.ProtoLens.FieldDescriptor CompleteMultipartRequest
        parts__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "parts"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor UploadedPart)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"parts")) ::
              Data.ProtoLens.FieldDescriptor CompleteMultipartRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor CompleteMultipartRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, uploadId__field_descriptor),
           (Data.ProtoLens.Tag 4, parts__field_descriptor),
           (Data.ProtoLens.Tag 5, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CompleteMultipartRequest'_unknownFields
        (\ x__ y__ -> x__ {_CompleteMultipartRequest'_unknownFields = y__})
  defMessage
    = CompleteMultipartRequest'_constructor
        {_CompleteMultipartRequest'bucket = Prelude.Nothing,
         _CompleteMultipartRequest'objectKey = Data.ProtoLens.fieldDefault,
         _CompleteMultipartRequest'uploadId = Data.ProtoLens.fieldDefault,
         _CompleteMultipartRequest'parts = Data.Vector.Generic.empty,
         _CompleteMultipartRequest'mutation = Prelude.Nothing,
         _CompleteMultipartRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CompleteMultipartRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld UploadedPart
             -> Data.ProtoLens.Encoding.Bytes.Parser CompleteMultipartRequest
        loop x mutable'parts
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'parts <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'parts)
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
                              (Data.ProtoLens.Field.field @"vec'parts") frozen'parts x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "bucket"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                                  mutable'parts
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                                  mutable'parts
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "upload_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"uploadId") y x)
                                  mutable'parts
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "parts"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'parts y)
                                loop x v
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                                  mutable'parts
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'parts
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'parts <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'parts)
          "CompleteMultipartRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"uploadId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
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
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                 ((Prelude..)
                                    (\ bs
                                       -> (Data.Monoid.<>)
                                            (Data.ProtoLens.Encoding.Bytes.putVarInt
                                               (Prelude.fromIntegral (Data.ByteString.length bs)))
                                            (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                    Data.ProtoLens.encodeMessage _v))
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'parts") _x))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                   ((Prelude..)
                                      (\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                      Data.ProtoLens.encodeMessage _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData CompleteMultipartRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CompleteMultipartRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CompleteMultipartRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_CompleteMultipartRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_CompleteMultipartRequest'uploadId x__)
                      (Control.DeepSeq.deepseq
                         (_CompleteMultipartRequest'parts x__)
                         (Control.DeepSeq.deepseq
                            (_CompleteMultipartRequest'mutation x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.name' @:: Lens' CreateBucketRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' CreateBucketRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' CreateBucketRequest (Prelude.Maybe MutationIdentity)@ -}
data CreateBucketRequest
  = CreateBucketRequest'_constructor {_CreateBucketRequest'name :: !Data.Text.Text,
                                      _CreateBucketRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                      _CreateBucketRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateBucketRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateBucketRequest "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateBucketRequest'name
           (\ x__ y__ -> x__ {_CreateBucketRequest'name = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateBucketRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateBucketRequest'mutation
           (\ x__ y__ -> x__ {_CreateBucketRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateBucketRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateBucketRequest'mutation
           (\ x__ y__ -> x__ {_CreateBucketRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateBucketRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.CreateBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3CreateBucketRequest\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2@\n\
      \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        name__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "name"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"name")) ::
              Data.ProtoLens.FieldDescriptor CreateBucketRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor CreateBucketRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, name__field_descriptor),
           (Data.ProtoLens.Tag 2, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateBucketRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateBucketRequest'_unknownFields = y__})
  defMessage
    = CreateBucketRequest'_constructor
        {_CreateBucketRequest'name = Data.ProtoLens.fieldDefault,
         _CreateBucketRequest'mutation = Prelude.Nothing,
         _CreateBucketRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateBucketRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CreateBucketRequest
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
                                       "name"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"name") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CreateBucketRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"name") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData CreateBucketRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateBucketRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateBucketRequest'name x__)
                (Control.DeepSeq.deepseq (_CreateBucketRequest'mutation x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' CreateMultipartRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' CreateMultipartRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' CreateMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.metadata' @:: Lens' CreateMultipartRequest ObjectMetadata@
         * 'Proto.Objects.V1.Objects_Fields.maybe'metadata' @:: Lens' CreateMultipartRequest (Prelude.Maybe ObjectMetadata)@
         * 'Proto.Objects.V1.Objects_Fields.preconditions' @:: Lens' CreateMultipartRequest Preconditions@
         * 'Proto.Objects.V1.Objects_Fields.maybe'preconditions' @:: Lens' CreateMultipartRequest (Prelude.Maybe Preconditions)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' CreateMultipartRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' CreateMultipartRequest (Prelude.Maybe MutationIdentity)@ -}
data CreateMultipartRequest
  = CreateMultipartRequest'_constructor {_CreateMultipartRequest'bucket :: !(Prelude.Maybe BucketRef),
                                         _CreateMultipartRequest'objectKey :: !Data.Text.Text,
                                         _CreateMultipartRequest'metadata :: !(Prelude.Maybe ObjectMetadata),
                                         _CreateMultipartRequest'preconditions :: !(Prelude.Maybe Preconditions),
                                         _CreateMultipartRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                         _CreateMultipartRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateMultipartRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'bucket
           (\ x__ y__ -> x__ {_CreateMultipartRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'bucket
           (\ x__ y__ -> x__ {_CreateMultipartRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'objectKey
           (\ x__ y__ -> x__ {_CreateMultipartRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "metadata" ObjectMetadata where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'metadata
           (\ x__ y__ -> x__ {_CreateMultipartRequest'metadata = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "maybe'metadata" (Prelude.Maybe ObjectMetadata) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'metadata
           (\ x__ y__ -> x__ {_CreateMultipartRequest'metadata = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "preconditions" Preconditions where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'preconditions
           (\ x__ y__ -> x__ {_CreateMultipartRequest'preconditions = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "maybe'preconditions" (Prelude.Maybe Preconditions) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'preconditions
           (\ x__ y__ -> x__ {_CreateMultipartRequest'preconditions = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'mutation
           (\ x__ y__ -> x__ {_CreateMultipartRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMultipartRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMultipartRequest'mutation
           (\ x__ y__ -> x__ {_CreateMultipartRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateMultipartRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.CreateMultipartRequest"
  packedMessageDescriptor _
    = "\n\
      \\SYNCreateMultipartRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
      \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v1.ObjectMetadataR\bmetadata\DC2G\n\
      \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v1.PreconditionsR\rpreconditions\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor CreateMultipartRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor CreateMultipartRequest
        metadata__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metadata"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectMetadata)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'metadata")) ::
              Data.ProtoLens.FieldDescriptor CreateMultipartRequest
        preconditions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "preconditions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Preconditions)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'preconditions")) ::
              Data.ProtoLens.FieldDescriptor CreateMultipartRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor CreateMultipartRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, metadata__field_descriptor),
           (Data.ProtoLens.Tag 4, preconditions__field_descriptor),
           (Data.ProtoLens.Tag 5, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateMultipartRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateMultipartRequest'_unknownFields = y__})
  defMessage
    = CreateMultipartRequest'_constructor
        {_CreateMultipartRequest'bucket = Prelude.Nothing,
         _CreateMultipartRequest'objectKey = Data.ProtoLens.fieldDefault,
         _CreateMultipartRequest'metadata = Prelude.Nothing,
         _CreateMultipartRequest'preconditions = Prelude.Nothing,
         _CreateMultipartRequest'mutation = Prelude.Nothing,
         _CreateMultipartRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateMultipartRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CreateMultipartRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "metadata"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"metadata") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "preconditions"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"preconditions") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CreateMultipartRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'metadata") _x
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
                                Data.ProtoLens.encodeMessage _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'preconditions") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                ((Prelude..)
                                   (\ bs
                                      -> (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt
                                              (Prelude.fromIntegral (Data.ByteString.length bs)))
                                           (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                   Data.ProtoLens.encodeMessage _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                   ((Prelude..)
                                      (\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                      Data.ProtoLens.encodeMessage _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData CreateMultipartRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateMultipartRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateMultipartRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_CreateMultipartRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_CreateMultipartRequest'metadata x__)
                      (Control.DeepSeq.deepseq
                         (_CreateMultipartRequest'preconditions x__)
                         (Control.DeepSeq.deepseq
                            (_CreateMultipartRequest'mutation x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' CreateSnapshotRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' CreateSnapshotRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' CreateSnapshotRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' CreateSnapshotRequest (Prelude.Maybe MutationIdentity)@ -}
data CreateSnapshotRequest
  = CreateSnapshotRequest'_constructor {_CreateSnapshotRequest'bucket :: !(Prelude.Maybe BucketRef),
                                        _CreateSnapshotRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                        _CreateSnapshotRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateSnapshotRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateSnapshotRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateSnapshotRequest'bucket
           (\ x__ y__ -> x__ {_CreateSnapshotRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateSnapshotRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateSnapshotRequest'bucket
           (\ x__ y__ -> x__ {_CreateSnapshotRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateSnapshotRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateSnapshotRequest'mutation
           (\ x__ y__ -> x__ {_CreateSnapshotRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateSnapshotRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateSnapshotRequest'mutation
           (\ x__ y__ -> x__ {_CreateSnapshotRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateSnapshotRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.CreateSnapshotRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKCreateSnapshotRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2@\n\
      \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor CreateSnapshotRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor CreateSnapshotRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateSnapshotRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateSnapshotRequest'_unknownFields = y__})
  defMessage
    = CreateSnapshotRequest'_constructor
        {_CreateSnapshotRequest'bucket = Prelude.Nothing,
         _CreateSnapshotRequest'mutation = Prelude.Nothing,
         _CreateSnapshotRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateSnapshotRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CreateSnapshotRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CreateSnapshotRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData CreateSnapshotRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateSnapshotRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateSnapshotRequest'bucket x__)
                (Control.DeepSeq.deepseq (_CreateSnapshotRequest'mutation x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' DeleteBucketRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' DeleteBucketRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' DeleteBucketRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' DeleteBucketRequest (Prelude.Maybe MutationIdentity)@ -}
data DeleteBucketRequest
  = DeleteBucketRequest'_constructor {_DeleteBucketRequest'bucket :: !(Prelude.Maybe BucketRef),
                                      _DeleteBucketRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                      _DeleteBucketRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show DeleteBucketRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField DeleteBucketRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteBucketRequest'bucket
           (\ x__ y__ -> x__ {_DeleteBucketRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DeleteBucketRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteBucketRequest'bucket
           (\ x__ y__ -> x__ {_DeleteBucketRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DeleteBucketRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteBucketRequest'mutation
           (\ x__ y__ -> x__ {_DeleteBucketRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DeleteBucketRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteBucketRequest'mutation
           (\ x__ y__ -> x__ {_DeleteBucketRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message DeleteBucketRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.DeleteBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3DeleteBucketRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2@\n\
      \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor DeleteBucketRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor DeleteBucketRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DeleteBucketRequest'_unknownFields
        (\ x__ y__ -> x__ {_DeleteBucketRequest'_unknownFields = y__})
  defMessage
    = DeleteBucketRequest'_constructor
        {_DeleteBucketRequest'bucket = Prelude.Nothing,
         _DeleteBucketRequest'mutation = Prelude.Nothing,
         _DeleteBucketRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          DeleteBucketRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser DeleteBucketRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "DeleteBucketRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData DeleteBucketRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DeleteBucketRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_DeleteBucketRequest'bucket x__)
                (Control.DeepSeq.deepseq (_DeleteBucketRequest'mutation x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.existed' @:: Lens' DeleteBucketResponse Prelude.Bool@ -}
data DeleteBucketResponse
  = DeleteBucketResponse'_constructor {_DeleteBucketResponse'existed :: !Prelude.Bool,
                                       _DeleteBucketResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show DeleteBucketResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField DeleteBucketResponse "existed" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteBucketResponse'existed
           (\ x__ y__ -> x__ {_DeleteBucketResponse'existed = y__}))
        Prelude.id
instance Data.ProtoLens.Message DeleteBucketResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.DeleteBucketResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC4DeleteBucketResponse\DC2\CAN\n\
      \\aexisted\CAN\SOH \SOH(\bR\aexisted"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        existed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "existed"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"existed")) ::
              Data.ProtoLens.FieldDescriptor DeleteBucketResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, existed__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DeleteBucketResponse'_unknownFields
        (\ x__ y__ -> x__ {_DeleteBucketResponse'_unknownFields = y__})
  defMessage
    = DeleteBucketResponse'_constructor
        {_DeleteBucketResponse'existed = Data.ProtoLens.fieldDefault,
         _DeleteBucketResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          DeleteBucketResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser DeleteBucketResponse
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
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "existed"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"existed") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "DeleteBucketResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"existed") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                         _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData DeleteBucketResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DeleteBucketResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_DeleteBucketResponse'existed x__) ())
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' DeleteObjectRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' DeleteObjectRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' DeleteObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.versionId' @:: Lens' DeleteObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.preconditions' @:: Lens' DeleteObjectRequest Preconditions@
         * 'Proto.Objects.V1.Objects_Fields.maybe'preconditions' @:: Lens' DeleteObjectRequest (Prelude.Maybe Preconditions)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' DeleteObjectRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' DeleteObjectRequest (Prelude.Maybe MutationIdentity)@ -}
data DeleteObjectRequest
  = DeleteObjectRequest'_constructor {_DeleteObjectRequest'bucket :: !(Prelude.Maybe BucketRef),
                                      _DeleteObjectRequest'objectKey :: !Data.Text.Text,
                                      _DeleteObjectRequest'versionId :: !Data.Text.Text,
                                      _DeleteObjectRequest'preconditions :: !(Prelude.Maybe Preconditions),
                                      _DeleteObjectRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                      _DeleteObjectRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show DeleteObjectRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'bucket
           (\ x__ y__ -> x__ {_DeleteObjectRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'bucket
           (\ x__ y__ -> x__ {_DeleteObjectRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'objectKey
           (\ x__ y__ -> x__ {_DeleteObjectRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "versionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'versionId
           (\ x__ y__ -> x__ {_DeleteObjectRequest'versionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "preconditions" Preconditions where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'preconditions
           (\ x__ y__ -> x__ {_DeleteObjectRequest'preconditions = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "maybe'preconditions" (Prelude.Maybe Preconditions) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'preconditions
           (\ x__ y__ -> x__ {_DeleteObjectRequest'preconditions = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'mutation
           (\ x__ y__ -> x__ {_DeleteObjectRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DeleteObjectRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectRequest'mutation
           (\ x__ y__ -> x__ {_DeleteObjectRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message DeleteObjectRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.DeleteObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3DeleteObjectRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\GS\n\
      \\n\
      \version_id\CAN\ETX \SOH(\tR\tversionId\DC2G\n\
      \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v1.PreconditionsR\rpreconditions\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectRequest
        versionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"versionId")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectRequest
        preconditions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "preconditions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Preconditions)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'preconditions")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, versionId__field_descriptor),
           (Data.ProtoLens.Tag 4, preconditions__field_descriptor),
           (Data.ProtoLens.Tag 5, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DeleteObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_DeleteObjectRequest'_unknownFields = y__})
  defMessage
    = DeleteObjectRequest'_constructor
        {_DeleteObjectRequest'bucket = Prelude.Nothing,
         _DeleteObjectRequest'objectKey = Data.ProtoLens.fieldDefault,
         _DeleteObjectRequest'versionId = Data.ProtoLens.fieldDefault,
         _DeleteObjectRequest'preconditions = Prelude.Nothing,
         _DeleteObjectRequest'mutation = Prelude.Nothing,
         _DeleteObjectRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          DeleteObjectRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser DeleteObjectRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "version_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"versionId") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "preconditions"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"preconditions") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "DeleteObjectRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"versionId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
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
                             (Data.ProtoLens.Field.field @"maybe'preconditions") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                ((Prelude..)
                                   (\ bs
                                      -> (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt
                                              (Prelude.fromIntegral (Data.ByteString.length bs)))
                                           (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                   Data.ProtoLens.encodeMessage _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                   ((Prelude..)
                                      (\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                      Data.ProtoLens.encodeMessage _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData DeleteObjectRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DeleteObjectRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_DeleteObjectRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_DeleteObjectRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_DeleteObjectRequest'versionId x__)
                      (Control.DeepSeq.deepseq
                         (_DeleteObjectRequest'preconditions x__)
                         (Control.DeepSeq.deepseq
                            (_DeleteObjectRequest'mutation x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.existed' @:: Lens' DeleteObjectResponse Prelude.Bool@
         * 'Proto.Objects.V1.Objects_Fields.version' @:: Lens' DeleteObjectResponse ObjectVersion@
         * 'Proto.Objects.V1.Objects_Fields.maybe'version' @:: Lens' DeleteObjectResponse (Prelude.Maybe ObjectVersion)@ -}
data DeleteObjectResponse
  = DeleteObjectResponse'_constructor {_DeleteObjectResponse'existed :: !Prelude.Bool,
                                       _DeleteObjectResponse'version :: !(Prelude.Maybe ObjectVersion),
                                       _DeleteObjectResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show DeleteObjectResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField DeleteObjectResponse "existed" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectResponse'existed
           (\ x__ y__ -> x__ {_DeleteObjectResponse'existed = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DeleteObjectResponse "version" ObjectVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectResponse'version
           (\ x__ y__ -> x__ {_DeleteObjectResponse'version = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DeleteObjectResponse "maybe'version" (Prelude.Maybe ObjectVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DeleteObjectResponse'version
           (\ x__ y__ -> x__ {_DeleteObjectResponse'version = y__}))
        Prelude.id
instance Data.ProtoLens.Message DeleteObjectResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.DeleteObjectResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC4DeleteObjectResponse\DC2\CAN\n\
      \\aexisted\CAN\SOH \SOH(\bR\aexisted\DC2;\n\
      \\aversion\CAN\STX \SOH(\v2!.acyclic.objects.v1.ObjectVersionR\aversion"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        existed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "existed"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"existed")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectResponse
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'version")) ::
              Data.ProtoLens.FieldDescriptor DeleteObjectResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, existed__field_descriptor),
           (Data.ProtoLens.Tag 2, version__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DeleteObjectResponse'_unknownFields
        (\ x__ y__ -> x__ {_DeleteObjectResponse'_unknownFields = y__})
  defMessage
    = DeleteObjectResponse'_constructor
        {_DeleteObjectResponse'existed = Data.ProtoLens.fieldDefault,
         _DeleteObjectResponse'version = Prelude.Nothing,
         _DeleteObjectResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          DeleteObjectResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser DeleteObjectResponse
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
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "existed"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"existed") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "version"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"version") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "DeleteObjectResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"existed") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                         _v))
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'version") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData DeleteObjectResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DeleteObjectResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_DeleteObjectResponse'existed x__)
                (Control.DeepSeq.deepseq (_DeleteObjectResponse'version x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.snapshot' @:: Lens' DestroySnapshotRequest SnapshotRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'snapshot' @:: Lens' DestroySnapshotRequest (Prelude.Maybe SnapshotRef)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' DestroySnapshotRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' DestroySnapshotRequest (Prelude.Maybe MutationIdentity)@ -}
data DestroySnapshotRequest
  = DestroySnapshotRequest'_constructor {_DestroySnapshotRequest'snapshot :: !(Prelude.Maybe SnapshotRef),
                                         _DestroySnapshotRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                         _DestroySnapshotRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show DestroySnapshotRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField DestroySnapshotRequest "snapshot" SnapshotRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DestroySnapshotRequest'snapshot
           (\ x__ y__ -> x__ {_DestroySnapshotRequest'snapshot = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DestroySnapshotRequest "maybe'snapshot" (Prelude.Maybe SnapshotRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DestroySnapshotRequest'snapshot
           (\ x__ y__ -> x__ {_DestroySnapshotRequest'snapshot = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField DestroySnapshotRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DestroySnapshotRequest'mutation
           (\ x__ y__ -> x__ {_DestroySnapshotRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField DestroySnapshotRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DestroySnapshotRequest'mutation
           (\ x__ y__ -> x__ {_DestroySnapshotRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message DestroySnapshotRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.DestroySnapshotRequest"
  packedMessageDescriptor _
    = "\n\
      \\SYNDestroySnapshotRequest\DC2;\n\
      \\bsnapshot\CAN\SOH \SOH(\v2\US.acyclic.objects.v1.SnapshotRefR\bsnapshot\DC2@\n\
      \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        snapshot__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "snapshot"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SnapshotRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'snapshot")) ::
              Data.ProtoLens.FieldDescriptor DestroySnapshotRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor DestroySnapshotRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, snapshot__field_descriptor),
           (Data.ProtoLens.Tag 2, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DestroySnapshotRequest'_unknownFields
        (\ x__ y__ -> x__ {_DestroySnapshotRequest'_unknownFields = y__})
  defMessage
    = DestroySnapshotRequest'_constructor
        {_DestroySnapshotRequest'snapshot = Prelude.Nothing,
         _DestroySnapshotRequest'mutation = Prelude.Nothing,
         _DestroySnapshotRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          DestroySnapshotRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser DestroySnapshotRequest
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
                                       "snapshot"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"snapshot") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "DestroySnapshotRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'snapshot") _x
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
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData DestroySnapshotRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DestroySnapshotRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_DestroySnapshotRequest'snapshot x__)
                (Control.DeepSeq.deepseq
                   (_DestroySnapshotRequest'mutation x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.existed' @:: Lens' DestroySnapshotResponse Prelude.Bool@ -}
data DestroySnapshotResponse
  = DestroySnapshotResponse'_constructor {_DestroySnapshotResponse'existed :: !Prelude.Bool,
                                          _DestroySnapshotResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show DestroySnapshotResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField DestroySnapshotResponse "existed" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _DestroySnapshotResponse'existed
           (\ x__ y__ -> x__ {_DestroySnapshotResponse'existed = y__}))
        Prelude.id
instance Data.ProtoLens.Message DestroySnapshotResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.DestroySnapshotResponse"
  packedMessageDescriptor _
    = "\n\
      \\ETBDestroySnapshotResponse\DC2\CAN\n\
      \\aexisted\CAN\SOH \SOH(\bR\aexisted"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        existed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "existed"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"existed")) ::
              Data.ProtoLens.FieldDescriptor DestroySnapshotResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, existed__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DestroySnapshotResponse'_unknownFields
        (\ x__ y__ -> x__ {_DestroySnapshotResponse'_unknownFields = y__})
  defMessage
    = DestroySnapshotResponse'_constructor
        {_DestroySnapshotResponse'existed = Data.ProtoLens.fieldDefault,
         _DestroySnapshotResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          DestroySnapshotResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser DestroySnapshotResponse
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
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "existed"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"existed") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "DestroySnapshotResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"existed") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                         _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData DestroySnapshotResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DestroySnapshotResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_DestroySnapshotResponse'existed x__) ())
newtype ErrorCode'UnrecognizedValue
  = ErrorCode'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ErrorCode
  = ERROR_CODE_UNSPECIFIED |
    ERROR_CODE_INVALID_ARGUMENT |
    ERROR_CODE_NOT_FOUND |
    ERROR_CODE_ALREADY_EXISTS |
    ERROR_CODE_PRECONDITION_FAILED |
    ERROR_CODE_IDEMPOTENCY_MISMATCH |
    ERROR_CODE_TOKEN_EXPIRED |
    ERROR_CODE_QUOTA_EXCEEDED |
    ERROR_CODE_UNSUPPORTED |
    ERROR_CODE_UNAVAILABLE |
    ErrorCode'Unrecognized !ErrorCode'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ErrorCode where
  maybeToEnum 0 = Prelude.Just ERROR_CODE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just ERROR_CODE_INVALID_ARGUMENT
  maybeToEnum 2 = Prelude.Just ERROR_CODE_NOT_FOUND
  maybeToEnum 3 = Prelude.Just ERROR_CODE_ALREADY_EXISTS
  maybeToEnum 4 = Prelude.Just ERROR_CODE_PRECONDITION_FAILED
  maybeToEnum 5 = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
  maybeToEnum 6 = Prelude.Just ERROR_CODE_TOKEN_EXPIRED
  maybeToEnum 7 = Prelude.Just ERROR_CODE_QUOTA_EXCEEDED
  maybeToEnum 8 = Prelude.Just ERROR_CODE_UNSUPPORTED
  maybeToEnum 9 = Prelude.Just ERROR_CODE_UNAVAILABLE
  maybeToEnum k
    = Prelude.Just
        (ErrorCode'Unrecognized
           (ErrorCode'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum ERROR_CODE_UNSPECIFIED = "ERROR_CODE_UNSPECIFIED"
  showEnum ERROR_CODE_INVALID_ARGUMENT
    = "ERROR_CODE_INVALID_ARGUMENT"
  showEnum ERROR_CODE_NOT_FOUND = "ERROR_CODE_NOT_FOUND"
  showEnum ERROR_CODE_ALREADY_EXISTS = "ERROR_CODE_ALREADY_EXISTS"
  showEnum ERROR_CODE_PRECONDITION_FAILED
    = "ERROR_CODE_PRECONDITION_FAILED"
  showEnum ERROR_CODE_IDEMPOTENCY_MISMATCH
    = "ERROR_CODE_IDEMPOTENCY_MISMATCH"
  showEnum ERROR_CODE_TOKEN_EXPIRED = "ERROR_CODE_TOKEN_EXPIRED"
  showEnum ERROR_CODE_QUOTA_EXCEEDED = "ERROR_CODE_QUOTA_EXCEEDED"
  showEnum ERROR_CODE_UNSUPPORTED = "ERROR_CODE_UNSUPPORTED"
  showEnum ERROR_CODE_UNAVAILABLE = "ERROR_CODE_UNAVAILABLE"
  showEnum (ErrorCode'Unrecognized (ErrorCode'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "ERROR_CODE_UNSPECIFIED"
    = Prelude.Just ERROR_CODE_UNSPECIFIED
    | (Prelude.==) k "ERROR_CODE_INVALID_ARGUMENT"
    = Prelude.Just ERROR_CODE_INVALID_ARGUMENT
    | (Prelude.==) k "ERROR_CODE_NOT_FOUND"
    = Prelude.Just ERROR_CODE_NOT_FOUND
    | (Prelude.==) k "ERROR_CODE_ALREADY_EXISTS"
    = Prelude.Just ERROR_CODE_ALREADY_EXISTS
    | (Prelude.==) k "ERROR_CODE_PRECONDITION_FAILED"
    = Prelude.Just ERROR_CODE_PRECONDITION_FAILED
    | (Prelude.==) k "ERROR_CODE_IDEMPOTENCY_MISMATCH"
    = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
    | (Prelude.==) k "ERROR_CODE_TOKEN_EXPIRED"
    = Prelude.Just ERROR_CODE_TOKEN_EXPIRED
    | (Prelude.==) k "ERROR_CODE_QUOTA_EXCEEDED"
    = Prelude.Just ERROR_CODE_QUOTA_EXCEEDED
    | (Prelude.==) k "ERROR_CODE_UNSUPPORTED"
    = Prelude.Just ERROR_CODE_UNSUPPORTED
    | (Prelude.==) k "ERROR_CODE_UNAVAILABLE"
    = Prelude.Just ERROR_CODE_UNAVAILABLE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ErrorCode where
  minBound = ERROR_CODE_UNSPECIFIED
  maxBound = ERROR_CODE_UNAVAILABLE
instance Prelude.Enum ErrorCode where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ErrorCode: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum ERROR_CODE_UNSPECIFIED = 0
  fromEnum ERROR_CODE_INVALID_ARGUMENT = 1
  fromEnum ERROR_CODE_NOT_FOUND = 2
  fromEnum ERROR_CODE_ALREADY_EXISTS = 3
  fromEnum ERROR_CODE_PRECONDITION_FAILED = 4
  fromEnum ERROR_CODE_IDEMPOTENCY_MISMATCH = 5
  fromEnum ERROR_CODE_TOKEN_EXPIRED = 6
  fromEnum ERROR_CODE_QUOTA_EXCEEDED = 7
  fromEnum ERROR_CODE_UNSUPPORTED = 8
  fromEnum ERROR_CODE_UNAVAILABLE = 9
  fromEnum (ErrorCode'Unrecognized (ErrorCode'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ ERROR_CODE_UNAVAILABLE
    = Prelude.error
        "ErrorCode.succ: bad argument ERROR_CODE_UNAVAILABLE. This value would be out of bounds."
  succ ERROR_CODE_UNSPECIFIED = ERROR_CODE_INVALID_ARGUMENT
  succ ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_NOT_FOUND
  succ ERROR_CODE_NOT_FOUND = ERROR_CODE_ALREADY_EXISTS
  succ ERROR_CODE_ALREADY_EXISTS = ERROR_CODE_PRECONDITION_FAILED
  succ ERROR_CODE_PRECONDITION_FAILED
    = ERROR_CODE_IDEMPOTENCY_MISMATCH
  succ ERROR_CODE_IDEMPOTENCY_MISMATCH = ERROR_CODE_TOKEN_EXPIRED
  succ ERROR_CODE_TOKEN_EXPIRED = ERROR_CODE_QUOTA_EXCEEDED
  succ ERROR_CODE_QUOTA_EXCEEDED = ERROR_CODE_UNSUPPORTED
  succ ERROR_CODE_UNSUPPORTED = ERROR_CODE_UNAVAILABLE
  succ (ErrorCode'Unrecognized _)
    = Prelude.error "ErrorCode.succ: bad argument: unrecognized value"
  pred ERROR_CODE_UNSPECIFIED
    = Prelude.error
        "ErrorCode.pred: bad argument ERROR_CODE_UNSPECIFIED. This value would be out of bounds."
  pred ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_UNSPECIFIED
  pred ERROR_CODE_NOT_FOUND = ERROR_CODE_INVALID_ARGUMENT
  pred ERROR_CODE_ALREADY_EXISTS = ERROR_CODE_NOT_FOUND
  pred ERROR_CODE_PRECONDITION_FAILED = ERROR_CODE_ALREADY_EXISTS
  pred ERROR_CODE_IDEMPOTENCY_MISMATCH
    = ERROR_CODE_PRECONDITION_FAILED
  pred ERROR_CODE_TOKEN_EXPIRED = ERROR_CODE_IDEMPOTENCY_MISMATCH
  pred ERROR_CODE_QUOTA_EXCEEDED = ERROR_CODE_TOKEN_EXPIRED
  pred ERROR_CODE_UNSUPPORTED = ERROR_CODE_QUOTA_EXCEEDED
  pred ERROR_CODE_UNAVAILABLE = ERROR_CODE_UNSUPPORTED
  pred (ErrorCode'Unrecognized _)
    = Prelude.error "ErrorCode.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ErrorCode where
  fieldDefault = ERROR_CODE_UNSPECIFIED
instance Control.DeepSeq.NFData ErrorCode where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.code' @:: Lens' ErrorDetail ErrorCode@
         * 'Proto.Objects.V1.Objects_Fields.requestId' @:: Lens' ErrorDetail Data.Text.Text@ -}
data ErrorDetail
  = ErrorDetail'_constructor {_ErrorDetail'code :: !ErrorCode,
                              _ErrorDetail'requestId :: !Data.Text.Text,
                              _ErrorDetail'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ErrorDetail where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ErrorDetail "code" ErrorCode where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ErrorDetail'code (\ x__ y__ -> x__ {_ErrorDetail'code = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ErrorDetail "requestId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ErrorDetail'requestId
           (\ x__ y__ -> x__ {_ErrorDetail'requestId = y__}))
        Prelude.id
instance Data.ProtoLens.Message ErrorDetail where
  messageName _ = Data.Text.pack "acyclic.objects.v1.ErrorDetail"
  packedMessageDescriptor _
    = "\n\
      \\vErrorDetail\DC21\n\
      \\EOTcode\CAN\SOH \SOH(\SO2\GS.acyclic.objects.v1.ErrorCodeR\EOTcode\DC2\GS\n\
      \\n\
      \request_id\CAN\STX \SOH(\tR\trequestId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        code__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "code"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ErrorCode)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"code")) ::
              Data.ProtoLens.FieldDescriptor ErrorDetail
        requestId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "request_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"requestId")) ::
              Data.ProtoLens.FieldDescriptor ErrorDetail
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, code__field_descriptor),
           (Data.ProtoLens.Tag 2, requestId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ErrorDetail'_unknownFields
        (\ x__ y__ -> x__ {_ErrorDetail'_unknownFields = y__})
  defMessage
    = ErrorDetail'_constructor
        {_ErrorDetail'code = Data.ProtoLens.fieldDefault,
         _ErrorDetail'requestId = Data.ProtoLens.fieldDefault,
         _ErrorDetail'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ErrorDetail -> Data.ProtoLens.Encoding.Bytes.Parser ErrorDetail
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
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "code"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"code") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "request_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"requestId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ErrorDetail"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"code") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         ((Prelude..)
                            Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral)
                         Prelude.fromEnum _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"requestId") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ErrorDetail where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ErrorDetail'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ErrorDetail'code x__)
                (Control.DeepSeq.deepseq (_ErrorDetail'requestId x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.source' @:: Lens' ForkBucketRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'source' @:: Lens' ForkBucketRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.destinationName' @:: Lens' ForkBucketRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' ForkBucketRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' ForkBucketRequest (Prelude.Maybe MutationIdentity)@ -}
data ForkBucketRequest
  = ForkBucketRequest'_constructor {_ForkBucketRequest'source :: !(Prelude.Maybe BucketRef),
                                    _ForkBucketRequest'destinationName :: !Data.Text.Text,
                                    _ForkBucketRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                    _ForkBucketRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkBucketRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkBucketRequest "source" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkBucketRequest'source
           (\ x__ y__ -> x__ {_ForkBucketRequest'source = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkBucketRequest "maybe'source" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkBucketRequest'source
           (\ x__ y__ -> x__ {_ForkBucketRequest'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkBucketRequest "destinationName" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkBucketRequest'destinationName
           (\ x__ y__ -> x__ {_ForkBucketRequest'destinationName = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkBucketRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkBucketRequest'mutation
           (\ x__ y__ -> x__ {_ForkBucketRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkBucketRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkBucketRequest'mutation
           (\ x__ y__ -> x__ {_ForkBucketRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkBucketRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ForkBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1ForkBucketRequest\DC25\n\
      \\ACKsource\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKsource\DC2)\n\
      \\DLEdestination_name\CAN\STX \SOH(\tR\SIdestinationName\DC2@\n\
      \\bmutation\CAN\ETX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'source")) ::
              Data.ProtoLens.FieldDescriptor ForkBucketRequest
        destinationName__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destination_name"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"destinationName")) ::
              Data.ProtoLens.FieldDescriptor ForkBucketRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor ForkBucketRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, destinationName__field_descriptor),
           (Data.ProtoLens.Tag 3, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkBucketRequest'_unknownFields
        (\ x__ y__ -> x__ {_ForkBucketRequest'_unknownFields = y__})
  defMessage
    = ForkBucketRequest'_constructor
        {_ForkBucketRequest'source = Prelude.Nothing,
         _ForkBucketRequest'destinationName = Data.ProtoLens.fieldDefault,
         _ForkBucketRequest'mutation = Prelude.Nothing,
         _ForkBucketRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkBucketRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ForkBucketRequest
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
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "destination_name"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"destinationName") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ForkBucketRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'source") _x
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
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"destinationName") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                                Data.ProtoLens.encodeMessage _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ForkBucketRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkBucketRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkBucketRequest'source x__)
                (Control.DeepSeq.deepseq
                   (_ForkBucketRequest'destinationName x__)
                   (Control.DeepSeq.deepseq (_ForkBucketRequest'mutation x__) ())))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.snapshot' @:: Lens' ForkSnapshotRequest SnapshotRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'snapshot' @:: Lens' ForkSnapshotRequest (Prelude.Maybe SnapshotRef)@
         * 'Proto.Objects.V1.Objects_Fields.destinationName' @:: Lens' ForkSnapshotRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' ForkSnapshotRequest MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' ForkSnapshotRequest (Prelude.Maybe MutationIdentity)@ -}
data ForkSnapshotRequest
  = ForkSnapshotRequest'_constructor {_ForkSnapshotRequest'snapshot :: !(Prelude.Maybe SnapshotRef),
                                      _ForkSnapshotRequest'destinationName :: !Data.Text.Text,
                                      _ForkSnapshotRequest'mutation :: !(Prelude.Maybe MutationIdentity),
                                      _ForkSnapshotRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkSnapshotRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkSnapshotRequest "snapshot" SnapshotRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkSnapshotRequest'snapshot
           (\ x__ y__ -> x__ {_ForkSnapshotRequest'snapshot = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkSnapshotRequest "maybe'snapshot" (Prelude.Maybe SnapshotRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkSnapshotRequest'snapshot
           (\ x__ y__ -> x__ {_ForkSnapshotRequest'snapshot = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkSnapshotRequest "destinationName" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkSnapshotRequest'destinationName
           (\ x__ y__ -> x__ {_ForkSnapshotRequest'destinationName = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkSnapshotRequest "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkSnapshotRequest'mutation
           (\ x__ y__ -> x__ {_ForkSnapshotRequest'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkSnapshotRequest "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkSnapshotRequest'mutation
           (\ x__ y__ -> x__ {_ForkSnapshotRequest'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkSnapshotRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ForkSnapshotRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3ForkSnapshotRequest\DC2;\n\
      \\bsnapshot\CAN\SOH \SOH(\v2\US.acyclic.objects.v1.SnapshotRefR\bsnapshot\DC2)\n\
      \\DLEdestination_name\CAN\STX \SOH(\tR\SIdestinationName\DC2@\n\
      \\bmutation\CAN\ETX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        snapshot__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "snapshot"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SnapshotRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'snapshot")) ::
              Data.ProtoLens.FieldDescriptor ForkSnapshotRequest
        destinationName__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destination_name"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"destinationName")) ::
              Data.ProtoLens.FieldDescriptor ForkSnapshotRequest
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor ForkSnapshotRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, snapshot__field_descriptor),
           (Data.ProtoLens.Tag 2, destinationName__field_descriptor),
           (Data.ProtoLens.Tag 3, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkSnapshotRequest'_unknownFields
        (\ x__ y__ -> x__ {_ForkSnapshotRequest'_unknownFields = y__})
  defMessage
    = ForkSnapshotRequest'_constructor
        {_ForkSnapshotRequest'snapshot = Prelude.Nothing,
         _ForkSnapshotRequest'destinationName = Data.ProtoLens.fieldDefault,
         _ForkSnapshotRequest'mutation = Prelude.Nothing,
         _ForkSnapshotRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkSnapshotRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ForkSnapshotRequest
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
                                       "snapshot"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"snapshot") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "destination_name"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"destinationName") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ForkSnapshotRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'snapshot") _x
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
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"destinationName") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                                Data.ProtoLens.encodeMessage _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ForkSnapshotRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkSnapshotRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkSnapshotRequest'snapshot x__)
                (Control.DeepSeq.deepseq
                   (_ForkSnapshotRequest'destinationName x__)
                   (Control.DeepSeq.deepseq (_ForkSnapshotRequest'mutation x__) ())))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.target' @:: Lens' GetObjectRequest ReadTarget@
         * 'Proto.Objects.V1.Objects_Fields.maybe'target' @:: Lens' GetObjectRequest (Prelude.Maybe ReadTarget)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' GetObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.versionId' @:: Lens' GetObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.rangeStart' @:: Lens' GetObjectRequest Data.Word.Word64@
         * 'Proto.Objects.V1.Objects_Fields.rangeEndInclusive' @:: Lens' GetObjectRequest Data.Word.Word64@
         * 'Proto.Objects.V1.Objects_Fields.maybe'rangeEndInclusive' @:: Lens' GetObjectRequest (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Objects.V1.Objects_Fields.ifMatch' @:: Lens' GetObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.ifNoneMatch' @:: Lens' GetObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.rangeRequested' @:: Lens' GetObjectRequest Prelude.Bool@ -}
data GetObjectRequest
  = GetObjectRequest'_constructor {_GetObjectRequest'target :: !(Prelude.Maybe ReadTarget),
                                   _GetObjectRequest'objectKey :: !Data.Text.Text,
                                   _GetObjectRequest'versionId :: !Data.Text.Text,
                                   _GetObjectRequest'rangeStart :: !Data.Word.Word64,
                                   _GetObjectRequest'rangeEndInclusive :: !(Prelude.Maybe Data.Word.Word64),
                                   _GetObjectRequest'ifMatch :: !Data.Text.Text,
                                   _GetObjectRequest'ifNoneMatch :: !Data.Text.Text,
                                   _GetObjectRequest'rangeRequested :: !Prelude.Bool,
                                   _GetObjectRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GetObjectRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField GetObjectRequest "target" ReadTarget where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'target
           (\ x__ y__ -> x__ {_GetObjectRequest'target = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GetObjectRequest "maybe'target" (Prelude.Maybe ReadTarget) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'target
           (\ x__ y__ -> x__ {_GetObjectRequest'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'objectKey
           (\ x__ y__ -> x__ {_GetObjectRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "versionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'versionId
           (\ x__ y__ -> x__ {_GetObjectRequest'versionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "rangeStart" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'rangeStart
           (\ x__ y__ -> x__ {_GetObjectRequest'rangeStart = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "rangeEndInclusive" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'rangeEndInclusive
           (\ x__ y__ -> x__ {_GetObjectRequest'rangeEndInclusive = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField GetObjectRequest "maybe'rangeEndInclusive" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'rangeEndInclusive
           (\ x__ y__ -> x__ {_GetObjectRequest'rangeEndInclusive = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "ifMatch" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'ifMatch
           (\ x__ y__ -> x__ {_GetObjectRequest'ifMatch = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "ifNoneMatch" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'ifNoneMatch
           (\ x__ y__ -> x__ {_GetObjectRequest'ifNoneMatch = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "rangeRequested" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'rangeRequested
           (\ x__ y__ -> x__ {_GetObjectRequest'rangeRequested = y__}))
        Prelude.id
instance Data.ProtoLens.Message GetObjectRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.GetObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEGetObjectRequest\DC26\n\
      \\ACKtarget\CAN\SOH \SOH(\v2\RS.acyclic.objects.v1.ReadTargetR\ACKtarget\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\GS\n\
      \\n\
      \version_id\CAN\ETX \SOH(\tR\tversionId\DC2\US\n\
      \\vrange_start\CAN\EOT \SOH(\EOTR\n\
      \rangeStart\DC23\n\
      \\DC3range_end_inclusive\CAN\ENQ \SOH(\EOTH\NULR\DC1rangeEndInclusive\136\SOH\SOH\DC2\EM\n\
      \\bif_match\CAN\ACK \SOH(\tR\aifMatch\DC2\"\n\
      \\rif_none_match\CAN\a \SOH(\tR\vifNoneMatch\DC2'\n\
      \\SIrange_requested\CAN\b \SOH(\bR\SOrangeRequestedB\SYN\n\
      \\DC4_range_end_inclusive"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        target__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "target"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ReadTarget)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'target")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        versionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"versionId")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        rangeStart__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "range_start"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"rangeStart")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        rangeEndInclusive__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "range_end_inclusive"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'rangeEndInclusive")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        ifMatch__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_match"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"ifMatch")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        ifNoneMatch__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_none_match"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"ifNoneMatch")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
        rangeRequested__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "range_requested"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"rangeRequested")) ::
              Data.ProtoLens.FieldDescriptor GetObjectRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, target__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, versionId__field_descriptor),
           (Data.ProtoLens.Tag 4, rangeStart__field_descriptor),
           (Data.ProtoLens.Tag 5, rangeEndInclusive__field_descriptor),
           (Data.ProtoLens.Tag 6, ifMatch__field_descriptor),
           (Data.ProtoLens.Tag 7, ifNoneMatch__field_descriptor),
           (Data.ProtoLens.Tag 8, rangeRequested__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GetObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_GetObjectRequest'_unknownFields = y__})
  defMessage
    = GetObjectRequest'_constructor
        {_GetObjectRequest'target = Prelude.Nothing,
         _GetObjectRequest'objectKey = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'versionId = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'rangeStart = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'rangeEndInclusive = Prelude.Nothing,
         _GetObjectRequest'ifMatch = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'ifNoneMatch = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'rangeRequested = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          GetObjectRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser GetObjectRequest
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
                                       "target"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"target") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "version_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"versionId") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "range_start"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"rangeStart") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "range_end_inclusive"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"rangeEndInclusive") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "if_match"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"ifMatch") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "if_none_match"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"ifNoneMatch") y x)
                        64
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "range_requested"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"rangeRequested") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "GetObjectRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'target") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"versionId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"rangeStart") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view
                                (Data.ProtoLens.Field.field @"maybe'rangeEndInclusive") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         ((Data.Monoid.<>)
                            (let
                               _v = Lens.Family2.view (Data.ProtoLens.Field.field @"ifMatch") _x
                             in
                               if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                   Data.Monoid.mempty
                               else
                                   (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                                     ((Prelude..)
                                        (\ bs
                                           -> (Data.Monoid.<>)
                                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                   (Prelude.fromIntegral
                                                      (Data.ByteString.length bs)))
                                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                        Data.Text.Encoding.encodeUtf8 _v))
                            ((Data.Monoid.<>)
                               (let
                                  _v
                                    = Lens.Family2.view
                                        (Data.ProtoLens.Field.field @"ifNoneMatch") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                                        ((Prelude..)
                                           (\ bs
                                              -> (Data.Monoid.<>)
                                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                      (Prelude.fromIntegral
                                                         (Data.ByteString.length bs)))
                                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                           Data.Text.Encoding.encodeUtf8 _v))
                               ((Data.Monoid.<>)
                                  (let
                                     _v
                                       = Lens.Family2.view
                                           (Data.ProtoLens.Field.field @"rangeRequested") _x
                                   in
                                     if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                         Data.Monoid.mempty
                                     else
                                         (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt 64)
                                           ((Prelude..)
                                              Data.ProtoLens.Encoding.Bytes.putVarInt
                                              (\ b -> if b then 1 else 0) _v))
                                  (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                     (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))))
instance Control.DeepSeq.NFData GetObjectRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GetObjectRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_GetObjectRequest'target x__)
                (Control.DeepSeq.deepseq
                   (_GetObjectRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_GetObjectRequest'versionId x__)
                      (Control.DeepSeq.deepseq
                         (_GetObjectRequest'rangeStart x__)
                         (Control.DeepSeq.deepseq
                            (_GetObjectRequest'rangeEndInclusive x__)
                            (Control.DeepSeq.deepseq
                               (_GetObjectRequest'ifMatch x__)
                               (Control.DeepSeq.deepseq
                                  (_GetObjectRequest'ifNoneMatch x__)
                                  (Control.DeepSeq.deepseq
                                     (_GetObjectRequest'rangeRequested x__) ()))))))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.maybe'frame' @:: Lens' GetObjectResponse (Prelude.Maybe GetObjectResponse'Frame)@
         * 'Proto.Objects.V1.Objects_Fields.maybe'version' @:: Lens' GetObjectResponse (Prelude.Maybe ObjectVersion)@
         * 'Proto.Objects.V1.Objects_Fields.version' @:: Lens' GetObjectResponse ObjectVersion@
         * 'Proto.Objects.V1.Objects_Fields.maybe'body' @:: Lens' GetObjectResponse (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Objects.V1.Objects_Fields.body' @:: Lens' GetObjectResponse Data.ByteString.ByteString@ -}
data GetObjectResponse
  = GetObjectResponse'_constructor {_GetObjectResponse'frame :: !(Prelude.Maybe GetObjectResponse'Frame),
                                    _GetObjectResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GetObjectResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data GetObjectResponse'Frame
  = GetObjectResponse'Version !ObjectVersion |
    GetObjectResponse'Body !Data.ByteString.ByteString
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField GetObjectResponse "maybe'frame" (Prelude.Maybe GetObjectResponse'Frame) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectResponse "maybe'version" (Prelude.Maybe ObjectVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (GetObjectResponse'Version x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap GetObjectResponse'Version y__))
instance Data.ProtoLens.Field.HasField GetObjectResponse "version" ObjectVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (GetObjectResponse'Version x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap GetObjectResponse'Version y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField GetObjectResponse "maybe'body" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (GetObjectResponse'Body x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap GetObjectResponse'Body y__))
instance Data.ProtoLens.Field.HasField GetObjectResponse "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (GetObjectResponse'Body x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap GetObjectResponse'Body y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message GetObjectResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.GetObjectResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1GetObjectResponse\DC2=\n\
      \\aversion\CAN\SOH \SOH(\v2!.acyclic.objects.v1.ObjectVersionH\NULR\aversion\DC2\DC4\n\
      \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbodyB\a\n\
      \\ENQframe"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'version")) ::
              Data.ProtoLens.FieldDescriptor GetObjectResponse
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'body")) ::
              Data.ProtoLens.FieldDescriptor GetObjectResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, version__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GetObjectResponse'_unknownFields
        (\ x__ y__ -> x__ {_GetObjectResponse'_unknownFields = y__})
  defMessage
    = GetObjectResponse'_constructor
        {_GetObjectResponse'frame = Prelude.Nothing,
         _GetObjectResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          GetObjectResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser GetObjectResponse
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
                                       "version"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"version") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "GetObjectResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'frame") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (GetObjectResponse'Version v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (GetObjectResponse'Body v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((\ bs
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                   (Prelude.fromIntegral (Data.ByteString.length bs)))
                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData GetObjectResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GetObjectResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_GetObjectResponse'frame x__) ())
instance Control.DeepSeq.NFData GetObjectResponse'Frame where
  rnf (GetObjectResponse'Version x__) = Control.DeepSeq.rnf x__
  rnf (GetObjectResponse'Body x__) = Control.DeepSeq.rnf x__
_GetObjectResponse'Version ::
  Data.ProtoLens.Prism.Prism' GetObjectResponse'Frame ObjectVersion
_GetObjectResponse'Version
  = Data.ProtoLens.Prism.prism'
      GetObjectResponse'Version
      (\ p__
         -> case p__ of
              (GetObjectResponse'Version p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_GetObjectResponse'Body ::
  Data.ProtoLens.Prism.Prism' GetObjectResponse'Frame Data.ByteString.ByteString
_GetObjectResponse'Body
  = Data.ProtoLens.Prism.prism'
      GetObjectResponse'Body
      (\ p__
         -> case p__ of
              (GetObjectResponse'Body p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' HeadBucketRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' HeadBucketRequest (Prelude.Maybe BucketRef)@ -}
data HeadBucketRequest
  = HeadBucketRequest'_constructor {_HeadBucketRequest'bucket :: !(Prelude.Maybe BucketRef),
                                    _HeadBucketRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show HeadBucketRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField HeadBucketRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadBucketRequest'bucket
           (\ x__ y__ -> x__ {_HeadBucketRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HeadBucketRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadBucketRequest'bucket
           (\ x__ y__ -> x__ {_HeadBucketRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Message HeadBucketRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.HeadBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1HeadBucketRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor HeadBucketRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HeadBucketRequest'_unknownFields
        (\ x__ y__ -> x__ {_HeadBucketRequest'_unknownFields = y__})
  defMessage
    = HeadBucketRequest'_constructor
        {_HeadBucketRequest'bucket = Prelude.Nothing,
         _HeadBucketRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          HeadBucketRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser HeadBucketRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "HeadBucketRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
instance Control.DeepSeq.NFData HeadBucketRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_HeadBucketRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_HeadBucketRequest'bucket x__) ())
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.target' @:: Lens' HeadObjectRequest ReadTarget@
         * 'Proto.Objects.V1.Objects_Fields.maybe'target' @:: Lens' HeadObjectRequest (Prelude.Maybe ReadTarget)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' HeadObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.versionId' @:: Lens' HeadObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.ifMatch' @:: Lens' HeadObjectRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.ifNoneMatch' @:: Lens' HeadObjectRequest Data.Text.Text@ -}
data HeadObjectRequest
  = HeadObjectRequest'_constructor {_HeadObjectRequest'target :: !(Prelude.Maybe ReadTarget),
                                    _HeadObjectRequest'objectKey :: !Data.Text.Text,
                                    _HeadObjectRequest'versionId :: !Data.Text.Text,
                                    _HeadObjectRequest'ifMatch :: !Data.Text.Text,
                                    _HeadObjectRequest'ifNoneMatch :: !Data.Text.Text,
                                    _HeadObjectRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show HeadObjectRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField HeadObjectRequest "target" ReadTarget where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'target
           (\ x__ y__ -> x__ {_HeadObjectRequest'target = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HeadObjectRequest "maybe'target" (Prelude.Maybe ReadTarget) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'target
           (\ x__ y__ -> x__ {_HeadObjectRequest'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HeadObjectRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'objectKey
           (\ x__ y__ -> x__ {_HeadObjectRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HeadObjectRequest "versionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'versionId
           (\ x__ y__ -> x__ {_HeadObjectRequest'versionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HeadObjectRequest "ifMatch" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'ifMatch
           (\ x__ y__ -> x__ {_HeadObjectRequest'ifMatch = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HeadObjectRequest "ifNoneMatch" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'ifNoneMatch
           (\ x__ y__ -> x__ {_HeadObjectRequest'ifNoneMatch = y__}))
        Prelude.id
instance Data.ProtoLens.Message HeadObjectRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.HeadObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1HeadObjectRequest\DC26\n\
      \\ACKtarget\CAN\SOH \SOH(\v2\RS.acyclic.objects.v1.ReadTargetR\ACKtarget\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\GS\n\
      \\n\
      \version_id\CAN\ETX \SOH(\tR\tversionId\DC2\EM\n\
      \\bif_match\CAN\EOT \SOH(\tR\aifMatch\DC2\"\n\
      \\rif_none_match\CAN\ENQ \SOH(\tR\vifNoneMatch"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        target__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "target"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ReadTarget)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'target")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectRequest
        versionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"versionId")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectRequest
        ifMatch__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_match"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"ifMatch")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectRequest
        ifNoneMatch__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_none_match"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"ifNoneMatch")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, target__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, versionId__field_descriptor),
           (Data.ProtoLens.Tag 4, ifMatch__field_descriptor),
           (Data.ProtoLens.Tag 5, ifNoneMatch__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HeadObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_HeadObjectRequest'_unknownFields = y__})
  defMessage
    = HeadObjectRequest'_constructor
        {_HeadObjectRequest'target = Prelude.Nothing,
         _HeadObjectRequest'objectKey = Data.ProtoLens.fieldDefault,
         _HeadObjectRequest'versionId = Data.ProtoLens.fieldDefault,
         _HeadObjectRequest'ifMatch = Data.ProtoLens.fieldDefault,
         _HeadObjectRequest'ifNoneMatch = Data.ProtoLens.fieldDefault,
         _HeadObjectRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          HeadObjectRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser HeadObjectRequest
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
                                       "target"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"target") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "version_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"versionId") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "if_match"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"ifMatch") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "if_none_match"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"ifNoneMatch") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "HeadObjectRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'target") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"versionId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                            ((Prelude..)
                               (\ bs
                                  -> (Data.Monoid.<>)
                                       (Data.ProtoLens.Encoding.Bytes.putVarInt
                                          (Prelude.fromIntegral (Data.ByteString.length bs)))
                                       (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                               Data.Text.Encoding.encodeUtf8 _v))
                   ((Data.Monoid.<>)
                      (let
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"ifMatch") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
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
                              = Lens.Family2.view (Data.ProtoLens.Field.field @"ifNoneMatch") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                  ((Prelude..)
                                     (\ bs
                                        -> (Data.Monoid.<>)
                                             (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                (Prelude.fromIntegral (Data.ByteString.length bs)))
                                             (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                     Data.Text.Encoding.encodeUtf8 _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData HeadObjectRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_HeadObjectRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_HeadObjectRequest'target x__)
                (Control.DeepSeq.deepseq
                   (_HeadObjectRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_HeadObjectRequest'versionId x__)
                      (Control.DeepSeq.deepseq
                         (_HeadObjectRequest'ifMatch x__)
                         (Control.DeepSeq.deepseq
                            (_HeadObjectRequest'ifNoneMatch x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.version' @:: Lens' HeadObjectResponse ObjectVersion@
         * 'Proto.Objects.V1.Objects_Fields.maybe'version' @:: Lens' HeadObjectResponse (Prelude.Maybe ObjectVersion)@ -}
data HeadObjectResponse
  = HeadObjectResponse'_constructor {_HeadObjectResponse'version :: !(Prelude.Maybe ObjectVersion),
                                     _HeadObjectResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show HeadObjectResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField HeadObjectResponse "version" ObjectVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectResponse'version
           (\ x__ y__ -> x__ {_HeadObjectResponse'version = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HeadObjectResponse "maybe'version" (Prelude.Maybe ObjectVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectResponse'version
           (\ x__ y__ -> x__ {_HeadObjectResponse'version = y__}))
        Prelude.id
instance Data.ProtoLens.Message HeadObjectResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.HeadObjectResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC2HeadObjectResponse\DC2;\n\
      \\aversion\CAN\SOH \SOH(\v2!.acyclic.objects.v1.ObjectVersionR\aversion"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'version")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, version__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HeadObjectResponse'_unknownFields
        (\ x__ y__ -> x__ {_HeadObjectResponse'_unknownFields = y__})
  defMessage
    = HeadObjectResponse'_constructor
        {_HeadObjectResponse'version = Prelude.Nothing,
         _HeadObjectResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          HeadObjectResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser HeadObjectResponse
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
                                       "version"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"version") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "HeadObjectResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'version") _x
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
instance Control.DeepSeq.NFData HeadObjectResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_HeadObjectResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_HeadObjectResponse'version x__) ())
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' ListEntry Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.version' @:: Lens' ListEntry ObjectVersion@
         * 'Proto.Objects.V1.Objects_Fields.maybe'version' @:: Lens' ListEntry (Prelude.Maybe ObjectVersion)@ -}
data ListEntry
  = ListEntry'_constructor {_ListEntry'objectKey :: !Data.Text.Text,
                            _ListEntry'version :: !(Prelude.Maybe ObjectVersion),
                            _ListEntry'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListEntry where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListEntry "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListEntry'objectKey
           (\ x__ y__ -> x__ {_ListEntry'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListEntry "version" ObjectVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListEntry'version (\ x__ y__ -> x__ {_ListEntry'version = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListEntry "maybe'version" (Prelude.Maybe ObjectVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListEntry'version (\ x__ y__ -> x__ {_ListEntry'version = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListEntry where
  messageName _ = Data.Text.pack "acyclic.objects.v1.ListEntry"
  packedMessageDescriptor _
    = "\n\
      \\tListEntry\DC2\GS\n\
      \\n\
      \object_key\CAN\SOH \SOH(\tR\tobjectKey\DC2;\n\
      \\aversion\CAN\STX \SOH(\v2!.acyclic.objects.v1.ObjectVersionR\aversion"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor ListEntry
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'version")) ::
              Data.ProtoLens.FieldDescriptor ListEntry
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 2, version__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListEntry'_unknownFields
        (\ x__ y__ -> x__ {_ListEntry'_unknownFields = y__})
  defMessage
    = ListEntry'_constructor
        {_ListEntry'objectKey = Data.ProtoLens.fieldDefault,
         _ListEntry'version = Prelude.Nothing,
         _ListEntry'_unknownFields = []}
  parseMessage
    = let
        loop :: ListEntry -> Data.ProtoLens.Encoding.Bytes.Parser ListEntry
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
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "version"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"version") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ListEntry"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'version") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ListEntry where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListEntry'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListEntry'objectKey x__)
                (Control.DeepSeq.deepseq (_ListEntry'version x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.target' @:: Lens' ListObjectsRequest ReadTarget@
         * 'Proto.Objects.V1.Objects_Fields.maybe'target' @:: Lens' ListObjectsRequest (Prelude.Maybe ReadTarget)@
         * 'Proto.Objects.V1.Objects_Fields.prefix' @:: Lens' ListObjectsRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.delimiter' @:: Lens' ListObjectsRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.mode' @:: Lens' ListObjectsRequest ListingMode@
         * 'Proto.Objects.V1.Objects_Fields.pageSize' @:: Lens' ListObjectsRequest Data.Word.Word32@
         * 'Proto.Objects.V1.Objects_Fields.continuationToken' @:: Lens' ListObjectsRequest Data.Text.Text@ -}
data ListObjectsRequest
  = ListObjectsRequest'_constructor {_ListObjectsRequest'target :: !(Prelude.Maybe ReadTarget),
                                     _ListObjectsRequest'prefix :: !Data.Text.Text,
                                     _ListObjectsRequest'delimiter :: !Data.Text.Text,
                                     _ListObjectsRequest'mode :: !ListingMode,
                                     _ListObjectsRequest'pageSize :: !Data.Word.Word32,
                                     _ListObjectsRequest'continuationToken :: !Data.Text.Text,
                                     _ListObjectsRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListObjectsRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListObjectsRequest "target" ReadTarget where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'target
           (\ x__ y__ -> x__ {_ListObjectsRequest'target = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListObjectsRequest "maybe'target" (Prelude.Maybe ReadTarget) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'target
           (\ x__ y__ -> x__ {_ListObjectsRequest'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsRequest "prefix" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'prefix
           (\ x__ y__ -> x__ {_ListObjectsRequest'prefix = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsRequest "delimiter" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'delimiter
           (\ x__ y__ -> x__ {_ListObjectsRequest'delimiter = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsRequest "mode" ListingMode where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'mode
           (\ x__ y__ -> x__ {_ListObjectsRequest'mode = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsRequest "pageSize" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'pageSize
           (\ x__ y__ -> x__ {_ListObjectsRequest'pageSize = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsRequest "continuationToken" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'continuationToken
           (\ x__ y__ -> x__ {_ListObjectsRequest'continuationToken = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListObjectsRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ListObjectsRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2ListObjectsRequest\DC26\n\
      \\ACKtarget\CAN\SOH \SOH(\v2\RS.acyclic.objects.v1.ReadTargetR\ACKtarget\DC2\SYN\n\
      \\ACKprefix\CAN\STX \SOH(\tR\ACKprefix\DC2\FS\n\
      \\tdelimiter\CAN\ETX \SOH(\tR\tdelimiter\DC23\n\
      \\EOTmode\CAN\EOT \SOH(\SO2\US.acyclic.objects.v1.ListingModeR\EOTmode\DC2\ESC\n\
      \\tpage_size\CAN\ENQ \SOH(\rR\bpageSize\DC2-\n\
      \\DC2continuation_token\CAN\ACK \SOH(\tR\DC1continuationToken"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        target__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "target"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ReadTarget)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'target")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsRequest
        prefix__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "prefix"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"prefix")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsRequest
        delimiter__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "delimiter"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"delimiter")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsRequest
        mode__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mode"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ListingMode)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"mode")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsRequest
        pageSize__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "page_size"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"pageSize")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsRequest
        continuationToken__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "continuation_token"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"continuationToken")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, target__field_descriptor),
           (Data.ProtoLens.Tag 2, prefix__field_descriptor),
           (Data.ProtoLens.Tag 3, delimiter__field_descriptor),
           (Data.ProtoLens.Tag 4, mode__field_descriptor),
           (Data.ProtoLens.Tag 5, pageSize__field_descriptor),
           (Data.ProtoLens.Tag 6, continuationToken__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListObjectsRequest'_unknownFields
        (\ x__ y__ -> x__ {_ListObjectsRequest'_unknownFields = y__})
  defMessage
    = ListObjectsRequest'_constructor
        {_ListObjectsRequest'target = Prelude.Nothing,
         _ListObjectsRequest'prefix = Data.ProtoLens.fieldDefault,
         _ListObjectsRequest'delimiter = Data.ProtoLens.fieldDefault,
         _ListObjectsRequest'mode = Data.ProtoLens.fieldDefault,
         _ListObjectsRequest'pageSize = Data.ProtoLens.fieldDefault,
         _ListObjectsRequest'continuationToken = Data.ProtoLens.fieldDefault,
         _ListObjectsRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListObjectsRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ListObjectsRequest
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
                                       "target"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"target") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "prefix"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"prefix") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "delimiter"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"delimiter") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "mode"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"mode") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "page_size"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"pageSize") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "continuation_token"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"continuationToken") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ListObjectsRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'target") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"prefix") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"delimiter") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                            ((Prelude..)
                               (\ bs
                                  -> (Data.Monoid.<>)
                                       (Data.ProtoLens.Encoding.Bytes.putVarInt
                                          (Prelude.fromIntegral (Data.ByteString.length bs)))
                                       (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                               Data.Text.Encoding.encodeUtf8 _v))
                   ((Data.Monoid.<>)
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"mode") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  ((Prelude..)
                                     Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral)
                                  Prelude.fromEnum _v))
                      ((Data.Monoid.<>)
                         (let
                            _v = Lens.Family2.view (Data.ProtoLens.Field.field @"pageSize") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  ((Prelude..)
                                     Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral
                                     _v))
                         ((Data.Monoid.<>)
                            (let
                               _v
                                 = Lens.Family2.view
                                     (Data.ProtoLens.Field.field @"continuationToken") _x
                             in
                               if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                   Data.Monoid.mempty
                               else
                                   (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                                     ((Prelude..)
                                        (\ bs
                                           -> (Data.Monoid.<>)
                                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                   (Prelude.fromIntegral
                                                      (Data.ByteString.length bs)))
                                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                        Data.Text.Encoding.encodeUtf8 _v))
                            (Data.ProtoLens.Encoding.Wire.buildFieldSet
                               (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))
instance Control.DeepSeq.NFData ListObjectsRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListObjectsRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListObjectsRequest'target x__)
                (Control.DeepSeq.deepseq
                   (_ListObjectsRequest'prefix x__)
                   (Control.DeepSeq.deepseq
                      (_ListObjectsRequest'delimiter x__)
                      (Control.DeepSeq.deepseq
                         (_ListObjectsRequest'mode x__)
                         (Control.DeepSeq.deepseq
                            (_ListObjectsRequest'pageSize x__)
                            (Control.DeepSeq.deepseq
                               (_ListObjectsRequest'continuationToken x__) ()))))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.entries' @:: Lens' ListObjectsResponse [ListEntry]@
         * 'Proto.Objects.V1.Objects_Fields.vec'entries' @:: Lens' ListObjectsResponse (Data.Vector.Vector ListEntry)@
         * 'Proto.Objects.V1.Objects_Fields.commonPrefixes' @:: Lens' ListObjectsResponse [Data.Text.Text]@
         * 'Proto.Objects.V1.Objects_Fields.vec'commonPrefixes' @:: Lens' ListObjectsResponse (Data.Vector.Vector Data.Text.Text)@
         * 'Proto.Objects.V1.Objects_Fields.continuationToken' @:: Lens' ListObjectsResponse Data.Text.Text@ -}
data ListObjectsResponse
  = ListObjectsResponse'_constructor {_ListObjectsResponse'entries :: !(Data.Vector.Vector ListEntry),
                                      _ListObjectsResponse'commonPrefixes :: !(Data.Vector.Vector Data.Text.Text),
                                      _ListObjectsResponse'continuationToken :: !Data.Text.Text,
                                      _ListObjectsResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListObjectsResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListObjectsResponse "entries" [ListEntry] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsResponse'entries
           (\ x__ y__ -> x__ {_ListObjectsResponse'entries = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ListObjectsResponse "vec'entries" (Data.Vector.Vector ListEntry) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsResponse'entries
           (\ x__ y__ -> x__ {_ListObjectsResponse'entries = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsResponse "commonPrefixes" [Data.Text.Text] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsResponse'commonPrefixes
           (\ x__ y__ -> x__ {_ListObjectsResponse'commonPrefixes = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ListObjectsResponse "vec'commonPrefixes" (Data.Vector.Vector Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsResponse'commonPrefixes
           (\ x__ y__ -> x__ {_ListObjectsResponse'commonPrefixes = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListObjectsResponse "continuationToken" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsResponse'continuationToken
           (\ x__ y__ -> x__ {_ListObjectsResponse'continuationToken = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListObjectsResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ListObjectsResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC3ListObjectsResponse\DC27\n\
      \\aentries\CAN\SOH \ETX(\v2\GS.acyclic.objects.v1.ListEntryR\aentries\DC2'\n\
      \\SIcommon_prefixes\CAN\STX \ETX(\tR\SOcommonPrefixes\DC2-\n\
      \\DC2continuation_token\CAN\ETX \SOH(\tR\DC1continuationToken"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        entries__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "entries"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ListEntry)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"entries")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsResponse
        commonPrefixes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "common_prefixes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"commonPrefixes")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsResponse
        continuationToken__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "continuation_token"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"continuationToken")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, entries__field_descriptor),
           (Data.ProtoLens.Tag 2, commonPrefixes__field_descriptor),
           (Data.ProtoLens.Tag 3, continuationToken__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListObjectsResponse'_unknownFields
        (\ x__ y__ -> x__ {_ListObjectsResponse'_unknownFields = y__})
  defMessage
    = ListObjectsResponse'_constructor
        {_ListObjectsResponse'entries = Data.Vector.Generic.empty,
         _ListObjectsResponse'commonPrefixes = Data.Vector.Generic.empty,
         _ListObjectsResponse'continuationToken = Data.ProtoLens.fieldDefault,
         _ListObjectsResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListObjectsResponse
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.Text.Text
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld ListEntry
                -> Data.ProtoLens.Encoding.Bytes.Parser ListObjectsResponse
        loop x mutable'commonPrefixes mutable'entries
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'commonPrefixes <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                 (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                    mutable'commonPrefixes)
                      frozen'entries <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'entries)
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
                              (Data.ProtoLens.Field.field @"vec'commonPrefixes")
                              frozen'commonPrefixes
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'entries") frozen'entries x)))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "entries"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'entries y)
                                loop x mutable'commonPrefixes v
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getText
                                              (Prelude.fromIntegral len))
                                        "common_prefixes"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'commonPrefixes y)
                                loop x v mutable'entries
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "continuation_token"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"continuationToken") y x)
                                  mutable'commonPrefixes mutable'entries
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'commonPrefixes mutable'entries
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'commonPrefixes <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          Data.ProtoLens.Encoding.Growing.new
              mutable'entries <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop
                Data.ProtoLens.defMessage mutable'commonPrefixes mutable'entries)
          "ListObjectsResponse"
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
                (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'entries") _x))
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
                              Data.Text.Encoding.encodeUtf8 _v))
                   (Lens.Family2.view
                      (Data.ProtoLens.Field.field @"vec'commonPrefixes") _x))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"continuationToken") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
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
instance Control.DeepSeq.NFData ListObjectsResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListObjectsResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListObjectsResponse'entries x__)
                (Control.DeepSeq.deepseq
                   (_ListObjectsResponse'commonPrefixes x__)
                   (Control.DeepSeq.deepseq
                      (_ListObjectsResponse'continuationToken x__) ())))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' ListPartsRequest BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' ListPartsRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' ListPartsRequest Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.uploadId' @:: Lens' ListPartsRequest Data.Text.Text@ -}
data ListPartsRequest
  = ListPartsRequest'_constructor {_ListPartsRequest'bucket :: !(Prelude.Maybe BucketRef),
                                   _ListPartsRequest'objectKey :: !Data.Text.Text,
                                   _ListPartsRequest'uploadId :: !Data.Text.Text,
                                   _ListPartsRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListPartsRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListPartsRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsRequest'bucket
           (\ x__ y__ -> x__ {_ListPartsRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListPartsRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsRequest'bucket
           (\ x__ y__ -> x__ {_ListPartsRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListPartsRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsRequest'objectKey
           (\ x__ y__ -> x__ {_ListPartsRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListPartsRequest "uploadId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsRequest'uploadId
           (\ x__ y__ -> x__ {_ListPartsRequest'uploadId = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListPartsRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ListPartsRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEListPartsRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor ListPartsRequest
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor ListPartsRequest
        uploadId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "upload_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"uploadId")) ::
              Data.ProtoLens.FieldDescriptor ListPartsRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, uploadId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListPartsRequest'_unknownFields
        (\ x__ y__ -> x__ {_ListPartsRequest'_unknownFields = y__})
  defMessage
    = ListPartsRequest'_constructor
        {_ListPartsRequest'bucket = Prelude.Nothing,
         _ListPartsRequest'objectKey = Data.ProtoLens.fieldDefault,
         _ListPartsRequest'uploadId = Data.ProtoLens.fieldDefault,
         _ListPartsRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListPartsRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ListPartsRequest
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "upload_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"uploadId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ListPartsRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"uploadId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
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
instance Control.DeepSeq.NFData ListPartsRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListPartsRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListPartsRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_ListPartsRequest'objectKey x__)
                   (Control.DeepSeq.deepseq (_ListPartsRequest'uploadId x__) ())))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.parts' @:: Lens' ListPartsResponse [UploadedPart]@
         * 'Proto.Objects.V1.Objects_Fields.vec'parts' @:: Lens' ListPartsResponse (Data.Vector.Vector UploadedPart)@ -}
data ListPartsResponse
  = ListPartsResponse'_constructor {_ListPartsResponse'parts :: !(Data.Vector.Vector UploadedPart),
                                    _ListPartsResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListPartsResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListPartsResponse "parts" [UploadedPart] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsResponse'parts
           (\ x__ y__ -> x__ {_ListPartsResponse'parts = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ListPartsResponse "vec'parts" (Data.Vector.Vector UploadedPart) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsResponse'parts
           (\ x__ y__ -> x__ {_ListPartsResponse'parts = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListPartsResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ListPartsResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1ListPartsResponse\DC26\n\
      \\ENQparts\CAN\SOH \ETX(\v2 .acyclic.objects.v1.UploadedPartR\ENQparts"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        parts__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "parts"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor UploadedPart)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"parts")) ::
              Data.ProtoLens.FieldDescriptor ListPartsResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, parts__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListPartsResponse'_unknownFields
        (\ x__ y__ -> x__ {_ListPartsResponse'_unknownFields = y__})
  defMessage
    = ListPartsResponse'_constructor
        {_ListPartsResponse'parts = Data.Vector.Generic.empty,
         _ListPartsResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListPartsResponse
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld UploadedPart
             -> Data.ProtoLens.Encoding.Bytes.Parser ListPartsResponse
        loop x mutable'parts
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'parts <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'parts)
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
                              (Data.ProtoLens.Field.field @"vec'parts") frozen'parts x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "parts"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'parts y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'parts
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'parts <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'parts)
          "ListPartsResponse"
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
                (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'parts") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ListPartsResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListPartsResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ListPartsResponse'parts x__) ())
newtype ListingMode'UnrecognizedValue
  = ListingMode'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ListingMode
  = LISTING_MODE_UNSPECIFIED |
    LISTING_MODE_CURRENT |
    LISTING_MODE_VERSIONS |
    ListingMode'Unrecognized !ListingMode'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ListingMode where
  maybeToEnum 0 = Prelude.Just LISTING_MODE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just LISTING_MODE_CURRENT
  maybeToEnum 2 = Prelude.Just LISTING_MODE_VERSIONS
  maybeToEnum k
    = Prelude.Just
        (ListingMode'Unrecognized
           (ListingMode'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum LISTING_MODE_UNSPECIFIED = "LISTING_MODE_UNSPECIFIED"
  showEnum LISTING_MODE_CURRENT = "LISTING_MODE_CURRENT"
  showEnum LISTING_MODE_VERSIONS = "LISTING_MODE_VERSIONS"
  showEnum
    (ListingMode'Unrecognized (ListingMode'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "LISTING_MODE_UNSPECIFIED"
    = Prelude.Just LISTING_MODE_UNSPECIFIED
    | (Prelude.==) k "LISTING_MODE_CURRENT"
    = Prelude.Just LISTING_MODE_CURRENT
    | (Prelude.==) k "LISTING_MODE_VERSIONS"
    = Prelude.Just LISTING_MODE_VERSIONS
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ListingMode where
  minBound = LISTING_MODE_UNSPECIFIED
  maxBound = LISTING_MODE_VERSIONS
instance Prelude.Enum ListingMode where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ListingMode: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum LISTING_MODE_UNSPECIFIED = 0
  fromEnum LISTING_MODE_CURRENT = 1
  fromEnum LISTING_MODE_VERSIONS = 2
  fromEnum
    (ListingMode'Unrecognized (ListingMode'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ LISTING_MODE_VERSIONS
    = Prelude.error
        "ListingMode.succ: bad argument LISTING_MODE_VERSIONS. This value would be out of bounds."
  succ LISTING_MODE_UNSPECIFIED = LISTING_MODE_CURRENT
  succ LISTING_MODE_CURRENT = LISTING_MODE_VERSIONS
  succ (ListingMode'Unrecognized _)
    = Prelude.error
        "ListingMode.succ: bad argument: unrecognized value"
  pred LISTING_MODE_UNSPECIFIED
    = Prelude.error
        "ListingMode.pred: bad argument LISTING_MODE_UNSPECIFIED. This value would be out of bounds."
  pred LISTING_MODE_CURRENT = LISTING_MODE_UNSPECIFIED
  pred LISTING_MODE_VERSIONS = LISTING_MODE_CURRENT
  pred (ListingMode'Unrecognized _)
    = Prelude.error
        "ListingMode.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ListingMode where
  fieldDefault = LISTING_MODE_UNSPECIFIED
instance Control.DeepSeq.NFData ListingMode where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.uploadId' @:: Lens' MultipartUpload Data.Text.Text@ -}
data MultipartUpload
  = MultipartUpload'_constructor {_MultipartUpload'uploadId :: !Data.Text.Text,
                                  _MultipartUpload'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MultipartUpload where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MultipartUpload "uploadId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MultipartUpload'uploadId
           (\ x__ y__ -> x__ {_MultipartUpload'uploadId = y__}))
        Prelude.id
instance Data.ProtoLens.Message MultipartUpload where
  messageName _ = Data.Text.pack "acyclic.objects.v1.MultipartUpload"
  packedMessageDescriptor _
    = "\n\
      \\SIMultipartUpload\DC2\ESC\n\
      \\tupload_id\CAN\SOH \SOH(\tR\buploadId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        uploadId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "upload_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"uploadId")) ::
              Data.ProtoLens.FieldDescriptor MultipartUpload
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, uploadId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MultipartUpload'_unknownFields
        (\ x__ y__ -> x__ {_MultipartUpload'_unknownFields = y__})
  defMessage
    = MultipartUpload'_constructor
        {_MultipartUpload'uploadId = Data.ProtoLens.fieldDefault,
         _MultipartUpload'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MultipartUpload
          -> Data.ProtoLens.Encoding.Bytes.Parser MultipartUpload
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
                                       "upload_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"uploadId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MultipartUpload"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"uploadId") _x
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
instance Control.DeepSeq.NFData MultipartUpload where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MultipartUpload'_unknownFields x__)
             (Control.DeepSeq.deepseq (_MultipartUpload'uploadId x__) ())
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.idempotencyKey' @:: Lens' MutationIdentity Data.Text.Text@ -}
data MutationIdentity
  = MutationIdentity'_constructor {_MutationIdentity'idempotencyKey :: !Data.Text.Text,
                                   _MutationIdentity'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MutationIdentity where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MutationIdentity "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationIdentity'idempotencyKey
           (\ x__ y__ -> x__ {_MutationIdentity'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message MutationIdentity where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.MutationIdentity"
  packedMessageDescriptor _
    = "\n\
      \\DLEMutationIdentity\DC2'\n\
      \\SIidempotency_key\CAN\SOH \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor MutationIdentity
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MutationIdentity'_unknownFields
        (\ x__ y__ -> x__ {_MutationIdentity'_unknownFields = y__})
  defMessage
    = MutationIdentity'_constructor
        {_MutationIdentity'idempotencyKey = Data.ProtoLens.fieldDefault,
         _MutationIdentity'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MutationIdentity
          -> Data.ProtoLens.Encoding.Bytes.Parser MutationIdentity
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
          (do loop Data.ProtoLens.defMessage) "MutationIdentity"
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
                      ((Prelude..)
                         (\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         Data.Text.Encoding.encodeUtf8 _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData MutationIdentity where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MutationIdentity'_unknownFields x__)
             (Control.DeepSeq.deepseq (_MutationIdentity'idempotencyKey x__) ())
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.contentType' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.user' @:: Lens' ObjectMetadata (Data.Map.Map Data.Text.Text Data.Text.Text)@
         * 'Proto.Objects.V1.Objects_Fields.contentEncoding' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.cacheControl' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.contentDisposition' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.contentLanguage' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.expiresUnixSeconds' @:: Lens' ObjectMetadata Data.Int.Int64@
         * 'Proto.Objects.V1.Objects_Fields.maybe'expiresUnixSeconds' @:: Lens' ObjectMetadata (Prelude.Maybe Data.Int.Int64)@ -}
data ObjectMetadata
  = ObjectMetadata'_constructor {_ObjectMetadata'contentType :: !Data.Text.Text,
                                 _ObjectMetadata'user :: !(Data.Map.Map Data.Text.Text Data.Text.Text),
                                 _ObjectMetadata'contentEncoding :: !Data.Text.Text,
                                 _ObjectMetadata'cacheControl :: !Data.Text.Text,
                                 _ObjectMetadata'contentDisposition :: !Data.Text.Text,
                                 _ObjectMetadata'contentLanguage :: !Data.Text.Text,
                                 _ObjectMetadata'expiresUnixSeconds :: !(Prelude.Maybe Data.Int.Int64),
                                 _ObjectMetadata'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ObjectMetadata where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ObjectMetadata "contentType" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'contentType
           (\ x__ y__ -> x__ {_ObjectMetadata'contentType = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata "user" (Data.Map.Map Data.Text.Text Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'user
           (\ x__ y__ -> x__ {_ObjectMetadata'user = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata "contentEncoding" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'contentEncoding
           (\ x__ y__ -> x__ {_ObjectMetadata'contentEncoding = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata "cacheControl" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'cacheControl
           (\ x__ y__ -> x__ {_ObjectMetadata'cacheControl = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata "contentDisposition" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'contentDisposition
           (\ x__ y__ -> x__ {_ObjectMetadata'contentDisposition = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata "contentLanguage" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'contentLanguage
           (\ x__ y__ -> x__ {_ObjectMetadata'contentLanguage = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata "expiresUnixSeconds" Data.Int.Int64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'expiresUnixSeconds
           (\ x__ y__ -> x__ {_ObjectMetadata'expiresUnixSeconds = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ObjectMetadata "maybe'expiresUnixSeconds" (Prelude.Maybe Data.Int.Int64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'expiresUnixSeconds
           (\ x__ y__ -> x__ {_ObjectMetadata'expiresUnixSeconds = y__}))
        Prelude.id
instance Data.ProtoLens.Message ObjectMetadata where
  messageName _ = Data.Text.pack "acyclic.objects.v1.ObjectMetadata"
  packedMessageDescriptor _
    = "\n\
      \\SOObjectMetadata\DC2!\n\
      \\fcontent_type\CAN\SOH \SOH(\tR\vcontentType\DC2@\n\
      \\EOTuser\CAN\STX \ETX(\v2,.acyclic.objects.v1.ObjectMetadata.UserEntryR\EOTuser\DC2)\n\
      \\DLEcontent_encoding\CAN\ETX \SOH(\tR\SIcontentEncoding\DC2#\n\
      \\rcache_control\CAN\EOT \SOH(\tR\fcacheControl\DC2/\n\
      \\DC3content_disposition\CAN\ENQ \SOH(\tR\DC2contentDisposition\DC2)\n\
      \\DLEcontent_language\CAN\ACK \SOH(\tR\SIcontentLanguage\DC25\n\
      \\DC4expires_unix_seconds\CAN\a \SOH(\ETXH\NULR\DC2expiresUnixSeconds\136\SOH\SOH\SUB7\n\
      \\tUserEntry\DC2\DLE\n\
      \\ETXkey\CAN\SOH \SOH(\tR\ETXkey\DC2\DC4\n\
      \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue:\STX8\SOHB\ETB\n\
      \\NAK_expires_unix_seconds"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        contentType__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "content_type"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"contentType")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
        user__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "user"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectMetadata'UserEntry)
              (Data.ProtoLens.MapField
                 (Data.ProtoLens.Field.field @"key")
                 (Data.ProtoLens.Field.field @"value")
                 (Data.ProtoLens.Field.field @"user")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
        contentEncoding__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "content_encoding"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"contentEncoding")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
        cacheControl__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "cache_control"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"cacheControl")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
        contentDisposition__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "content_disposition"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"contentDisposition")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
        contentLanguage__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "content_language"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"contentLanguage")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
        expiresUnixSeconds__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expires_unix_seconds"
              (Data.ProtoLens.ScalarField Data.ProtoLens.Int64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Int.Int64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'expiresUnixSeconds")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, contentType__field_descriptor),
           (Data.ProtoLens.Tag 2, user__field_descriptor),
           (Data.ProtoLens.Tag 3, contentEncoding__field_descriptor),
           (Data.ProtoLens.Tag 4, cacheControl__field_descriptor),
           (Data.ProtoLens.Tag 5, contentDisposition__field_descriptor),
           (Data.ProtoLens.Tag 6, contentLanguage__field_descriptor),
           (Data.ProtoLens.Tag 7, expiresUnixSeconds__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ObjectMetadata'_unknownFields
        (\ x__ y__ -> x__ {_ObjectMetadata'_unknownFields = y__})
  defMessage
    = ObjectMetadata'_constructor
        {_ObjectMetadata'contentType = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'user = Data.Map.empty,
         _ObjectMetadata'contentEncoding = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'cacheControl = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'contentDisposition = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'contentLanguage = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'expiresUnixSeconds = Prelude.Nothing,
         _ObjectMetadata'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ObjectMetadata
          -> Data.ProtoLens.Encoding.Bytes.Parser ObjectMetadata
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
                                       "content_type"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contentType") y x)
                        18
                          -> do !(entry :: ObjectMetadata'UserEntry) <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                                                          (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                                                              Data.ProtoLens.Encoding.Bytes.isolate
                                                                                (Prelude.fromIntegral
                                                                                   len)
                                                                                Data.ProtoLens.parseMessage)
                                                                          "user"
                                (let
                                   key = Lens.Family2.view (Data.ProtoLens.Field.field @"key") entry
                                   value
                                     = Lens.Family2.view (Data.ProtoLens.Field.field @"value") entry
                                 in
                                   loop
                                     (Lens.Family2.over
                                        (Data.ProtoLens.Field.field @"user")
                                        (\ !t -> Data.Map.insert key value t) x))
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "content_encoding"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"contentEncoding") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "cache_control"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"cacheControl") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "content_disposition"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"contentDisposition") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "content_language"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"contentLanguage") y x)
                        56
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "expires_unix_seconds"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"expiresUnixSeconds") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ObjectMetadata"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"contentType") _x
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
                (Data.Monoid.mconcat
                   (Prelude.map
                      (\ _v
                         -> (Data.Monoid.<>)
                              (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                              ((Prelude..)
                                 (\ bs
                                    -> (Data.Monoid.<>)
                                         (Data.ProtoLens.Encoding.Bytes.putVarInt
                                            (Prelude.fromIntegral (Data.ByteString.length bs)))
                                         (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                 Data.ProtoLens.encodeMessage
                                 (Lens.Family2.set
                                    (Data.ProtoLens.Field.field @"key") (Prelude.fst _v)
                                    (Lens.Family2.set
                                       (Data.ProtoLens.Field.field @"value") (Prelude.snd _v)
                                       (Data.ProtoLens.defMessage :: ObjectMetadata'UserEntry)))))
                      (Data.Map.toList
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"user") _x))))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"contentEncoding") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"cacheControl") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
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
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"contentDisposition") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
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
                                 = Lens.Family2.view
                                     (Data.ProtoLens.Field.field @"contentLanguage") _x
                             in
                               if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                   Data.Monoid.mempty
                               else
                                   (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                                     ((Prelude..)
                                        (\ bs
                                           -> (Data.Monoid.<>)
                                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                   (Prelude.fromIntegral
                                                      (Data.ByteString.length bs)))
                                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                        Data.Text.Encoding.encodeUtf8 _v))
                            ((Data.Monoid.<>)
                               (case
                                    Lens.Family2.view
                                      (Data.ProtoLens.Field.field @"maybe'expiresUnixSeconds") _x
                                of
                                  Prelude.Nothing -> Data.Monoid.mempty
                                  (Prelude.Just _v)
                                    -> (Data.Monoid.<>)
                                         (Data.ProtoLens.Encoding.Bytes.putVarInt 56)
                                         ((Prelude..)
                                            Data.ProtoLens.Encoding.Bytes.putVarInt
                                            Prelude.fromIntegral _v))
                               (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                  (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))))
instance Control.DeepSeq.NFData ObjectMetadata where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ObjectMetadata'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ObjectMetadata'contentType x__)
                (Control.DeepSeq.deepseq
                   (_ObjectMetadata'user x__)
                   (Control.DeepSeq.deepseq
                      (_ObjectMetadata'contentEncoding x__)
                      (Control.DeepSeq.deepseq
                         (_ObjectMetadata'cacheControl x__)
                         (Control.DeepSeq.deepseq
                            (_ObjectMetadata'contentDisposition x__)
                            (Control.DeepSeq.deepseq
                               (_ObjectMetadata'contentLanguage x__)
                               (Control.DeepSeq.deepseq
                                  (_ObjectMetadata'expiresUnixSeconds x__) ())))))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.key' @:: Lens' ObjectMetadata'UserEntry Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.value' @:: Lens' ObjectMetadata'UserEntry Data.Text.Text@ -}
data ObjectMetadata'UserEntry
  = ObjectMetadata'UserEntry'_constructor {_ObjectMetadata'UserEntry'key :: !Data.Text.Text,
                                           _ObjectMetadata'UserEntry'value :: !Data.Text.Text,
                                           _ObjectMetadata'UserEntry'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ObjectMetadata'UserEntry where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ObjectMetadata'UserEntry "key" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'UserEntry'key
           (\ x__ y__ -> x__ {_ObjectMetadata'UserEntry'key = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectMetadata'UserEntry "value" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectMetadata'UserEntry'value
           (\ x__ y__ -> x__ {_ObjectMetadata'UserEntry'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message ObjectMetadata'UserEntry where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.ObjectMetadata.UserEntry"
  packedMessageDescriptor _
    = "\n\
      \\tUserEntry\DC2\DLE\n\
      \\ETXkey\CAN\SOH \SOH(\tR\ETXkey\DC2\DC4\n\
      \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue:\STX8\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        key__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"key")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata'UserEntry
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor ObjectMetadata'UserEntry
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, key__field_descriptor),
           (Data.ProtoLens.Tag 2, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ObjectMetadata'UserEntry'_unknownFields
        (\ x__ y__ -> x__ {_ObjectMetadata'UserEntry'_unknownFields = y__})
  defMessage
    = ObjectMetadata'UserEntry'_constructor
        {_ObjectMetadata'UserEntry'key = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'UserEntry'value = Data.ProtoLens.fieldDefault,
         _ObjectMetadata'UserEntry'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ObjectMetadata'UserEntry
          -> Data.ProtoLens.Encoding.Bytes.Parser ObjectMetadata'UserEntry
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
                                       "key"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"key") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "value"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"value") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UserEntry"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"key") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"value") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ObjectMetadata'UserEntry where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ObjectMetadata'UserEntry'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ObjectMetadata'UserEntry'key x__)
                (Control.DeepSeq.deepseq (_ObjectMetadata'UserEntry'value x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.versionId' @:: Lens' ObjectVersion Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.etag' @:: Lens' ObjectVersion Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.size' @:: Lens' ObjectVersion Data.Word.Word64@
         * 'Proto.Objects.V1.Objects_Fields.deleteMarker' @:: Lens' ObjectVersion Prelude.Bool@
         * 'Proto.Objects.V1.Objects_Fields.metadata' @:: Lens' ObjectVersion ObjectMetadata@
         * 'Proto.Objects.V1.Objects_Fields.maybe'metadata' @:: Lens' ObjectVersion (Prelude.Maybe ObjectMetadata)@
         * 'Proto.Objects.V1.Objects_Fields.createdAt' @:: Lens' ObjectVersion Proto.Google.Protobuf.Timestamp.Timestamp@
         * 'Proto.Objects.V1.Objects_Fields.maybe'createdAt' @:: Lens' ObjectVersion (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp)@ -}
data ObjectVersion
  = ObjectVersion'_constructor {_ObjectVersion'versionId :: !Data.Text.Text,
                                _ObjectVersion'etag :: !Data.Text.Text,
                                _ObjectVersion'size :: !Data.Word.Word64,
                                _ObjectVersion'deleteMarker :: !Prelude.Bool,
                                _ObjectVersion'metadata :: !(Prelude.Maybe ObjectMetadata),
                                _ObjectVersion'createdAt :: !(Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp),
                                _ObjectVersion'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ObjectVersion where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ObjectVersion "versionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'versionId
           (\ x__ y__ -> x__ {_ObjectVersion'versionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectVersion "etag" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'etag (\ x__ y__ -> x__ {_ObjectVersion'etag = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectVersion "size" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'size (\ x__ y__ -> x__ {_ObjectVersion'size = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectVersion "deleteMarker" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'deleteMarker
           (\ x__ y__ -> x__ {_ObjectVersion'deleteMarker = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectVersion "metadata" ObjectMetadata where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'metadata
           (\ x__ y__ -> x__ {_ObjectVersion'metadata = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ObjectVersion "maybe'metadata" (Prelude.Maybe ObjectMetadata) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'metadata
           (\ x__ y__ -> x__ {_ObjectVersion'metadata = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectVersion "createdAt" Proto.Google.Protobuf.Timestamp.Timestamp where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'createdAt
           (\ x__ y__ -> x__ {_ObjectVersion'createdAt = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ObjectVersion "maybe'createdAt" (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectVersion'createdAt
           (\ x__ y__ -> x__ {_ObjectVersion'createdAt = y__}))
        Prelude.id
instance Data.ProtoLens.Message ObjectVersion where
  messageName _ = Data.Text.pack "acyclic.objects.v1.ObjectVersion"
  packedMessageDescriptor _
    = "\n\
      \\rObjectVersion\DC2\GS\n\
      \\n\
      \version_id\CAN\SOH \SOH(\tR\tversionId\DC2\DC2\n\
      \\EOTetag\CAN\STX \SOH(\tR\EOTetag\DC2\DC2\n\
      \\EOTsize\CAN\ETX \SOH(\EOTR\EOTsize\DC2#\n\
      \\rdelete_marker\CAN\EOT \SOH(\bR\fdeleteMarker\DC2>\n\
      \\bmetadata\CAN\ENQ \SOH(\v2\".acyclic.objects.v1.ObjectMetadataR\bmetadata\DC29\n\
      \\n\
      \created_at\CAN\ACK \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        versionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"versionId")) ::
              Data.ProtoLens.FieldDescriptor ObjectVersion
        etag__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "etag"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"etag")) ::
              Data.ProtoLens.FieldDescriptor ObjectVersion
        size__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "size"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"size")) ::
              Data.ProtoLens.FieldDescriptor ObjectVersion
        deleteMarker__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "delete_marker"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"deleteMarker")) ::
              Data.ProtoLens.FieldDescriptor ObjectVersion
        metadata__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metadata"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectMetadata)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'metadata")) ::
              Data.ProtoLens.FieldDescriptor ObjectVersion
        createdAt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created_at"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Proto.Google.Protobuf.Timestamp.Timestamp)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'createdAt")) ::
              Data.ProtoLens.FieldDescriptor ObjectVersion
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, versionId__field_descriptor),
           (Data.ProtoLens.Tag 2, etag__field_descriptor),
           (Data.ProtoLens.Tag 3, size__field_descriptor),
           (Data.ProtoLens.Tag 4, deleteMarker__field_descriptor),
           (Data.ProtoLens.Tag 5, metadata__field_descriptor),
           (Data.ProtoLens.Tag 6, createdAt__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ObjectVersion'_unknownFields
        (\ x__ y__ -> x__ {_ObjectVersion'_unknownFields = y__})
  defMessage
    = ObjectVersion'_constructor
        {_ObjectVersion'versionId = Data.ProtoLens.fieldDefault,
         _ObjectVersion'etag = Data.ProtoLens.fieldDefault,
         _ObjectVersion'size = Data.ProtoLens.fieldDefault,
         _ObjectVersion'deleteMarker = Data.ProtoLens.fieldDefault,
         _ObjectVersion'metadata = Prelude.Nothing,
         _ObjectVersion'createdAt = Prelude.Nothing,
         _ObjectVersion'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ObjectVersion -> Data.ProtoLens.Encoding.Bytes.Parser ObjectVersion
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
                                       "version_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"versionId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "etag"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"etag") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "size"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"size") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "delete_marker"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"deleteMarker") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "metadata"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"metadata") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "created_at"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"createdAt") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ObjectVersion"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"versionId") _x
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"etag") _x
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
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"size") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let
                         _v
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"deleteMarker") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (\ b -> if b then 1 else 0) _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'metadata") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                   ((Prelude..)
                                      (\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                      Data.ProtoLens.encodeMessage _v))
                         ((Data.Monoid.<>)
                            (case
                                 Lens.Family2.view
                                   (Data.ProtoLens.Field.field @"maybe'createdAt") _x
                             of
                               Prelude.Nothing -> Data.Monoid.mempty
                               (Prelude.Just _v)
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                                      ((Prelude..)
                                         (\ bs
                                            -> (Data.Monoid.<>)
                                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                    (Prelude.fromIntegral
                                                       (Data.ByteString.length bs)))
                                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                         Data.ProtoLens.encodeMessage _v))
                            (Data.ProtoLens.Encoding.Wire.buildFieldSet
                               (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))
instance Control.DeepSeq.NFData ObjectVersion where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ObjectVersion'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ObjectVersion'versionId x__)
                (Control.DeepSeq.deepseq
                   (_ObjectVersion'etag x__)
                   (Control.DeepSeq.deepseq
                      (_ObjectVersion'size x__)
                      (Control.DeepSeq.deepseq
                         (_ObjectVersion'deleteMarker x__)
                         (Control.DeepSeq.deepseq
                            (_ObjectVersion'metadata x__)
                            (Control.DeepSeq.deepseq (_ObjectVersion'createdAt x__) ()))))))
newtype ObjectsLimit'UnrecognizedValue
  = ObjectsLimit'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ObjectsLimit
  = OBJECTS_LIMIT_UNSPECIFIED |
    OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES |
    ObjectsLimit'Unrecognized !ObjectsLimit'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ObjectsLimit where
  maybeToEnum 0 = Prelude.Just OBJECTS_LIMIT_UNSPECIFIED
  maybeToEnum 256
    = Prelude.Just OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  maybeToEnum k
    = Prelude.Just
        (ObjectsLimit'Unrecognized
           (ObjectsLimit'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum OBJECTS_LIMIT_UNSPECIFIED = "OBJECTS_LIMIT_UNSPECIFIED"
  showEnum OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = "OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES"
  showEnum
    (ObjectsLimit'Unrecognized (ObjectsLimit'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "OBJECTS_LIMIT_UNSPECIFIED"
    = Prelude.Just OBJECTS_LIMIT_UNSPECIFIED
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES"
    = Prelude.Just OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ObjectsLimit where
  minBound = OBJECTS_LIMIT_UNSPECIFIED
  maxBound = OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
instance Prelude.Enum ObjectsLimit where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ObjectsLimit: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum OBJECTS_LIMIT_UNSPECIFIED = 0
  fromEnum OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES = 256
  fromEnum
    (ObjectsLimit'Unrecognized (ObjectsLimit'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = Prelude.error
        "ObjectsLimit.succ: bad argument OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES. This value would be out of bounds."
  succ OBJECTS_LIMIT_UNSPECIFIED
    = OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  succ (ObjectsLimit'Unrecognized _)
    = Prelude.error
        "ObjectsLimit.succ: bad argument: unrecognized value"
  pred OBJECTS_LIMIT_UNSPECIFIED
    = Prelude.error
        "ObjectsLimit.pred: bad argument OBJECTS_LIMIT_UNSPECIFIED. This value would be out of bounds."
  pred OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = OBJECTS_LIMIT_UNSPECIFIED
  pred (ObjectsLimit'Unrecognized _)
    = Prelude.error
        "ObjectsLimit.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ObjectsLimit where
  fieldDefault = OBJECTS_LIMIT_UNSPECIFIED
instance Control.DeepSeq.NFData ObjectsLimit where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.maybe'condition' @:: Lens' Preconditions (Prelude.Maybe Preconditions'Condition)@
         * 'Proto.Objects.V1.Objects_Fields.maybe'ifAbsent' @:: Lens' Preconditions (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Objects.V1.Objects_Fields.ifAbsent' @:: Lens' Preconditions Prelude.Bool@
         * 'Proto.Objects.V1.Objects_Fields.maybe'ifMatch' @:: Lens' Preconditions (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Objects.V1.Objects_Fields.ifMatch' @:: Lens' Preconditions Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.maybe'ifVersion' @:: Lens' Preconditions (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Objects.V1.Objects_Fields.ifVersion' @:: Lens' Preconditions Data.Text.Text@ -}
data Preconditions
  = Preconditions'_constructor {_Preconditions'condition :: !(Prelude.Maybe Preconditions'Condition),
                                _Preconditions'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Preconditions where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data Preconditions'Condition
  = Preconditions'IfAbsent !Prelude.Bool |
    Preconditions'IfMatch !Data.Text.Text |
    Preconditions'IfVersion !Data.Text.Text
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField Preconditions "maybe'condition" (Prelude.Maybe Preconditions'Condition) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Preconditions "maybe'ifAbsent" (Prelude.Maybe Prelude.Bool) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Preconditions'IfAbsent x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Preconditions'IfAbsent y__))
instance Data.ProtoLens.Field.HasField Preconditions "ifAbsent" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Preconditions'IfAbsent x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Preconditions'IfAbsent y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField Preconditions "maybe'ifMatch" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Preconditions'IfMatch x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Preconditions'IfMatch y__))
instance Data.ProtoLens.Field.HasField Preconditions "ifMatch" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Preconditions'IfMatch x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Preconditions'IfMatch y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField Preconditions "maybe'ifVersion" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Preconditions'IfVersion x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Preconditions'IfVersion y__))
instance Data.ProtoLens.Field.HasField Preconditions "ifVersion" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Preconditions'condition
           (\ x__ y__ -> x__ {_Preconditions'condition = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Preconditions'IfVersion x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Preconditions'IfVersion y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message Preconditions where
  messageName _ = Data.Text.pack "acyclic.objects.v1.Preconditions"
  packedMessageDescriptor _
    = "\n\
      \\rPreconditions\DC2\GS\n\
      \\tif_absent\CAN\SOH \SOH(\bH\NULR\bifAbsent\DC2\ESC\n\
      \\bif_match\CAN\STX \SOH(\tH\NULR\aifMatch\DC2\US\n\
      \\n\
      \if_version\CAN\ETX \SOH(\tH\NULR\tifVersionB\v\n\
      \\tcondition"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        ifAbsent__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_absent"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'ifAbsent")) ::
              Data.ProtoLens.FieldDescriptor Preconditions
        ifMatch__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_match"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'ifMatch")) ::
              Data.ProtoLens.FieldDescriptor Preconditions
        ifVersion__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "if_version"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'ifVersion")) ::
              Data.ProtoLens.FieldDescriptor Preconditions
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, ifAbsent__field_descriptor),
           (Data.ProtoLens.Tag 2, ifMatch__field_descriptor),
           (Data.ProtoLens.Tag 3, ifVersion__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Preconditions'_unknownFields
        (\ x__ y__ -> x__ {_Preconditions'_unknownFields = y__})
  defMessage
    = Preconditions'_constructor
        {_Preconditions'condition = Prelude.Nothing,
         _Preconditions'_unknownFields = []}
  parseMessage
    = let
        loop ::
          Preconditions -> Data.ProtoLens.Encoding.Bytes.Parser Preconditions
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
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "if_absent"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"ifAbsent") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "if_match"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"ifMatch") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "if_version"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"ifVersion") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Preconditions"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'condition") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (Preconditions'IfAbsent v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                       ((Prelude..)
                          Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                          v)
                (Prelude.Just (Preconditions'IfMatch v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.Text.Encoding.encodeUtf8 v)
                (Prelude.Just (Preconditions'IfVersion v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.Text.Encoding.encodeUtf8 v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData Preconditions where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Preconditions'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Preconditions'condition x__) ())
instance Control.DeepSeq.NFData Preconditions'Condition where
  rnf (Preconditions'IfAbsent x__) = Control.DeepSeq.rnf x__
  rnf (Preconditions'IfMatch x__) = Control.DeepSeq.rnf x__
  rnf (Preconditions'IfVersion x__) = Control.DeepSeq.rnf x__
_Preconditions'IfAbsent ::
  Data.ProtoLens.Prism.Prism' Preconditions'Condition Prelude.Bool
_Preconditions'IfAbsent
  = Data.ProtoLens.Prism.prism'
      Preconditions'IfAbsent
      (\ p__
         -> case p__ of
              (Preconditions'IfAbsent p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Preconditions'IfMatch ::
  Data.ProtoLens.Prism.Prism' Preconditions'Condition Data.Text.Text
_Preconditions'IfMatch
  = Data.ProtoLens.Prism.prism'
      Preconditions'IfMatch
      (\ p__
         -> case p__ of
              (Preconditions'IfMatch p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Preconditions'IfVersion ::
  Data.ProtoLens.Prism.Prism' Preconditions'Condition Data.Text.Text
_Preconditions'IfVersion
  = Data.ProtoLens.Prism.prism'
      Preconditions'IfVersion
      (\ p__
         -> case p__ of
              (Preconditions'IfVersion p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' PutObjectHeader BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' PutObjectHeader (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' PutObjectHeader Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.metadata' @:: Lens' PutObjectHeader ObjectMetadata@
         * 'Proto.Objects.V1.Objects_Fields.maybe'metadata' @:: Lens' PutObjectHeader (Prelude.Maybe ObjectMetadata)@
         * 'Proto.Objects.V1.Objects_Fields.preconditions' @:: Lens' PutObjectHeader Preconditions@
         * 'Proto.Objects.V1.Objects_Fields.maybe'preconditions' @:: Lens' PutObjectHeader (Prelude.Maybe Preconditions)@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' PutObjectHeader MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' PutObjectHeader (Prelude.Maybe MutationIdentity)@ -}
data PutObjectHeader
  = PutObjectHeader'_constructor {_PutObjectHeader'bucket :: !(Prelude.Maybe BucketRef),
                                  _PutObjectHeader'objectKey :: !Data.Text.Text,
                                  _PutObjectHeader'metadata :: !(Prelude.Maybe ObjectMetadata),
                                  _PutObjectHeader'preconditions :: !(Prelude.Maybe Preconditions),
                                  _PutObjectHeader'mutation :: !(Prelude.Maybe MutationIdentity),
                                  _PutObjectHeader'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show PutObjectHeader where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField PutObjectHeader "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'bucket
           (\ x__ y__ -> x__ {_PutObjectHeader'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PutObjectHeader "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'bucket
           (\ x__ y__ -> x__ {_PutObjectHeader'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PutObjectHeader "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'objectKey
           (\ x__ y__ -> x__ {_PutObjectHeader'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PutObjectHeader "metadata" ObjectMetadata where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'metadata
           (\ x__ y__ -> x__ {_PutObjectHeader'metadata = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PutObjectHeader "maybe'metadata" (Prelude.Maybe ObjectMetadata) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'metadata
           (\ x__ y__ -> x__ {_PutObjectHeader'metadata = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PutObjectHeader "preconditions" Preconditions where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'preconditions
           (\ x__ y__ -> x__ {_PutObjectHeader'preconditions = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PutObjectHeader "maybe'preconditions" (Prelude.Maybe Preconditions) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'preconditions
           (\ x__ y__ -> x__ {_PutObjectHeader'preconditions = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PutObjectHeader "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'mutation
           (\ x__ y__ -> x__ {_PutObjectHeader'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PutObjectHeader "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectHeader'mutation
           (\ x__ y__ -> x__ {_PutObjectHeader'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message PutObjectHeader where
  messageName _ = Data.Text.pack "acyclic.objects.v1.PutObjectHeader"
  packedMessageDescriptor _
    = "\n\
      \\SIPutObjectHeader\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
      \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v1.ObjectMetadataR\bmetadata\DC2G\n\
      \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v1.PreconditionsR\rpreconditions\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor PutObjectHeader
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor PutObjectHeader
        metadata__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metadata"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectMetadata)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'metadata")) ::
              Data.ProtoLens.FieldDescriptor PutObjectHeader
        preconditions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "preconditions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Preconditions)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'preconditions")) ::
              Data.ProtoLens.FieldDescriptor PutObjectHeader
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor PutObjectHeader
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, metadata__field_descriptor),
           (Data.ProtoLens.Tag 4, preconditions__field_descriptor),
           (Data.ProtoLens.Tag 5, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _PutObjectHeader'_unknownFields
        (\ x__ y__ -> x__ {_PutObjectHeader'_unknownFields = y__})
  defMessage
    = PutObjectHeader'_constructor
        {_PutObjectHeader'bucket = Prelude.Nothing,
         _PutObjectHeader'objectKey = Data.ProtoLens.fieldDefault,
         _PutObjectHeader'metadata = Prelude.Nothing,
         _PutObjectHeader'preconditions = Prelude.Nothing,
         _PutObjectHeader'mutation = Prelude.Nothing,
         _PutObjectHeader'_unknownFields = []}
  parseMessage
    = let
        loop ::
          PutObjectHeader
          -> Data.ProtoLens.Encoding.Bytes.Parser PutObjectHeader
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "metadata"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"metadata") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "preconditions"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"preconditions") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "PutObjectHeader"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'metadata") _x
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
                                Data.ProtoLens.encodeMessage _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'preconditions") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                                ((Prelude..)
                                   (\ bs
                                      -> (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt
                                              (Prelude.fromIntegral (Data.ByteString.length bs)))
                                           (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                   Data.ProtoLens.encodeMessage _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                   ((Prelude..)
                                      (\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                      Data.ProtoLens.encodeMessage _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData PutObjectHeader where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_PutObjectHeader'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_PutObjectHeader'bucket x__)
                (Control.DeepSeq.deepseq
                   (_PutObjectHeader'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_PutObjectHeader'metadata x__)
                      (Control.DeepSeq.deepseq
                         (_PutObjectHeader'preconditions x__)
                         (Control.DeepSeq.deepseq (_PutObjectHeader'mutation x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.maybe'frame' @:: Lens' PutObjectRequest (Prelude.Maybe PutObjectRequest'Frame)@
         * 'Proto.Objects.V1.Objects_Fields.maybe'header' @:: Lens' PutObjectRequest (Prelude.Maybe PutObjectHeader)@
         * 'Proto.Objects.V1.Objects_Fields.header' @:: Lens' PutObjectRequest PutObjectHeader@
         * 'Proto.Objects.V1.Objects_Fields.maybe'body' @:: Lens' PutObjectRequest (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Objects.V1.Objects_Fields.body' @:: Lens' PutObjectRequest Data.ByteString.ByteString@ -}
data PutObjectRequest
  = PutObjectRequest'_constructor {_PutObjectRequest'frame :: !(Prelude.Maybe PutObjectRequest'Frame),
                                   _PutObjectRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show PutObjectRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data PutObjectRequest'Frame
  = PutObjectRequest'Header !PutObjectHeader |
    PutObjectRequest'Body !Data.ByteString.ByteString
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField PutObjectRequest "maybe'frame" (Prelude.Maybe PutObjectRequest'Frame) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PutObjectRequest "maybe'header" (Prelude.Maybe PutObjectHeader) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (PutObjectRequest'Header x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap PutObjectRequest'Header y__))
instance Data.ProtoLens.Field.HasField PutObjectRequest "header" PutObjectHeader where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (PutObjectRequest'Header x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap PutObjectRequest'Header y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField PutObjectRequest "maybe'body" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (PutObjectRequest'Body x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap PutObjectRequest'Body y__))
instance Data.ProtoLens.Field.HasField PutObjectRequest "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (PutObjectRequest'Body x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap PutObjectRequest'Body y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message PutObjectRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.PutObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEPutObjectRequest\DC2=\n\
      \\ACKheader\CAN\SOH \SOH(\v2#.acyclic.objects.v1.PutObjectHeaderH\NULR\ACKheader\DC2\DC4\n\
      \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbodyB\a\n\
      \\ENQframe"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        header__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "header"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor PutObjectHeader)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'header")) ::
              Data.ProtoLens.FieldDescriptor PutObjectRequest
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'body")) ::
              Data.ProtoLens.FieldDescriptor PutObjectRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, header__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _PutObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_PutObjectRequest'_unknownFields = y__})
  defMessage
    = PutObjectRequest'_constructor
        {_PutObjectRequest'frame = Prelude.Nothing,
         _PutObjectRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          PutObjectRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser PutObjectRequest
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
                                       "header"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"header") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "PutObjectRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'frame") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (PutObjectRequest'Header v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (PutObjectRequest'Body v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((\ bs
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                   (Prelude.fromIntegral (Data.ByteString.length bs)))
                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData PutObjectRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_PutObjectRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_PutObjectRequest'frame x__) ())
instance Control.DeepSeq.NFData PutObjectRequest'Frame where
  rnf (PutObjectRequest'Header x__) = Control.DeepSeq.rnf x__
  rnf (PutObjectRequest'Body x__) = Control.DeepSeq.rnf x__
_PutObjectRequest'Header ::
  Data.ProtoLens.Prism.Prism' PutObjectRequest'Frame PutObjectHeader
_PutObjectRequest'Header
  = Data.ProtoLens.Prism.prism'
      PutObjectRequest'Header
      (\ p__
         -> case p__ of
              (PutObjectRequest'Header p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_PutObjectRequest'Body ::
  Data.ProtoLens.Prism.Prism' PutObjectRequest'Frame Data.ByteString.ByteString
_PutObjectRequest'Body
  = Data.ProtoLens.Prism.prism'
      PutObjectRequest'Body
      (\ p__
         -> case p__ of
              (PutObjectRequest'Body p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.maybe'target' @:: Lens' ReadTarget (Prelude.Maybe ReadTarget'Target)@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' ReadTarget (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' ReadTarget BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'snapshot' @:: Lens' ReadTarget (Prelude.Maybe SnapshotRef)@
         * 'Proto.Objects.V1.Objects_Fields.snapshot' @:: Lens' ReadTarget SnapshotRef@ -}
data ReadTarget
  = ReadTarget'_constructor {_ReadTarget'target :: !(Prelude.Maybe ReadTarget'Target),
                             _ReadTarget'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ReadTarget where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data ReadTarget'Target
  = ReadTarget'Bucket !BucketRef | ReadTarget'Snapshot !SnapshotRef
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField ReadTarget "maybe'target" (Prelude.Maybe ReadTarget'Target) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadTarget'target (\ x__ y__ -> x__ {_ReadTarget'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ReadTarget "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadTarget'target (\ x__ y__ -> x__ {_ReadTarget'target = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ReadTarget'Bucket x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ReadTarget'Bucket y__))
instance Data.ProtoLens.Field.HasField ReadTarget "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadTarget'target (\ x__ y__ -> x__ {_ReadTarget'target = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ReadTarget'Bucket x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ReadTarget'Bucket y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ReadTarget "maybe'snapshot" (Prelude.Maybe SnapshotRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadTarget'target (\ x__ y__ -> x__ {_ReadTarget'target = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ReadTarget'Snapshot x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ReadTarget'Snapshot y__))
instance Data.ProtoLens.Field.HasField ReadTarget "snapshot" SnapshotRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReadTarget'target (\ x__ y__ -> x__ {_ReadTarget'target = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ReadTarget'Snapshot x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ReadTarget'Snapshot y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message ReadTarget where
  messageName _ = Data.Text.pack "acyclic.objects.v1.ReadTarget"
  packedMessageDescriptor _
    = "\n\
      \\n\
      \ReadTarget\DC27\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefH\NULR\ACKbucket\DC2=\n\
      \\bsnapshot\CAN\STX \SOH(\v2\US.acyclic.objects.v1.SnapshotRefH\NULR\bsnapshotB\b\n\
      \\ACKtarget"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor ReadTarget
        snapshot__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "snapshot"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SnapshotRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'snapshot")) ::
              Data.ProtoLens.FieldDescriptor ReadTarget
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, snapshot__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ReadTarget'_unknownFields
        (\ x__ y__ -> x__ {_ReadTarget'_unknownFields = y__})
  defMessage
    = ReadTarget'_constructor
        {_ReadTarget'target = Prelude.Nothing,
         _ReadTarget'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ReadTarget -> Data.ProtoLens.Encoding.Bytes.Parser ReadTarget
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "snapshot"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"snapshot") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ReadTarget"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'target") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (ReadTarget'Bucket v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ReadTarget'Snapshot v))
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
instance Control.DeepSeq.NFData ReadTarget where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ReadTarget'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ReadTarget'target x__) ())
instance Control.DeepSeq.NFData ReadTarget'Target where
  rnf (ReadTarget'Bucket x__) = Control.DeepSeq.rnf x__
  rnf (ReadTarget'Snapshot x__) = Control.DeepSeq.rnf x__
_ReadTarget'Bucket ::
  Data.ProtoLens.Prism.Prism' ReadTarget'Target BucketRef
_ReadTarget'Bucket
  = Data.ProtoLens.Prism.prism'
      ReadTarget'Bucket
      (\ p__
         -> case p__ of
              (ReadTarget'Bucket p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ReadTarget'Snapshot ::
  Data.ProtoLens.Prism.Prism' ReadTarget'Target SnapshotRef
_ReadTarget'Snapshot
  = Data.ProtoLens.Prism.prism'
      ReadTarget'Snapshot
      (\ p__
         -> case p__ of
              (ReadTarget'Snapshot p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.snapshot' @:: Lens' Snapshot SnapshotRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'snapshot' @:: Lens' Snapshot (Prelude.Maybe SnapshotRef)@
         * 'Proto.Objects.V1.Objects_Fields.createdAt' @:: Lens' Snapshot Proto.Google.Protobuf.Timestamp.Timestamp@
         * 'Proto.Objects.V1.Objects_Fields.maybe'createdAt' @:: Lens' Snapshot (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp)@ -}
data Snapshot
  = Snapshot'_constructor {_Snapshot'snapshot :: !(Prelude.Maybe SnapshotRef),
                           _Snapshot'createdAt :: !(Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp),
                           _Snapshot'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Snapshot where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Snapshot "snapshot" SnapshotRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Snapshot'snapshot (\ x__ y__ -> x__ {_Snapshot'snapshot = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField Snapshot "maybe'snapshot" (Prelude.Maybe SnapshotRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Snapshot'snapshot (\ x__ y__ -> x__ {_Snapshot'snapshot = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Snapshot "createdAt" Proto.Google.Protobuf.Timestamp.Timestamp where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Snapshot'createdAt (\ x__ y__ -> x__ {_Snapshot'createdAt = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField Snapshot "maybe'createdAt" (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Snapshot'createdAt (\ x__ y__ -> x__ {_Snapshot'createdAt = y__}))
        Prelude.id
instance Data.ProtoLens.Message Snapshot where
  messageName _ = Data.Text.pack "acyclic.objects.v1.Snapshot"
  packedMessageDescriptor _
    = "\n\
      \\bSnapshot\DC2;\n\
      \\bsnapshot\CAN\SOH \SOH(\v2\US.acyclic.objects.v1.SnapshotRefR\bsnapshot\DC29\n\
      \\n\
      \created_at\CAN\STX \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        snapshot__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "snapshot"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SnapshotRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'snapshot")) ::
              Data.ProtoLens.FieldDescriptor Snapshot
        createdAt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created_at"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Proto.Google.Protobuf.Timestamp.Timestamp)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'createdAt")) ::
              Data.ProtoLens.FieldDescriptor Snapshot
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, snapshot__field_descriptor),
           (Data.ProtoLens.Tag 2, createdAt__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Snapshot'_unknownFields
        (\ x__ y__ -> x__ {_Snapshot'_unknownFields = y__})
  defMessage
    = Snapshot'_constructor
        {_Snapshot'snapshot = Prelude.Nothing,
         _Snapshot'createdAt = Prelude.Nothing,
         _Snapshot'_unknownFields = []}
  parseMessage
    = let
        loop :: Snapshot -> Data.ProtoLens.Encoding.Bytes.Parser Snapshot
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
                                       "snapshot"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"snapshot") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "created_at"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"createdAt") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Snapshot"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'snapshot") _x
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
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view
                       (Data.ProtoLens.Field.field @"maybe'createdAt") _x
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
                             Data.ProtoLens.encodeMessage _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData Snapshot where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Snapshot'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Snapshot'snapshot x__)
                (Control.DeepSeq.deepseq (_Snapshot'createdAt x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.snapshotId' @:: Lens' SnapshotRef Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.sourceBucketId' @:: Lens' SnapshotRef Data.Text.Text@ -}
data SnapshotRef
  = SnapshotRef'_constructor {_SnapshotRef'snapshotId :: !Data.Text.Text,
                              _SnapshotRef'sourceBucketId :: !Data.Text.Text,
                              _SnapshotRef'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SnapshotRef where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SnapshotRef "snapshotId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SnapshotRef'snapshotId
           (\ x__ y__ -> x__ {_SnapshotRef'snapshotId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SnapshotRef "sourceBucketId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SnapshotRef'sourceBucketId
           (\ x__ y__ -> x__ {_SnapshotRef'sourceBucketId = y__}))
        Prelude.id
instance Data.ProtoLens.Message SnapshotRef where
  messageName _ = Data.Text.pack "acyclic.objects.v1.SnapshotRef"
  packedMessageDescriptor _
    = "\n\
      \\vSnapshotRef\DC2\US\n\
      \\vsnapshot_id\CAN\SOH \SOH(\tR\n\
      \snapshotId\DC2(\n\
      \\DLEsource_bucket_id\CAN\STX \SOH(\tR\SOsourceBucketId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        snapshotId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "snapshot_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"snapshotId")) ::
              Data.ProtoLens.FieldDescriptor SnapshotRef
        sourceBucketId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source_bucket_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sourceBucketId")) ::
              Data.ProtoLens.FieldDescriptor SnapshotRef
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, snapshotId__field_descriptor),
           (Data.ProtoLens.Tag 2, sourceBucketId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SnapshotRef'_unknownFields
        (\ x__ y__ -> x__ {_SnapshotRef'_unknownFields = y__})
  defMessage
    = SnapshotRef'_constructor
        {_SnapshotRef'snapshotId = Data.ProtoLens.fieldDefault,
         _SnapshotRef'sourceBucketId = Data.ProtoLens.fieldDefault,
         _SnapshotRef'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SnapshotRef -> Data.ProtoLens.Encoding.Bytes.Parser SnapshotRef
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
                                       "snapshot_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"snapshotId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "source_bucket_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"sourceBucketId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SnapshotRef"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"snapshotId") _x
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
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"sourceBucketId") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData SnapshotRef where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SnapshotRef'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SnapshotRef'snapshotId x__)
                (Control.DeepSeq.deepseq (_SnapshotRef'sourceBucketId x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.bucket' @:: Lens' UploadPartHeader BucketRef@
         * 'Proto.Objects.V1.Objects_Fields.maybe'bucket' @:: Lens' UploadPartHeader (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V1.Objects_Fields.objectKey' @:: Lens' UploadPartHeader Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.uploadId' @:: Lens' UploadPartHeader Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.partNumber' @:: Lens' UploadPartHeader Data.Word.Word32@
         * 'Proto.Objects.V1.Objects_Fields.mutation' @:: Lens' UploadPartHeader MutationIdentity@
         * 'Proto.Objects.V1.Objects_Fields.maybe'mutation' @:: Lens' UploadPartHeader (Prelude.Maybe MutationIdentity)@ -}
data UploadPartHeader
  = UploadPartHeader'_constructor {_UploadPartHeader'bucket :: !(Prelude.Maybe BucketRef),
                                   _UploadPartHeader'objectKey :: !Data.Text.Text,
                                   _UploadPartHeader'uploadId :: !Data.Text.Text,
                                   _UploadPartHeader'partNumber :: !Data.Word.Word32,
                                   _UploadPartHeader'mutation :: !(Prelude.Maybe MutationIdentity),
                                   _UploadPartHeader'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UploadPartHeader where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UploadPartHeader "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'bucket
           (\ x__ y__ -> x__ {_UploadPartHeader'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UploadPartHeader "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'bucket
           (\ x__ y__ -> x__ {_UploadPartHeader'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadPartHeader "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'objectKey
           (\ x__ y__ -> x__ {_UploadPartHeader'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadPartHeader "uploadId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'uploadId
           (\ x__ y__ -> x__ {_UploadPartHeader'uploadId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadPartHeader "partNumber" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'partNumber
           (\ x__ y__ -> x__ {_UploadPartHeader'partNumber = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadPartHeader "mutation" MutationIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'mutation
           (\ x__ y__ -> x__ {_UploadPartHeader'mutation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UploadPartHeader "maybe'mutation" (Prelude.Maybe MutationIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartHeader'mutation
           (\ x__ y__ -> x__ {_UploadPartHeader'mutation = y__}))
        Prelude.id
instance Data.ProtoLens.Message UploadPartHeader where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.UploadPartHeader"
  packedMessageDescriptor _
    = "\n\
      \\DLEUploadPartHeader\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2\US\n\
      \\vpart_number\CAN\EOT \SOH(\rR\n\
      \partNumber\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor BucketRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bucket")) ::
              Data.ProtoLens.FieldDescriptor UploadPartHeader
        objectKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"objectKey")) ::
              Data.ProtoLens.FieldDescriptor UploadPartHeader
        uploadId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "upload_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"uploadId")) ::
              Data.ProtoLens.FieldDescriptor UploadPartHeader
        partNumber__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "part_number"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"partNumber")) ::
              Data.ProtoLens.FieldDescriptor UploadPartHeader
        mutation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mutation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'mutation")) ::
              Data.ProtoLens.FieldDescriptor UploadPartHeader
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, uploadId__field_descriptor),
           (Data.ProtoLens.Tag 4, partNumber__field_descriptor),
           (Data.ProtoLens.Tag 5, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UploadPartHeader'_unknownFields
        (\ x__ y__ -> x__ {_UploadPartHeader'_unknownFields = y__})
  defMessage
    = UploadPartHeader'_constructor
        {_UploadPartHeader'bucket = Prelude.Nothing,
         _UploadPartHeader'objectKey = Data.ProtoLens.fieldDefault,
         _UploadPartHeader'uploadId = Data.ProtoLens.fieldDefault,
         _UploadPartHeader'partNumber = Data.ProtoLens.fieldDefault,
         _UploadPartHeader'mutation = Prelude.Nothing,
         _UploadPartHeader'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UploadPartHeader
          -> Data.ProtoLens.Encoding.Bytes.Parser UploadPartHeader
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "object_key"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"objectKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "upload_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"uploadId") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "part_number"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"partNumber") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "mutation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mutation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UploadPartHeader"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'bucket") _x
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
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"objectKey") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"uploadId") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"partNumber") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                                   ((Prelude..)
                                      (\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                      Data.ProtoLens.encodeMessage _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData UploadPartHeader where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UploadPartHeader'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_UploadPartHeader'bucket x__)
                (Control.DeepSeq.deepseq
                   (_UploadPartHeader'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_UploadPartHeader'uploadId x__)
                      (Control.DeepSeq.deepseq
                         (_UploadPartHeader'partNumber x__)
                         (Control.DeepSeq.deepseq (_UploadPartHeader'mutation x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.maybe'frame' @:: Lens' UploadPartRequest (Prelude.Maybe UploadPartRequest'Frame)@
         * 'Proto.Objects.V1.Objects_Fields.maybe'header' @:: Lens' UploadPartRequest (Prelude.Maybe UploadPartHeader)@
         * 'Proto.Objects.V1.Objects_Fields.header' @:: Lens' UploadPartRequest UploadPartHeader@
         * 'Proto.Objects.V1.Objects_Fields.maybe'body' @:: Lens' UploadPartRequest (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Objects.V1.Objects_Fields.body' @:: Lens' UploadPartRequest Data.ByteString.ByteString@ -}
data UploadPartRequest
  = UploadPartRequest'_constructor {_UploadPartRequest'frame :: !(Prelude.Maybe UploadPartRequest'Frame),
                                    _UploadPartRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UploadPartRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data UploadPartRequest'Frame
  = UploadPartRequest'Header !UploadPartHeader |
    UploadPartRequest'Body !Data.ByteString.ByteString
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField UploadPartRequest "maybe'frame" (Prelude.Maybe UploadPartRequest'Frame) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadPartRequest "maybe'header" (Prelude.Maybe UploadPartHeader) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (UploadPartRequest'Header x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap UploadPartRequest'Header y__))
instance Data.ProtoLens.Field.HasField UploadPartRequest "header" UploadPartHeader where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (UploadPartRequest'Header x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap UploadPartRequest'Header y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField UploadPartRequest "maybe'body" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (UploadPartRequest'Body x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap UploadPartRequest'Body y__))
instance Data.ProtoLens.Field.HasField UploadPartRequest "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (UploadPartRequest'Body x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap UploadPartRequest'Body y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message UploadPartRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v1.UploadPartRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1UploadPartRequest\DC2>\n\
      \\ACKheader\CAN\SOH \SOH(\v2$.acyclic.objects.v1.UploadPartHeaderH\NULR\ACKheader\DC2\DC4\n\
      \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbodyB\a\n\
      \\ENQframe"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        header__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "header"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor UploadPartHeader)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'header")) ::
              Data.ProtoLens.FieldDescriptor UploadPartRequest
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'body")) ::
              Data.ProtoLens.FieldDescriptor UploadPartRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, header__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UploadPartRequest'_unknownFields
        (\ x__ y__ -> x__ {_UploadPartRequest'_unknownFields = y__})
  defMessage
    = UploadPartRequest'_constructor
        {_UploadPartRequest'frame = Prelude.Nothing,
         _UploadPartRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UploadPartRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser UploadPartRequest
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
                                       "header"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"header") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UploadPartRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'frame") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (UploadPartRequest'Header v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (UploadPartRequest'Body v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((\ bs
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                   (Prelude.fromIntegral (Data.ByteString.length bs)))
                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData UploadPartRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UploadPartRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_UploadPartRequest'frame x__) ())
instance Control.DeepSeq.NFData UploadPartRequest'Frame where
  rnf (UploadPartRequest'Header x__) = Control.DeepSeq.rnf x__
  rnf (UploadPartRequest'Body x__) = Control.DeepSeq.rnf x__
_UploadPartRequest'Header ::
  Data.ProtoLens.Prism.Prism' UploadPartRequest'Frame UploadPartHeader
_UploadPartRequest'Header
  = Data.ProtoLens.Prism.prism'
      UploadPartRequest'Header
      (\ p__
         -> case p__ of
              (UploadPartRequest'Header p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_UploadPartRequest'Body ::
  Data.ProtoLens.Prism.Prism' UploadPartRequest'Frame Data.ByteString.ByteString
_UploadPartRequest'Body
  = Data.ProtoLens.Prism.prism'
      UploadPartRequest'Body
      (\ p__
         -> case p__ of
              (UploadPartRequest'Body p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V1.Objects_Fields.partNumber' @:: Lens' UploadedPart Data.Word.Word32@
         * 'Proto.Objects.V1.Objects_Fields.etag' @:: Lens' UploadedPart Data.Text.Text@
         * 'Proto.Objects.V1.Objects_Fields.size' @:: Lens' UploadedPart Data.Word.Word64@ -}
data UploadedPart
  = UploadedPart'_constructor {_UploadedPart'partNumber :: !Data.Word.Word32,
                               _UploadedPart'etag :: !Data.Text.Text,
                               _UploadedPart'size :: !Data.Word.Word64,
                               _UploadedPart'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UploadedPart where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UploadedPart "partNumber" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadedPart'partNumber
           (\ x__ y__ -> x__ {_UploadedPart'partNumber = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadedPart "etag" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadedPart'etag (\ x__ y__ -> x__ {_UploadedPart'etag = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UploadedPart "size" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadedPart'size (\ x__ y__ -> x__ {_UploadedPart'size = y__}))
        Prelude.id
instance Data.ProtoLens.Message UploadedPart where
  messageName _ = Data.Text.pack "acyclic.objects.v1.UploadedPart"
  packedMessageDescriptor _
    = "\n\
      \\fUploadedPart\DC2\US\n\
      \\vpart_number\CAN\SOH \SOH(\rR\n\
      \partNumber\DC2\DC2\n\
      \\EOTetag\CAN\STX \SOH(\tR\EOTetag\DC2\DC2\n\
      \\EOTsize\CAN\ETX \SOH(\EOTR\EOTsize"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        partNumber__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "part_number"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"partNumber")) ::
              Data.ProtoLens.FieldDescriptor UploadedPart
        etag__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "etag"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"etag")) ::
              Data.ProtoLens.FieldDescriptor UploadedPart
        size__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "size"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"size")) ::
              Data.ProtoLens.FieldDescriptor UploadedPart
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, partNumber__field_descriptor),
           (Data.ProtoLens.Tag 2, etag__field_descriptor),
           (Data.ProtoLens.Tag 3, size__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UploadedPart'_unknownFields
        (\ x__ y__ -> x__ {_UploadedPart'_unknownFields = y__})
  defMessage
    = UploadedPart'_constructor
        {_UploadedPart'partNumber = Data.ProtoLens.fieldDefault,
         _UploadedPart'etag = Data.ProtoLens.fieldDefault,
         _UploadedPart'size = Data.ProtoLens.fieldDefault,
         _UploadedPart'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UploadedPart -> Data.ProtoLens.Encoding.Bytes.Parser UploadedPart
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
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "part_number"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"partNumber") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "etag"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"etag") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "size"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"size") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UploadedPart"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"partNumber") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
             ((Data.Monoid.<>)
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"etag") _x
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
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"size") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData UploadedPart where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UploadedPart'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_UploadedPart'partNumber x__)
                (Control.DeepSeq.deepseq
                   (_UploadedPart'etag x__)
                   (Control.DeepSeq.deepseq (_UploadedPart'size x__) ())))
data BucketsService = BucketsService {}
instance Data.ProtoLens.Service.Types.Service BucketsService where
  type ServiceName BucketsService = "BucketsService"
  type ServicePackage BucketsService = "acyclic.objects.v1"
  type ServiceMethods BucketsService = '["createBucket",
                                         "deleteBucket",
                                         "headBucket"]
  packedServiceDescriptor _
    = "\n\
      \\SOBucketsService\DC2S\n\
      \\fCreateBucket\DC2'.acyclic.objects.v1.CreateBucketRequest\SUB\SUB.acyclic.objects.v1.Bucket\DC2O\n\
      \\n\
      \HeadBucket\DC2%.acyclic.objects.v1.HeadBucketRequest\SUB\SUB.acyclic.objects.v1.Bucket\DC2a\n\
      \\fDeleteBucket\DC2'.acyclic.objects.v1.DeleteBucketRequest\SUB(.acyclic.objects.v1.DeleteBucketResponse"
instance Data.ProtoLens.Service.Types.HasMethodImpl BucketsService "createBucket" where
  type MethodName BucketsService "createBucket" = "CreateBucket"
  type MethodInput BucketsService "createBucket" = CreateBucketRequest
  type MethodOutput BucketsService "createBucket" = Bucket
  type MethodStreamingType BucketsService "createBucket" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl BucketsService "headBucket" where
  type MethodName BucketsService "headBucket" = "HeadBucket"
  type MethodInput BucketsService "headBucket" = HeadBucketRequest
  type MethodOutput BucketsService "headBucket" = Bucket
  type MethodStreamingType BucketsService "headBucket" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl BucketsService "deleteBucket" where
  type MethodName BucketsService "deleteBucket" = "DeleteBucket"
  type MethodInput BucketsService "deleteBucket" = DeleteBucketRequest
  type MethodOutput BucketsService "deleteBucket" = DeleteBucketResponse
  type MethodStreamingType BucketsService "deleteBucket" = 'Data.ProtoLens.Service.Types.NonStreaming
data ObjectsService = ObjectsService {}
instance Data.ProtoLens.Service.Types.Service ObjectsService where
  type ServiceName ObjectsService = "ObjectsService"
  type ServicePackage ObjectsService = "acyclic.objects.v1"
  type ServiceMethods ObjectsService = '["deleteObject",
                                         "getObject",
                                         "headObject",
                                         "listObjects",
                                         "putObject"]
  packedServiceDescriptor _
    = "\n\
      \\SOObjectsService\DC2V\n\
      \\tPutObject\DC2$.acyclic.objects.v1.PutObjectRequest\SUB!.acyclic.objects.v1.ObjectVersion(\SOH\DC2Z\n\
      \\tGetObject\DC2$.acyclic.objects.v1.GetObjectRequest\SUB%.acyclic.objects.v1.GetObjectResponse0\SOH\DC2[\n\
      \\n\
      \HeadObject\DC2%.acyclic.objects.v1.HeadObjectRequest\SUB&.acyclic.objects.v1.HeadObjectResponse\DC2a\n\
      \\fDeleteObject\DC2'.acyclic.objects.v1.DeleteObjectRequest\SUB(.acyclic.objects.v1.DeleteObjectResponse\DC2^\n\
      \\vListObjects\DC2&.acyclic.objects.v1.ListObjectsRequest\SUB'.acyclic.objects.v1.ListObjectsResponse"
instance Data.ProtoLens.Service.Types.HasMethodImpl ObjectsService "putObject" where
  type MethodName ObjectsService "putObject" = "PutObject"
  type MethodInput ObjectsService "putObject" = PutObjectRequest
  type MethodOutput ObjectsService "putObject" = ObjectVersion
  type MethodStreamingType ObjectsService "putObject" = 'Data.ProtoLens.Service.Types.ClientStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ObjectsService "getObject" where
  type MethodName ObjectsService "getObject" = "GetObject"
  type MethodInput ObjectsService "getObject" = GetObjectRequest
  type MethodOutput ObjectsService "getObject" = GetObjectResponse
  type MethodStreamingType ObjectsService "getObject" = 'Data.ProtoLens.Service.Types.ServerStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ObjectsService "headObject" where
  type MethodName ObjectsService "headObject" = "HeadObject"
  type MethodInput ObjectsService "headObject" = HeadObjectRequest
  type MethodOutput ObjectsService "headObject" = HeadObjectResponse
  type MethodStreamingType ObjectsService "headObject" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ObjectsService "deleteObject" where
  type MethodName ObjectsService "deleteObject" = "DeleteObject"
  type MethodInput ObjectsService "deleteObject" = DeleteObjectRequest
  type MethodOutput ObjectsService "deleteObject" = DeleteObjectResponse
  type MethodStreamingType ObjectsService "deleteObject" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ObjectsService "listObjects" where
  type MethodName ObjectsService "listObjects" = "ListObjects"
  type MethodInput ObjectsService "listObjects" = ListObjectsRequest
  type MethodOutput ObjectsService "listObjects" = ListObjectsResponse
  type MethodStreamingType ObjectsService "listObjects" = 'Data.ProtoLens.Service.Types.NonStreaming
data MultipartService = MultipartService {}
instance Data.ProtoLens.Service.Types.Service MultipartService where
  type ServiceName MultipartService = "MultipartService"
  type ServicePackage MultipartService = "acyclic.objects.v1"
  type ServiceMethods MultipartService = '["abortMultipart",
                                           "completeMultipart",
                                           "createMultipart",
                                           "listParts",
                                           "uploadPart"]
  packedServiceDescriptor _
    = "\n\
      \\DLEMultipartService\DC2b\n\
      \\SICreateMultipart\DC2*.acyclic.objects.v1.CreateMultipartRequest\SUB#.acyclic.objects.v1.MultipartUpload\DC2W\n\
      \\n\
      \UploadPart\DC2%.acyclic.objects.v1.UploadPartRequest\SUB .acyclic.objects.v1.UploadedPart(\SOH\DC2X\n\
      \\tListParts\DC2$.acyclic.objects.v1.ListPartsRequest\SUB%.acyclic.objects.v1.ListPartsResponse\DC2d\n\
      \\DC1CompleteMultipart\DC2,.acyclic.objects.v1.CompleteMultipartRequest\SUB!.acyclic.objects.v1.ObjectVersion\DC2g\n\
      \\SOAbortMultipart\DC2).acyclic.objects.v1.AbortMultipartRequest\SUB*.acyclic.objects.v1.AbortMultipartResponse"
instance Data.ProtoLens.Service.Types.HasMethodImpl MultipartService "createMultipart" where
  type MethodName MultipartService "createMultipart" = "CreateMultipart"
  type MethodInput MultipartService "createMultipart" = CreateMultipartRequest
  type MethodOutput MultipartService "createMultipart" = MultipartUpload
  type MethodStreamingType MultipartService "createMultipart" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MultipartService "uploadPart" where
  type MethodName MultipartService "uploadPart" = "UploadPart"
  type MethodInput MultipartService "uploadPart" = UploadPartRequest
  type MethodOutput MultipartService "uploadPart" = UploadedPart
  type MethodStreamingType MultipartService "uploadPart" = 'Data.ProtoLens.Service.Types.ClientStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MultipartService "listParts" where
  type MethodName MultipartService "listParts" = "ListParts"
  type MethodInput MultipartService "listParts" = ListPartsRequest
  type MethodOutput MultipartService "listParts" = ListPartsResponse
  type MethodStreamingType MultipartService "listParts" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MultipartService "completeMultipart" where
  type MethodName MultipartService "completeMultipart" = "CompleteMultipart"
  type MethodInput MultipartService "completeMultipart" = CompleteMultipartRequest
  type MethodOutput MultipartService "completeMultipart" = ObjectVersion
  type MethodStreamingType MultipartService "completeMultipart" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MultipartService "abortMultipart" where
  type MethodName MultipartService "abortMultipart" = "AbortMultipart"
  type MethodInput MultipartService "abortMultipart" = AbortMultipartRequest
  type MethodOutput MultipartService "abortMultipart" = AbortMultipartResponse
  type MethodStreamingType MultipartService "abortMultipart" = 'Data.ProtoLens.Service.Types.NonStreaming
data SnapshotsService = SnapshotsService {}
instance Data.ProtoLens.Service.Types.Service SnapshotsService where
  type ServiceName SnapshotsService = "SnapshotsService"
  type ServicePackage SnapshotsService = "acyclic.objects.v1"
  type ServiceMethods SnapshotsService = '["createSnapshot",
                                           "destroySnapshot",
                                           "forkBucket",
                                           "forkSnapshot"]
  packedServiceDescriptor _
    = "\n\
      \\DLESnapshotsService\DC2Y\n\
      \\SOCreateSnapshot\DC2).acyclic.objects.v1.CreateSnapshotRequest\SUB\FS.acyclic.objects.v1.Snapshot\DC2j\n\
      \\SIDestroySnapshot\DC2*.acyclic.objects.v1.DestroySnapshotRequest\SUB+.acyclic.objects.v1.DestroySnapshotResponse\DC2S\n\
      \\fForkSnapshot\DC2'.acyclic.objects.v1.ForkSnapshotRequest\SUB\SUB.acyclic.objects.v1.Bucket\DC2O\n\
      \\n\
      \ForkBucket\DC2%.acyclic.objects.v1.ForkBucketRequest\SUB\SUB.acyclic.objects.v1.Bucket"
instance Data.ProtoLens.Service.Types.HasMethodImpl SnapshotsService "createSnapshot" where
  type MethodName SnapshotsService "createSnapshot" = "CreateSnapshot"
  type MethodInput SnapshotsService "createSnapshot" = CreateSnapshotRequest
  type MethodOutput SnapshotsService "createSnapshot" = Snapshot
  type MethodStreamingType SnapshotsService "createSnapshot" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl SnapshotsService "destroySnapshot" where
  type MethodName SnapshotsService "destroySnapshot" = "DestroySnapshot"
  type MethodInput SnapshotsService "destroySnapshot" = DestroySnapshotRequest
  type MethodOutput SnapshotsService "destroySnapshot" = DestroySnapshotResponse
  type MethodStreamingType SnapshotsService "destroySnapshot" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl SnapshotsService "forkSnapshot" where
  type MethodName SnapshotsService "forkSnapshot" = "ForkSnapshot"
  type MethodInput SnapshotsService "forkSnapshot" = ForkSnapshotRequest
  type MethodOutput SnapshotsService "forkSnapshot" = Bucket
  type MethodStreamingType SnapshotsService "forkSnapshot" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl SnapshotsService "forkBucket" where
  type MethodName SnapshotsService "forkBucket" = "ForkBucket"
  type MethodInput SnapshotsService "forkBucket" = ForkBucketRequest
  type MethodOutput SnapshotsService "forkBucket" = Bucket
  type MethodStreamingType SnapshotsService "forkBucket" = 'Data.ProtoLens.Service.Types.NonStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\CANobjects/v1/objects.proto\DC2\DC2acyclic.objects.v1\SUB\USgoogle/protobuf/timestamp.proto\"<\n\
    \\tBucketRef\DC2\ESC\n\
    \\tbucket_id\CAN\SOH \SOH(\tR\bbucketId\DC2\DC2\n\
    \\EOTname\CAN\STX \SOH(\tR\EOTname\"X\n\
    \\vSnapshotRef\DC2\US\n\
    \\vsnapshot_id\CAN\SOH \SOH(\tR\n\
    \snapshotId\DC2(\n\
    \\DLEsource_bucket_id\CAN\STX \SOH(\tR\SOsourceBucketId\"\142\SOH\n\
    \\n\
    \ReadTarget\DC27\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefH\NULR\ACKbucket\DC2=\n\
    \\bsnapshot\CAN\STX \SOH(\v2\US.acyclic.objects.v1.SnapshotRefH\NULR\bsnapshotB\b\n\
    \\ACKtarget\"\170\ETX\n\
    \\SOObjectMetadata\DC2!\n\
    \\fcontent_type\CAN\SOH \SOH(\tR\vcontentType\DC2@\n\
    \\EOTuser\CAN\STX \ETX(\v2,.acyclic.objects.v1.ObjectMetadata.UserEntryR\EOTuser\DC2)\n\
    \\DLEcontent_encoding\CAN\ETX \SOH(\tR\SIcontentEncoding\DC2#\n\
    \\rcache_control\CAN\EOT \SOH(\tR\fcacheControl\DC2/\n\
    \\DC3content_disposition\CAN\ENQ \SOH(\tR\DC2contentDisposition\DC2)\n\
    \\DLEcontent_language\CAN\ACK \SOH(\tR\SIcontentLanguage\DC25\n\
    \\DC4expires_unix_seconds\CAN\a \SOH(\ETXH\NULR\DC2expiresUnixSeconds\136\SOH\SOH\SUB7\n\
    \\tUserEntry\DC2\DLE\n\
    \\ETXkey\CAN\SOH \SOH(\tR\ETXkey\DC2\DC4\n\
    \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue:\STX8\SOHB\ETB\n\
    \\NAK_expires_unix_seconds\"y\n\
    \\rPreconditions\DC2\GS\n\
    \\tif_absent\CAN\SOH \SOH(\bH\NULR\bifAbsent\DC2\ESC\n\
    \\bif_match\CAN\STX \SOH(\tH\NULR\aifMatch\DC2\US\n\
    \\n\
    \if_version\CAN\ETX \SOH(\tH\NULR\tifVersionB\v\n\
    \\tcondition\";\n\
    \\DLEMutationIdentity\DC2'\n\
    \\SIidempotency_key\CAN\SOH \SOH(\tR\SOidempotencyKey\"z\n\
    \\ACKBucket\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC29\n\
    \\n\
    \created_at\CAN\STX \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt\"k\n\
    \\DC3CreateBucketRequest\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2@\n\
    \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"J\n\
    \\DC1HeadBucketRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\"\142\SOH\n\
    \\DC3DeleteBucketRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2@\n\
    \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"0\n\
    \\DC4DeleteBucketResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\"\178\STX\n\
    \\SIPutObjectHeader\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
    \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v1.ObjectMetadataR\bmetadata\DC2G\n\
    \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v1.PreconditionsR\rpreconditions\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"p\n\
    \\DLEPutObjectRequest\DC2=\n\
    \\ACKheader\CAN\SOH \SOH(\v2#.acyclic.objects.v1.PutObjectHeaderH\NULR\ACKheader\DC2\DC4\n\
    \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbodyB\a\n\
    \\ENQframe\"\246\SOH\n\
    \\rObjectVersion\DC2\GS\n\
    \\n\
    \version_id\CAN\SOH \SOH(\tR\tversionId\DC2\DC2\n\
    \\EOTetag\CAN\STX \SOH(\tR\EOTetag\DC2\DC2\n\
    \\EOTsize\CAN\ETX \SOH(\EOTR\EOTsize\DC2#\n\
    \\rdelete_marker\CAN\EOT \SOH(\bR\fdeleteMarker\DC2>\n\
    \\bmetadata\CAN\ENQ \SOH(\v2\".acyclic.objects.v1.ObjectMetadataR\bmetadata\DC29\n\
    \\n\
    \created_at\CAN\ACK \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt\"\222\STX\n\
    \\DLEGetObjectRequest\DC26\n\
    \\ACKtarget\CAN\SOH \SOH(\v2\RS.acyclic.objects.v1.ReadTargetR\ACKtarget\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\GS\n\
    \\n\
    \version_id\CAN\ETX \SOH(\tR\tversionId\DC2\US\n\
    \\vrange_start\CAN\EOT \SOH(\EOTR\n\
    \rangeStart\DC23\n\
    \\DC3range_end_inclusive\CAN\ENQ \SOH(\EOTH\NULR\DC1rangeEndInclusive\136\SOH\SOH\DC2\EM\n\
    \\bif_match\CAN\ACK \SOH(\tR\aifMatch\DC2\"\n\
    \\rif_none_match\CAN\a \SOH(\tR\vifNoneMatch\DC2'\n\
    \\SIrange_requested\CAN\b \SOH(\bR\SOrangeRequestedB\SYN\n\
    \\DC4_range_end_inclusive\"q\n\
    \\DC1GetObjectResponse\DC2=\n\
    \\aversion\CAN\SOH \SOH(\v2!.acyclic.objects.v1.ObjectVersionH\NULR\aversion\DC2\DC4\n\
    \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbodyB\a\n\
    \\ENQframe\"\200\SOH\n\
    \\DC1HeadObjectRequest\DC26\n\
    \\ACKtarget\CAN\SOH \SOH(\v2\RS.acyclic.objects.v1.ReadTargetR\ACKtarget\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\GS\n\
    \\n\
    \version_id\CAN\ETX \SOH(\tR\tversionId\DC2\EM\n\
    \\bif_match\CAN\EOT \SOH(\tR\aifMatch\DC2\"\n\
    \\rif_none_match\CAN\ENQ \SOH(\tR\vifNoneMatch\"Q\n\
    \\DC2HeadObjectResponse\DC2;\n\
    \\aversion\CAN\SOH \SOH(\v2!.acyclic.objects.v1.ObjectVersionR\aversion\"\149\STX\n\
    \\DC3DeleteObjectRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\GS\n\
    \\n\
    \version_id\CAN\ETX \SOH(\tR\tversionId\DC2G\n\
    \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v1.PreconditionsR\rpreconditions\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"m\n\
    \\DC4DeleteObjectResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\DC2;\n\
    \\aversion\CAN\STX \SOH(\v2!.acyclic.objects.v1.ObjectVersionR\aversion\"\131\STX\n\
    \\DC2ListObjectsRequest\DC26\n\
    \\ACKtarget\CAN\SOH \SOH(\v2\RS.acyclic.objects.v1.ReadTargetR\ACKtarget\DC2\SYN\n\
    \\ACKprefix\CAN\STX \SOH(\tR\ACKprefix\DC2\FS\n\
    \\tdelimiter\CAN\ETX \SOH(\tR\tdelimiter\DC23\n\
    \\EOTmode\CAN\EOT \SOH(\SO2\US.acyclic.objects.v1.ListingModeR\EOTmode\DC2\ESC\n\
    \\tpage_size\CAN\ENQ \SOH(\rR\bpageSize\DC2-\n\
    \\DC2continuation_token\CAN\ACK \SOH(\tR\DC1continuationToken\"g\n\
    \\tListEntry\DC2\GS\n\
    \\n\
    \object_key\CAN\SOH \SOH(\tR\tobjectKey\DC2;\n\
    \\aversion\CAN\STX \SOH(\v2!.acyclic.objects.v1.ObjectVersionR\aversion\"\166\SOH\n\
    \\DC3ListObjectsResponse\DC27\n\
    \\aentries\CAN\SOH \ETX(\v2\GS.acyclic.objects.v1.ListEntryR\aentries\DC2'\n\
    \\SIcommon_prefixes\CAN\STX \ETX(\tR\SOcommonPrefixes\DC2-\n\
    \\DC2continuation_token\CAN\ETX \SOH(\tR\DC1continuationToken\"\185\STX\n\
    \\SYNCreateMultipartRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
    \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v1.ObjectMetadataR\bmetadata\DC2G\n\
    \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v1.PreconditionsR\rpreconditions\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\".\n\
    \\SIMultipartUpload\DC2\ESC\n\
    \\tupload_id\CAN\SOH \SOH(\tR\buploadId\"\232\SOH\n\
    \\DLEUploadPartHeader\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2\US\n\
    \\vpart_number\CAN\EOT \SOH(\rR\n\
    \partNumber\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"r\n\
    \\DC1UploadPartRequest\DC2>\n\
    \\ACKheader\CAN\SOH \SOH(\v2$.acyclic.objects.v1.UploadPartHeaderH\NULR\ACKheader\DC2\DC4\n\
    \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbodyB\a\n\
    \\ENQframe\"W\n\
    \\fUploadedPart\DC2\US\n\
    \\vpart_number\CAN\SOH \SOH(\rR\n\
    \partNumber\DC2\DC2\n\
    \\EOTetag\CAN\STX \SOH(\tR\EOTetag\DC2\DC2\n\
    \\EOTsize\CAN\ETX \SOH(\EOTR\EOTsize\"\133\SOH\n\
    \\DLEListPartsRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\"K\n\
    \\DC1ListPartsResponse\DC26\n\
    \\ENQparts\CAN\SOH \ETX(\v2 .acyclic.objects.v1.UploadedPartR\ENQparts\"\135\STX\n\
    \\CANCompleteMultipartRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC26\n\
    \\ENQparts\CAN\EOT \ETX(\v2 .acyclic.objects.v1.UploadedPartR\ENQparts\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"\204\SOH\n\
    \\NAKAbortMultipartRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2@\n\
    \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"2\n\
    \\SYNAbortMultipartResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\"\130\SOH\n\
    \\bSnapshot\DC2;\n\
    \\bsnapshot\CAN\SOH \SOH(\v2\US.acyclic.objects.v1.SnapshotRefR\bsnapshot\DC29\n\
    \\n\
    \created_at\CAN\STX \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt\"\144\SOH\n\
    \\NAKCreateSnapshotRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKbucket\DC2@\n\
    \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"\151\SOH\n\
    \\SYNDestroySnapshotRequest\DC2;\n\
    \\bsnapshot\CAN\SOH \SOH(\v2\US.acyclic.objects.v1.SnapshotRefR\bsnapshot\DC2@\n\
    \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"3\n\
    \\ETBDestroySnapshotResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\"\191\SOH\n\
    \\DC3ForkSnapshotRequest\DC2;\n\
    \\bsnapshot\CAN\SOH \SOH(\v2\US.acyclic.objects.v1.SnapshotRefR\bsnapshot\DC2)\n\
    \\DLEdestination_name\CAN\STX \SOH(\tR\SIdestinationName\DC2@\n\
    \\bmutation\CAN\ETX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"\183\SOH\n\
    \\DC1ForkBucketRequest\DC25\n\
    \\ACKsource\CAN\SOH \SOH(\v2\GS.acyclic.objects.v1.BucketRefR\ACKsource\DC2)\n\
    \\DLEdestination_name\CAN\STX \SOH(\tR\SIdestinationName\DC2@\n\
    \\bmutation\CAN\ETX \SOH(\v2$.acyclic.objects.v1.MutationIdentityR\bmutation\"_\n\
    \\vErrorDetail\DC21\n\
    \\EOTcode\CAN\SOH \SOH(\SO2\GS.acyclic.objects.v1.ErrorCodeR\EOTcode\DC2\GS\n\
    \\n\
    \request_id\CAN\STX \SOH(\tR\trequestId*[\n\
    \\fObjectsLimit\DC2\GS\n\
    \\EMOBJECTS_LIMIT_UNSPECIFIED\DLE\NUL\DC2,\n\
    \'OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES\DLE\128\STX*`\n\
    \\vListingMode\DC2\FS\n\
    \\CANLISTING_MODE_UNSPECIFIED\DLE\NUL\DC2\CAN\n\
    \\DC4LISTING_MODE_CURRENT\DLE\SOH\DC2\EM\n\
    \\NAKLISTING_MODE_VERSIONS\DLE\STX*\191\STX\n\
    \\tErrorCode\DC2\SUB\n\
    \\SYNERROR_CODE_UNSPECIFIED\DLE\NUL\DC2\US\n\
    \\ESCERROR_CODE_INVALID_ARGUMENT\DLE\SOH\DC2\CAN\n\
    \\DC4ERROR_CODE_NOT_FOUND\DLE\STX\DC2\GS\n\
    \\EMERROR_CODE_ALREADY_EXISTS\DLE\ETX\DC2\"\n\
    \\RSERROR_CODE_PRECONDITION_FAILED\DLE\EOT\DC2#\n\
    \\USERROR_CODE_IDEMPOTENCY_MISMATCH\DLE\ENQ\DC2\FS\n\
    \\CANERROR_CODE_TOKEN_EXPIRED\DLE\ACK\DC2\GS\n\
    \\EMERROR_CODE_QUOTA_EXCEEDED\DLE\a\DC2\SUB\n\
    \\SYNERROR_CODE_UNSUPPORTED\DLE\b\DC2\SUB\n\
    \\SYNERROR_CODE_UNAVAILABLE\DLE\t2\153\STX\n\
    \\SOBucketsService\DC2S\n\
    \\fCreateBucket\DC2'.acyclic.objects.v1.CreateBucketRequest\SUB\SUB.acyclic.objects.v1.Bucket\DC2O\n\
    \\n\
    \HeadBucket\DC2%.acyclic.objects.v1.HeadBucketRequest\SUB\SUB.acyclic.objects.v1.Bucket\DC2a\n\
    \\fDeleteBucket\DC2'.acyclic.objects.v1.DeleteBucketRequest\SUB(.acyclic.objects.v1.DeleteBucketResponse2\228\ETX\n\
    \\SOObjectsService\DC2V\n\
    \\tPutObject\DC2$.acyclic.objects.v1.PutObjectRequest\SUB!.acyclic.objects.v1.ObjectVersion(\SOH\DC2Z\n\
    \\tGetObject\DC2$.acyclic.objects.v1.GetObjectRequest\SUB%.acyclic.objects.v1.GetObjectResponse0\SOH\DC2[\n\
    \\n\
    \HeadObject\DC2%.acyclic.objects.v1.HeadObjectRequest\SUB&.acyclic.objects.v1.HeadObjectResponse\DC2a\n\
    \\fDeleteObject\DC2'.acyclic.objects.v1.DeleteObjectRequest\SUB(.acyclic.objects.v1.DeleteObjectResponse\DC2^\n\
    \\vListObjects\DC2&.acyclic.objects.v1.ListObjectsRequest\SUB'.acyclic.objects.v1.ListObjectsResponse2\248\ETX\n\
    \\DLEMultipartService\DC2b\n\
    \\SICreateMultipart\DC2*.acyclic.objects.v1.CreateMultipartRequest\SUB#.acyclic.objects.v1.MultipartUpload\DC2W\n\
    \\n\
    \UploadPart\DC2%.acyclic.objects.v1.UploadPartRequest\SUB .acyclic.objects.v1.UploadedPart(\SOH\DC2X\n\
    \\tListParts\DC2$.acyclic.objects.v1.ListPartsRequest\SUB%.acyclic.objects.v1.ListPartsResponse\DC2d\n\
    \\DC1CompleteMultipart\DC2,.acyclic.objects.v1.CompleteMultipartRequest\SUB!.acyclic.objects.v1.ObjectVersion\DC2g\n\
    \\SOAbortMultipart\DC2).acyclic.objects.v1.AbortMultipartRequest\SUB*.acyclic.objects.v1.AbortMultipartResponse2\255\STX\n\
    \\DLESnapshotsService\DC2Y\n\
    \\SOCreateSnapshot\DC2).acyclic.objects.v1.CreateSnapshotRequest\SUB\FS.acyclic.objects.v1.Snapshot\DC2j\n\
    \\SIDestroySnapshot\DC2*.acyclic.objects.v1.DestroySnapshotRequest\SUB+.acyclic.objects.v1.DestroySnapshotResponse\DC2S\n\
    \\fForkSnapshot\DC2'.acyclic.objects.v1.ForkSnapshotRequest\SUB\SUB.acyclic.objects.v1.Bucket\DC2O\n\
    \\n\
    \ForkBucket\DC2%.acyclic.objects.v1.ForkBucketRequest\SUB\SUB.acyclic.objects.v1.BucketB9Z7github.com/acyclic-labs/sdk/go/gen/objects/v1;objectsv1J\134O\n\
    \\a\DC2\ENQ\NUL\NUL\182\STX\SOH\n\
    \\b\n\
    \\SOH\f\DC2\ETX\NUL\NUL\DC2\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\STX\NUL\ESC\n\
    \\t\n\
    \\STX\ETX\NUL\DC2\ETX\EOT\NUL)\n\
    \\b\n\
    \\SOH\b\DC2\ETX\ACK\NULN\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\ACK\NULN\n\
    \\n\
    \\n\
    \\STX\EOT\NUL\DC2\EOT\b\NUL\v\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\b\b\DC1\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\t\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\t\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\t\t\DC2\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\t\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\SOH\DC2\ETX\n\
    \\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ENQ\DC2\ETX\n\
    \\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\SOH\DC2\ETX\n\
    \\t\r\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ETX\DC2\ETX\n\
    \\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\r\NUL\DLE\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\r\b\DC3\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\SO\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ENQ\DC2\ETX\SO\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\SO\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\SO\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\SOH\DC2\ETX\SI\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ENQ\DC2\ETX\SI\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\SOH\DC2\ETX\SI\t\EM\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ETX\DC2\ETX\SI\FS\GS\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\DC2\NUL\ETB\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\DC2\b\DC2\n\
    \\f\n\
    \\EOT\EOT\STX\b\NUL\DC2\EOT\DC3\STX\SYN\ETX\n\
    \\f\n\
    \\ENQ\EOT\STX\b\NUL\SOH\DC2\ETX\DC3\b\SO\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\DC4\EOT\EM\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ACK\DC2\ETX\DC4\EOT\r\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\DC4\SO\DC4\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\DC4\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\STX\STX\SOH\DC2\ETX\NAK\EOT\GS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ACK\DC2\ETX\NAK\EOT\SI\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\SOH\DC2\ETX\NAK\DLE\CAN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ETX\DC2\ETX\NAK\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT\EM\NUL!\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX\EM\b\SYN\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX\SUB\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ENQ\DC2\ETX\SUB\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX\SUB\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX\SUB\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\SOH\DC2\ETX\ESC\STX\US\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ACK\DC2\ETX\ESC\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\SOH\DC2\ETX\ESC\SYN\SUB\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ETX\DC2\ETX\ESC\GS\RS\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\STX\DC2\ETX\FS\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\ENQ\DC2\ETX\FS\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\SOH\DC2\ETX\FS\t\EM\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\ETX\DC2\ETX\FS\FS\GS\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\ETX\DC2\ETX\GS\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ETX\ENQ\DC2\ETX\GS\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ETX\SOH\DC2\ETX\GS\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ETX\ETX\DC2\ETX\GS\EM\SUB\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\EOT\DC2\ETX\RS\STX!\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\EOT\ENQ\DC2\ETX\RS\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\EOT\SOH\DC2\ETX\RS\t\FS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\EOT\ETX\DC2\ETX\RS\US \n\
    \\v\n\
    \\EOT\EOT\ETX\STX\ENQ\DC2\ETX\US\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ENQ\ENQ\DC2\ETX\US\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ENQ\SOH\DC2\ETX\US\t\EM\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ENQ\ETX\DC2\ETX\US\FS\GS\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\ACK\DC2\ETX \STX*\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ACK\EOT\DC2\ETX \STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ACK\ENQ\DC2\ETX \v\DLE\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ACK\SOH\DC2\ETX \DC1%\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ACK\ETX\DC2\ETX ()\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT#\NUL)\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX#\b\NAK\n\
    \\f\n\
    \\EOT\EOT\EOT\b\NUL\DC2\EOT$\STX(\ETX\n\
    \\f\n\
    \\ENQ\EOT\EOT\b\NUL\SOH\DC2\ETX$\b\DC1\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX%\EOT\ETB\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ENQ\DC2\ETX%\EOT\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX%\t\DC2\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX%\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\SOH\DC2\ETX&\EOT\CAN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ENQ\DC2\ETX&\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\SOH\DC2\ETX&\v\DC3\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ETX\DC2\ETX&\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\STX\DC2\ETX'\EOT\SUB\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ENQ\DC2\ETX'\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\SOH\DC2\ETX'\v\NAK\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ETX\DC2\ETX'\CAN\EM\n\
    \\n\
    \\n\
    \\STX\EOT\ENQ\DC2\EOT+\NUL-\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETX+\b\CAN\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETX,\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ENQ\DC2\ETX,\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETX,\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETX,\ESC\FS\n\
    \=\n\
    \\STX\ENQ\NUL\DC2\EOT0\NUL3\SOH\SUB1 Fixed limits shared by every Objects transport.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETX0\ENQ\DC1\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETX1\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETX1\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETX1\RS\US\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETX2\STX0\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETX2\STX)\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETX2,/\n\
    \\n\
    \\n\
    \\STX\EOT\ACK\DC2\EOT5\NUL8\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETX5\b\SO\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX6\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ACK\DC2\ETX6\STX\v\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX6\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX6\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\SOH\DC2\ETX7\STX+\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ACK\DC2\ETX7\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\SOH\DC2\ETX7\FS&\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ETX\DC2\ETX7)*\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOT:\NUL=\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETX:\b\ESC\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETX;\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ENQ\DC2\ETX;\STX\b\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETX;\t\r\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETX;\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\a\STX\SOH\DC2\ETX<\STX \n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ACK\DC2\ETX<\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\SOH\DC2\ETX<\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ETX\DC2\ETX<\RS\US\n\
    \\n\
    \\n\
    \\STX\EOT\b\DC2\EOT?\NULA\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETX?\b\EM\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETX@\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ACK\DC2\ETX@\STX\v\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETX@\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETX@\NAK\SYN\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOTC\NULF\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETXC\b\ESC\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETXD\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ACK\DC2\ETXD\STX\v\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETXD\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETXD\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\t\STX\SOH\DC2\ETXE\STX \n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ACK\DC2\ETXE\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\SOH\DC2\ETXE\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ETX\DC2\ETXE\RS\US\n\
    \\n\
    \\n\
    \\STX\EOT\n\
    \\DC2\EOTH\NULJ\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\n\
    \\SOH\DC2\ETXH\b\FS\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\ETXI\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ENQ\DC2\ETXI\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\ETXI\a\SO\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\ETXI\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\ACK\NUL\DC2\EOTL\NULP\SOH\n\
    \\n\
    \\n\
    \\ETX\ACK\NUL\SOH\DC2\ETXL\b\SYN\n\
    \\v\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\ETXM\STX9\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\ETXM\ACK\DC2\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\ETXM\DC3&\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\ETXM17\n\
    \\v\n\
    \\EOT\ACK\NUL\STX\SOH\DC2\ETXN\STX5\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\SOH\SOH\DC2\ETXN\ACK\DLE\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\SOH\STX\DC2\ETXN\DC1\"\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\SOH\ETX\DC2\ETXN-3\n\
    \\v\n\
    \\EOT\ACK\NUL\STX\STX\DC2\ETXO\STXG\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\STX\SOH\DC2\ETXO\ACK\DC2\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\STX\STX\DC2\ETXO\DC3&\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\STX\ETX\DC2\ETXO1E\n\
    \\n\
    \\n\
    \\STX\EOT\v\DC2\EOTR\NULX\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\v\SOH\DC2\ETXR\b\ETB\n\
    \\v\n\
    \\EOT\EOT\v\STX\NUL\DC2\ETXS\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ACK\DC2\ETXS\STX\v\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\ETXS\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\ETXS\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\v\STX\SOH\DC2\ETXT\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ENQ\DC2\ETXT\STX\b\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\SOH\DC2\ETXT\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ETX\DC2\ETXT\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\v\STX\STX\DC2\ETXU\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\ACK\DC2\ETXU\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\SOH\DC2\ETXU\DC1\EM\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\ETX\DC2\ETXU\FS\GS\n\
    \\v\n\
    \\EOT\EOT\v\STX\ETX\DC2\ETXV\STX\"\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ETX\ACK\DC2\ETXV\STX\SI\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ETX\SOH\DC2\ETXV\DLE\GS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ETX\ETX\DC2\ETXV !\n\
    \\v\n\
    \\EOT\EOT\v\STX\EOT\DC2\ETXW\STX \n\
    \\f\n\
    \\ENQ\EOT\v\STX\EOT\ACK\DC2\ETXW\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\v\STX\EOT\SOH\DC2\ETXW\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\v\STX\EOT\ETX\DC2\ETXW\RS\US\n\
    \\n\
    \\n\
    \\STX\EOT\f\DC2\EOTZ\NUL_\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\f\SOH\DC2\ETXZ\b\CAN\n\
    \\f\n\
    \\EOT\EOT\f\b\NUL\DC2\EOT[\STX^\ETX\n\
    \\f\n\
    \\ENQ\EOT\f\b\NUL\SOH\DC2\ETX[\b\r\n\
    \\v\n\
    \\EOT\EOT\f\STX\NUL\DC2\ETX\\\EOT\US\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ACK\DC2\ETX\\\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\ETX\\\DC4\SUB\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\ETX\\\GS\RS\n\
    \\v\n\
    \\EOT\EOT\f\STX\SOH\DC2\ETX]\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ENQ\DC2\ETX]\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\ETX]\n\
    \\SO\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\ETX]\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\EOT\r\DC2\EOTa\NULh\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\r\SOH\DC2\ETXa\b\NAK\n\
    \\v\n\
    \\EOT\EOT\r\STX\NUL\DC2\ETXb\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ENQ\DC2\ETXb\STX\b\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\ETXb\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\ETXb\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\r\STX\SOH\DC2\ETXc\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\ENQ\DC2\ETXc\STX\b\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\SOH\DC2\ETXc\t\r\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\ETX\DC2\ETXc\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\r\STX\STX\DC2\ETXd\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\STX\ENQ\DC2\ETXd\STX\b\n\
    \\f\n\
    \\ENQ\EOT\r\STX\STX\SOH\DC2\ETXd\t\r\n\
    \\f\n\
    \\ENQ\EOT\r\STX\STX\ETX\DC2\ETXd\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\r\STX\ETX\DC2\ETXe\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ETX\ENQ\DC2\ETXe\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ETX\SOH\DC2\ETXe\a\DC4\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ETX\ETX\DC2\ETXe\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\r\STX\EOT\DC2\ETXf\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\EOT\ACK\DC2\ETXf\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\r\STX\EOT\SOH\DC2\ETXf\DC1\EM\n\
    \\f\n\
    \\ENQ\EOT\r\STX\EOT\ETX\DC2\ETXf\FS\GS\n\
    \\v\n\
    \\EOT\EOT\r\STX\ENQ\DC2\ETXg\STX+\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ENQ\ACK\DC2\ETXg\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ENQ\SOH\DC2\ETXg\FS&\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ENQ\ETX\DC2\ETXg)*\n\
    \\n\
    \\n\
    \\STX\EOT\SO\DC2\EOTj\NULv\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SO\SOH\DC2\ETXj\b\CAN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\NUL\DC2\ETXk\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ACK\DC2\ETXk\STX\f\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\ETXk\r\DC3\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\ETXk\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\SO\STX\SOH\DC2\ETXl\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ENQ\DC2\ETXl\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\SOH\DC2\ETXl\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ETX\DC2\ETXl\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\SO\STX\STX\DC2\ETXm\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ENQ\DC2\ETXm\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\SOH\DC2\ETXm\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ETX\DC2\ETXm\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ETX\DC2\ETXn\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\ENQ\DC2\ETXn\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\SOH\DC2\ETXn\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\ETX\DC2\ETXn\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\EOT\DC2\ETXo\STX*\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\EOT\DC2\ETXo\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\ENQ\DC2\ETXo\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\SOH\DC2\ETXo\DC2%\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\ETX\DC2\ETXo()\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ENQ\DC2\ETXp\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ENQ\ENQ\DC2\ETXp\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ENQ\SOH\DC2\ETXp\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ENQ\ETX\DC2\ETXp\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ACK\DC2\ETXq\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ACK\ENQ\DC2\ETXq\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ACK\SOH\DC2\ETXq\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ACK\ETX\DC2\ETXq\EM\SUB\n\
    \\171\SOH\n\
    \\EOT\EOT\SO\STX\a\DC2\ETXu\STX\ESC\SUB\157\SOH Required for every ranged read, distinguishing an open-ended range\n\
    \ beginning at byte zero from no range. Range fields are invalid when this\n\
    \ bit is false.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\a\ENQ\DC2\ETXu\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\a\SOH\DC2\ETXu\a\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\a\ETX\DC2\ETXu\EM\SUB\n\
    \\n\
    \\n\
    \\STX\EOT\SI\DC2\EOTx\NUL}\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SI\SOH\DC2\ETXx\b\EM\n\
    \\f\n\
    \\EOT\EOT\SI\b\NUL\DC2\EOTy\STX|\ETX\n\
    \\f\n\
    \\ENQ\EOT\SI\b\NUL\SOH\DC2\ETXy\b\r\n\
    \\v\n\
    \\EOT\EOT\SI\STX\NUL\DC2\ETXz\EOT\RS\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ACK\DC2\ETXz\EOT\DC1\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\ETXz\DC2\EM\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\ETXz\FS\GS\n\
    \\v\n\
    \\EOT\EOT\SI\STX\SOH\DC2\ETX{\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ENQ\DC2\ETX{\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\SOH\DC2\ETX{\n\
    \\SO\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ETX\DC2\ETX{\DC1\DC2\n\
    \\v\n\
    \\STX\EOT\DLE\DC2\ENQ\DEL\NUL\133\SOH\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DLE\SOH\DC2\ETX\DEL\b\EM\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\EOT\128\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\ACK\DC2\EOT\128\SOH\STX\f\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\EOT\128\SOH\r\DC3\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\EOT\128\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\SOH\DC2\EOT\129\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\ENQ\DC2\EOT\129\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\SOH\DC2\EOT\129\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\ETX\DC2\EOT\129\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\STX\DC2\EOT\130\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\ENQ\DC2\EOT\130\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\SOH\DC2\EOT\130\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\ETX\DC2\EOT\130\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\ETX\DC2\EOT\131\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\ENQ\DC2\EOT\131\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\SOH\DC2\EOT\131\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\ETX\DC2\EOT\131\SOH\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\EOT\DC2\EOT\132\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\EOT\ENQ\DC2\EOT\132\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\EOT\SOH\DC2\EOT\132\SOH\t\SYN\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\EOT\ETX\DC2\EOT\132\SOH\EM\SUB\n\
    \\f\n\
    \\STX\EOT\DC1\DC2\ACK\135\SOH\NUL\137\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC1\SOH\DC2\EOT\135\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\EOT\136\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\ACK\DC2\EOT\136\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\EOT\136\SOH\DLE\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\EOT\136\SOH\SUB\ESC\n\
    \\f\n\
    \\STX\EOT\DC2\DC2\ACK\139\SOH\NUL\145\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC2\SOH\DC2\EOT\139\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\EOT\140\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\ACK\DC2\EOT\140\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\EOT\140\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\EOT\140\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\SOH\DC2\EOT\141\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\SOH\ENQ\DC2\EOT\141\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\SOH\SOH\DC2\EOT\141\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\SOH\ETX\DC2\EOT\141\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\STX\DC2\EOT\142\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\STX\ENQ\DC2\EOT\142\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\STX\SOH\DC2\EOT\142\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\STX\ETX\DC2\EOT\142\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\ETX\DC2\EOT\143\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\ETX\ACK\DC2\EOT\143\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\ETX\SOH\DC2\EOT\143\SOH\DLE\GS\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\ETX\ETX\DC2\EOT\143\SOH !\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\EOT\DC2\EOT\144\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\EOT\ACK\DC2\EOT\144\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\EOT\SOH\DC2\EOT\144\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\EOT\ETX\DC2\EOT\144\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\DC3\DC2\ACK\147\SOH\NUL\150\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC3\SOH\DC2\EOT\147\SOH\b\FS\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\EOT\148\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ENQ\DC2\EOT\148\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\EOT\148\SOH\a\SO\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\EOT\148\SOH\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\SOH\DC2\EOT\149\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\ACK\DC2\EOT\149\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\SOH\DC2\EOT\149\SOH\DLE\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\ETX\DC2\EOT\149\SOH\SUB\ESC\n\
    \\f\n\
    \\STX\ENQ\SOH\DC2\ACK\152\SOH\NUL\156\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\SOH\SOH\DC2\EOT\152\SOH\ENQ\DLE\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\NUL\DC2\EOT\153\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\SOH\DC2\EOT\153\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\STX\DC2\EOT\153\SOH\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\SOH\DC2\EOT\154\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\SOH\DC2\EOT\154\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\STX\DC2\EOT\154\SOH\EM\SUB\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\STX\DC2\EOT\155\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\SOH\DC2\EOT\155\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\STX\DC2\EOT\155\SOH\SUB\ESC\n\
    \\f\n\
    \\STX\EOT\DC4\DC2\ACK\158\SOH\NUL\165\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC4\SOH\DC2\EOT\158\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\EOT\159\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ACK\DC2\EOT\159\SOH\STX\f\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\EOT\159\SOH\r\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\EOT\159\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\EOT\160\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ENQ\DC2\EOT\160\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\EOT\160\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\EOT\160\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\STX\DC2\EOT\161\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\STX\ENQ\DC2\EOT\161\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\STX\SOH\DC2\EOT\161\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\STX\ETX\DC2\EOT\161\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\ETX\DC2\EOT\162\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\ACK\DC2\EOT\162\SOH\STX\r\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\SOH\DC2\EOT\162\SOH\SO\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\ETX\DC2\EOT\162\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\EOT\DC2\EOT\163\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\EOT\ENQ\DC2\EOT\163\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\EOT\SOH\DC2\EOT\163\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\EOT\ETX\DC2\EOT\163\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\ENQ\DC2\EOT\164\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ENQ\ENQ\DC2\EOT\164\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ENQ\SOH\DC2\EOT\164\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ENQ\ETX\DC2\EOT\164\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\NAK\DC2\ACK\167\SOH\NUL\170\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\167\SOH\b\DC1\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\168\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ENQ\DC2\EOT\168\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\168\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\168\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\SOH\DC2\EOT\169\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ACK\DC2\EOT\169\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\SOH\DC2\EOT\169\SOH\DLE\ETB\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ETX\DC2\EOT\169\SOH\SUB\ESC\n\
    \\f\n\
    \\STX\EOT\SYN\DC2\ACK\172\SOH\NUL\176\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\172\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\173\SOH\STX!\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\EOT\DC2\EOT\173\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ACK\DC2\EOT\173\SOH\v\DC4\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\173\SOH\NAK\FS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\173\SOH\US \n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\174\SOH\STX&\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\EOT\DC2\EOT\174\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ENQ\DC2\EOT\174\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\174\SOH\DC2!\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\174\SOH$%\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\STX\DC2\EOT\175\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ENQ\DC2\EOT\175\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\SOH\DC2\EOT\175\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ETX\DC2\EOT\175\SOH\RS\US\n\
    \\f\n\
    \\STX\ACK\SOH\DC2\ACK\178\SOH\NUL\184\SOH\SOH\n\
    \\v\n\
    \\ETX\ACK\SOH\SOH\DC2\EOT\178\SOH\b\SYN\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\NUL\DC2\EOT\179\SOH\STXA\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\SOH\DC2\EOT\179\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\ENQ\DC2\EOT\179\SOH\DLE\SYN\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\STX\DC2\EOT\179\SOH\ETB'\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\ETX\DC2\EOT\179\SOH2?\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\SOH\DC2\EOT\180\SOH\STXE\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\SOH\DC2\EOT\180\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\STX\DC2\EOT\180\SOH\DLE \n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\ACK\DC2\EOT\180\SOH+1\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\ETX\DC2\EOT\180\SOH2C\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\STX\DC2\EOT\181\SOH\STXA\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\STX\SOH\DC2\EOT\181\SOH\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\STX\STX\DC2\EOT\181\SOH\DC1\"\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\STX\ETX\DC2\EOT\181\SOH-?\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\ETX\DC2\EOT\182\SOH\STXG\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\ETX\SOH\DC2\EOT\182\SOH\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\ETX\STX\DC2\EOT\182\SOH\DC3&\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\ETX\ETX\DC2\EOT\182\SOH1E\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\EOT\DC2\EOT\183\SOH\STXD\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\EOT\SOH\DC2\EOT\183\SOH\ACK\DC1\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\EOT\STX\DC2\EOT\183\SOH\DC2$\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\EOT\ETX\DC2\EOT\183\SOH/B\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\186\SOH\NUL\192\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\186\SOH\b\RS\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\187\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ACK\DC2\EOT\187\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\187\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\187\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\188\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ENQ\DC2\EOT\188\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\188\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\188\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\STX\DC2\EOT\189\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ACK\DC2\EOT\189\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\SOH\DC2\EOT\189\SOH\DC1\EM\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ETX\DC2\EOT\189\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\ETX\DC2\EOT\190\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\ACK\DC2\EOT\190\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\SOH\DC2\EOT\190\SOH\DLE\GS\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\ETX\DC2\EOT\190\SOH !\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\EOT\DC2\EOT\191\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\EOT\ACK\DC2\EOT\191\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\EOT\SOH\DC2\EOT\191\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\EOT\ETX\DC2\EOT\191\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\CAN\DC2\ACK\194\SOH\NUL\196\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\CAN\SOH\DC2\EOT\194\SOH\b\ETB\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\NUL\DC2\EOT\195\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ENQ\DC2\EOT\195\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\SOH\DC2\EOT\195\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ETX\DC2\EOT\195\SOH\NAK\SYN\n\
    \\f\n\
    \\STX\EOT\EM\DC2\ACK\198\SOH\NUL\204\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\EM\SOH\DC2\EOT\198\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\NUL\DC2\EOT\199\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ACK\DC2\EOT\199\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\SOH\DC2\EOT\199\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ETX\DC2\EOT\199\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\SOH\DC2\EOT\200\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ENQ\DC2\EOT\200\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\SOH\DC2\EOT\200\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ETX\DC2\EOT\200\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\EM\STX\STX\DC2\EOT\201\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ENQ\DC2\EOT\201\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\SOH\DC2\EOT\201\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ETX\DC2\EOT\201\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\ETX\DC2\EOT\202\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ENQ\DC2\EOT\202\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\SOH\DC2\EOT\202\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ETX\DC2\EOT\202\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\EOT\DC2\EOT\203\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\ACK\DC2\EOT\203\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\SOH\DC2\EOT\203\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\ETX\DC2\EOT\203\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\SUB\DC2\ACK\206\SOH\NUL\211\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SUB\SOH\DC2\EOT\206\SOH\b\EM\n\
    \\SO\n\
    \\EOT\EOT\SUB\b\NUL\DC2\ACK\207\SOH\STX\210\SOH\ETX\n\
    \\r\n\
    \\ENQ\EOT\SUB\b\NUL\SOH\DC2\EOT\207\SOH\b\r\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\NUL\DC2\EOT\208\SOH\EOT \n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ACK\DC2\EOT\208\SOH\EOT\DC4\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\SOH\DC2\EOT\208\SOH\NAK\ESC\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ETX\DC2\EOT\208\SOH\RS\US\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\SOH\DC2\EOT\209\SOH\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ENQ\DC2\EOT\209\SOH\EOT\t\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\SOH\DC2\EOT\209\SOH\n\
    \\SO\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ETX\DC2\EOT\209\SOH\DC1\DC2\n\
    \\f\n\
    \\STX\EOT\ESC\DC2\ACK\213\SOH\NUL\217\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ESC\SOH\DC2\EOT\213\SOH\b\DC4\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\NUL\DC2\EOT\214\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ENQ\DC2\EOT\214\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\SOH\DC2\EOT\214\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ETX\DC2\EOT\214\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\SOH\DC2\EOT\215\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ENQ\DC2\EOT\215\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\SOH\DC2\EOT\215\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ETX\DC2\EOT\215\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\STX\DC2\EOT\216\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ENQ\DC2\EOT\216\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\SOH\DC2\EOT\216\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ETX\DC2\EOT\216\SOH\DLE\DC1\n\
    \\f\n\
    \\STX\EOT\FS\DC2\ACK\219\SOH\NUL\223\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\FS\SOH\DC2\EOT\219\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\FS\STX\NUL\DC2\EOT\220\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ACK\DC2\EOT\220\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\SOH\DC2\EOT\220\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ETX\DC2\EOT\220\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\FS\STX\SOH\DC2\EOT\221\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ENQ\DC2\EOT\221\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\SOH\DC2\EOT\221\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ETX\DC2\EOT\221\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\FS\STX\STX\DC2\EOT\222\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ENQ\DC2\EOT\222\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\SOH\DC2\EOT\222\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ETX\DC2\EOT\222\SOH\NAK\SYN\n\
    \\f\n\
    \\STX\EOT\GS\DC2\ACK\225\SOH\NUL\227\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\GS\SOH\DC2\EOT\225\SOH\b\EM\n\
    \\f\n\
    \\EOT\EOT\GS\STX\NUL\DC2\EOT\226\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\EOT\DC2\EOT\226\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ACK\DC2\EOT\226\SOH\v\ETB\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\SOH\DC2\EOT\226\SOH\CAN\GS\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ETX\DC2\EOT\226\SOH !\n\
    \\f\n\
    \\STX\EOT\RS\DC2\ACK\229\SOH\NUL\235\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\RS\SOH\DC2\EOT\229\SOH\b \n\
    \\f\n\
    \\EOT\EOT\RS\STX\NUL\DC2\EOT\230\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ACK\DC2\EOT\230\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\SOH\DC2\EOT\230\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ETX\DC2\EOT\230\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\RS\STX\SOH\DC2\EOT\231\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ENQ\DC2\EOT\231\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\SOH\DC2\EOT\231\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ETX\DC2\EOT\231\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\RS\STX\STX\DC2\EOT\232\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\ENQ\DC2\EOT\232\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\SOH\DC2\EOT\232\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\ETX\DC2\EOT\232\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\RS\STX\ETX\DC2\EOT\233\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\EOT\DC2\EOT\233\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\ACK\DC2\EOT\233\SOH\v\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\SOH\DC2\EOT\233\SOH\CAN\GS\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\ETX\DC2\EOT\233\SOH !\n\
    \\f\n\
    \\EOT\EOT\RS\STX\EOT\DC2\EOT\234\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\RS\STX\EOT\ACK\DC2\EOT\234\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\EOT\SOH\DC2\EOT\234\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\EOT\ETX\DC2\EOT\234\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\US\DC2\ACK\237\SOH\NUL\242\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\US\SOH\DC2\EOT\237\SOH\b\GS\n\
    \\f\n\
    \\EOT\EOT\US\STX\NUL\DC2\EOT\238\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ACK\DC2\EOT\238\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\SOH\DC2\EOT\238\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ETX\DC2\EOT\238\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\US\STX\SOH\DC2\EOT\239\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ENQ\DC2\EOT\239\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\SOH\DC2\EOT\239\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ETX\DC2\EOT\239\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\US\STX\STX\DC2\EOT\240\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\ENQ\DC2\EOT\240\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\SOH\DC2\EOT\240\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\ETX\DC2\EOT\240\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\US\STX\ETX\DC2\EOT\241\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\US\STX\ETX\ACK\DC2\EOT\241\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\US\STX\ETX\SOH\DC2\EOT\241\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\US\STX\ETX\ETX\DC2\EOT\241\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT \DC2\ACK\244\SOH\NUL\246\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT \SOH\DC2\EOT\244\SOH\b\RS\n\
    \\f\n\
    \\EOT\EOT \STX\NUL\DC2\EOT\245\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ENQ\DC2\EOT\245\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\SOH\DC2\EOT\245\SOH\a\SO\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ETX\DC2\EOT\245\SOH\DC1\DC2\n\
    \\f\n\
    \\STX\ACK\STX\DC2\ACK\248\SOH\NUL\254\SOH\SOH\n\
    \\v\n\
    \\ETX\ACK\STX\SOH\DC2\EOT\248\SOH\b\CAN\n\
    \\f\n\
    \\EOT\ACK\STX\STX\NUL\DC2\EOT\249\SOH\STXH\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\NUL\SOH\DC2\EOT\249\SOH\ACK\NAK\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\NUL\STX\DC2\EOT\249\SOH\SYN,\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\NUL\ETX\DC2\EOT\249\SOH7F\n\
    \\f\n\
    \\EOT\ACK\STX\STX\SOH\DC2\EOT\250\SOH\STXB\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\SOH\DC2\EOT\250\SOH\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\ENQ\DC2\EOT\250\SOH\DC1\ETB\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\STX\DC2\EOT\250\SOH\CAN)\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\ETX\DC2\EOT\250\SOH4@\n\
    \\f\n\
    \\EOT\ACK\STX\STX\STX\DC2\EOT\251\SOH\STX>\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\STX\SOH\DC2\EOT\251\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\STX\STX\DC2\EOT\251\SOH\DLE \n\
    \\r\n\
    \\ENQ\ACK\STX\STX\STX\ETX\DC2\EOT\251\SOH+<\n\
    \\f\n\
    \\EOT\ACK\STX\STX\ETX\DC2\EOT\252\SOH\STXJ\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\ETX\SOH\DC2\EOT\252\SOH\ACK\ETB\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\ETX\STX\DC2\EOT\252\SOH\CAN0\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\ETX\ETX\DC2\EOT\252\SOH;H\n\
    \\f\n\
    \\EOT\ACK\STX\STX\EOT\DC2\EOT\253\SOH\STXM\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\EOT\SOH\DC2\EOT\253\SOH\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\EOT\STX\DC2\EOT\253\SOH\NAK*\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\EOT\ETX\DC2\EOT\253\SOH5K\n\
    \\f\n\
    \\STX\EOT!\DC2\ACK\128\STX\NUL\131\STX\SOH\n\
    \\v\n\
    \\ETX\EOT!\SOH\DC2\EOT\128\STX\b\DLE\n\
    \\f\n\
    \\EOT\EOT!\STX\NUL\DC2\EOT\129\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ACK\DC2\EOT\129\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\SOH\DC2\EOT\129\STX\SO\SYN\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ETX\DC2\EOT\129\STX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT!\STX\SOH\DC2\EOT\130\STX\STX+\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ACK\DC2\EOT\130\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\SOH\DC2\EOT\130\STX\FS&\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ETX\DC2\EOT\130\STX)*\n\
    \\f\n\
    \\STX\EOT\"\DC2\ACK\133\STX\NUL\136\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\"\SOH\DC2\EOT\133\STX\b\GS\n\
    \\f\n\
    \\EOT\EOT\"\STX\NUL\DC2\EOT\134\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ACK\DC2\EOT\134\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\SOH\DC2\EOT\134\STX\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ETX\DC2\EOT\134\STX\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\"\STX\SOH\DC2\EOT\135\STX\STX \n\
    \\r\n\
    \\ENQ\EOT\"\STX\SOH\ACK\DC2\EOT\135\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\"\STX\SOH\SOH\DC2\EOT\135\STX\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\"\STX\SOH\ETX\DC2\EOT\135\STX\RS\US\n\
    \\f\n\
    \\STX\EOT#\DC2\ACK\138\STX\NUL\141\STX\SOH\n\
    \\v\n\
    \\ETX\EOT#\SOH\DC2\EOT\138\STX\b\RS\n\
    \\f\n\
    \\EOT\EOT#\STX\NUL\DC2\EOT\139\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ACK\DC2\EOT\139\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\SOH\DC2\EOT\139\STX\SO\SYN\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ETX\DC2\EOT\139\STX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT#\STX\SOH\DC2\EOT\140\STX\STX \n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ACK\DC2\EOT\140\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\SOH\DC2\EOT\140\STX\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ETX\DC2\EOT\140\STX\RS\US\n\
    \\f\n\
    \\STX\EOT$\DC2\ACK\143\STX\NUL\145\STX\SOH\n\
    \\v\n\
    \\ETX\EOT$\SOH\DC2\EOT\143\STX\b\US\n\
    \\f\n\
    \\EOT\EOT$\STX\NUL\DC2\EOT\144\STX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ENQ\DC2\EOT\144\STX\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\SOH\DC2\EOT\144\STX\a\SO\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ETX\DC2\EOT\144\STX\DC1\DC2\n\
    \\f\n\
    \\STX\EOT%\DC2\ACK\147\STX\NUL\151\STX\SOH\n\
    \\v\n\
    \\ETX\EOT%\SOH\DC2\EOT\147\STX\b\ESC\n\
    \\f\n\
    \\EOT\EOT%\STX\NUL\DC2\EOT\148\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ACK\DC2\EOT\148\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\SOH\DC2\EOT\148\STX\SO\SYN\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ETX\DC2\EOT\148\STX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT%\STX\SOH\DC2\EOT\149\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\ENQ\DC2\EOT\149\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\SOH\DC2\EOT\149\STX\t\EM\n\
    \\r\n\
    \\ENQ\EOT%\STX\SOH\ETX\DC2\EOT\149\STX\FS\GS\n\
    \\f\n\
    \\EOT\EOT%\STX\STX\DC2\EOT\150\STX\STX \n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\ACK\DC2\EOT\150\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\SOH\DC2\EOT\150\STX\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT%\STX\STX\ETX\DC2\EOT\150\STX\RS\US\n\
    \\f\n\
    \\STX\EOT&\DC2\ACK\153\STX\NUL\157\STX\SOH\n\
    \\v\n\
    \\ETX\EOT&\SOH\DC2\EOT\153\STX\b\EM\n\
    \\f\n\
    \\EOT\EOT&\STX\NUL\DC2\EOT\154\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ACK\DC2\EOT\154\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\SOH\DC2\EOT\154\STX\f\DC2\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ETX\DC2\EOT\154\STX\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT&\STX\SOH\DC2\EOT\155\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ENQ\DC2\EOT\155\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\SOH\DC2\EOT\155\STX\t\EM\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ETX\DC2\EOT\155\STX\FS\GS\n\
    \\f\n\
    \\EOT\EOT&\STX\STX\DC2\EOT\156\STX\STX \n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\ACK\DC2\EOT\156\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\SOH\DC2\EOT\156\STX\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\ETX\DC2\EOT\156\STX\RS\US\n\
    \\f\n\
    \\STX\ACK\ETX\DC2\ACK\159\STX\NUL\164\STX\SOH\n\
    \\v\n\
    \\ETX\ACK\ETX\SOH\DC2\EOT\159\STX\b\CAN\n\
    \\f\n\
    \\EOT\ACK\ETX\STX\NUL\DC2\EOT\160\STX\STX?\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\SOH\DC2\EOT\160\STX\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\STX\DC2\EOT\160\STX\NAK*\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\ETX\DC2\EOT\160\STX5=\n\
    \\f\n\
    \\EOT\ACK\ETX\STX\SOH\DC2\EOT\161\STX\STXP\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\SOH\DC2\EOT\161\STX\ACK\NAK\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\STX\DC2\EOT\161\STX\SYN,\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\ETX\DC2\EOT\161\STX7N\n\
    \\f\n\
    \\EOT\ACK\ETX\STX\STX\DC2\EOT\162\STX\STX9\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\SOH\DC2\EOT\162\STX\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\STX\DC2\EOT\162\STX\DC3&\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\ETX\DC2\EOT\162\STX17\n\
    \\f\n\
    \\EOT\ACK\ETX\STX\ETX\DC2\EOT\163\STX\STX5\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\SOH\DC2\EOT\163\STX\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\STX\DC2\EOT\163\STX\DC1\"\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\ETX\DC2\EOT\163\STX-3\n\
    \\f\n\
    \\STX\ENQ\STX\DC2\ACK\166\STX\NUL\177\STX\SOH\n\
    \\v\n\
    \\ETX\ENQ\STX\SOH\DC2\EOT\166\STX\ENQ\SO\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\NUL\DC2\EOT\167\STX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\NUL\SOH\DC2\EOT\167\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\NUL\STX\DC2\EOT\167\STX\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\SOH\DC2\EOT\168\STX\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\SOH\SOH\DC2\EOT\168\STX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\SOH\STX\DC2\EOT\168\STX !\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\STX\DC2\EOT\169\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\STX\SOH\DC2\EOT\169\STX\STX\SYN\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\STX\STX\DC2\EOT\169\STX\EM\SUB\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ETX\DC2\EOT\170\STX\STX \n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ETX\SOH\DC2\EOT\170\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ETX\STX\DC2\EOT\170\STX\RS\US\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\EOT\DC2\EOT\171\STX\STX%\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\EOT\SOH\DC2\EOT\171\STX\STX \n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\EOT\STX\DC2\EOT\171\STX#$\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ENQ\DC2\EOT\172\STX\STX&\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ENQ\SOH\DC2\EOT\172\STX\STX!\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ENQ\STX\DC2\EOT\172\STX$%\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ACK\DC2\EOT\173\STX\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ACK\SOH\DC2\EOT\173\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ACK\STX\DC2\EOT\173\STX\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\a\DC2\EOT\174\STX\STX \n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\a\SOH\DC2\EOT\174\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\a\STX\DC2\EOT\174\STX\RS\US\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\b\DC2\EOT\175\STX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\b\SOH\DC2\EOT\175\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\b\STX\DC2\EOT\175\STX\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\t\DC2\EOT\176\STX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\t\SOH\DC2\EOT\176\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\t\STX\DC2\EOT\176\STX\ESC\FS\n\
    \\f\n\
    \\STX\EOT'\DC2\ACK\179\STX\NUL\182\STX\SOH\n\
    \\v\n\
    \\ETX\EOT'\SOH\DC2\EOT\179\STX\b\DC3\n\
    \\f\n\
    \\EOT\EOT'\STX\NUL\DC2\EOT\180\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\ACK\DC2\EOT\180\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\SOH\DC2\EOT\180\STX\f\DLE\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\ETX\DC2\EOT\180\STX\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT'\STX\SOH\DC2\EOT\181\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\ENQ\DC2\EOT\181\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\SOH\DC2\EOT\181\STX\t\DC3\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\ETX\DC2\EOT\181\STX\SYN\ETBb\ACKproto3"