{- This file was auto-generated from objects/v2/objects.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Objects.V2.Objects (
        BucketsService(..), ObjectsService(..), MultipartService(..),
        AbortMultipartRequest(), AbortMultipartResponse(), Bucket(),
        BucketRef(), ByteRange(), ByteRange'Selection(..),
        _ByteRange'Bytes, _ByteRange'SuffixLength,
        CompleteMultipartRequest(), ContentRange(), CreateBucketRequest(),
        CreateMultipartRequest(), DeleteBucketRequest(),
        DeleteBucketResponse(), DeleteObjectRequest(),
        DeleteObjectResponse(), ErrorCode(..), ErrorCode(),
        ErrorCode'UnrecognizedValue, ErrorDetail(), GetObjectHeader(),
        GetObjectRequest(), GetObjectResponse(),
        GetObjectResponse'Frame(..), _GetObjectResponse'Header,
        _GetObjectResponse'Body, _GetObjectResponse'Error,
        HeadBucketRequest(), HeadObjectRequest(), HeadObjectResponse(),
        InclusiveRange(), ListEntry(), ListObjectsRequest(),
        ListObjectsResponse(), ListPartsRequest(), ListPartsResponse(),
        MultipartUpload(), MutationIdentity(), ObjectInfo(),
        ObjectMetadata(), ObjectMetadata'UserEntry(), ObjectsLimit(..),
        ObjectsLimit(), ObjectsLimit'UnrecognizedValue, Preconditions(),
        Preconditions'Condition(..), _Preconditions'IfAbsent,
        _Preconditions'IfMatch, PutObjectHeader(), PutObjectRequest(),
        PutObjectRequest'Frame(..), _PutObjectRequest'Header,
        _PutObjectRequest'Body, _PutObjectRequest'Complete,
        UploadPartHeader(), UploadPartRequest(),
        UploadPartRequest'Frame(..), _UploadPartRequest'Header,
        _UploadPartRequest'Body, _UploadPartRequest'Complete,
        UploadedPart()
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
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' AbortMultipartRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' AbortMultipartRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' AbortMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.uploadId' @:: Lens' AbortMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' AbortMultipartRequest MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' AbortMultipartRequest (Prelude.Maybe MutationIdentity)@ -}
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
    = Data.Text.pack "acyclic.objects.v2.AbortMultipartRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKAbortMultipartRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2@\n\
      \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
     
         * 'Proto.Objects.V2.Objects_Fields.existed' @:: Lens' AbortMultipartResponse Prelude.Bool@ -}
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
    = Data.Text.pack "acyclic.objects.v2.AbortMultipartResponse"
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
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' Bucket BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' Bucket (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.createdAt' @:: Lens' Bucket Proto.Google.Protobuf.Timestamp.Timestamp@
         * 'Proto.Objects.V2.Objects_Fields.maybe'createdAt' @:: Lens' Bucket (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp)@ -}
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
  messageName _ = Data.Text.pack "acyclic.objects.v2.Bucket"
  packedMessageDescriptor _
    = "\n\
      \\ACKBucket\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC29\n\
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
     
         * 'Proto.Objects.V2.Objects_Fields.name' @:: Lens' BucketRef Data.Text.Text@ -}
data BucketRef
  = BucketRef'_constructor {_BucketRef'name :: !Data.Text.Text,
                            _BucketRef'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show BucketRef where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField BucketRef "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _BucketRef'name (\ x__ y__ -> x__ {_BucketRef'name = y__}))
        Prelude.id
instance Data.ProtoLens.Message BucketRef where
  messageName _ = Data.Text.pack "acyclic.objects.v2.BucketRef"
  packedMessageDescriptor _
    = "\n\
      \\tBucketRef\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname"
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
              Data.ProtoLens.FieldDescriptor BucketRef
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, name__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _BucketRef'_unknownFields
        (\ x__ y__ -> x__ {_BucketRef'_unknownFields = y__})
  defMessage
    = BucketRef'_constructor
        {_BucketRef'name = Data.ProtoLens.fieldDefault,
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
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData BucketRef where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_BucketRef'_unknownFields x__)
             (Control.DeepSeq.deepseq (_BucketRef'name x__) ())
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.maybe'selection' @:: Lens' ByteRange (Prelude.Maybe ByteRange'Selection)@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bytes' @:: Lens' ByteRange (Prelude.Maybe InclusiveRange)@
         * 'Proto.Objects.V2.Objects_Fields.bytes' @:: Lens' ByteRange InclusiveRange@
         * 'Proto.Objects.V2.Objects_Fields.maybe'suffixLength' @:: Lens' ByteRange (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Objects.V2.Objects_Fields.suffixLength' @:: Lens' ByteRange Data.Word.Word64@ -}
data ByteRange
  = ByteRange'_constructor {_ByteRange'selection :: !(Prelude.Maybe ByteRange'Selection),
                            _ByteRange'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ByteRange where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data ByteRange'Selection
  = ByteRange'Bytes !InclusiveRange |
    ByteRange'SuffixLength !Data.Word.Word64
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField ByteRange "maybe'selection" (Prelude.Maybe ByteRange'Selection) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ByteRange'selection
           (\ x__ y__ -> x__ {_ByteRange'selection = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ByteRange "maybe'bytes" (Prelude.Maybe InclusiveRange) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ByteRange'selection
           (\ x__ y__ -> x__ {_ByteRange'selection = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ByteRange'Bytes x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ByteRange'Bytes y__))
instance Data.ProtoLens.Field.HasField ByteRange "bytes" InclusiveRange where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ByteRange'selection
           (\ x__ y__ -> x__ {_ByteRange'selection = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ByteRange'Bytes x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ByteRange'Bytes y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ByteRange "maybe'suffixLength" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ByteRange'selection
           (\ x__ y__ -> x__ {_ByteRange'selection = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ByteRange'SuffixLength x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ByteRange'SuffixLength y__))
instance Data.ProtoLens.Field.HasField ByteRange "suffixLength" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ByteRange'selection
           (\ x__ y__ -> x__ {_ByteRange'selection = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ByteRange'SuffixLength x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ByteRange'SuffixLength y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message ByteRange where
  messageName _ = Data.Text.pack "acyclic.objects.v2.ByteRange"
  packedMessageDescriptor _
    = "\n\
      \\tByteRange\DC2:\n\
      \\ENQbytes\CAN\SOH \SOH(\v2\".acyclic.objects.v2.InclusiveRangeH\NULR\ENQbytes\DC2%\n\
      \\rsuffix_length\CAN\STX \SOH(\EOTH\NULR\fsuffixLengthB\v\n\
      \\tselection"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bytes"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor InclusiveRange)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'bytes")) ::
              Data.ProtoLens.FieldDescriptor ByteRange
        suffixLength__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suffix_length"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suffixLength")) ::
              Data.ProtoLens.FieldDescriptor ByteRange
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bytes__field_descriptor),
           (Data.ProtoLens.Tag 2, suffixLength__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ByteRange'_unknownFields
        (\ x__ y__ -> x__ {_ByteRange'_unknownFields = y__})
  defMessage
    = ByteRange'_constructor
        {_ByteRange'selection = Prelude.Nothing,
         _ByteRange'_unknownFields = []}
  parseMessage
    = let
        loop :: ByteRange -> Data.ProtoLens.Encoding.Bytes.Parser ByteRange
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
                                       "bytes"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bytes") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "suffix_length"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"suffixLength") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ByteRange"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'selection") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (ByteRange'Bytes v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ByteRange'SuffixLength v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ByteRange where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ByteRange'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ByteRange'selection x__) ())
instance Control.DeepSeq.NFData ByteRange'Selection where
  rnf (ByteRange'Bytes x__) = Control.DeepSeq.rnf x__
  rnf (ByteRange'SuffixLength x__) = Control.DeepSeq.rnf x__
_ByteRange'Bytes ::
  Data.ProtoLens.Prism.Prism' ByteRange'Selection InclusiveRange
_ByteRange'Bytes
  = Data.ProtoLens.Prism.prism'
      ByteRange'Bytes
      (\ p__
         -> case p__ of
              (ByteRange'Bytes p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ByteRange'SuffixLength ::
  Data.ProtoLens.Prism.Prism' ByteRange'Selection Data.Word.Word64
_ByteRange'SuffixLength
  = Data.ProtoLens.Prism.prism'
      ByteRange'SuffixLength
      (\ p__
         -> case p__ of
              (ByteRange'SuffixLength p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' CompleteMultipartRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' CompleteMultipartRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' CompleteMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.uploadId' @:: Lens' CompleteMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.parts' @:: Lens' CompleteMultipartRequest [UploadedPart]@
         * 'Proto.Objects.V2.Objects_Fields.vec'parts' @:: Lens' CompleteMultipartRequest (Data.Vector.Vector UploadedPart)@
         * 'Proto.Objects.V2.Objects_Fields.preconditions' @:: Lens' CompleteMultipartRequest Preconditions@
         * 'Proto.Objects.V2.Objects_Fields.maybe'preconditions' @:: Lens' CompleteMultipartRequest (Prelude.Maybe Preconditions)@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' CompleteMultipartRequest MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' CompleteMultipartRequest (Prelude.Maybe MutationIdentity)@ -}
data CompleteMultipartRequest
  = CompleteMultipartRequest'_constructor {_CompleteMultipartRequest'bucket :: !(Prelude.Maybe BucketRef),
                                           _CompleteMultipartRequest'objectKey :: !Data.Text.Text,
                                           _CompleteMultipartRequest'uploadId :: !Data.Text.Text,
                                           _CompleteMultipartRequest'parts :: !(Data.Vector.Vector UploadedPart),
                                           _CompleteMultipartRequest'preconditions :: !(Prelude.Maybe Preconditions),
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
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "preconditions" Preconditions where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'preconditions
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'preconditions = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CompleteMultipartRequest "maybe'preconditions" (Prelude.Maybe Preconditions) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompleteMultipartRequest'preconditions
           (\ x__ y__ -> x__ {_CompleteMultipartRequest'preconditions = y__}))
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
    = Data.Text.pack "acyclic.objects.v2.CompleteMultipartRequest"
  packedMessageDescriptor _
    = "\n\
      \\CANCompleteMultipartRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC26\n\
      \\ENQparts\CAN\EOT \ETX(\v2 .acyclic.objects.v2.UploadedPartR\ENQparts\DC2G\n\
      \\rpreconditions\CAN\ENQ \SOH(\v2!.acyclic.objects.v2.PreconditionsR\rpreconditions\DC2@\n\
      \\bmutation\CAN\ACK \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
        preconditions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "preconditions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Preconditions)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'preconditions")) ::
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
           (Data.ProtoLens.Tag 5, preconditions__field_descriptor),
           (Data.ProtoLens.Tag 6, mutation__field_descriptor)]
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
         _CompleteMultipartRequest'preconditions = Prelude.Nothing,
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
                                       "preconditions"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"preconditions") y x)
                                  mutable'parts
                        50
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
                              Lens.Family2.view
                                (Data.ProtoLens.Field.field @"maybe'preconditions") _x
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
                                 Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'mutation") _x
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
                            (_CompleteMultipartRequest'preconditions x__)
                            (Control.DeepSeq.deepseq
                               (_CompleteMultipartRequest'mutation x__) ()))))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.start' @:: Lens' ContentRange Data.Word.Word64@
         * 'Proto.Objects.V2.Objects_Fields.end' @:: Lens' ContentRange Data.Word.Word64@
         * 'Proto.Objects.V2.Objects_Fields.total' @:: Lens' ContentRange Data.Word.Word64@ -}
data ContentRange
  = ContentRange'_constructor {_ContentRange'start :: !Data.Word.Word64,
                               _ContentRange'end :: !Data.Word.Word64,
                               _ContentRange'total :: !Data.Word.Word64,
                               _ContentRange'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ContentRange where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ContentRange "start" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContentRange'start (\ x__ y__ -> x__ {_ContentRange'start = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContentRange "end" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContentRange'end (\ x__ y__ -> x__ {_ContentRange'end = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContentRange "total" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContentRange'total (\ x__ y__ -> x__ {_ContentRange'total = y__}))
        Prelude.id
instance Data.ProtoLens.Message ContentRange where
  messageName _ = Data.Text.pack "acyclic.objects.v2.ContentRange"
  packedMessageDescriptor _
    = "\n\
      \\fContentRange\DC2\DC4\n\
      \\ENQstart\CAN\SOH \SOH(\EOTR\ENQstart\DC2\DLE\n\
      \\ETXend\CAN\STX \SOH(\EOTR\ETXend\DC2\DC4\n\
      \\ENQtotal\CAN\ETX \SOH(\EOTR\ENQtotal"
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
              Data.ProtoLens.FieldDescriptor ContentRange
        end__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "end"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"end")) ::
              Data.ProtoLens.FieldDescriptor ContentRange
        total__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "total"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"total")) ::
              Data.ProtoLens.FieldDescriptor ContentRange
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, start__field_descriptor),
           (Data.ProtoLens.Tag 2, end__field_descriptor),
           (Data.ProtoLens.Tag 3, total__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ContentRange'_unknownFields
        (\ x__ y__ -> x__ {_ContentRange'_unknownFields = y__})
  defMessage
    = ContentRange'_constructor
        {_ContentRange'start = Data.ProtoLens.fieldDefault,
         _ContentRange'end = Data.ProtoLens.fieldDefault,
         _ContentRange'total = Data.ProtoLens.fieldDefault,
         _ContentRange'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ContentRange -> Data.ProtoLens.Encoding.Bytes.Parser ContentRange
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
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "total"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"total") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ContentRange"
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
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"total") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ContentRange where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ContentRange'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ContentRange'start x__)
                (Control.DeepSeq.deepseq
                   (_ContentRange'end x__)
                   (Control.DeepSeq.deepseq (_ContentRange'total x__) ())))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.name' @:: Lens' CreateBucketRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' CreateBucketRequest MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' CreateBucketRequest (Prelude.Maybe MutationIdentity)@ -}
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
    = Data.Text.pack "acyclic.objects.v2.CreateBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3CreateBucketRequest\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2@\n\
      \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' CreateMultipartRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' CreateMultipartRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' CreateMultipartRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.metadata' @:: Lens' CreateMultipartRequest ObjectMetadata@
         * 'Proto.Objects.V2.Objects_Fields.maybe'metadata' @:: Lens' CreateMultipartRequest (Prelude.Maybe ObjectMetadata)@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' CreateMultipartRequest MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' CreateMultipartRequest (Prelude.Maybe MutationIdentity)@ -}
data CreateMultipartRequest
  = CreateMultipartRequest'_constructor {_CreateMultipartRequest'bucket :: !(Prelude.Maybe BucketRef),
                                         _CreateMultipartRequest'objectKey :: !Data.Text.Text,
                                         _CreateMultipartRequest'metadata :: !(Prelude.Maybe ObjectMetadata),
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
    = Data.Text.pack "acyclic.objects.v2.CreateMultipartRequest"
  packedMessageDescriptor _
    = "\n\
      \\SYNCreateMultipartRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
      \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v2.ObjectMetadataR\bmetadata\DC2@\n\
      \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
           (Data.ProtoLens.Tag 4, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateMultipartRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateMultipartRequest'_unknownFields = y__})
  defMessage
    = CreateMultipartRequest'_constructor
        {_CreateMultipartRequest'bucket = Prelude.Nothing,
         _CreateMultipartRequest'objectKey = Data.ProtoLens.fieldDefault,
         _CreateMultipartRequest'metadata = Prelude.Nothing,
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
                         (_CreateMultipartRequest'mutation x__) ()))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' DeleteBucketRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' DeleteBucketRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' DeleteBucketRequest MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' DeleteBucketRequest (Prelude.Maybe MutationIdentity)@ -}
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
    = Data.Text.pack "acyclic.objects.v2.DeleteBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3DeleteBucketRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2@\n\
      \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
     
         * 'Proto.Objects.V2.Objects_Fields.existed' @:: Lens' DeleteBucketResponse Prelude.Bool@ -}
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
    = Data.Text.pack "acyclic.objects.v2.DeleteBucketResponse"
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
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' DeleteObjectRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' DeleteObjectRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' DeleteObjectRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.preconditions' @:: Lens' DeleteObjectRequest Preconditions@
         * 'Proto.Objects.V2.Objects_Fields.maybe'preconditions' @:: Lens' DeleteObjectRequest (Prelude.Maybe Preconditions)@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' DeleteObjectRequest MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' DeleteObjectRequest (Prelude.Maybe MutationIdentity)@ -}
data DeleteObjectRequest
  = DeleteObjectRequest'_constructor {_DeleteObjectRequest'bucket :: !(Prelude.Maybe BucketRef),
                                      _DeleteObjectRequest'objectKey :: !Data.Text.Text,
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
    = Data.Text.pack "acyclic.objects.v2.DeleteObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3DeleteObjectRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2G\n\
      \\rpreconditions\CAN\ETX \SOH(\v2!.acyclic.objects.v2.PreconditionsR\rpreconditions\DC2@\n\
      \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
           (Data.ProtoLens.Tag 3, preconditions__field_descriptor),
           (Data.ProtoLens.Tag 4, mutation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DeleteObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_DeleteObjectRequest'_unknownFields = y__})
  defMessage
    = DeleteObjectRequest'_constructor
        {_DeleteObjectRequest'bucket = Prelude.Nothing,
         _DeleteObjectRequest'objectKey = Data.ProtoLens.fieldDefault,
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "preconditions"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"preconditions") y x)
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
                   (case
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'preconditions") _x
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
                      (_DeleteObjectRequest'preconditions x__)
                      (Control.DeepSeq.deepseq (_DeleteObjectRequest'mutation x__) ()))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.existed' @:: Lens' DeleteObjectResponse Prelude.Bool@ -}
data DeleteObjectResponse
  = DeleteObjectResponse'_constructor {_DeleteObjectResponse'existed :: !Prelude.Bool,
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
instance Data.ProtoLens.Message DeleteObjectResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.DeleteObjectResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC4DeleteObjectResponse\DC2\CAN\n\
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
              Data.ProtoLens.FieldDescriptor DeleteObjectResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, existed__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _DeleteObjectResponse'_unknownFields
        (\ x__ y__ -> x__ {_DeleteObjectResponse'_unknownFields = y__})
  defMessage
    = DeleteObjectResponse'_constructor
        {_DeleteObjectResponse'existed = Data.ProtoLens.fieldDefault,
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
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData DeleteObjectResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_DeleteObjectResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_DeleteObjectResponse'existed x__) ())
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
    ERROR_CODE_QUOTA_EXCEEDED |
    ERROR_CODE_UNSUPPORTED |
    ERROR_CODE_UNAVAILABLE |
    ERROR_CODE_ACCESS_DENIED |
    ERROR_CODE_RANGE_NOT_SATISFIABLE |
    ERROR_CODE_NOT_MODIFIED |
    ErrorCode'Unrecognized !ErrorCode'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ErrorCode where
  maybeToEnum 0 = Prelude.Just ERROR_CODE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just ERROR_CODE_INVALID_ARGUMENT
  maybeToEnum 2 = Prelude.Just ERROR_CODE_NOT_FOUND
  maybeToEnum 3 = Prelude.Just ERROR_CODE_ALREADY_EXISTS
  maybeToEnum 4 = Prelude.Just ERROR_CODE_PRECONDITION_FAILED
  maybeToEnum 5 = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
  maybeToEnum 6 = Prelude.Just ERROR_CODE_QUOTA_EXCEEDED
  maybeToEnum 7 = Prelude.Just ERROR_CODE_UNSUPPORTED
  maybeToEnum 8 = Prelude.Just ERROR_CODE_UNAVAILABLE
  maybeToEnum 9 = Prelude.Just ERROR_CODE_ACCESS_DENIED
  maybeToEnum 10 = Prelude.Just ERROR_CODE_RANGE_NOT_SATISFIABLE
  maybeToEnum 11 = Prelude.Just ERROR_CODE_NOT_MODIFIED
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
  showEnum ERROR_CODE_QUOTA_EXCEEDED = "ERROR_CODE_QUOTA_EXCEEDED"
  showEnum ERROR_CODE_UNSUPPORTED = "ERROR_CODE_UNSUPPORTED"
  showEnum ERROR_CODE_UNAVAILABLE = "ERROR_CODE_UNAVAILABLE"
  showEnum ERROR_CODE_ACCESS_DENIED = "ERROR_CODE_ACCESS_DENIED"
  showEnum ERROR_CODE_RANGE_NOT_SATISFIABLE
    = "ERROR_CODE_RANGE_NOT_SATISFIABLE"
  showEnum ERROR_CODE_NOT_MODIFIED = "ERROR_CODE_NOT_MODIFIED"
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
    | (Prelude.==) k "ERROR_CODE_QUOTA_EXCEEDED"
    = Prelude.Just ERROR_CODE_QUOTA_EXCEEDED
    | (Prelude.==) k "ERROR_CODE_UNSUPPORTED"
    = Prelude.Just ERROR_CODE_UNSUPPORTED
    | (Prelude.==) k "ERROR_CODE_UNAVAILABLE"
    = Prelude.Just ERROR_CODE_UNAVAILABLE
    | (Prelude.==) k "ERROR_CODE_ACCESS_DENIED"
    = Prelude.Just ERROR_CODE_ACCESS_DENIED
    | (Prelude.==) k "ERROR_CODE_RANGE_NOT_SATISFIABLE"
    = Prelude.Just ERROR_CODE_RANGE_NOT_SATISFIABLE
    | (Prelude.==) k "ERROR_CODE_NOT_MODIFIED"
    = Prelude.Just ERROR_CODE_NOT_MODIFIED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ErrorCode where
  minBound = ERROR_CODE_UNSPECIFIED
  maxBound = ERROR_CODE_NOT_MODIFIED
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
  fromEnum ERROR_CODE_QUOTA_EXCEEDED = 6
  fromEnum ERROR_CODE_UNSUPPORTED = 7
  fromEnum ERROR_CODE_UNAVAILABLE = 8
  fromEnum ERROR_CODE_ACCESS_DENIED = 9
  fromEnum ERROR_CODE_RANGE_NOT_SATISFIABLE = 10
  fromEnum ERROR_CODE_NOT_MODIFIED = 11
  fromEnum (ErrorCode'Unrecognized (ErrorCode'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ ERROR_CODE_NOT_MODIFIED
    = Prelude.error
        "ErrorCode.succ: bad argument ERROR_CODE_NOT_MODIFIED. This value would be out of bounds."
  succ ERROR_CODE_UNSPECIFIED = ERROR_CODE_INVALID_ARGUMENT
  succ ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_NOT_FOUND
  succ ERROR_CODE_NOT_FOUND = ERROR_CODE_ALREADY_EXISTS
  succ ERROR_CODE_ALREADY_EXISTS = ERROR_CODE_PRECONDITION_FAILED
  succ ERROR_CODE_PRECONDITION_FAILED
    = ERROR_CODE_IDEMPOTENCY_MISMATCH
  succ ERROR_CODE_IDEMPOTENCY_MISMATCH = ERROR_CODE_QUOTA_EXCEEDED
  succ ERROR_CODE_QUOTA_EXCEEDED = ERROR_CODE_UNSUPPORTED
  succ ERROR_CODE_UNSUPPORTED = ERROR_CODE_UNAVAILABLE
  succ ERROR_CODE_UNAVAILABLE = ERROR_CODE_ACCESS_DENIED
  succ ERROR_CODE_ACCESS_DENIED = ERROR_CODE_RANGE_NOT_SATISFIABLE
  succ ERROR_CODE_RANGE_NOT_SATISFIABLE = ERROR_CODE_NOT_MODIFIED
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
  pred ERROR_CODE_QUOTA_EXCEEDED = ERROR_CODE_IDEMPOTENCY_MISMATCH
  pred ERROR_CODE_UNSUPPORTED = ERROR_CODE_QUOTA_EXCEEDED
  pred ERROR_CODE_UNAVAILABLE = ERROR_CODE_UNSUPPORTED
  pred ERROR_CODE_ACCESS_DENIED = ERROR_CODE_UNAVAILABLE
  pred ERROR_CODE_RANGE_NOT_SATISFIABLE = ERROR_CODE_ACCESS_DENIED
  pred ERROR_CODE_NOT_MODIFIED = ERROR_CODE_RANGE_NOT_SATISFIABLE
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
     
         * 'Proto.Objects.V2.Objects_Fields.code' @:: Lens' ErrorDetail ErrorCode@
         * 'Proto.Objects.V2.Objects_Fields.requestId' @:: Lens' ErrorDetail Data.Text.Text@ -}
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
  messageName _ = Data.Text.pack "acyclic.objects.v2.ErrorDetail"
  packedMessageDescriptor _
    = "\n\
      \\vErrorDetail\DC21\n\
      \\EOTcode\CAN\SOH \SOH(\SO2\GS.acyclic.objects.v2.ErrorCodeR\EOTcode\DC2\GS\n\
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
     
         * 'Proto.Objects.V2.Objects_Fields.object' @:: Lens' GetObjectHeader ObjectInfo@
         * 'Proto.Objects.V2.Objects_Fields.maybe'object' @:: Lens' GetObjectHeader (Prelude.Maybe ObjectInfo)@
         * 'Proto.Objects.V2.Objects_Fields.contentRange' @:: Lens' GetObjectHeader ContentRange@
         * 'Proto.Objects.V2.Objects_Fields.maybe'contentRange' @:: Lens' GetObjectHeader (Prelude.Maybe ContentRange)@ -}
data GetObjectHeader
  = GetObjectHeader'_constructor {_GetObjectHeader'object :: !(Prelude.Maybe ObjectInfo),
                                  _GetObjectHeader'contentRange :: !(Prelude.Maybe ContentRange),
                                  _GetObjectHeader'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GetObjectHeader where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField GetObjectHeader "object" ObjectInfo where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectHeader'object
           (\ x__ y__ -> x__ {_GetObjectHeader'object = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GetObjectHeader "maybe'object" (Prelude.Maybe ObjectInfo) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectHeader'object
           (\ x__ y__ -> x__ {_GetObjectHeader'object = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectHeader "contentRange" ContentRange where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectHeader'contentRange
           (\ x__ y__ -> x__ {_GetObjectHeader'contentRange = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GetObjectHeader "maybe'contentRange" (Prelude.Maybe ContentRange) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectHeader'contentRange
           (\ x__ y__ -> x__ {_GetObjectHeader'contentRange = y__}))
        Prelude.id
instance Data.ProtoLens.Message GetObjectHeader where
  messageName _ = Data.Text.pack "acyclic.objects.v2.GetObjectHeader"
  packedMessageDescriptor _
    = "\n\
      \\SIGetObjectHeader\DC26\n\
      \\ACKobject\CAN\SOH \SOH(\v2\RS.acyclic.objects.v2.ObjectInfoR\ACKobject\DC2E\n\
      \\rcontent_range\CAN\STX \SOH(\v2 .acyclic.objects.v2.ContentRangeR\fcontentRange"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        object__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectInfo)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'object")) ::
              Data.ProtoLens.FieldDescriptor GetObjectHeader
        contentRange__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "content_range"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ContentRange)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contentRange")) ::
              Data.ProtoLens.FieldDescriptor GetObjectHeader
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, object__field_descriptor),
           (Data.ProtoLens.Tag 2, contentRange__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GetObjectHeader'_unknownFields
        (\ x__ y__ -> x__ {_GetObjectHeader'_unknownFields = y__})
  defMessage
    = GetObjectHeader'_constructor
        {_GetObjectHeader'object = Prelude.Nothing,
         _GetObjectHeader'contentRange = Prelude.Nothing,
         _GetObjectHeader'_unknownFields = []}
  parseMessage
    = let
        loop ::
          GetObjectHeader
          -> Data.ProtoLens.Encoding.Bytes.Parser GetObjectHeader
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
                                       "object"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"object") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "content_range"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"contentRange") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "GetObjectHeader"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'object") _x
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
                       (Data.ProtoLens.Field.field @"maybe'contentRange") _x
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
instance Control.DeepSeq.NFData GetObjectHeader where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GetObjectHeader'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_GetObjectHeader'object x__)
                (Control.DeepSeq.deepseq (_GetObjectHeader'contentRange x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' GetObjectRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' GetObjectRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' GetObjectRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.range' @:: Lens' GetObjectRequest ByteRange@
         * 'Proto.Objects.V2.Objects_Fields.maybe'range' @:: Lens' GetObjectRequest (Prelude.Maybe ByteRange)@
         * 'Proto.Objects.V2.Objects_Fields.ifMatch' @:: Lens' GetObjectRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.ifNoneMatch' @:: Lens' GetObjectRequest Data.Text.Text@ -}
data GetObjectRequest
  = GetObjectRequest'_constructor {_GetObjectRequest'bucket :: !(Prelude.Maybe BucketRef),
                                   _GetObjectRequest'objectKey :: !Data.Text.Text,
                                   _GetObjectRequest'range :: !(Prelude.Maybe ByteRange),
                                   _GetObjectRequest'ifMatch :: !Data.Text.Text,
                                   _GetObjectRequest'ifNoneMatch :: !Data.Text.Text,
                                   _GetObjectRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GetObjectRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField GetObjectRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'bucket
           (\ x__ y__ -> x__ {_GetObjectRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GetObjectRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'bucket
           (\ x__ y__ -> x__ {_GetObjectRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'objectKey
           (\ x__ y__ -> x__ {_GetObjectRequest'objectKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectRequest "range" ByteRange where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'range
           (\ x__ y__ -> x__ {_GetObjectRequest'range = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GetObjectRequest "maybe'range" (Prelude.Maybe ByteRange) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectRequest'range
           (\ x__ y__ -> x__ {_GetObjectRequest'range = y__}))
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
instance Data.ProtoLens.Message GetObjectRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.GetObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEGetObjectRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC23\n\
      \\ENQrange\CAN\ETX \SOH(\v2\GS.acyclic.objects.v2.ByteRangeR\ENQrange\DC2\EM\n\
      \\bif_match\CAN\EOT \SOH(\tR\aifMatch\DC2\"\n\
      \\rif_none_match\CAN\ENQ \SOH(\tR\vifNoneMatch"
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
        range__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "range"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ByteRange)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'range")) ::
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
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, range__field_descriptor),
           (Data.ProtoLens.Tag 4, ifMatch__field_descriptor),
           (Data.ProtoLens.Tag 5, ifNoneMatch__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GetObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_GetObjectRequest'_unknownFields = y__})
  defMessage
    = GetObjectRequest'_constructor
        {_GetObjectRequest'bucket = Prelude.Nothing,
         _GetObjectRequest'objectKey = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'range = Prelude.Nothing,
         _GetObjectRequest'ifMatch = Data.ProtoLens.fieldDefault,
         _GetObjectRequest'ifNoneMatch = Data.ProtoLens.fieldDefault,
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
                                       "range"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"range") y x)
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
          (do loop Data.ProtoLens.defMessage) "GetObjectRequest"
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'range") _x
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
instance Control.DeepSeq.NFData GetObjectRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GetObjectRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_GetObjectRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_GetObjectRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_GetObjectRequest'range x__)
                      (Control.DeepSeq.deepseq
                         (_GetObjectRequest'ifMatch x__)
                         (Control.DeepSeq.deepseq
                            (_GetObjectRequest'ifNoneMatch x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.maybe'frame' @:: Lens' GetObjectResponse (Prelude.Maybe GetObjectResponse'Frame)@
         * 'Proto.Objects.V2.Objects_Fields.maybe'header' @:: Lens' GetObjectResponse (Prelude.Maybe GetObjectHeader)@
         * 'Proto.Objects.V2.Objects_Fields.header' @:: Lens' GetObjectResponse GetObjectHeader@
         * 'Proto.Objects.V2.Objects_Fields.maybe'body' @:: Lens' GetObjectResponse (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Objects.V2.Objects_Fields.body' @:: Lens' GetObjectResponse Data.ByteString.ByteString@
         * 'Proto.Objects.V2.Objects_Fields.maybe'error' @:: Lens' GetObjectResponse (Prelude.Maybe ErrorDetail)@
         * 'Proto.Objects.V2.Objects_Fields.error' @:: Lens' GetObjectResponse ErrorDetail@ -}
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
  = GetObjectResponse'Header !GetObjectHeader |
    GetObjectResponse'Body !Data.ByteString.ByteString |
    GetObjectResponse'Error !ErrorDetail
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField GetObjectResponse "maybe'frame" (Prelude.Maybe GetObjectResponse'Frame) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GetObjectResponse "maybe'header" (Prelude.Maybe GetObjectHeader) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (GetObjectResponse'Header x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap GetObjectResponse'Header y__))
instance Data.ProtoLens.Field.HasField GetObjectResponse "header" GetObjectHeader where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (GetObjectResponse'Header x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap GetObjectResponse'Header y__))
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
instance Data.ProtoLens.Field.HasField GetObjectResponse "maybe'error" (Prelude.Maybe ErrorDetail) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (GetObjectResponse'Error x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap GetObjectResponse'Error y__))
instance Data.ProtoLens.Field.HasField GetObjectResponse "error" ErrorDetail where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GetObjectResponse'frame
           (\ x__ y__ -> x__ {_GetObjectResponse'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (GetObjectResponse'Error x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap GetObjectResponse'Error y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message GetObjectResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.GetObjectResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1GetObjectResponse\DC2=\n\
      \\ACKheader\CAN\SOH \SOH(\v2#.acyclic.objects.v2.GetObjectHeaderH\NULR\ACKheader\DC2\DC4\n\
      \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbody\DC27\n\
      \\ENQerror\CAN\ETX \SOH(\v2\US.acyclic.objects.v2.ErrorDetailH\NULR\ENQerrorB\a\n\
      \\ENQframe"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        header__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "header"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor GetObjectHeader)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'header")) ::
              Data.ProtoLens.FieldDescriptor GetObjectResponse
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'body")) ::
              Data.ProtoLens.FieldDescriptor GetObjectResponse
        error__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "error"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ErrorDetail)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'error")) ::
              Data.ProtoLens.FieldDescriptor GetObjectResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, header__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor),
           (Data.ProtoLens.Tag 3, error__field_descriptor)]
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
                                       "header"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"header") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "error"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"error") y x)
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
                (Prelude.Just (GetObjectResponse'Header v))
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
                          v)
                (Prelude.Just (GetObjectResponse'Error v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData GetObjectResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GetObjectResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_GetObjectResponse'frame x__) ())
instance Control.DeepSeq.NFData GetObjectResponse'Frame where
  rnf (GetObjectResponse'Header x__) = Control.DeepSeq.rnf x__
  rnf (GetObjectResponse'Body x__) = Control.DeepSeq.rnf x__
  rnf (GetObjectResponse'Error x__) = Control.DeepSeq.rnf x__
_GetObjectResponse'Header ::
  Data.ProtoLens.Prism.Prism' GetObjectResponse'Frame GetObjectHeader
_GetObjectResponse'Header
  = Data.ProtoLens.Prism.prism'
      GetObjectResponse'Header
      (\ p__
         -> case p__ of
              (GetObjectResponse'Header p__val) -> Prelude.Just p__val
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
_GetObjectResponse'Error ::
  Data.ProtoLens.Prism.Prism' GetObjectResponse'Frame ErrorDetail
_GetObjectResponse'Error
  = Data.ProtoLens.Prism.prism'
      GetObjectResponse'Error
      (\ p__
         -> case p__ of
              (GetObjectResponse'Error p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' HeadBucketRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' HeadBucketRequest (Prelude.Maybe BucketRef)@ -}
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
    = Data.Text.pack "acyclic.objects.v2.HeadBucketRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1HeadBucketRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket"
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
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' HeadObjectRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' HeadObjectRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' HeadObjectRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.ifMatch' @:: Lens' HeadObjectRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.ifNoneMatch' @:: Lens' HeadObjectRequest Data.Text.Text@ -}
data HeadObjectRequest
  = HeadObjectRequest'_constructor {_HeadObjectRequest'bucket :: !(Prelude.Maybe BucketRef),
                                    _HeadObjectRequest'objectKey :: !Data.Text.Text,
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
instance Data.ProtoLens.Field.HasField HeadObjectRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'bucket
           (\ x__ y__ -> x__ {_HeadObjectRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HeadObjectRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'bucket
           (\ x__ y__ -> x__ {_HeadObjectRequest'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HeadObjectRequest "objectKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectRequest'objectKey
           (\ x__ y__ -> x__ {_HeadObjectRequest'objectKey = y__}))
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
    = Data.Text.pack "acyclic.objects.v2.HeadObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1HeadObjectRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\EM\n\
      \\bif_match\CAN\ETX \SOH(\tR\aifMatch\DC2\"\n\
      \\rif_none_match\CAN\EOT \SOH(\tR\vifNoneMatch"
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
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, ifMatch__field_descriptor),
           (Data.ProtoLens.Tag 4, ifNoneMatch__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HeadObjectRequest'_unknownFields
        (\ x__ y__ -> x__ {_HeadObjectRequest'_unknownFields = y__})
  defMessage
    = HeadObjectRequest'_constructor
        {_HeadObjectRequest'bucket = Prelude.Nothing,
         _HeadObjectRequest'objectKey = Data.ProtoLens.fieldDefault,
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
                                       "if_match"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"ifMatch") y x)
                        34
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"ifMatch") _x
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"ifNoneMatch") _x
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
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData HeadObjectRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_HeadObjectRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_HeadObjectRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_HeadObjectRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_HeadObjectRequest'ifMatch x__)
                      (Control.DeepSeq.deepseq
                         (_HeadObjectRequest'ifNoneMatch x__) ()))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.object' @:: Lens' HeadObjectResponse ObjectInfo@
         * 'Proto.Objects.V2.Objects_Fields.maybe'object' @:: Lens' HeadObjectResponse (Prelude.Maybe ObjectInfo)@ -}
data HeadObjectResponse
  = HeadObjectResponse'_constructor {_HeadObjectResponse'object :: !(Prelude.Maybe ObjectInfo),
                                     _HeadObjectResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show HeadObjectResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField HeadObjectResponse "object" ObjectInfo where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectResponse'object
           (\ x__ y__ -> x__ {_HeadObjectResponse'object = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HeadObjectResponse "maybe'object" (Prelude.Maybe ObjectInfo) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HeadObjectResponse'object
           (\ x__ y__ -> x__ {_HeadObjectResponse'object = y__}))
        Prelude.id
instance Data.ProtoLens.Message HeadObjectResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.HeadObjectResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC2HeadObjectResponse\DC26\n\
      \\ACKobject\CAN\SOH \SOH(\v2\RS.acyclic.objects.v2.ObjectInfoR\ACKobject"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        object__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectInfo)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'object")) ::
              Data.ProtoLens.FieldDescriptor HeadObjectResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, object__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HeadObjectResponse'_unknownFields
        (\ x__ y__ -> x__ {_HeadObjectResponse'_unknownFields = y__})
  defMessage
    = HeadObjectResponse'_constructor
        {_HeadObjectResponse'object = Prelude.Nothing,
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
                                       "object"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"object") y x)
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
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'object") _x
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
             (Control.DeepSeq.deepseq (_HeadObjectResponse'object x__) ())
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.start' @:: Lens' InclusiveRange Data.Word.Word64@
         * 'Proto.Objects.V2.Objects_Fields.end' @:: Lens' InclusiveRange Data.Word.Word64@
         * 'Proto.Objects.V2.Objects_Fields.maybe'end' @:: Lens' InclusiveRange (Prelude.Maybe Data.Word.Word64)@ -}
data InclusiveRange
  = InclusiveRange'_constructor {_InclusiveRange'start :: !Data.Word.Word64,
                                 _InclusiveRange'end :: !(Prelude.Maybe Data.Word.Word64),
                                 _InclusiveRange'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InclusiveRange where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InclusiveRange "start" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InclusiveRange'start
           (\ x__ y__ -> x__ {_InclusiveRange'start = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InclusiveRange "end" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InclusiveRange'end (\ x__ y__ -> x__ {_InclusiveRange'end = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField InclusiveRange "maybe'end" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InclusiveRange'end (\ x__ y__ -> x__ {_InclusiveRange'end = y__}))
        Prelude.id
instance Data.ProtoLens.Message InclusiveRange where
  messageName _ = Data.Text.pack "acyclic.objects.v2.InclusiveRange"
  packedMessageDescriptor _
    = "\n\
      \\SOInclusiveRange\DC2\DC4\n\
      \\ENQstart\CAN\SOH \SOH(\EOTR\ENQstart\DC2\NAK\n\
      \\ETXend\CAN\STX \SOH(\EOTH\NULR\ETXend\136\SOH\SOHB\ACK\n\
      \\EOT_end"
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
              Data.ProtoLens.FieldDescriptor InclusiveRange
        end__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "end"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'end")) ::
              Data.ProtoLens.FieldDescriptor InclusiveRange
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, start__field_descriptor),
           (Data.ProtoLens.Tag 2, end__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InclusiveRange'_unknownFields
        (\ x__ y__ -> x__ {_InclusiveRange'_unknownFields = y__})
  defMessage
    = InclusiveRange'_constructor
        {_InclusiveRange'start = Data.ProtoLens.fieldDefault,
         _InclusiveRange'end = Prelude.Nothing,
         _InclusiveRange'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InclusiveRange
          -> Data.ProtoLens.Encoding.Bytes.Parser InclusiveRange
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
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InclusiveRange"
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'end") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just _v)
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData InclusiveRange where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InclusiveRange'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InclusiveRange'start x__)
                (Control.DeepSeq.deepseq (_InclusiveRange'end x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' ListEntry Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.object' @:: Lens' ListEntry ObjectInfo@
         * 'Proto.Objects.V2.Objects_Fields.maybe'object' @:: Lens' ListEntry (Prelude.Maybe ObjectInfo)@ -}
data ListEntry
  = ListEntry'_constructor {_ListEntry'objectKey :: !Data.Text.Text,
                            _ListEntry'object :: !(Prelude.Maybe ObjectInfo),
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
instance Data.ProtoLens.Field.HasField ListEntry "object" ObjectInfo where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListEntry'object (\ x__ y__ -> x__ {_ListEntry'object = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListEntry "maybe'object" (Prelude.Maybe ObjectInfo) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListEntry'object (\ x__ y__ -> x__ {_ListEntry'object = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListEntry where
  messageName _ = Data.Text.pack "acyclic.objects.v2.ListEntry"
  packedMessageDescriptor _
    = "\n\
      \\tListEntry\DC2\GS\n\
      \\n\
      \object_key\CAN\SOH \SOH(\tR\tobjectKey\DC26\n\
      \\ACKobject\CAN\STX \SOH(\v2\RS.acyclic.objects.v2.ObjectInfoR\ACKobject"
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
        object__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectInfo)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'object")) ::
              Data.ProtoLens.FieldDescriptor ListEntry
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 2, object__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListEntry'_unknownFields
        (\ x__ y__ -> x__ {_ListEntry'_unknownFields = y__})
  defMessage
    = ListEntry'_constructor
        {_ListEntry'objectKey = Data.ProtoLens.fieldDefault,
         _ListEntry'object = Prelude.Nothing,
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
                                       "object"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"object") y x)
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'object") _x
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
                (Control.DeepSeq.deepseq (_ListEntry'object x__) ()))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' ListObjectsRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' ListObjectsRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.prefix' @:: Lens' ListObjectsRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.delimiter' @:: Lens' ListObjectsRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.pageSize' @:: Lens' ListObjectsRequest Data.Word.Word32@
         * 'Proto.Objects.V2.Objects_Fields.continuationToken' @:: Lens' ListObjectsRequest Data.Text.Text@ -}
data ListObjectsRequest
  = ListObjectsRequest'_constructor {_ListObjectsRequest'bucket :: !(Prelude.Maybe BucketRef),
                                     _ListObjectsRequest'prefix :: !Data.Text.Text,
                                     _ListObjectsRequest'delimiter :: !Data.Text.Text,
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
instance Data.ProtoLens.Field.HasField ListObjectsRequest "bucket" BucketRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'bucket
           (\ x__ y__ -> x__ {_ListObjectsRequest'bucket = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListObjectsRequest "maybe'bucket" (Prelude.Maybe BucketRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsRequest'bucket
           (\ x__ y__ -> x__ {_ListObjectsRequest'bucket = y__}))
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
    = Data.Text.pack "acyclic.objects.v2.ListObjectsRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2ListObjectsRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\SYN\n\
      \\ACKprefix\CAN\STX \SOH(\tR\ACKprefix\DC2\FS\n\
      \\tdelimiter\CAN\ETX \SOH(\tR\tdelimiter\DC2\ESC\n\
      \\tpage_size\CAN\EOT \SOH(\rR\bpageSize\DC2-\n\
      \\DC2continuation_token\CAN\ENQ \SOH(\tR\DC1continuationToken"
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
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, prefix__field_descriptor),
           (Data.ProtoLens.Tag 3, delimiter__field_descriptor),
           (Data.ProtoLens.Tag 4, pageSize__field_descriptor),
           (Data.ProtoLens.Tag 5, continuationToken__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListObjectsRequest'_unknownFields
        (\ x__ y__ -> x__ {_ListObjectsRequest'_unknownFields = y__})
  defMessage
    = ListObjectsRequest'_constructor
        {_ListObjectsRequest'bucket = Prelude.Nothing,
         _ListObjectsRequest'prefix = Data.ProtoLens.fieldDefault,
         _ListObjectsRequest'delimiter = Data.ProtoLens.fieldDefault,
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
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
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "page_size"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"pageSize") y x)
                        42
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
                      (let
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"pageSize") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
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
instance Control.DeepSeq.NFData ListObjectsRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListObjectsRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListObjectsRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_ListObjectsRequest'prefix x__)
                   (Control.DeepSeq.deepseq
                      (_ListObjectsRequest'delimiter x__)
                      (Control.DeepSeq.deepseq
                         (_ListObjectsRequest'pageSize x__)
                         (Control.DeepSeq.deepseq
                            (_ListObjectsRequest'continuationToken x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.entries' @:: Lens' ListObjectsResponse [ListEntry]@
         * 'Proto.Objects.V2.Objects_Fields.vec'entries' @:: Lens' ListObjectsResponse (Data.Vector.Vector ListEntry)@
         * 'Proto.Objects.V2.Objects_Fields.commonPrefixes' @:: Lens' ListObjectsResponse [Data.Text.Text]@
         * 'Proto.Objects.V2.Objects_Fields.vec'commonPrefixes' @:: Lens' ListObjectsResponse (Data.Vector.Vector Data.Text.Text)@
         * 'Proto.Objects.V2.Objects_Fields.continuationToken' @:: Lens' ListObjectsResponse Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.isTruncated' @:: Lens' ListObjectsResponse Prelude.Bool@ -}
data ListObjectsResponse
  = ListObjectsResponse'_constructor {_ListObjectsResponse'entries :: !(Data.Vector.Vector ListEntry),
                                      _ListObjectsResponse'commonPrefixes :: !(Data.Vector.Vector Data.Text.Text),
                                      _ListObjectsResponse'continuationToken :: !Data.Text.Text,
                                      _ListObjectsResponse'isTruncated :: !Prelude.Bool,
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
instance Data.ProtoLens.Field.HasField ListObjectsResponse "isTruncated" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListObjectsResponse'isTruncated
           (\ x__ y__ -> x__ {_ListObjectsResponse'isTruncated = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListObjectsResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.ListObjectsResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC3ListObjectsResponse\DC27\n\
      \\aentries\CAN\SOH \ETX(\v2\GS.acyclic.objects.v2.ListEntryR\aentries\DC2'\n\
      \\SIcommon_prefixes\CAN\STX \ETX(\tR\SOcommonPrefixes\DC2-\n\
      \\DC2continuation_token\CAN\ETX \SOH(\tR\DC1continuationToken\DC2!\n\
      \\fis_truncated\CAN\EOT \SOH(\bR\visTruncated"
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
        isTruncated__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "is_truncated"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"isTruncated")) ::
              Data.ProtoLens.FieldDescriptor ListObjectsResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, entries__field_descriptor),
           (Data.ProtoLens.Tag 2, commonPrefixes__field_descriptor),
           (Data.ProtoLens.Tag 3, continuationToken__field_descriptor),
           (Data.ProtoLens.Tag 4, isTruncated__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListObjectsResponse'_unknownFields
        (\ x__ y__ -> x__ {_ListObjectsResponse'_unknownFields = y__})
  defMessage
    = ListObjectsResponse'_constructor
        {_ListObjectsResponse'entries = Data.Vector.Generic.empty,
         _ListObjectsResponse'commonPrefixes = Data.Vector.Generic.empty,
         _ListObjectsResponse'continuationToken = Data.ProtoLens.fieldDefault,
         _ListObjectsResponse'isTruncated = Data.ProtoLens.fieldDefault,
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
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "is_truncated"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"isTruncated") y x)
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
                   ((Data.Monoid.<>)
                      (let
                         _v
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"isTruncated") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (\ b -> if b then 1 else 0) _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
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
                      (_ListObjectsResponse'continuationToken x__)
                      (Control.DeepSeq.deepseq
                         (_ListObjectsResponse'isTruncated x__) ()))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' ListPartsRequest BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' ListPartsRequest (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' ListPartsRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.uploadId' @:: Lens' ListPartsRequest Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.afterPartNumber' @:: Lens' ListPartsRequest Data.Word.Word32@
         * 'Proto.Objects.V2.Objects_Fields.pageSize' @:: Lens' ListPartsRequest Data.Word.Word32@ -}
data ListPartsRequest
  = ListPartsRequest'_constructor {_ListPartsRequest'bucket :: !(Prelude.Maybe BucketRef),
                                   _ListPartsRequest'objectKey :: !Data.Text.Text,
                                   _ListPartsRequest'uploadId :: !Data.Text.Text,
                                   _ListPartsRequest'afterPartNumber :: !Data.Word.Word32,
                                   _ListPartsRequest'pageSize :: !Data.Word.Word32,
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
instance Data.ProtoLens.Field.HasField ListPartsRequest "afterPartNumber" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsRequest'afterPartNumber
           (\ x__ y__ -> x__ {_ListPartsRequest'afterPartNumber = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListPartsRequest "pageSize" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsRequest'pageSize
           (\ x__ y__ -> x__ {_ListPartsRequest'pageSize = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListPartsRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.ListPartsRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEListPartsRequest\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2*\n\
      \\DC1after_part_number\CAN\EOT \SOH(\rR\SIafterPartNumber\DC2\ESC\n\
      \\tpage_size\CAN\ENQ \SOH(\rR\bpageSize"
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
        afterPartNumber__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "after_part_number"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"afterPartNumber")) ::
              Data.ProtoLens.FieldDescriptor ListPartsRequest
        pageSize__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "page_size"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"pageSize")) ::
              Data.ProtoLens.FieldDescriptor ListPartsRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, objectKey__field_descriptor),
           (Data.ProtoLens.Tag 3, uploadId__field_descriptor),
           (Data.ProtoLens.Tag 4, afterPartNumber__field_descriptor),
           (Data.ProtoLens.Tag 5, pageSize__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListPartsRequest'_unknownFields
        (\ x__ y__ -> x__ {_ListPartsRequest'_unknownFields = y__})
  defMessage
    = ListPartsRequest'_constructor
        {_ListPartsRequest'bucket = Prelude.Nothing,
         _ListPartsRequest'objectKey = Data.ProtoLens.fieldDefault,
         _ListPartsRequest'uploadId = Data.ProtoLens.fieldDefault,
         _ListPartsRequest'afterPartNumber = Data.ProtoLens.fieldDefault,
         _ListPartsRequest'pageSize = Data.ProtoLens.fieldDefault,
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
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "after_part_number"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"afterPartNumber") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "page_size"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"pageSize") y x)
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
                   ((Data.Monoid.<>)
                      (let
                         _v
                           = Lens.Family2.view
                               (Data.ProtoLens.Field.field @"afterPartNumber") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
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
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData ListPartsRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListPartsRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListPartsRequest'bucket x__)
                (Control.DeepSeq.deepseq
                   (_ListPartsRequest'objectKey x__)
                   (Control.DeepSeq.deepseq
                      (_ListPartsRequest'uploadId x__)
                      (Control.DeepSeq.deepseq
                         (_ListPartsRequest'afterPartNumber x__)
                         (Control.DeepSeq.deepseq (_ListPartsRequest'pageSize x__) ())))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.parts' @:: Lens' ListPartsResponse [UploadedPart]@
         * 'Proto.Objects.V2.Objects_Fields.vec'parts' @:: Lens' ListPartsResponse (Data.Vector.Vector UploadedPart)@
         * 'Proto.Objects.V2.Objects_Fields.nextPartNumber' @:: Lens' ListPartsResponse Data.Word.Word32@
         * 'Proto.Objects.V2.Objects_Fields.isTruncated' @:: Lens' ListPartsResponse Prelude.Bool@ -}
data ListPartsResponse
  = ListPartsResponse'_constructor {_ListPartsResponse'parts :: !(Data.Vector.Vector UploadedPart),
                                    _ListPartsResponse'nextPartNumber :: !Data.Word.Word32,
                                    _ListPartsResponse'isTruncated :: !Prelude.Bool,
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
instance Data.ProtoLens.Field.HasField ListPartsResponse "nextPartNumber" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsResponse'nextPartNumber
           (\ x__ y__ -> x__ {_ListPartsResponse'nextPartNumber = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListPartsResponse "isTruncated" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListPartsResponse'isTruncated
           (\ x__ y__ -> x__ {_ListPartsResponse'isTruncated = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListPartsResponse where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.ListPartsResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1ListPartsResponse\DC26\n\
      \\ENQparts\CAN\SOH \ETX(\v2 .acyclic.objects.v2.UploadedPartR\ENQparts\DC2(\n\
      \\DLEnext_part_number\CAN\STX \SOH(\rR\SOnextPartNumber\DC2!\n\
      \\fis_truncated\CAN\ETX \SOH(\bR\visTruncated"
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
        nextPartNumber__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "next_part_number"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"nextPartNumber")) ::
              Data.ProtoLens.FieldDescriptor ListPartsResponse
        isTruncated__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "is_truncated"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"isTruncated")) ::
              Data.ProtoLens.FieldDescriptor ListPartsResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, parts__field_descriptor),
           (Data.ProtoLens.Tag 2, nextPartNumber__field_descriptor),
           (Data.ProtoLens.Tag 3, isTruncated__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListPartsResponse'_unknownFields
        (\ x__ y__ -> x__ {_ListPartsResponse'_unknownFields = y__})
  defMessage
    = ListPartsResponse'_constructor
        {_ListPartsResponse'parts = Data.Vector.Generic.empty,
         _ListPartsResponse'nextPartNumber = Data.ProtoLens.fieldDefault,
         _ListPartsResponse'isTruncated = Data.ProtoLens.fieldDefault,
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
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "next_part_number"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"nextPartNumber") y x)
                                  mutable'parts
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "is_truncated"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"isTruncated") y x)
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
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"nextPartNumber") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         ((Prelude..)
                            Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"isTruncated") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            ((Prelude..)
                               Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                               _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ListPartsResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListPartsResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListPartsResponse'parts x__)
                (Control.DeepSeq.deepseq
                   (_ListPartsResponse'nextPartNumber x__)
                   (Control.DeepSeq.deepseq (_ListPartsResponse'isTruncated x__) ())))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.uploadId' @:: Lens' MultipartUpload Data.Text.Text@ -}
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
  messageName _ = Data.Text.pack "acyclic.objects.v2.MultipartUpload"
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
     
         * 'Proto.Objects.V2.Objects_Fields.idempotencyKey' @:: Lens' MutationIdentity Data.Text.Text@ -}
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
    = Data.Text.pack "acyclic.objects.v2.MutationIdentity"
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
     
         * 'Proto.Objects.V2.Objects_Fields.etag' @:: Lens' ObjectInfo Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.size' @:: Lens' ObjectInfo Data.Word.Word64@
         * 'Proto.Objects.V2.Objects_Fields.metadata' @:: Lens' ObjectInfo ObjectMetadata@
         * 'Proto.Objects.V2.Objects_Fields.maybe'metadata' @:: Lens' ObjectInfo (Prelude.Maybe ObjectMetadata)@
         * 'Proto.Objects.V2.Objects_Fields.lastModified' @:: Lens' ObjectInfo Proto.Google.Protobuf.Timestamp.Timestamp@
         * 'Proto.Objects.V2.Objects_Fields.maybe'lastModified' @:: Lens' ObjectInfo (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp)@ -}
data ObjectInfo
  = ObjectInfo'_constructor {_ObjectInfo'etag :: !Data.Text.Text,
                             _ObjectInfo'size :: !Data.Word.Word64,
                             _ObjectInfo'metadata :: !(Prelude.Maybe ObjectMetadata),
                             _ObjectInfo'lastModified :: !(Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp),
                             _ObjectInfo'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ObjectInfo where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ObjectInfo "etag" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectInfo'etag (\ x__ y__ -> x__ {_ObjectInfo'etag = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectInfo "size" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectInfo'size (\ x__ y__ -> x__ {_ObjectInfo'size = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectInfo "metadata" ObjectMetadata where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectInfo'metadata
           (\ x__ y__ -> x__ {_ObjectInfo'metadata = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ObjectInfo "maybe'metadata" (Prelude.Maybe ObjectMetadata) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectInfo'metadata
           (\ x__ y__ -> x__ {_ObjectInfo'metadata = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectInfo "lastModified" Proto.Google.Protobuf.Timestamp.Timestamp where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectInfo'lastModified
           (\ x__ y__ -> x__ {_ObjectInfo'lastModified = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ObjectInfo "maybe'lastModified" (Prelude.Maybe Proto.Google.Protobuf.Timestamp.Timestamp) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectInfo'lastModified
           (\ x__ y__ -> x__ {_ObjectInfo'lastModified = y__}))
        Prelude.id
instance Data.ProtoLens.Message ObjectInfo where
  messageName _ = Data.Text.pack "acyclic.objects.v2.ObjectInfo"
  packedMessageDescriptor _
    = "\n\
      \\n\
      \ObjectInfo\DC2\DC2\n\
      \\EOTetag\CAN\SOH \SOH(\tR\EOTetag\DC2\DC2\n\
      \\EOTsize\CAN\STX \SOH(\EOTR\EOTsize\DC2>\n\
      \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v2.ObjectMetadataR\bmetadata\DC2?\n\
      \\rlast_modified\CAN\EOT \SOH(\v2\SUB.google.protobuf.TimestampR\flastModified"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        etag__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "etag"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"etag")) ::
              Data.ProtoLens.FieldDescriptor ObjectInfo
        size__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "size"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"size")) ::
              Data.ProtoLens.FieldDescriptor ObjectInfo
        metadata__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metadata"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectMetadata)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'metadata")) ::
              Data.ProtoLens.FieldDescriptor ObjectInfo
        lastModified__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "last_modified"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Proto.Google.Protobuf.Timestamp.Timestamp)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'lastModified")) ::
              Data.ProtoLens.FieldDescriptor ObjectInfo
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, etag__field_descriptor),
           (Data.ProtoLens.Tag 2, size__field_descriptor),
           (Data.ProtoLens.Tag 3, metadata__field_descriptor),
           (Data.ProtoLens.Tag 4, lastModified__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ObjectInfo'_unknownFields
        (\ x__ y__ -> x__ {_ObjectInfo'_unknownFields = y__})
  defMessage
    = ObjectInfo'_constructor
        {_ObjectInfo'etag = Data.ProtoLens.fieldDefault,
         _ObjectInfo'size = Data.ProtoLens.fieldDefault,
         _ObjectInfo'metadata = Prelude.Nothing,
         _ObjectInfo'lastModified = Prelude.Nothing,
         _ObjectInfo'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ObjectInfo -> Data.ProtoLens.Encoding.Bytes.Parser ObjectInfo
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
                                       "etag"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"etag") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "size"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"size") y x)
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
                                       "last_modified"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"lastModified") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ObjectInfo"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"etag") _x
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"size") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
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
                             (Data.ProtoLens.Field.field @"maybe'lastModified") _x
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
instance Control.DeepSeq.NFData ObjectInfo where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ObjectInfo'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ObjectInfo'etag x__)
                (Control.DeepSeq.deepseq
                   (_ObjectInfo'size x__)
                   (Control.DeepSeq.deepseq
                      (_ObjectInfo'metadata x__)
                      (Control.DeepSeq.deepseq (_ObjectInfo'lastModified x__) ()))))
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.contentType' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.user' @:: Lens' ObjectMetadata (Data.Map.Map Data.Text.Text Data.Text.Text)@
         * 'Proto.Objects.V2.Objects_Fields.contentEncoding' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.cacheControl' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.contentDisposition' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.contentLanguage' @:: Lens' ObjectMetadata Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.expiresUnixSeconds' @:: Lens' ObjectMetadata Data.Int.Int64@
         * 'Proto.Objects.V2.Objects_Fields.maybe'expiresUnixSeconds' @:: Lens' ObjectMetadata (Prelude.Maybe Data.Int.Int64)@ -}
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
  messageName _ = Data.Text.pack "acyclic.objects.v2.ObjectMetadata"
  packedMessageDescriptor _
    = "\n\
      \\SOObjectMetadata\DC2!\n\
      \\fcontent_type\CAN\SOH \SOH(\tR\vcontentType\DC2@\n\
      \\EOTuser\CAN\STX \ETX(\v2,.acyclic.objects.v2.ObjectMetadata.UserEntryR\EOTuser\DC2)\n\
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
     
         * 'Proto.Objects.V2.Objects_Fields.key' @:: Lens' ObjectMetadata'UserEntry Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.value' @:: Lens' ObjectMetadata'UserEntry Data.Text.Text@ -}
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
    = Data.Text.pack "acyclic.objects.v2.ObjectMetadata.UserEntry"
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
newtype ObjectsLimit'UnrecognizedValue
  = ObjectsLimit'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ObjectsLimit
  = OBJECTS_LIMIT_UNSPECIFIED |
    OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES |
    OBJECTS_LIMIT_MAX_PAGE_ENTRIES |
    OBJECTS_LIMIT_MAX_KEY_BYTES |
    OBJECTS_LIMIT_MAX_USER_METADATA_BYTES |
    OBJECTS_LIMIT_MAX_MULTIPART_PARTS |
    OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES |
    ObjectsLimit'Unrecognized !ObjectsLimit'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ObjectsLimit where
  maybeToEnum 0 = Prelude.Just OBJECTS_LIMIT_UNSPECIFIED
  maybeToEnum 256
    = Prelude.Just OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  maybeToEnum 1000 = Prelude.Just OBJECTS_LIMIT_MAX_PAGE_ENTRIES
  maybeToEnum 1024 = Prelude.Just OBJECTS_LIMIT_MAX_KEY_BYTES
  maybeToEnum 2048
    = Prelude.Just OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
  maybeToEnum 10000 = Prelude.Just OBJECTS_LIMIT_MAX_MULTIPART_PARTS
  maybeToEnum 65536 = Prelude.Just OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
  maybeToEnum k
    = Prelude.Just
        (ObjectsLimit'Unrecognized
           (ObjectsLimit'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum OBJECTS_LIMIT_UNSPECIFIED = "OBJECTS_LIMIT_UNSPECIFIED"
  showEnum OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = "OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES"
  showEnum OBJECTS_LIMIT_MAX_KEY_BYTES
    = "OBJECTS_LIMIT_MAX_KEY_BYTES"
  showEnum OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
    = "OBJECTS_LIMIT_MAX_USER_METADATA_BYTES"
  showEnum OBJECTS_LIMIT_MAX_PAGE_ENTRIES
    = "OBJECTS_LIMIT_MAX_PAGE_ENTRIES"
  showEnum OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
    = "OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES"
  showEnum OBJECTS_LIMIT_MAX_MULTIPART_PARTS
    = "OBJECTS_LIMIT_MAX_MULTIPART_PARTS"
  showEnum
    (ObjectsLimit'Unrecognized (ObjectsLimit'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "OBJECTS_LIMIT_UNSPECIFIED"
    = Prelude.Just OBJECTS_LIMIT_UNSPECIFIED
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES"
    = Prelude.Just OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_KEY_BYTES"
    = Prelude.Just OBJECTS_LIMIT_MAX_KEY_BYTES
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_USER_METADATA_BYTES"
    = Prelude.Just OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_PAGE_ENTRIES"
    = Prelude.Just OBJECTS_LIMIT_MAX_PAGE_ENTRIES
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES"
    = Prelude.Just OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
    | (Prelude.==) k "OBJECTS_LIMIT_MAX_MULTIPART_PARTS"
    = Prelude.Just OBJECTS_LIMIT_MAX_MULTIPART_PARTS
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ObjectsLimit where
  minBound = OBJECTS_LIMIT_UNSPECIFIED
  maxBound = OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
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
  fromEnum OBJECTS_LIMIT_MAX_PAGE_ENTRIES = 1000
  fromEnum OBJECTS_LIMIT_MAX_KEY_BYTES = 1024
  fromEnum OBJECTS_LIMIT_MAX_USER_METADATA_BYTES = 2048
  fromEnum OBJECTS_LIMIT_MAX_MULTIPART_PARTS = 10000
  fromEnum OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES = 65536
  fromEnum
    (ObjectsLimit'Unrecognized (ObjectsLimit'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
    = Prelude.error
        "ObjectsLimit.succ: bad argument OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES. This value would be out of bounds."
  succ OBJECTS_LIMIT_UNSPECIFIED
    = OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  succ OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = OBJECTS_LIMIT_MAX_PAGE_ENTRIES
  succ OBJECTS_LIMIT_MAX_PAGE_ENTRIES = OBJECTS_LIMIT_MAX_KEY_BYTES
  succ OBJECTS_LIMIT_MAX_KEY_BYTES
    = OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
  succ OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
    = OBJECTS_LIMIT_MAX_MULTIPART_PARTS
  succ OBJECTS_LIMIT_MAX_MULTIPART_PARTS
    = OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
  succ (ObjectsLimit'Unrecognized _)
    = Prelude.error
        "ObjectsLimit.succ: bad argument: unrecognized value"
  pred OBJECTS_LIMIT_UNSPECIFIED
    = Prelude.error
        "ObjectsLimit.pred: bad argument OBJECTS_LIMIT_UNSPECIFIED. This value would be out of bounds."
  pred OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
    = OBJECTS_LIMIT_UNSPECIFIED
  pred OBJECTS_LIMIT_MAX_PAGE_ENTRIES
    = OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES
  pred OBJECTS_LIMIT_MAX_KEY_BYTES = OBJECTS_LIMIT_MAX_PAGE_ENTRIES
  pred OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
    = OBJECTS_LIMIT_MAX_KEY_BYTES
  pred OBJECTS_LIMIT_MAX_MULTIPART_PARTS
    = OBJECTS_LIMIT_MAX_USER_METADATA_BYTES
  pred OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES
    = OBJECTS_LIMIT_MAX_MULTIPART_PARTS
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
     
         * 'Proto.Objects.V2.Objects_Fields.maybe'condition' @:: Lens' Preconditions (Prelude.Maybe Preconditions'Condition)@
         * 'Proto.Objects.V2.Objects_Fields.maybe'ifAbsent' @:: Lens' Preconditions (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Objects.V2.Objects_Fields.ifAbsent' @:: Lens' Preconditions Prelude.Bool@
         * 'Proto.Objects.V2.Objects_Fields.maybe'ifMatch' @:: Lens' Preconditions (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Objects.V2.Objects_Fields.ifMatch' @:: Lens' Preconditions Data.Text.Text@ -}
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
    Preconditions'IfMatch !Data.Text.Text
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
instance Data.ProtoLens.Message Preconditions where
  messageName _ = Data.Text.pack "acyclic.objects.v2.Preconditions"
  packedMessageDescriptor _
    = "\n\
      \\rPreconditions\DC2\GS\n\
      \\tif_absent\CAN\SOH \SOH(\bH\NULR\bifAbsent\DC2\ESC\n\
      \\bif_match\CAN\STX \SOH(\tH\NULR\aifMatchB\v\n\
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
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, ifAbsent__field_descriptor),
           (Data.ProtoLens.Tag 2, ifMatch__field_descriptor)]
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
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' PutObjectHeader BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' PutObjectHeader (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' PutObjectHeader Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.metadata' @:: Lens' PutObjectHeader ObjectMetadata@
         * 'Proto.Objects.V2.Objects_Fields.maybe'metadata' @:: Lens' PutObjectHeader (Prelude.Maybe ObjectMetadata)@
         * 'Proto.Objects.V2.Objects_Fields.preconditions' @:: Lens' PutObjectHeader Preconditions@
         * 'Proto.Objects.V2.Objects_Fields.maybe'preconditions' @:: Lens' PutObjectHeader (Prelude.Maybe Preconditions)@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' PutObjectHeader MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' PutObjectHeader (Prelude.Maybe MutationIdentity)@ -}
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
  messageName _ = Data.Text.pack "acyclic.objects.v2.PutObjectHeader"
  packedMessageDescriptor _
    = "\n\
      \\SIPutObjectHeader\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
      \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v2.ObjectMetadataR\bmetadata\DC2G\n\
      \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v2.PreconditionsR\rpreconditions\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
     
         * 'Proto.Objects.V2.Objects_Fields.maybe'frame' @:: Lens' PutObjectRequest (Prelude.Maybe PutObjectRequest'Frame)@
         * 'Proto.Objects.V2.Objects_Fields.maybe'header' @:: Lens' PutObjectRequest (Prelude.Maybe PutObjectHeader)@
         * 'Proto.Objects.V2.Objects_Fields.header' @:: Lens' PutObjectRequest PutObjectHeader@
         * 'Proto.Objects.V2.Objects_Fields.maybe'body' @:: Lens' PutObjectRequest (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Objects.V2.Objects_Fields.body' @:: Lens' PutObjectRequest Data.ByteString.ByteString@
         * 'Proto.Objects.V2.Objects_Fields.maybe'complete' @:: Lens' PutObjectRequest (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Objects.V2.Objects_Fields.complete' @:: Lens' PutObjectRequest Prelude.Bool@ -}
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
    PutObjectRequest'Body !Data.ByteString.ByteString |
    PutObjectRequest'Complete !Prelude.Bool
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
instance Data.ProtoLens.Field.HasField PutObjectRequest "maybe'complete" (Prelude.Maybe Prelude.Bool) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (PutObjectRequest'Complete x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap PutObjectRequest'Complete y__))
instance Data.ProtoLens.Field.HasField PutObjectRequest "complete" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PutObjectRequest'frame
           (\ x__ y__ -> x__ {_PutObjectRequest'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (PutObjectRequest'Complete x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap PutObjectRequest'Complete y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message PutObjectRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.PutObjectRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEPutObjectRequest\DC2=\n\
      \\ACKheader\CAN\SOH \SOH(\v2#.acyclic.objects.v2.PutObjectHeaderH\NULR\ACKheader\DC2\DC4\n\
      \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbody\DC2\FS\n\
      \\bcomplete\CAN\ETX \SOH(\bH\NULR\bcompleteB\a\n\
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
        complete__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "complete"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'complete")) ::
              Data.ProtoLens.FieldDescriptor PutObjectRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, header__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor),
           (Data.ProtoLens.Tag 3, complete__field_descriptor)]
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
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "complete"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"complete") y x)
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
                          v)
                (Prelude.Just (PutObjectRequest'Complete v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                       ((Prelude..)
                          Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
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
  rnf (PutObjectRequest'Complete x__) = Control.DeepSeq.rnf x__
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
_PutObjectRequest'Complete ::
  Data.ProtoLens.Prism.Prism' PutObjectRequest'Frame Prelude.Bool
_PutObjectRequest'Complete
  = Data.ProtoLens.Prism.prism'
      PutObjectRequest'Complete
      (\ p__
         -> case p__ of
              (PutObjectRequest'Complete p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.bucket' @:: Lens' UploadPartHeader BucketRef@
         * 'Proto.Objects.V2.Objects_Fields.maybe'bucket' @:: Lens' UploadPartHeader (Prelude.Maybe BucketRef)@
         * 'Proto.Objects.V2.Objects_Fields.objectKey' @:: Lens' UploadPartHeader Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.uploadId' @:: Lens' UploadPartHeader Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.partNumber' @:: Lens' UploadPartHeader Data.Word.Word32@
         * 'Proto.Objects.V2.Objects_Fields.mutation' @:: Lens' UploadPartHeader MutationIdentity@
         * 'Proto.Objects.V2.Objects_Fields.maybe'mutation' @:: Lens' UploadPartHeader (Prelude.Maybe MutationIdentity)@ -}
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
    = Data.Text.pack "acyclic.objects.v2.UploadPartHeader"
  packedMessageDescriptor _
    = "\n\
      \\DLEUploadPartHeader\DC25\n\
      \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
      \\n\
      \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
      \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2\US\n\
      \\vpart_number\CAN\EOT \SOH(\rR\n\
      \partNumber\DC2@\n\
      \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation"
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
     
         * 'Proto.Objects.V2.Objects_Fields.maybe'frame' @:: Lens' UploadPartRequest (Prelude.Maybe UploadPartRequest'Frame)@
         * 'Proto.Objects.V2.Objects_Fields.maybe'header' @:: Lens' UploadPartRequest (Prelude.Maybe UploadPartHeader)@
         * 'Proto.Objects.V2.Objects_Fields.header' @:: Lens' UploadPartRequest UploadPartHeader@
         * 'Proto.Objects.V2.Objects_Fields.maybe'body' @:: Lens' UploadPartRequest (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Objects.V2.Objects_Fields.body' @:: Lens' UploadPartRequest Data.ByteString.ByteString@
         * 'Proto.Objects.V2.Objects_Fields.maybe'complete' @:: Lens' UploadPartRequest (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Objects.V2.Objects_Fields.complete' @:: Lens' UploadPartRequest Prelude.Bool@ -}
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
    UploadPartRequest'Body !Data.ByteString.ByteString |
    UploadPartRequest'Complete !Prelude.Bool
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
instance Data.ProtoLens.Field.HasField UploadPartRequest "maybe'complete" (Prelude.Maybe Prelude.Bool) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (UploadPartRequest'Complete x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap UploadPartRequest'Complete y__))
instance Data.ProtoLens.Field.HasField UploadPartRequest "complete" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UploadPartRequest'frame
           (\ x__ y__ -> x__ {_UploadPartRequest'frame = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (UploadPartRequest'Complete x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap UploadPartRequest'Complete y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message UploadPartRequest where
  messageName _
    = Data.Text.pack "acyclic.objects.v2.UploadPartRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1UploadPartRequest\DC2>\n\
      \\ACKheader\CAN\SOH \SOH(\v2$.acyclic.objects.v2.UploadPartHeaderH\NULR\ACKheader\DC2\DC4\n\
      \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbody\DC2\FS\n\
      \\bcomplete\CAN\ETX \SOH(\bH\NULR\bcompleteB\a\n\
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
        complete__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "complete"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'complete")) ::
              Data.ProtoLens.FieldDescriptor UploadPartRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, header__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor),
           (Data.ProtoLens.Tag 3, complete__field_descriptor)]
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
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "complete"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"complete") y x)
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
                          v)
                (Prelude.Just (UploadPartRequest'Complete v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                       ((Prelude..)
                          Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
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
  rnf (UploadPartRequest'Complete x__) = Control.DeepSeq.rnf x__
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
_UploadPartRequest'Complete ::
  Data.ProtoLens.Prism.Prism' UploadPartRequest'Frame Prelude.Bool
_UploadPartRequest'Complete
  = Data.ProtoLens.Prism.prism'
      UploadPartRequest'Complete
      (\ p__
         -> case p__ of
              (UploadPartRequest'Complete p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Objects.V2.Objects_Fields.partNumber' @:: Lens' UploadedPart Data.Word.Word32@
         * 'Proto.Objects.V2.Objects_Fields.etag' @:: Lens' UploadedPart Data.Text.Text@
         * 'Proto.Objects.V2.Objects_Fields.size' @:: Lens' UploadedPart Data.Word.Word64@ -}
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
  messageName _ = Data.Text.pack "acyclic.objects.v2.UploadedPart"
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
  type ServicePackage BucketsService = "acyclic.objects.v2"
  type ServiceMethods BucketsService = '["createBucket",
                                         "deleteBucket",
                                         "headBucket"]
  packedServiceDescriptor _
    = "\n\
      \\SOBucketsService\DC2S\n\
      \\fCreateBucket\DC2'.acyclic.objects.v2.CreateBucketRequest\SUB\SUB.acyclic.objects.v2.Bucket\DC2O\n\
      \\n\
      \HeadBucket\DC2%.acyclic.objects.v2.HeadBucketRequest\SUB\SUB.acyclic.objects.v2.Bucket\DC2a\n\
      \\fDeleteBucket\DC2'.acyclic.objects.v2.DeleteBucketRequest\SUB(.acyclic.objects.v2.DeleteBucketResponse"
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
  type ServicePackage ObjectsService = "acyclic.objects.v2"
  type ServiceMethods ObjectsService = '["deleteObject",
                                         "getObject",
                                         "headObject",
                                         "listObjects",
                                         "putObject"]
  packedServiceDescriptor _
    = "\n\
      \\SOObjectsService\DC2S\n\
      \\tPutObject\DC2$.acyclic.objects.v2.PutObjectRequest\SUB\RS.acyclic.objects.v2.ObjectInfo(\SOH\DC2Z\n\
      \\tGetObject\DC2$.acyclic.objects.v2.GetObjectRequest\SUB%.acyclic.objects.v2.GetObjectResponse0\SOH\DC2[\n\
      \\n\
      \HeadObject\DC2%.acyclic.objects.v2.HeadObjectRequest\SUB&.acyclic.objects.v2.HeadObjectResponse\DC2a\n\
      \\fDeleteObject\DC2'.acyclic.objects.v2.DeleteObjectRequest\SUB(.acyclic.objects.v2.DeleteObjectResponse\DC2^\n\
      \\vListObjects\DC2&.acyclic.objects.v2.ListObjectsRequest\SUB'.acyclic.objects.v2.ListObjectsResponse"
instance Data.ProtoLens.Service.Types.HasMethodImpl ObjectsService "putObject" where
  type MethodName ObjectsService "putObject" = "PutObject"
  type MethodInput ObjectsService "putObject" = PutObjectRequest
  type MethodOutput ObjectsService "putObject" = ObjectInfo
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
  type ServicePackage MultipartService = "acyclic.objects.v2"
  type ServiceMethods MultipartService = '["abortMultipart",
                                           "completeMultipart",
                                           "createMultipart",
                                           "listParts",
                                           "uploadPart"]
  packedServiceDescriptor _
    = "\n\
      \\DLEMultipartService\DC2b\n\
      \\SICreateMultipart\DC2*.acyclic.objects.v2.CreateMultipartRequest\SUB#.acyclic.objects.v2.MultipartUpload\DC2W\n\
      \\n\
      \UploadPart\DC2%.acyclic.objects.v2.UploadPartRequest\SUB .acyclic.objects.v2.UploadedPart(\SOH\DC2X\n\
      \\tListParts\DC2$.acyclic.objects.v2.ListPartsRequest\SUB%.acyclic.objects.v2.ListPartsResponse\DC2a\n\
      \\DC1CompleteMultipart\DC2,.acyclic.objects.v2.CompleteMultipartRequest\SUB\RS.acyclic.objects.v2.ObjectInfo\DC2g\n\
      \\SOAbortMultipart\DC2).acyclic.objects.v2.AbortMultipartRequest\SUB*.acyclic.objects.v2.AbortMultipartResponse"
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
  type MethodOutput MultipartService "completeMultipart" = ObjectInfo
  type MethodStreamingType MultipartService "completeMultipart" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MultipartService "abortMultipart" where
  type MethodName MultipartService "abortMultipart" = "AbortMultipart"
  type MethodInput MultipartService "abortMultipart" = AbortMultipartRequest
  type MethodOutput MultipartService "abortMultipart" = AbortMultipartResponse
  type MethodStreamingType MultipartService "abortMultipart" = 'Data.ProtoLens.Service.Types.NonStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\CANobjects/v2/objects.proto\DC2\DC2acyclic.objects.v2\SUB\USgoogle/protobuf/timestamp.proto\"\US\n\
    \\tBucketRef\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\"z\n\
    \\ACKBucket\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC29\n\
    \\n\
    \created_at\CAN\STX \SOH(\v2\SUB.google.protobuf.TimestampR\tcreatedAt\"\170\ETX\n\
    \\SOObjectMetadata\DC2!\n\
    \\fcontent_type\CAN\SOH \SOH(\tR\vcontentType\DC2@\n\
    \\EOTuser\CAN\STX \ETX(\v2,.acyclic.objects.v2.ObjectMetadata.UserEntryR\EOTuser\DC2)\n\
    \\DLEcontent_encoding\CAN\ETX \SOH(\tR\SIcontentEncoding\DC2#\n\
    \\rcache_control\CAN\EOT \SOH(\tR\fcacheControl\DC2/\n\
    \\DC3content_disposition\CAN\ENQ \SOH(\tR\DC2contentDisposition\DC2)\n\
    \\DLEcontent_language\CAN\ACK \SOH(\tR\SIcontentLanguage\DC25\n\
    \\DC4expires_unix_seconds\CAN\a \SOH(\ETXH\NULR\DC2expiresUnixSeconds\136\SOH\SOH\SUB7\n\
    \\tUserEntry\DC2\DLE\n\
    \\ETXkey\CAN\SOH \SOH(\tR\ETXkey\DC2\DC4\n\
    \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue:\STX8\SOHB\ETB\n\
    \\NAK_expires_unix_seconds\"X\n\
    \\rPreconditions\DC2\GS\n\
    \\tif_absent\CAN\SOH \SOH(\bH\NULR\bifAbsent\DC2\ESC\n\
    \\bif_match\CAN\STX \SOH(\tH\NULR\aifMatchB\v\n\
    \\tcondition\";\n\
    \\DLEMutationIdentity\DC2'\n\
    \\SIidempotency_key\CAN\SOH \SOH(\tR\SOidempotencyKey\"\181\SOH\n\
    \\n\
    \ObjectInfo\DC2\DC2\n\
    \\EOTetag\CAN\SOH \SOH(\tR\EOTetag\DC2\DC2\n\
    \\EOTsize\CAN\STX \SOH(\EOTR\EOTsize\DC2>\n\
    \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v2.ObjectMetadataR\bmetadata\DC2?\n\
    \\rlast_modified\CAN\EOT \SOH(\v2\SUB.google.protobuf.TimestampR\flastModified\"k\n\
    \\DC3CreateBucketRequest\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2@\n\
    \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"J\n\
    \\DC1HeadBucketRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\"\142\SOH\n\
    \\DC3DeleteBucketRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2@\n\
    \\bmutation\CAN\STX \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"0\n\
    \\DC4DeleteBucketResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\"\178\STX\n\
    \\SIPutObjectHeader\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
    \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v2.ObjectMetadataR\bmetadata\DC2G\n\
    \\rpreconditions\CAN\EOT \SOH(\v2!.acyclic.objects.v2.PreconditionsR\rpreconditions\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"\142\SOH\n\
    \\DLEPutObjectRequest\DC2=\n\
    \\ACKheader\CAN\SOH \SOH(\v2#.acyclic.objects.v2.PutObjectHeaderH\NULR\ACKheader\DC2\DC4\n\
    \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbody\DC2\FS\n\
    \\bcomplete\CAN\ETX \SOH(\bH\NULR\bcompleteB\a\n\
    \\ENQframe\"E\n\
    \\SOInclusiveRange\DC2\DC4\n\
    \\ENQstart\CAN\SOH \SOH(\EOTR\ENQstart\DC2\NAK\n\
    \\ETXend\CAN\STX \SOH(\EOTH\NULR\ETXend\136\SOH\SOHB\ACK\n\
    \\EOT_end\"{\n\
    \\tByteRange\DC2:\n\
    \\ENQbytes\CAN\SOH \SOH(\v2\".acyclic.objects.v2.InclusiveRangeH\NULR\ENQbytes\DC2%\n\
    \\rsuffix_length\CAN\STX \SOH(\EOTH\NULR\fsuffixLengthB\v\n\
    \\tselection\"\220\SOH\n\
    \\DLEGetObjectRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC23\n\
    \\ENQrange\CAN\ETX \SOH(\v2\GS.acyclic.objects.v2.ByteRangeR\ENQrange\DC2\EM\n\
    \\bif_match\CAN\EOT \SOH(\tR\aifMatch\DC2\"\n\
    \\rif_none_match\CAN\ENQ \SOH(\tR\vifNoneMatch\"L\n\
    \\fContentRange\DC2\DC4\n\
    \\ENQstart\CAN\SOH \SOH(\EOTR\ENQstart\DC2\DLE\n\
    \\ETXend\CAN\STX \SOH(\EOTR\ETXend\DC2\DC4\n\
    \\ENQtotal\CAN\ETX \SOH(\EOTR\ENQtotal\"\144\SOH\n\
    \\SIGetObjectHeader\DC26\n\
    \\ACKobject\CAN\SOH \SOH(\v2\RS.acyclic.objects.v2.ObjectInfoR\ACKobject\DC2E\n\
    \\rcontent_range\CAN\STX \SOH(\v2 .acyclic.objects.v2.ContentRangeR\fcontentRange\"\170\SOH\n\
    \\DC1GetObjectResponse\DC2=\n\
    \\ACKheader\CAN\SOH \SOH(\v2#.acyclic.objects.v2.GetObjectHeaderH\NULR\ACKheader\DC2\DC4\n\
    \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbody\DC27\n\
    \\ENQerror\CAN\ETX \SOH(\v2\US.acyclic.objects.v2.ErrorDetailH\NULR\ENQerrorB\a\n\
    \\ENQframe\"\168\SOH\n\
    \\DC1HeadObjectRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\EM\n\
    \\bif_match\CAN\ETX \SOH(\tR\aifMatch\DC2\"\n\
    \\rif_none_match\CAN\EOT \SOH(\tR\vifNoneMatch\"L\n\
    \\DC2HeadObjectResponse\DC26\n\
    \\ACKobject\CAN\SOH \SOH(\v2\RS.acyclic.objects.v2.ObjectInfoR\ACKobject\"\246\SOH\n\
    \\DC3DeleteObjectRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2G\n\
    \\rpreconditions\CAN\ETX \SOH(\v2!.acyclic.objects.v2.PreconditionsR\rpreconditions\DC2@\n\
    \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"0\n\
    \\DC4DeleteObjectResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\"\205\SOH\n\
    \\DC2ListObjectsRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\SYN\n\
    \\ACKprefix\CAN\STX \SOH(\tR\ACKprefix\DC2\FS\n\
    \\tdelimiter\CAN\ETX \SOH(\tR\tdelimiter\DC2\ESC\n\
    \\tpage_size\CAN\EOT \SOH(\rR\bpageSize\DC2-\n\
    \\DC2continuation_token\CAN\ENQ \SOH(\tR\DC1continuationToken\"b\n\
    \\tListEntry\DC2\GS\n\
    \\n\
    \object_key\CAN\SOH \SOH(\tR\tobjectKey\DC26\n\
    \\ACKobject\CAN\STX \SOH(\v2\RS.acyclic.objects.v2.ObjectInfoR\ACKobject\"\201\SOH\n\
    \\DC3ListObjectsResponse\DC27\n\
    \\aentries\CAN\SOH \ETX(\v2\GS.acyclic.objects.v2.ListEntryR\aentries\DC2'\n\
    \\SIcommon_prefixes\CAN\STX \ETX(\tR\SOcommonPrefixes\DC2-\n\
    \\DC2continuation_token\CAN\ETX \SOH(\tR\DC1continuationToken\DC2!\n\
    \\fis_truncated\CAN\EOT \SOH(\bR\visTruncated\"\240\SOH\n\
    \\SYNCreateMultipartRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2>\n\
    \\bmetadata\CAN\ETX \SOH(\v2\".acyclic.objects.v2.ObjectMetadataR\bmetadata\DC2@\n\
    \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\".\n\
    \\SIMultipartUpload\DC2\ESC\n\
    \\tupload_id\CAN\SOH \SOH(\tR\buploadId\"\232\SOH\n\
    \\DLEUploadPartHeader\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2\US\n\
    \\vpart_number\CAN\EOT \SOH(\rR\n\
    \partNumber\DC2@\n\
    \\bmutation\CAN\ENQ \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"\144\SOH\n\
    \\DC1UploadPartRequest\DC2>\n\
    \\ACKheader\CAN\SOH \SOH(\v2$.acyclic.objects.v2.UploadPartHeaderH\NULR\ACKheader\DC2\DC4\n\
    \\EOTbody\CAN\STX \SOH(\fH\NULR\EOTbody\DC2\FS\n\
    \\bcomplete\CAN\ETX \SOH(\bH\NULR\bcompleteB\a\n\
    \\ENQframe\"W\n\
    \\fUploadedPart\DC2\US\n\
    \\vpart_number\CAN\SOH \SOH(\rR\n\
    \partNumber\DC2\DC2\n\
    \\EOTetag\CAN\STX \SOH(\tR\EOTetag\DC2\DC2\n\
    \\EOTsize\CAN\ETX \SOH(\EOTR\EOTsize\"\206\SOH\n\
    \\DLEListPartsRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2*\n\
    \\DC1after_part_number\CAN\EOT \SOH(\rR\SIafterPartNumber\DC2\ESC\n\
    \\tpage_size\CAN\ENQ \SOH(\rR\bpageSize\"\152\SOH\n\
    \\DC1ListPartsResponse\DC26\n\
    \\ENQparts\CAN\SOH \ETX(\v2 .acyclic.objects.v2.UploadedPartR\ENQparts\DC2(\n\
    \\DLEnext_part_number\CAN\STX \SOH(\rR\SOnextPartNumber\DC2!\n\
    \\fis_truncated\CAN\ETX \SOH(\bR\visTruncated\"\208\STX\n\
    \\CANCompleteMultipartRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC26\n\
    \\ENQparts\CAN\EOT \ETX(\v2 .acyclic.objects.v2.UploadedPartR\ENQparts\DC2G\n\
    \\rpreconditions\CAN\ENQ \SOH(\v2!.acyclic.objects.v2.PreconditionsR\rpreconditions\DC2@\n\
    \\bmutation\CAN\ACK \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"\204\SOH\n\
    \\NAKAbortMultipartRequest\DC25\n\
    \\ACKbucket\CAN\SOH \SOH(\v2\GS.acyclic.objects.v2.BucketRefR\ACKbucket\DC2\GS\n\
    \\n\
    \object_key\CAN\STX \SOH(\tR\tobjectKey\DC2\ESC\n\
    \\tupload_id\CAN\ETX \SOH(\tR\buploadId\DC2@\n\
    \\bmutation\CAN\EOT \SOH(\v2$.acyclic.objects.v2.MutationIdentityR\bmutation\"2\n\
    \\SYNAbortMultipartResponse\DC2\CAN\n\
    \\aexisted\CAN\SOH \SOH(\bR\aexisted\"_\n\
    \\vErrorDetail\DC21\n\
    \\EOTcode\CAN\SOH \SOH(\SO2\GS.acyclic.objects.v2.ErrorCodeR\EOTcode\DC2\GS\n\
    \\n\
    \request_id\CAN\STX \SOH(\tR\trequestId*\160\STX\n\
    \\fObjectsLimit\DC2\GS\n\
    \\EMOBJECTS_LIMIT_UNSPECIFIED\DLE\NUL\DC2,\n\
    \'OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES\DLE\128\STX\DC2 \n\
    \\ESCOBJECTS_LIMIT_MAX_KEY_BYTES\DLE\128\b\DC2*\n\
    \%OBJECTS_LIMIT_MAX_USER_METADATA_BYTES\DLE\128\DLE\DC2#\n\
    \\RSOBJECTS_LIMIT_MAX_PAGE_ENTRIES\DLE\232\a\DC2(\n\
    \\"OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES\DLE\128\128\EOT\DC2&\n\
    \!OBJECTS_LIMIT_MAX_MULTIPART_PARTS\DLE\144N*\130\ETX\n\
    \\tErrorCode\DC2\SUB\n\
    \\SYNERROR_CODE_UNSPECIFIED\DLE\NUL\DC2\US\n\
    \\ESCERROR_CODE_INVALID_ARGUMENT\DLE\SOH\DC2\CAN\n\
    \\DC4ERROR_CODE_NOT_FOUND\DLE\STX\DC2\GS\n\
    \\EMERROR_CODE_ALREADY_EXISTS\DLE\ETX\DC2\"\n\
    \\RSERROR_CODE_PRECONDITION_FAILED\DLE\EOT\DC2#\n\
    \\USERROR_CODE_IDEMPOTENCY_MISMATCH\DLE\ENQ\DC2\GS\n\
    \\EMERROR_CODE_QUOTA_EXCEEDED\DLE\ACK\DC2\SUB\n\
    \\SYNERROR_CODE_UNSUPPORTED\DLE\a\DC2\SUB\n\
    \\SYNERROR_CODE_UNAVAILABLE\DLE\b\DC2\FS\n\
    \\CANERROR_CODE_ACCESS_DENIED\DLE\t\DC2$\n\
    \ ERROR_CODE_RANGE_NOT_SATISFIABLE\DLE\n\
    \\DC2\ESC\n\
    \\ETBERROR_CODE_NOT_MODIFIED\DLE\v2\153\STX\n\
    \\SOBucketsService\DC2S\n\
    \\fCreateBucket\DC2'.acyclic.objects.v2.CreateBucketRequest\SUB\SUB.acyclic.objects.v2.Bucket\DC2O\n\
    \\n\
    \HeadBucket\DC2%.acyclic.objects.v2.HeadBucketRequest\SUB\SUB.acyclic.objects.v2.Bucket\DC2a\n\
    \\fDeleteBucket\DC2'.acyclic.objects.v2.DeleteBucketRequest\SUB(.acyclic.objects.v2.DeleteBucketResponse2\225\ETX\n\
    \\SOObjectsService\DC2S\n\
    \\tPutObject\DC2$.acyclic.objects.v2.PutObjectRequest\SUB\RS.acyclic.objects.v2.ObjectInfo(\SOH\DC2Z\n\
    \\tGetObject\DC2$.acyclic.objects.v2.GetObjectRequest\SUB%.acyclic.objects.v2.GetObjectResponse0\SOH\DC2[\n\
    \\n\
    \HeadObject\DC2%.acyclic.objects.v2.HeadObjectRequest\SUB&.acyclic.objects.v2.HeadObjectResponse\DC2a\n\
    \\fDeleteObject\DC2'.acyclic.objects.v2.DeleteObjectRequest\SUB(.acyclic.objects.v2.DeleteObjectResponse\DC2^\n\
    \\vListObjects\DC2&.acyclic.objects.v2.ListObjectsRequest\SUB'.acyclic.objects.v2.ListObjectsResponse2\245\ETX\n\
    \\DLEMultipartService\DC2b\n\
    \\SICreateMultipart\DC2*.acyclic.objects.v2.CreateMultipartRequest\SUB#.acyclic.objects.v2.MultipartUpload\DC2W\n\
    \\n\
    \UploadPart\DC2%.acyclic.objects.v2.UploadPartRequest\SUB .acyclic.objects.v2.UploadedPart(\SOH\DC2X\n\
    \\tListParts\DC2$.acyclic.objects.v2.ListPartsRequest\SUB%.acyclic.objects.v2.ListPartsResponse\DC2a\n\
    \\DC1CompleteMultipart\DC2,.acyclic.objects.v2.CompleteMultipartRequest\SUB\RS.acyclic.objects.v2.ObjectInfo\DC2g\n\
    \\SOAbortMultipart\DC2).acyclic.objects.v2.AbortMultipartRequest\SUB*.acyclic.objects.v2.AbortMultipartResponseB9Z7github.com/acyclic-labs/sdk/go/gen/objects/v2;objectsv2J\186O\n\
    \\a\DC2\ENQ\NUL\NUL\253\SOH\SOH\n\
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
    \^\n\
    \\STX\EOT\NUL\DC2\EOT\t\NUL\v\SOH\SUBR Tenant-scoped logical bucket name. Placement and storage identities are private.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\t\b\DC1\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\n\
    \\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\n\
    \\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\n\
    \\t\r\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\n\
    \\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\f\NUL\SI\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\f\b\SO\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\r\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ACK\DC2\ETX\r\STX\v\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\r\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\r\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\SOH\DC2\ETX\SO\STX+\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ACK\DC2\ETX\SO\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\SOH\DC2\ETX\SO\FS&\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ETX\DC2\ETX\SO)*\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\DLE\NUL\CAN\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\DLE\b\SYN\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\DC1\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ENQ\DC2\ETX\DC1\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\DC1\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\DC1\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\STX\STX\SOH\DC2\ETX\DC2\STX\US\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ACK\DC2\ETX\DC2\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\SOH\DC2\ETX\DC2\SYN\SUB\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ETX\DC2\ETX\DC2\GS\RS\n\
    \\v\n\
    \\EOT\EOT\STX\STX\STX\DC2\ETX\DC3\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ENQ\DC2\ETX\DC3\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\SOH\DC2\ETX\DC3\t\EM\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ETX\DC2\ETX\DC3\FS\GS\n\
    \\v\n\
    \\EOT\EOT\STX\STX\ETX\DC2\ETX\DC4\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\ENQ\DC2\ETX\DC4\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\SOH\DC2\ETX\DC4\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\ETX\DC2\ETX\DC4\EM\SUB\n\
    \\v\n\
    \\EOT\EOT\STX\STX\EOT\DC2\ETX\NAK\STX!\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\ENQ\DC2\ETX\NAK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\SOH\DC2\ETX\NAK\t\FS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\ETX\DC2\ETX\NAK\US \n\
    \\v\n\
    \\EOT\EOT\STX\STX\ENQ\DC2\ETX\SYN\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\ENQ\DC2\ETX\SYN\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\SOH\DC2\ETX\SYN\t\EM\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\ETX\DC2\ETX\SYN\FS\GS\n\
    \\v\n\
    \\EOT\EOT\STX\STX\ACK\DC2\ETX\ETB\STX*\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\EOT\DC2\ETX\ETB\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\ENQ\DC2\ETX\ETB\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\SOH\DC2\ETX\ETB\DC1%\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\ETX\DC2\ETX\ETB()\n\
    \\167\SOH\n\
    \\STX\EOT\ETX\DC2\EOT\ESC\NUL \SOH\SUB\154\SOH Evaluated atomically with single-object publication against the current value.\n\
    \ An eventual HEAD/GET may be stale and cause a safe precondition failure.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX\ESC\b\NAK\n\
    \\f\n\
    \\EOT\EOT\ETX\b\NUL\DC2\EOT\FS\STX\US\ETX\n\
    \\f\n\
    \\ENQ\EOT\ETX\b\NUL\SOH\DC2\ETX\FS\b\DC1\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX\GS\EOT\ETB\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ENQ\DC2\ETX\GS\EOT\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX\GS\t\DC2\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX\GS\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\SOH\DC2\ETX\RS\EOT\CAN\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ENQ\DC2\ETX\RS\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\SOH\DC2\ETX\RS\v\DC3\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ETX\DC2\ETX\RS\SYN\ETB\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT!\NUL#\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX!\b\CAN\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX\"\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ENQ\DC2\ETX\"\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX\"\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX\"\ESC\FS\n\
    \\n\
    \\n\
    \\STX\ENQ\NUL\DC2\EOT$\NUL,\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETX$\ENQ\DC1\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETX%\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETX%\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETX%\RS\US\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETX&\STX0\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETX&\STX)\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETX&,/\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\STX\DC2\ETX'\STX%\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\SOH\DC2\ETX'\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\STX\DC2\ETX' $\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ETX\DC2\ETX(\STX/\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\SOH\DC2\ETX(\STX'\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\STX\DC2\ETX(*.\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\EOT\DC2\ETX)\STX(\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\SOH\DC2\ETX)\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\STX\DC2\ETX)#'\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ENQ\DC2\ETX*\STX-\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ENQ\SOH\DC2\ETX*\STX$\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ENQ\STX\DC2\ETX*',\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ACK\DC2\ETX+\STX,\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ACK\SOH\DC2\ETX+\STX#\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ACK\STX\DC2\ETX+&+\n\
    \d\n\
    \\STX\EOT\ENQ\DC2\EOT.\NUL3\SOH\SUBX One complete current representation, with an opaque ETag rather than history identity.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETX.\b\DC2\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETX/\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ENQ\DC2\ETX/\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETX/\t\r\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETX/\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\SOH\DC2\ETX0\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ENQ\DC2\ETX0\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\SOH\DC2\ETX0\t\r\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ETX\DC2\ETX0\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\STX\DC2\ETX1\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\ACK\DC2\ETX1\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\SOH\DC2\ETX1\DC1\EM\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\ETX\DC2\ETX1\FS\GS\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\ETX\DC2\ETX2\STX.\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\ACK\DC2\ETX2\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\SOH\DC2\ETX2\FS)\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\ETX\DC2\ETX2,-\n\
    \\n\
    \\n\
    \\STX\EOT\ACK\DC2\EOT4\NUL7\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETX4\b\ESC\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX5\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ENQ\DC2\ETX5\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX5\t\r\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX5\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\SOH\DC2\ETX6\STX \n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ACK\DC2\ETX6\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\SOH\DC2\ETX6\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ETX\DC2\ETX6\RS\US\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOT8\NUL:\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETX8\b\EM\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETX9\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ACK\DC2\ETX9\STX\v\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETX9\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETX9\NAK\SYN\n\
    \\n\
    \\n\
    \\STX\EOT\b\DC2\EOT;\NUL>\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETX;\b\ESC\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETX<\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ACK\DC2\ETX<\STX\v\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETX<\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETX<\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\b\STX\SOH\DC2\ETX=\STX \n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ACK\DC2\ETX=\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\SOH\DC2\ETX=\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ETX\DC2\ETX=\RS\US\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOT?\NULA\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETX?\b\FS\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETX@\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ENQ\DC2\ETX@\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETX@\a\SO\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETX@\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\ACK\NUL\DC2\EOTB\NULF\SOH\n\
    \\n\
    \\n\
    \\ETX\ACK\NUL\SOH\DC2\ETXB\b\SYN\n\
    \\v\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\ETXC\STX9\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\ETXC\ACK\DC2\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\ETXC\DC3&\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\ETXC17\n\
    \\v\n\
    \\EOT\ACK\NUL\STX\SOH\DC2\ETXD\STX5\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\SOH\SOH\DC2\ETXD\ACK\DLE\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\SOH\STX\DC2\ETXD\DC1\"\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\SOH\ETX\DC2\ETXD-3\n\
    \\v\n\
    \\EOT\ACK\NUL\STX\STX\DC2\ETXE\STXG\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\STX\SOH\DC2\ETXE\ACK\DC2\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\STX\STX\DC2\ETXE\DC3&\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\STX\ETX\DC2\ETXE1E\n\
    \\n\
    \\n\
    \\STX\EOT\n\
    \\DC2\EOTH\NULN\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\n\
    \\SOH\DC2\ETXH\b\ETB\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\ETXI\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ACK\DC2\ETXI\STX\v\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\ETXI\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\ETXI\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\SOH\DC2\ETXJ\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ENQ\DC2\ETXJ\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\SOH\DC2\ETXJ\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ETX\DC2\ETXJ\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\STX\DC2\ETXK\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\STX\ACK\DC2\ETXK\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\STX\SOH\DC2\ETXK\DC1\EM\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\STX\ETX\DC2\ETXK\FS\GS\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\ETX\DC2\ETXL\STX\"\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\ETX\ACK\DC2\ETXL\STX\SI\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\ETX\SOH\DC2\ETXL\DLE\GS\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\ETX\ETX\DC2\ETXL !\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\EOT\DC2\ETXM\STX \n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\EOT\ACK\DC2\ETXM\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\EOT\SOH\DC2\ETXM\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\EOT\ETX\DC2\ETXM\RS\US\n\
    \\175\SOH\n\
    \\STX\EOT\v\DC2\EOTQ\NULW\SOH\SUB\162\SOH Header first, bounded body frames, then complete=true and EOF. Missing completion\n\
    \ never publishes bytes, even if transport cancellation appears as a clean EOF.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\v\SOH\DC2\ETXQ\b\CAN\n\
    \\f\n\
    \\EOT\EOT\v\b\NUL\DC2\EOTR\STXV\ETX\n\
    \\f\n\
    \\ENQ\EOT\v\b\NUL\SOH\DC2\ETXR\b\r\n\
    \\v\n\
    \\EOT\EOT\v\STX\NUL\DC2\ETXS\EOT\US\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ACK\DC2\ETXS\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\ETXS\DC4\SUB\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\ETXS\GS\RS\n\
    \\v\n\
    \\EOT\EOT\v\STX\SOH\DC2\ETXT\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ENQ\DC2\ETXT\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\SOH\DC2\ETXT\n\
    \\SO\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ETX\DC2\ETXT\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\v\STX\STX\DC2\ETXU\EOT\SYN\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\ENQ\DC2\ETXU\EOT\b\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\SOH\DC2\ETXU\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\ETX\DC2\ETXU\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\f\DC2\EOTX\NUL[\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\f\SOH\DC2\ETXX\b\SYN\n\
    \\v\n\
    \\EOT\EOT\f\STX\NUL\DC2\ETXY\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ENQ\DC2\ETXY\STX\b\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\ETXY\t\SO\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\ETXY\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\f\STX\SOH\DC2\ETXZ\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\EOT\DC2\ETXZ\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ENQ\DC2\ETXZ\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\ETXZ\DC2\NAK\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\ETXZ\CAN\EM\n\
    \\n\
    \\n\
    \\STX\EOT\r\DC2\EOT\\\NULa\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\r\SOH\DC2\ETX\\\b\DC1\n\
    \\f\n\
    \\EOT\EOT\r\b\NUL\DC2\EOT]\STX`\ETX\n\
    \\f\n\
    \\ENQ\EOT\r\b\NUL\SOH\DC2\ETX]\b\DC1\n\
    \\v\n\
    \\EOT\EOT\r\STX\NUL\DC2\ETX^\EOT\GS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ACK\DC2\ETX^\EOT\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\ETX^\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\ETX^\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\r\STX\SOH\DC2\ETX_\EOT\GS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\ENQ\DC2\ETX_\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\SOH\DC2\ETX_\v\CAN\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\ETX\DC2\ETX_\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\SO\DC2\EOTb\NULh\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SO\SOH\DC2\ETXb\b\CAN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\NUL\DC2\ETXc\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ACK\DC2\ETXc\STX\v\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\ETXc\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\ETXc\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\SOH\DC2\ETXd\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ENQ\DC2\ETXd\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\SOH\DC2\ETXd\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ETX\DC2\ETXd\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\SO\STX\STX\DC2\ETXe\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ACK\DC2\ETXe\STX\v\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\SOH\DC2\ETXe\f\DC1\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ETX\DC2\ETXe\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ETX\DC2\ETXf\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\ENQ\DC2\ETXf\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\SOH\DC2\ETXf\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\ETX\DC2\ETXf\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\SO\STX\EOT\DC2\ETXg\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\ENQ\DC2\ETXg\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\SOH\DC2\ETXg\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\ETX\DC2\ETXg\EM\SUB\n\
    \\n\
    \\n\
    \\STX\EOT\SI\DC2\EOTi\NULm\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SI\SOH\DC2\ETXi\b\DC4\n\
    \\v\n\
    \\EOT\EOT\SI\STX\NUL\DC2\ETXj\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ENQ\DC2\ETXj\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\ETXj\t\SO\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\ETXj\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\SI\STX\SOH\DC2\ETXk\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ENQ\DC2\ETXk\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\SOH\DC2\ETXk\t\f\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ETX\DC2\ETXk\SI\DLE\n\
    \\v\n\
    \\EOT\EOT\SI\STX\STX\DC2\ETXl\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\ENQ\DC2\ETXl\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\SOH\DC2\ETXl\t\SO\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\ETX\DC2\ETXl\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\EOT\DLE\DC2\EOTn\NULq\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DLE\SOH\DC2\ETXn\b\ETB\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\ETXo\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ACK\DC2\ETXo\STX\f\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\ETXo\r\DC3\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\ETXo\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\SOH\DC2\ETXp\STX!\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\ACK\DC2\ETXp\STX\SO\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\SOH\DC2\ETXp\SI\FS\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\ETX\DC2\ETXp\US \n\
    \\185\SOH\n\
    \\STX\EOT\DC1\DC2\EOTt\NULz\SOH\SUB\172\SOH Exactly one metadata header first. The body belongs to that complete representation.\n\
    \ A terminal semantic error can follow the header when an HTTP stream is already open.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\DC1\SOH\DC2\ETXt\b\EM\n\
    \\f\n\
    \\EOT\EOT\DC1\b\NUL\DC2\EOTu\STXy\ETX\n\
    \\f\n\
    \\ENQ\EOT\DC1\b\NUL\SOH\DC2\ETXu\b\r\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\ETXv\EOT\US\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ACK\DC2\ETXv\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\ETXv\DC4\SUB\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\ETXv\GS\RS\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\SOH\DC2\ETXw\EOT\DC3\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\ENQ\DC2\ETXw\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\SOH\DC2\ETXw\n\
    \\SO\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\ETX\DC2\ETXw\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\STX\DC2\ETXx\EOT\SUB\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\STX\ACK\DC2\ETXx\EOT\SI\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\STX\SOH\DC2\ETXx\DLE\NAK\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\STX\ETX\DC2\ETXx\CAN\EM\n\
    \\v\n\
    \\STX\EOT\DC2\DC2\ENQ{\NUL\128\SOH\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC2\SOH\DC2\ETX{\b\EM\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\ETX|\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ACK\DC2\ETX|\STX\v\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\ETX|\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\ETX|\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\SOH\DC2\ETX}\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\SOH\ENQ\DC2\ETX}\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\SOH\SOH\DC2\ETX}\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\SOH\ETX\DC2\ETX}\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\STX\DC2\ETX~\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\STX\ENQ\DC2\ETX~\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\STX\SOH\DC2\ETX~\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\STX\ETX\DC2\ETX~\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\ETX\DC2\ETX\DEL\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\ETX\ENQ\DC2\ETX\DEL\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\ETX\SOH\DC2\ETX\DEL\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\ETX\ETX\DC2\ETX\DEL\EM\SUB\n\
    \\f\n\
    \\STX\EOT\DC3\DC2\ACK\129\SOH\NUL\131\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC3\SOH\DC2\EOT\129\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\EOT\130\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ACK\DC2\EOT\130\SOH\STX\f\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\EOT\130\SOH\r\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\EOT\130\SOH\SYN\ETB\n\
    \\f\n\
    \\STX\EOT\DC4\DC2\ACK\132\SOH\NUL\137\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC4\SOH\DC2\EOT\132\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\EOT\133\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ACK\DC2\EOT\133\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\EOT\133\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\EOT\133\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\EOT\134\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ENQ\DC2\EOT\134\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\EOT\134\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\EOT\134\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\STX\DC2\EOT\135\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\STX\ACK\DC2\EOT\135\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\STX\SOH\DC2\EOT\135\SOH\DLE\GS\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\STX\ETX\DC2\EOT\135\SOH !\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\ETX\DC2\EOT\136\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\ACK\DC2\EOT\136\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\SOH\DC2\EOT\136\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\ETX\DC2\EOT\136\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\NAK\DC2\ACK\138\SOH\NUL\140\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\138\SOH\b\FS\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\139\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ENQ\DC2\EOT\139\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\139\SOH\a\SO\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\139\SOH\DC1\DC2\n\
    \\195\SOH\n\
    \\STX\EOT\SYN\DC2\ACK\143\SOH\NUL\149\SOH\SOH\SUB\180\SOH Lexicographic S3 ListObjectsV2 traversal of an eventual listing, never a captured view.\n\
    \ Concurrent insertions before the cursor can be missed. Tokens are opaque and query-bound.\n\
    \\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\143\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\144\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ACK\DC2\EOT\144\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\144\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\144\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\145\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ENQ\DC2\EOT\145\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\145\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\145\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\STX\DC2\EOT\146\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ENQ\DC2\EOT\146\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\SOH\DC2\EOT\146\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ETX\DC2\EOT\146\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\ETX\DC2\EOT\147\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\ETX\ENQ\DC2\EOT\147\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\ETX\SOH\DC2\EOT\147\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\ETX\ETX\DC2\EOT\147\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\EOT\DC2\EOT\148\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\ENQ\DC2\EOT\148\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\SOH\DC2\EOT\148\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\ETX\DC2\EOT\148\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\150\SOH\NUL\153\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\150\SOH\b\DC1\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\151\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ENQ\DC2\EOT\151\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\151\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\151\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\152\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ACK\DC2\EOT\152\SOH\STX\f\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\152\SOH\r\DC3\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\152\SOH\SYN\ETB\n\
    \\f\n\
    \\STX\EOT\CAN\DC2\ACK\154\SOH\NUL\159\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\CAN\SOH\DC2\EOT\154\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\NUL\DC2\EOT\155\SOH\STX!\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\EOT\DC2\EOT\155\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ACK\DC2\EOT\155\SOH\v\DC4\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\SOH\DC2\EOT\155\SOH\NAK\FS\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ETX\DC2\EOT\155\SOH\US \n\
    \\f\n\
    \\EOT\EOT\CAN\STX\SOH\DC2\EOT\156\SOH\STX&\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\EOT\DC2\EOT\156\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ENQ\DC2\EOT\156\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\SOH\DC2\EOT\156\SOH\DC2!\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ETX\DC2\EOT\156\SOH$%\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\STX\DC2\EOT\157\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\ENQ\DC2\EOT\157\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\SOH\DC2\EOT\157\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\ETX\DC2\EOT\157\SOH\RS\US\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\ETX\DC2\EOT\158\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\ENQ\DC2\EOT\158\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\SOH\DC2\EOT\158\SOH\a\DC3\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\ETX\DC2\EOT\158\SOH\SYN\ETB\n\
    \\f\n\
    \\STX\ACK\SOH\DC2\ACK\160\SOH\NUL\166\SOH\SOH\n\
    \\v\n\
    \\ETX\ACK\SOH\SOH\DC2\EOT\160\SOH\b\SYN\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\NUL\DC2\EOT\161\SOH\STX>\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\SOH\DC2\EOT\161\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\ENQ\DC2\EOT\161\SOH\DLE\SYN\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\STX\DC2\EOT\161\SOH\ETB'\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\NUL\ETX\DC2\EOT\161\SOH2<\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\SOH\DC2\EOT\162\SOH\STXE\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\SOH\DC2\EOT\162\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\STX\DC2\EOT\162\SOH\DLE \n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\ACK\DC2\EOT\162\SOH+1\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\SOH\ETX\DC2\EOT\162\SOH2C\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\STX\DC2\EOT\163\SOH\STXA\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\STX\SOH\DC2\EOT\163\SOH\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\STX\STX\DC2\EOT\163\SOH\DC1\"\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\STX\ETX\DC2\EOT\163\SOH-?\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\ETX\DC2\EOT\164\SOH\STXG\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\ETX\SOH\DC2\EOT\164\SOH\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\ETX\STX\DC2\EOT\164\SOH\DC3&\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\ETX\ETX\DC2\EOT\164\SOH1E\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\EOT\DC2\EOT\165\SOH\STXD\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\EOT\SOH\DC2\EOT\165\SOH\ACK\DC1\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\EOT\STX\DC2\EOT\165\SOH\DC2$\n\
    \\r\n\
    \\ENQ\ACK\SOH\STX\EOT\ETX\DC2\EOT\165\SOH/B\n\
    \\f\n\
    \\STX\EOT\EM\DC2\ACK\168\SOH\NUL\173\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\EM\SOH\DC2\EOT\168\SOH\b\RS\n\
    \\f\n\
    \\EOT\EOT\EM\STX\NUL\DC2\EOT\169\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ACK\DC2\EOT\169\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\SOH\DC2\EOT\169\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ETX\DC2\EOT\169\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\SOH\DC2\EOT\170\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ENQ\DC2\EOT\170\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\SOH\DC2\EOT\170\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ETX\DC2\EOT\170\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\EM\STX\STX\DC2\EOT\171\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ACK\DC2\EOT\171\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\SOH\DC2\EOT\171\SOH\DC1\EM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ETX\DC2\EOT\171\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\EM\STX\ETX\DC2\EOT\172\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ACK\DC2\EOT\172\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\SOH\DC2\EOT\172\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ETX\DC2\EOT\172\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\SUB\DC2\ACK\174\SOH\NUL\176\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SUB\SOH\DC2\EOT\174\SOH\b\ETB\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\NUL\DC2\EOT\175\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ENQ\DC2\EOT\175\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\SOH\DC2\EOT\175\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ETX\DC2\EOT\175\SOH\NAK\SYN\n\
    \\f\n\
    \\STX\EOT\ESC\DC2\ACK\177\SOH\NUL\183\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ESC\SOH\DC2\EOT\177\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\NUL\DC2\EOT\178\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ACK\DC2\EOT\178\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\SOH\DC2\EOT\178\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ETX\DC2\EOT\178\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\SOH\DC2\EOT\179\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ENQ\DC2\EOT\179\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\SOH\DC2\EOT\179\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ETX\DC2\EOT\179\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\STX\DC2\EOT\180\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ENQ\DC2\EOT\180\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\SOH\DC2\EOT\180\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ETX\DC2\EOT\180\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\ETX\DC2\EOT\181\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\ENQ\DC2\EOT\181\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\SOH\DC2\EOT\181\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\ETX\DC2\EOT\181\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\EOT\DC2\EOT\182\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\ACK\DC2\EOT\182\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\SOH\DC2\EOT\182\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\ETX\DC2\EOT\182\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\FS\DC2\ACK\184\SOH\NUL\191\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\FS\SOH\DC2\EOT\184\SOH\b\EM\n\
    \T\n\
    \\EOT\EOT\FS\b\NUL\DC2\ACK\186\SOH\STX\190\SOH\ETX\SUBD Same header/body/complete=true/EOF discipline as PutObjectRequest.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\FS\b\NUL\SOH\DC2\EOT\186\SOH\b\r\n\
    \\f\n\
    \\EOT\EOT\FS\STX\NUL\DC2\EOT\187\SOH\EOT \n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ACK\DC2\EOT\187\SOH\EOT\DC4\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\SOH\DC2\EOT\187\SOH\NAK\ESC\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ETX\DC2\EOT\187\SOH\RS\US\n\
    \\f\n\
    \\EOT\EOT\FS\STX\SOH\DC2\EOT\188\SOH\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ENQ\DC2\EOT\188\SOH\EOT\t\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\SOH\DC2\EOT\188\SOH\n\
    \\SO\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ETX\DC2\EOT\188\SOH\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT\FS\STX\STX\DC2\EOT\189\SOH\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ENQ\DC2\EOT\189\SOH\EOT\b\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\SOH\DC2\EOT\189\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ETX\DC2\EOT\189\SOH\DC4\NAK\n\
    \\f\n\
    \\STX\EOT\GS\DC2\ACK\192\SOH\NUL\196\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\GS\SOH\DC2\EOT\192\SOH\b\DC4\n\
    \\f\n\
    \\EOT\EOT\GS\STX\NUL\DC2\EOT\193\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ENQ\DC2\EOT\193\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\SOH\DC2\EOT\193\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ETX\DC2\EOT\193\SOH\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT\GS\STX\SOH\DC2\EOT\194\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\SOH\ENQ\DC2\EOT\194\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\SOH\SOH\DC2\EOT\194\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\SOH\ETX\DC2\EOT\194\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\GS\STX\STX\DC2\EOT\195\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\STX\ENQ\DC2\EOT\195\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\STX\SOH\DC2\EOT\195\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\STX\ETX\DC2\EOT\195\SOH\DLE\DC1\n\
    \\f\n\
    \\STX\EOT\RS\DC2\ACK\197\SOH\NUL\203\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\RS\SOH\DC2\EOT\197\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\RS\STX\NUL\DC2\EOT\198\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ACK\DC2\EOT\198\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\SOH\DC2\EOT\198\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ETX\DC2\EOT\198\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\RS\STX\SOH\DC2\EOT\199\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ENQ\DC2\EOT\199\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\SOH\DC2\EOT\199\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ETX\DC2\EOT\199\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\RS\STX\STX\DC2\EOT\200\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\ENQ\DC2\EOT\200\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\SOH\DC2\EOT\200\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\ETX\DC2\EOT\200\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\RS\STX\ETX\DC2\EOT\201\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\ENQ\DC2\EOT\201\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\SOH\DC2\EOT\201\SOH\t\SUB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\ETX\ETX\DC2\EOT\201\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\RS\STX\EOT\DC2\EOT\202\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\EOT\ENQ\DC2\EOT\202\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\EOT\SOH\DC2\EOT\202\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\EOT\ETX\DC2\EOT\202\SOH\NAK\SYN\n\
    \\f\n\
    \\STX\EOT\US\DC2\ACK\204\SOH\NUL\208\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\US\SOH\DC2\EOT\204\SOH\b\EM\n\
    \\f\n\
    \\EOT\EOT\US\STX\NUL\DC2\EOT\205\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\EOT\DC2\EOT\205\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ACK\DC2\EOT\205\SOH\v\ETB\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\SOH\DC2\EOT\205\SOH\CAN\GS\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ETX\DC2\EOT\205\SOH !\n\
    \\f\n\
    \\EOT\EOT\US\STX\SOH\DC2\EOT\206\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ENQ\DC2\EOT\206\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\SOH\DC2\EOT\206\SOH\t\EM\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ETX\DC2\EOT\206\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\US\STX\STX\DC2\EOT\207\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\ENQ\DC2\EOT\207\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\SOH\DC2\EOT\207\SOH\a\DC3\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\ETX\DC2\EOT\207\SOH\SYN\ETB\n\
    \\175\SOH\n\
    \\STX\EOT \DC2\ACK\211\SOH\NUL\218\SOH\SOH\SUB\160\SOH Ordered part numbers and exact part ETags select the staged bytes.\n\
    \ Non-final parts are at least 5 MiB. Publication and preconditions are atomic for this key.\n\
    \\n\
    \\v\n\
    \\ETX\EOT \SOH\DC2\EOT\211\SOH\b \n\
    \\f\n\
    \\EOT\EOT \STX\NUL\DC2\EOT\212\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ACK\DC2\EOT\212\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\SOH\DC2\EOT\212\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ETX\DC2\EOT\212\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT \STX\SOH\DC2\EOT\213\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ENQ\DC2\EOT\213\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\SOH\DC2\EOT\213\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ETX\DC2\EOT\213\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT \STX\STX\DC2\EOT\214\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT \STX\STX\ENQ\DC2\EOT\214\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT \STX\STX\SOH\DC2\EOT\214\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT \STX\STX\ETX\DC2\EOT\214\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT \STX\ETX\DC2\EOT\215\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\EOT\DC2\EOT\215\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\ACK\DC2\EOT\215\SOH\v\ETB\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\SOH\DC2\EOT\215\SOH\CAN\GS\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\ETX\DC2\EOT\215\SOH !\n\
    \\f\n\
    \\EOT\EOT \STX\EOT\DC2\EOT\216\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT \STX\EOT\ACK\DC2\EOT\216\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT \STX\EOT\SOH\DC2\EOT\216\SOH\DLE\GS\n\
    \\r\n\
    \\ENQ\EOT \STX\EOT\ETX\DC2\EOT\216\SOH !\n\
    \\f\n\
    \\EOT\EOT \STX\ENQ\DC2\EOT\217\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT \STX\ENQ\ACK\DC2\EOT\217\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT \STX\ENQ\SOH\DC2\EOT\217\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT \STX\ENQ\ETX\DC2\EOT\217\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT!\DC2\ACK\219\SOH\NUL\224\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT!\SOH\DC2\EOT\219\SOH\b\GS\n\
    \\f\n\
    \\EOT\EOT!\STX\NUL\DC2\EOT\220\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ACK\DC2\EOT\220\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\SOH\DC2\EOT\220\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ETX\DC2\EOT\220\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT!\STX\SOH\DC2\EOT\221\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ENQ\DC2\EOT\221\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\SOH\DC2\EOT\221\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ETX\DC2\EOT\221\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT!\STX\STX\DC2\EOT\222\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\ENQ\DC2\EOT\222\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\SOH\DC2\EOT\222\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\ETX\DC2\EOT\222\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT!\STX\ETX\DC2\EOT\223\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\ACK\DC2\EOT\223\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\SOH\DC2\EOT\223\SOH\DC3\ESC\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\ETX\DC2\EOT\223\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\"\DC2\ACK\225\SOH\NUL\227\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\"\SOH\DC2\EOT\225\SOH\b\RS\n\
    \\f\n\
    \\EOT\EOT\"\STX\NUL\DC2\EOT\226\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ENQ\DC2\EOT\226\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\SOH\DC2\EOT\226\SOH\a\SO\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ETX\DC2\EOT\226\SOH\DC1\DC2\n\
    \\f\n\
    \\STX\ACK\STX\DC2\ACK\228\SOH\NUL\234\SOH\SOH\n\
    \\v\n\
    \\ETX\ACK\STX\SOH\DC2\EOT\228\SOH\b\CAN\n\
    \\f\n\
    \\EOT\ACK\STX\STX\NUL\DC2\EOT\229\SOH\STXH\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\NUL\SOH\DC2\EOT\229\SOH\ACK\NAK\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\NUL\STX\DC2\EOT\229\SOH\SYN,\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\NUL\ETX\DC2\EOT\229\SOH7F\n\
    \\f\n\
    \\EOT\ACK\STX\STX\SOH\DC2\EOT\230\SOH\STXB\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\SOH\DC2\EOT\230\SOH\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\ENQ\DC2\EOT\230\SOH\DC1\ETB\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\STX\DC2\EOT\230\SOH\CAN)\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\SOH\ETX\DC2\EOT\230\SOH4@\n\
    \\f\n\
    \\EOT\ACK\STX\STX\STX\DC2\EOT\231\SOH\STX>\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\STX\SOH\DC2\EOT\231\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\STX\STX\DC2\EOT\231\SOH\DLE \n\
    \\r\n\
    \\ENQ\ACK\STX\STX\STX\ETX\DC2\EOT\231\SOH+<\n\
    \\f\n\
    \\EOT\ACK\STX\STX\ETX\DC2\EOT\232\SOH\STXG\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\ETX\SOH\DC2\EOT\232\SOH\ACK\ETB\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\ETX\STX\DC2\EOT\232\SOH\CAN0\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\ETX\ETX\DC2\EOT\232\SOH;E\n\
    \\f\n\
    \\EOT\ACK\STX\STX\EOT\DC2\EOT\233\SOH\STXM\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\EOT\SOH\DC2\EOT\233\SOH\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\EOT\STX\DC2\EOT\233\SOH\NAK*\n\
    \\r\n\
    \\ENQ\ACK\STX\STX\EOT\ETX\DC2\EOT\233\SOH5K\n\
    \\f\n\
    \\STX\ENQ\SOH\DC2\ACK\235\SOH\NUL\248\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\SOH\SOH\DC2\EOT\235\SOH\ENQ\SO\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\NUL\DC2\EOT\236\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\SOH\DC2\EOT\236\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\STX\DC2\EOT\236\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\SOH\DC2\EOT\237\SOH\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\SOH\DC2\EOT\237\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\STX\DC2\EOT\237\SOH !\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\STX\DC2\EOT\238\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\SOH\DC2\EOT\238\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\STX\DC2\EOT\238\SOH\EM\SUB\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ETX\DC2\EOT\239\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ETX\SOH\DC2\EOT\239\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ETX\STX\DC2\EOT\239\SOH\RS\US\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\EOT\DC2\EOT\240\SOH\STX%\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\EOT\SOH\DC2\EOT\240\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\EOT\STX\DC2\EOT\240\SOH#$\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ENQ\DC2\EOT\241\SOH\STX&\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ENQ\SOH\DC2\EOT\241\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ENQ\STX\DC2\EOT\241\SOH$%\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ACK\DC2\EOT\242\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ACK\SOH\DC2\EOT\242\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ACK\STX\DC2\EOT\242\SOH\RS\US\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\a\DC2\EOT\243\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\a\SOH\DC2\EOT\243\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\a\STX\DC2\EOT\243\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\b\DC2\EOT\244\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\b\SOH\DC2\EOT\244\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\b\STX\DC2\EOT\244\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\t\DC2\EOT\245\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\t\SOH\DC2\EOT\245\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\t\STX\DC2\EOT\245\SOH\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\n\
    \\DC2\EOT\246\SOH\STX(\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\n\
    \\SOH\DC2\EOT\246\SOH\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\n\
    \\STX\DC2\EOT\246\SOH%'\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\v\DC2\EOT\247\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\v\SOH\DC2\EOT\247\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\v\STX\DC2\EOT\247\SOH\FS\RS\n\
    \\\\n\
    \\STX\EOT#\DC2\ACK\250\SOH\NUL\253\SOH\SOH\SUBN Customer diagnostic identity; operator topology stays in protected evidence.\n\
    \\n\
    \\v\n\
    \\ETX\EOT#\SOH\DC2\EOT\250\SOH\b\DC3\n\
    \\f\n\
    \\EOT\EOT#\STX\NUL\DC2\EOT\251\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ACK\DC2\EOT\251\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\SOH\DC2\EOT\251\SOH\f\DLE\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ETX\DC2\EOT\251\SOH\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT#\STX\SOH\DC2\EOT\252\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ENQ\DC2\EOT\252\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\SOH\DC2\EOT\252\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ETX\DC2\EOT\252\SOH\SYN\ETBb\ACKproto3"