{- This file was auto-generated from workers/v1/workers.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Workers.V1.Workers (
        WorkersService(..), CancelJobRequest(), CancelJobResponse(),
        CodeVersion(), Deployment(), Error(), ErrorCode(..), ErrorCode(),
        ErrorCode'UnrecognizedValue, Header(), InspectJobRequest(),
        InspectJobResponse(), InvokeDeploymentRequest(), InvokeResponse(),
        InvokeVersionRequest(), JobLimits(), JobObservation(), JobResult(),
        JobState(..), JobState(), JobState'UnrecognizedValue, JobTarget(),
        JobTarget'Target(..), _JobTarget'DeploymentAlias,
        _JobTarget'VersionSha256, ObjectRef(), Payload(),
        Payload'Source(..), _Payload'InlineBytes, _Payload'Object,
        PublishVersionRequest(), PublishVersionResponse(), RetryPolicy(),
        SelectDeploymentRequest(), SelectDeploymentResponse(),
        SubmitJobRequest(), SubmitJobResponse()
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
     
         * 'Proto.Workers.V1.Workers_Fields.jobId' @:: Lens' CancelJobRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.idempotencyKey' @:: Lens' CancelJobRequest Data.Text.Text@ -}
data CancelJobRequest
  = CancelJobRequest'_constructor {_CancelJobRequest'jobId :: !Data.Text.Text,
                                   _CancelJobRequest'idempotencyKey :: !Data.Text.Text,
                                   _CancelJobRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CancelJobRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CancelJobRequest "jobId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CancelJobRequest'jobId
           (\ x__ y__ -> x__ {_CancelJobRequest'jobId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CancelJobRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CancelJobRequest'idempotencyKey
           (\ x__ y__ -> x__ {_CancelJobRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message CancelJobRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.CancelJobRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLECancelJobRequest\DC2\NAK\n\
      \\ACKjob_id\CAN\SOH \SOH(\tR\ENQjobId\DC2'\n\
      \\SIidempotency_key\CAN\STX \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        jobId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "job_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"jobId")) ::
              Data.ProtoLens.FieldDescriptor CancelJobRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CancelJobRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, jobId__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CancelJobRequest'_unknownFields
        (\ x__ y__ -> x__ {_CancelJobRequest'_unknownFields = y__})
  defMessage
    = CancelJobRequest'_constructor
        {_CancelJobRequest'jobId = Data.ProtoLens.fieldDefault,
         _CancelJobRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _CancelJobRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CancelJobRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CancelJobRequest
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
                                       "job_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"jobId") y x)
                        18
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
          (do loop Data.ProtoLens.defMessage) "CancelJobRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"jobId") _x
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
                         (Data.ProtoLens.Field.field @"idempotencyKey") _x
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
instance Control.DeepSeq.NFData CancelJobRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CancelJobRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CancelJobRequest'jobId x__)
                (Control.DeepSeq.deepseq
                   (_CancelJobRequest'idempotencyKey x__) ()))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.job' @:: Lens' CancelJobResponse JobObservation@
         * 'Proto.Workers.V1.Workers_Fields.maybe'job' @:: Lens' CancelJobResponse (Prelude.Maybe JobObservation)@ -}
data CancelJobResponse
  = CancelJobResponse'_constructor {_CancelJobResponse'job :: !(Prelude.Maybe JobObservation),
                                    _CancelJobResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CancelJobResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CancelJobResponse "job" JobObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CancelJobResponse'job
           (\ x__ y__ -> x__ {_CancelJobResponse'job = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CancelJobResponse "maybe'job" (Prelude.Maybe JobObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CancelJobResponse'job
           (\ x__ y__ -> x__ {_CancelJobResponse'job = y__}))
        Prelude.id
instance Data.ProtoLens.Message CancelJobResponse where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.CancelJobResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1CancelJobResponse\DC24\n\
      \\ETXjob\CAN\SOH \SOH(\v2\".acyclic.workers.v1.JobObservationR\ETXjob"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        job__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "job"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor JobObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'job")) ::
              Data.ProtoLens.FieldDescriptor CancelJobResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, job__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CancelJobResponse'_unknownFields
        (\ x__ y__ -> x__ {_CancelJobResponse'_unknownFields = y__})
  defMessage
    = CancelJobResponse'_constructor
        {_CancelJobResponse'job = Prelude.Nothing,
         _CancelJobResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CancelJobResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser CancelJobResponse
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
                                       "job"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"job") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CancelJobResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'job") _x
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
instance Control.DeepSeq.NFData CancelJobResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CancelJobResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CancelJobResponse'job x__) ())
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.sha256' @:: Lens' CodeVersion Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.sizeBytes' @:: Lens' CodeVersion Data.Word.Word64@ -}
data CodeVersion
  = CodeVersion'_constructor {_CodeVersion'sha256 :: !Data.ByteString.ByteString,
                              _CodeVersion'sizeBytes :: !Data.Word.Word64,
                              _CodeVersion'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CodeVersion where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CodeVersion "sha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CodeVersion'sha256 (\ x__ y__ -> x__ {_CodeVersion'sha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CodeVersion "sizeBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CodeVersion'sizeBytes
           (\ x__ y__ -> x__ {_CodeVersion'sizeBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Message CodeVersion where
  messageName _ = Data.Text.pack "acyclic.workers.v1.CodeVersion"
  packedMessageDescriptor _
    = "\n\
      \\vCodeVersion\DC2\SYN\n\
      \\ACKsha256\CAN\SOH \SOH(\fR\ACKsha256\DC2\GS\n\
      \\n\
      \size_bytes\CAN\STX \SOH(\EOTR\tsizeBytes"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        sha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"sha256")) ::
              Data.ProtoLens.FieldDescriptor CodeVersion
        sizeBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "size_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sizeBytes")) ::
              Data.ProtoLens.FieldDescriptor CodeVersion
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, sha256__field_descriptor),
           (Data.ProtoLens.Tag 2, sizeBytes__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CodeVersion'_unknownFields
        (\ x__ y__ -> x__ {_CodeVersion'_unknownFields = y__})
  defMessage
    = CodeVersion'_constructor
        {_CodeVersion'sha256 = Data.ProtoLens.fieldDefault,
         _CodeVersion'sizeBytes = Data.ProtoLens.fieldDefault,
         _CodeVersion'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CodeVersion -> Data.ProtoLens.Encoding.Bytes.Parser CodeVersion
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
                                       "sha256"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"sha256") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "size_bytes"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"sizeBytes") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CodeVersion"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"sha256") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"sizeBytes") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData CodeVersion where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CodeVersion'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CodeVersion'sha256 x__)
                (Control.DeepSeq.deepseq (_CodeVersion'sizeBytes x__) ()))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.alias' @:: Lens' Deployment Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.version' @:: Lens' Deployment CodeVersion@
         * 'Proto.Workers.V1.Workers_Fields.maybe'version' @:: Lens' Deployment (Prelude.Maybe CodeVersion)@
         * 'Proto.Workers.V1.Workers_Fields.revision' @:: Lens' Deployment Data.Word.Word64@ -}
data Deployment
  = Deployment'_constructor {_Deployment'alias :: !Data.Text.Text,
                             _Deployment'version :: !(Prelude.Maybe CodeVersion),
                             _Deployment'revision :: !Data.Word.Word64,
                             _Deployment'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Deployment where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Deployment "alias" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Deployment'alias (\ x__ y__ -> x__ {_Deployment'alias = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Deployment "version" CodeVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Deployment'version (\ x__ y__ -> x__ {_Deployment'version = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField Deployment "maybe'version" (Prelude.Maybe CodeVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Deployment'version (\ x__ y__ -> x__ {_Deployment'version = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Deployment "revision" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Deployment'revision
           (\ x__ y__ -> x__ {_Deployment'revision = y__}))
        Prelude.id
instance Data.ProtoLens.Message Deployment where
  messageName _ = Data.Text.pack "acyclic.workers.v1.Deployment"
  packedMessageDescriptor _
    = "\n\
      \\n\
      \Deployment\DC2\DC4\n\
      \\ENQalias\CAN\SOH \SOH(\tR\ENQalias\DC29\n\
      \\aversion\CAN\STX \SOH(\v2\US.acyclic.workers.v1.CodeVersionR\aversion\DC2\SUB\n\
      \\brevision\CAN\ETX \SOH(\EOTR\brevision"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        alias__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "alias"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"alias")) ::
              Data.ProtoLens.FieldDescriptor Deployment
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CodeVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'version")) ::
              Data.ProtoLens.FieldDescriptor Deployment
        revision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"revision")) ::
              Data.ProtoLens.FieldDescriptor Deployment
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, alias__field_descriptor),
           (Data.ProtoLens.Tag 2, version__field_descriptor),
           (Data.ProtoLens.Tag 3, revision__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Deployment'_unknownFields
        (\ x__ y__ -> x__ {_Deployment'_unknownFields = y__})
  defMessage
    = Deployment'_constructor
        {_Deployment'alias = Data.ProtoLens.fieldDefault,
         _Deployment'version = Prelude.Nothing,
         _Deployment'revision = Data.ProtoLens.fieldDefault,
         _Deployment'_unknownFields = []}
  parseMessage
    = let
        loop ::
          Deployment -> Data.ProtoLens.Encoding.Bytes.Parser Deployment
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
                                       "alias"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"alias") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "version"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"version") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "revision"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"revision") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Deployment"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"alias") _x
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
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"revision") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData Deployment where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Deployment'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Deployment'alias x__)
                (Control.DeepSeq.deepseq
                   (_Deployment'version x__)
                   (Control.DeepSeq.deepseq (_Deployment'revision x__) ())))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.code' @:: Lens' Error ErrorCode@
         * 'Proto.Workers.V1.Workers_Fields.message' @:: Lens' Error Data.Text.Text@ -}
data Error
  = Error'_constructor {_Error'code :: !ErrorCode,
                        _Error'message :: !Data.Text.Text,
                        _Error'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Error where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Error "code" ErrorCode where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Error'code (\ x__ y__ -> x__ {_Error'code = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Error "message" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Error'message (\ x__ y__ -> x__ {_Error'message = y__}))
        Prelude.id
instance Data.ProtoLens.Message Error where
  messageName _ = Data.Text.pack "acyclic.workers.v1.Error"
  packedMessageDescriptor _
    = "\n\
      \\ENQError\DC21\n\
      \\EOTcode\CAN\SOH \SOH(\SO2\GS.acyclic.workers.v1.ErrorCodeR\EOTcode\DC2\CAN\n\
      \\amessage\CAN\STX \SOH(\tR\amessage"
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
              Data.ProtoLens.FieldDescriptor Error
        message__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "message"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"message")) ::
              Data.ProtoLens.FieldDescriptor Error
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, code__field_descriptor),
           (Data.ProtoLens.Tag 2, message__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Error'_unknownFields
        (\ x__ y__ -> x__ {_Error'_unknownFields = y__})
  defMessage
    = Error'_constructor
        {_Error'code = Data.ProtoLens.fieldDefault,
         _Error'message = Data.ProtoLens.fieldDefault,
         _Error'_unknownFields = []}
  parseMessage
    = let
        loop :: Error -> Data.ProtoLens.Encoding.Bytes.Parser Error
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
                                       "message"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"message") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Error"
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"message") _x
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
instance Control.DeepSeq.NFData Error where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Error'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Error'code x__)
                (Control.DeepSeq.deepseq (_Error'message x__) ()))
newtype ErrorCode'UnrecognizedValue
  = ErrorCode'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ErrorCode
  = ERROR_CODE_UNSPECIFIED |
    ERROR_CODE_INVALID_ARGUMENT |
    ERROR_CODE_CAPABILITY_DENIED |
    ERROR_CODE_CAPABILITY_EXPIRED |
    ERROR_CODE_VERSION_NOT_FOUND |
    ERROR_CODE_DEPLOYMENT_NOT_FOUND |
    ERROR_CODE_JOB_NOT_FOUND |
    ERROR_CODE_IDEMPOTENCY_MISMATCH |
    ERROR_CODE_REVISION_CONFLICT |
    ERROR_CODE_OVERLOADED |
    ERROR_CODE_TERMINAL_JOB_FAILURE |
    ErrorCode'Unrecognized !ErrorCode'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ErrorCode where
  maybeToEnum 0 = Prelude.Just ERROR_CODE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just ERROR_CODE_INVALID_ARGUMENT
  maybeToEnum 2 = Prelude.Just ERROR_CODE_CAPABILITY_DENIED
  maybeToEnum 3 = Prelude.Just ERROR_CODE_CAPABILITY_EXPIRED
  maybeToEnum 4 = Prelude.Just ERROR_CODE_VERSION_NOT_FOUND
  maybeToEnum 5 = Prelude.Just ERROR_CODE_DEPLOYMENT_NOT_FOUND
  maybeToEnum 6 = Prelude.Just ERROR_CODE_JOB_NOT_FOUND
  maybeToEnum 7 = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
  maybeToEnum 8 = Prelude.Just ERROR_CODE_REVISION_CONFLICT
  maybeToEnum 9 = Prelude.Just ERROR_CODE_OVERLOADED
  maybeToEnum 10 = Prelude.Just ERROR_CODE_TERMINAL_JOB_FAILURE
  maybeToEnum k
    = Prelude.Just
        (ErrorCode'Unrecognized
           (ErrorCode'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum ERROR_CODE_UNSPECIFIED = "ERROR_CODE_UNSPECIFIED"
  showEnum ERROR_CODE_INVALID_ARGUMENT
    = "ERROR_CODE_INVALID_ARGUMENT"
  showEnum ERROR_CODE_CAPABILITY_DENIED
    = "ERROR_CODE_CAPABILITY_DENIED"
  showEnum ERROR_CODE_CAPABILITY_EXPIRED
    = "ERROR_CODE_CAPABILITY_EXPIRED"
  showEnum ERROR_CODE_VERSION_NOT_FOUND
    = "ERROR_CODE_VERSION_NOT_FOUND"
  showEnum ERROR_CODE_DEPLOYMENT_NOT_FOUND
    = "ERROR_CODE_DEPLOYMENT_NOT_FOUND"
  showEnum ERROR_CODE_JOB_NOT_FOUND = "ERROR_CODE_JOB_NOT_FOUND"
  showEnum ERROR_CODE_IDEMPOTENCY_MISMATCH
    = "ERROR_CODE_IDEMPOTENCY_MISMATCH"
  showEnum ERROR_CODE_REVISION_CONFLICT
    = "ERROR_CODE_REVISION_CONFLICT"
  showEnum ERROR_CODE_OVERLOADED = "ERROR_CODE_OVERLOADED"
  showEnum ERROR_CODE_TERMINAL_JOB_FAILURE
    = "ERROR_CODE_TERMINAL_JOB_FAILURE"
  showEnum (ErrorCode'Unrecognized (ErrorCode'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "ERROR_CODE_UNSPECIFIED"
    = Prelude.Just ERROR_CODE_UNSPECIFIED
    | (Prelude.==) k "ERROR_CODE_INVALID_ARGUMENT"
    = Prelude.Just ERROR_CODE_INVALID_ARGUMENT
    | (Prelude.==) k "ERROR_CODE_CAPABILITY_DENIED"
    = Prelude.Just ERROR_CODE_CAPABILITY_DENIED
    | (Prelude.==) k "ERROR_CODE_CAPABILITY_EXPIRED"
    = Prelude.Just ERROR_CODE_CAPABILITY_EXPIRED
    | (Prelude.==) k "ERROR_CODE_VERSION_NOT_FOUND"
    = Prelude.Just ERROR_CODE_VERSION_NOT_FOUND
    | (Prelude.==) k "ERROR_CODE_DEPLOYMENT_NOT_FOUND"
    = Prelude.Just ERROR_CODE_DEPLOYMENT_NOT_FOUND
    | (Prelude.==) k "ERROR_CODE_JOB_NOT_FOUND"
    = Prelude.Just ERROR_CODE_JOB_NOT_FOUND
    | (Prelude.==) k "ERROR_CODE_IDEMPOTENCY_MISMATCH"
    = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
    | (Prelude.==) k "ERROR_CODE_REVISION_CONFLICT"
    = Prelude.Just ERROR_CODE_REVISION_CONFLICT
    | (Prelude.==) k "ERROR_CODE_OVERLOADED"
    = Prelude.Just ERROR_CODE_OVERLOADED
    | (Prelude.==) k "ERROR_CODE_TERMINAL_JOB_FAILURE"
    = Prelude.Just ERROR_CODE_TERMINAL_JOB_FAILURE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ErrorCode where
  minBound = ERROR_CODE_UNSPECIFIED
  maxBound = ERROR_CODE_TERMINAL_JOB_FAILURE
instance Prelude.Enum ErrorCode where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ErrorCode: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum ERROR_CODE_UNSPECIFIED = 0
  fromEnum ERROR_CODE_INVALID_ARGUMENT = 1
  fromEnum ERROR_CODE_CAPABILITY_DENIED = 2
  fromEnum ERROR_CODE_CAPABILITY_EXPIRED = 3
  fromEnum ERROR_CODE_VERSION_NOT_FOUND = 4
  fromEnum ERROR_CODE_DEPLOYMENT_NOT_FOUND = 5
  fromEnum ERROR_CODE_JOB_NOT_FOUND = 6
  fromEnum ERROR_CODE_IDEMPOTENCY_MISMATCH = 7
  fromEnum ERROR_CODE_REVISION_CONFLICT = 8
  fromEnum ERROR_CODE_OVERLOADED = 9
  fromEnum ERROR_CODE_TERMINAL_JOB_FAILURE = 10
  fromEnum (ErrorCode'Unrecognized (ErrorCode'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ ERROR_CODE_TERMINAL_JOB_FAILURE
    = Prelude.error
        "ErrorCode.succ: bad argument ERROR_CODE_TERMINAL_JOB_FAILURE. This value would be out of bounds."
  succ ERROR_CODE_UNSPECIFIED = ERROR_CODE_INVALID_ARGUMENT
  succ ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_CAPABILITY_DENIED
  succ ERROR_CODE_CAPABILITY_DENIED = ERROR_CODE_CAPABILITY_EXPIRED
  succ ERROR_CODE_CAPABILITY_EXPIRED = ERROR_CODE_VERSION_NOT_FOUND
  succ ERROR_CODE_VERSION_NOT_FOUND = ERROR_CODE_DEPLOYMENT_NOT_FOUND
  succ ERROR_CODE_DEPLOYMENT_NOT_FOUND = ERROR_CODE_JOB_NOT_FOUND
  succ ERROR_CODE_JOB_NOT_FOUND = ERROR_CODE_IDEMPOTENCY_MISMATCH
  succ ERROR_CODE_IDEMPOTENCY_MISMATCH = ERROR_CODE_REVISION_CONFLICT
  succ ERROR_CODE_REVISION_CONFLICT = ERROR_CODE_OVERLOADED
  succ ERROR_CODE_OVERLOADED = ERROR_CODE_TERMINAL_JOB_FAILURE
  succ (ErrorCode'Unrecognized _)
    = Prelude.error "ErrorCode.succ: bad argument: unrecognized value"
  pred ERROR_CODE_UNSPECIFIED
    = Prelude.error
        "ErrorCode.pred: bad argument ERROR_CODE_UNSPECIFIED. This value would be out of bounds."
  pred ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_UNSPECIFIED
  pred ERROR_CODE_CAPABILITY_DENIED = ERROR_CODE_INVALID_ARGUMENT
  pred ERROR_CODE_CAPABILITY_EXPIRED = ERROR_CODE_CAPABILITY_DENIED
  pred ERROR_CODE_VERSION_NOT_FOUND = ERROR_CODE_CAPABILITY_EXPIRED
  pred ERROR_CODE_DEPLOYMENT_NOT_FOUND = ERROR_CODE_VERSION_NOT_FOUND
  pred ERROR_CODE_JOB_NOT_FOUND = ERROR_CODE_DEPLOYMENT_NOT_FOUND
  pred ERROR_CODE_IDEMPOTENCY_MISMATCH = ERROR_CODE_JOB_NOT_FOUND
  pred ERROR_CODE_REVISION_CONFLICT = ERROR_CODE_IDEMPOTENCY_MISMATCH
  pred ERROR_CODE_OVERLOADED = ERROR_CODE_REVISION_CONFLICT
  pred ERROR_CODE_TERMINAL_JOB_FAILURE = ERROR_CODE_OVERLOADED
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
     
         * 'Proto.Workers.V1.Workers_Fields.name' @:: Lens' Header Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.value' @:: Lens' Header Data.Text.Text@ -}
data Header
  = Header'_constructor {_Header'name :: !Data.Text.Text,
                         _Header'value :: !Data.Text.Text,
                         _Header'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Header where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Header "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Header'name (\ x__ y__ -> x__ {_Header'name = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Header "value" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Header'value (\ x__ y__ -> x__ {_Header'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message Header where
  messageName _ = Data.Text.pack "acyclic.workers.v1.Header"
  packedMessageDescriptor _
    = "\n\
      \\ACKHeader\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\DC4\n\
      \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue"
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
              Data.ProtoLens.FieldDescriptor Header
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor Header
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, name__field_descriptor),
           (Data.ProtoLens.Tag 2, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Header'_unknownFields
        (\ x__ y__ -> x__ {_Header'_unknownFields = y__})
  defMessage
    = Header'_constructor
        {_Header'name = Data.ProtoLens.fieldDefault,
         _Header'value = Data.ProtoLens.fieldDefault,
         _Header'_unknownFields = []}
  parseMessage
    = let
        loop :: Header -> Data.ProtoLens.Encoding.Bytes.Parser Header
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
          (do loop Data.ProtoLens.defMessage) "Header"
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
instance Control.DeepSeq.NFData Header where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Header'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Header'name x__)
                (Control.DeepSeq.deepseq (_Header'value x__) ()))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.jobId' @:: Lens' InspectJobRequest Data.Text.Text@ -}
data InspectJobRequest
  = InspectJobRequest'_constructor {_InspectJobRequest'jobId :: !Data.Text.Text,
                                    _InspectJobRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectJobRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectJobRequest "jobId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectJobRequest'jobId
           (\ x__ y__ -> x__ {_InspectJobRequest'jobId = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectJobRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.InspectJobRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1InspectJobRequest\DC2\NAK\n\
      \\ACKjob_id\CAN\SOH \SOH(\tR\ENQjobId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        jobId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "job_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"jobId")) ::
              Data.ProtoLens.FieldDescriptor InspectJobRequest
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, jobId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectJobRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectJobRequest'_unknownFields = y__})
  defMessage
    = InspectJobRequest'_constructor
        {_InspectJobRequest'jobId = Data.ProtoLens.fieldDefault,
         _InspectJobRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectJobRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectJobRequest
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
                                       "job_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"jobId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectJobRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"jobId") _x
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
instance Control.DeepSeq.NFData InspectJobRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectJobRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectJobRequest'jobId x__) ())
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.job' @:: Lens' InspectJobResponse JobObservation@
         * 'Proto.Workers.V1.Workers_Fields.maybe'job' @:: Lens' InspectJobResponse (Prelude.Maybe JobObservation)@ -}
data InspectJobResponse
  = InspectJobResponse'_constructor {_InspectJobResponse'job :: !(Prelude.Maybe JobObservation),
                                     _InspectJobResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectJobResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectJobResponse "job" JobObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectJobResponse'job
           (\ x__ y__ -> x__ {_InspectJobResponse'job = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectJobResponse "maybe'job" (Prelude.Maybe JobObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectJobResponse'job
           (\ x__ y__ -> x__ {_InspectJobResponse'job = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectJobResponse where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.InspectJobResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC2InspectJobResponse\DC24\n\
      \\ETXjob\CAN\SOH \SOH(\v2\".acyclic.workers.v1.JobObservationR\ETXjob"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        job__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "job"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor JobObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'job")) ::
              Data.ProtoLens.FieldDescriptor InspectJobResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, job__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectJobResponse'_unknownFields
        (\ x__ y__ -> x__ {_InspectJobResponse'_unknownFields = y__})
  defMessage
    = InspectJobResponse'_constructor
        {_InspectJobResponse'job = Prelude.Nothing,
         _InspectJobResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectJobResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectJobResponse
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
                                       "job"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"job") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectJobResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'job") _x
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
instance Control.DeepSeq.NFData InspectJobResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectJobResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectJobResponse'job x__) ())
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.alias' @:: Lens' InvokeDeploymentRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.method' @:: Lens' InvokeDeploymentRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.url' @:: Lens' InvokeDeploymentRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.headers' @:: Lens' InvokeDeploymentRequest [Header]@
         * 'Proto.Workers.V1.Workers_Fields.vec'headers' @:: Lens' InvokeDeploymentRequest (Data.Vector.Vector Header)@
         * 'Proto.Workers.V1.Workers_Fields.body' @:: Lens' InvokeDeploymentRequest Data.ByteString.ByteString@ -}
data InvokeDeploymentRequest
  = InvokeDeploymentRequest'_constructor {_InvokeDeploymentRequest'alias :: !Data.Text.Text,
                                          _InvokeDeploymentRequest'method :: !Data.Text.Text,
                                          _InvokeDeploymentRequest'url :: !Data.Text.Text,
                                          _InvokeDeploymentRequest'headers :: !(Data.Vector.Vector Header),
                                          _InvokeDeploymentRequest'body :: !Data.ByteString.ByteString,
                                          _InvokeDeploymentRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InvokeDeploymentRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InvokeDeploymentRequest "alias" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeDeploymentRequest'alias
           (\ x__ y__ -> x__ {_InvokeDeploymentRequest'alias = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeDeploymentRequest "method" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeDeploymentRequest'method
           (\ x__ y__ -> x__ {_InvokeDeploymentRequest'method = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeDeploymentRequest "url" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeDeploymentRequest'url
           (\ x__ y__ -> x__ {_InvokeDeploymentRequest'url = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeDeploymentRequest "headers" [Header] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeDeploymentRequest'headers
           (\ x__ y__ -> x__ {_InvokeDeploymentRequest'headers = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField InvokeDeploymentRequest "vec'headers" (Data.Vector.Vector Header) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeDeploymentRequest'headers
           (\ x__ y__ -> x__ {_InvokeDeploymentRequest'headers = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeDeploymentRequest "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeDeploymentRequest'body
           (\ x__ y__ -> x__ {_InvokeDeploymentRequest'body = y__}))
        Prelude.id
instance Data.ProtoLens.Message InvokeDeploymentRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.InvokeDeploymentRequest"
  packedMessageDescriptor _
    = "\n\
      \\ETBInvokeDeploymentRequest\DC2\DC4\n\
      \\ENQalias\CAN\SOH \SOH(\tR\ENQalias\DC2\SYN\n\
      \\ACKmethod\CAN\STX \SOH(\tR\ACKmethod\DC2\DLE\n\
      \\ETXurl\CAN\ETX \SOH(\tR\ETXurl\DC24\n\
      \\aheaders\CAN\EOT \ETX(\v2\SUB.acyclic.workers.v1.HeaderR\aheaders\DC2\DC2\n\
      \\EOTbody\CAN\ENQ \SOH(\fR\EOTbody"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        alias__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "alias"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"alias")) ::
              Data.ProtoLens.FieldDescriptor InvokeDeploymentRequest
        method__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "method"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"method")) ::
              Data.ProtoLens.FieldDescriptor InvokeDeploymentRequest
        url__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "url"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"url")) ::
              Data.ProtoLens.FieldDescriptor InvokeDeploymentRequest
        headers__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "headers"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Header)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"headers")) ::
              Data.ProtoLens.FieldDescriptor InvokeDeploymentRequest
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"body")) ::
              Data.ProtoLens.FieldDescriptor InvokeDeploymentRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, alias__field_descriptor),
           (Data.ProtoLens.Tag 2, method__field_descriptor),
           (Data.ProtoLens.Tag 3, url__field_descriptor),
           (Data.ProtoLens.Tag 4, headers__field_descriptor),
           (Data.ProtoLens.Tag 5, body__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InvokeDeploymentRequest'_unknownFields
        (\ x__ y__ -> x__ {_InvokeDeploymentRequest'_unknownFields = y__})
  defMessage
    = InvokeDeploymentRequest'_constructor
        {_InvokeDeploymentRequest'alias = Data.ProtoLens.fieldDefault,
         _InvokeDeploymentRequest'method = Data.ProtoLens.fieldDefault,
         _InvokeDeploymentRequest'url = Data.ProtoLens.fieldDefault,
         _InvokeDeploymentRequest'headers = Data.Vector.Generic.empty,
         _InvokeDeploymentRequest'body = Data.ProtoLens.fieldDefault,
         _InvokeDeploymentRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InvokeDeploymentRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Header
             -> Data.ProtoLens.Encoding.Bytes.Parser InvokeDeploymentRequest
        loop x mutable'headers
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'headers <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'headers)
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
                              (Data.ProtoLens.Field.field @"vec'headers") frozen'headers x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "alias"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"alias") y x)
                                  mutable'headers
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "method"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"method") y x)
                                  mutable'headers
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "url"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"url") y x)
                                  mutable'headers
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "headers"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'headers y)
                                loop x v
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                                  mutable'headers
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'headers
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'headers <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'headers)
          "InvokeDeploymentRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"alias") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"method") _x
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
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"url") _x
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
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'headers") _x))
                      ((Data.Monoid.<>)
                         (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"body") _x
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
instance Control.DeepSeq.NFData InvokeDeploymentRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InvokeDeploymentRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InvokeDeploymentRequest'alias x__)
                (Control.DeepSeq.deepseq
                   (_InvokeDeploymentRequest'method x__)
                   (Control.DeepSeq.deepseq
                      (_InvokeDeploymentRequest'url x__)
                      (Control.DeepSeq.deepseq
                         (_InvokeDeploymentRequest'headers x__)
                         (Control.DeepSeq.deepseq
                            (_InvokeDeploymentRequest'body x__) ())))))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.status' @:: Lens' InvokeResponse Data.Word.Word32@
         * 'Proto.Workers.V1.Workers_Fields.headers' @:: Lens' InvokeResponse [Header]@
         * 'Proto.Workers.V1.Workers_Fields.vec'headers' @:: Lens' InvokeResponse (Data.Vector.Vector Header)@
         * 'Proto.Workers.V1.Workers_Fields.body' @:: Lens' InvokeResponse Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.resolvedSha256' @:: Lens' InvokeResponse Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.resolvedRevision' @:: Lens' InvokeResponse Data.Word.Word64@
         * 'Proto.Workers.V1.Workers_Fields.maybe'resolvedRevision' @:: Lens' InvokeResponse (Prelude.Maybe Data.Word.Word64)@ -}
data InvokeResponse
  = InvokeResponse'_constructor {_InvokeResponse'status :: !Data.Word.Word32,
                                 _InvokeResponse'headers :: !(Data.Vector.Vector Header),
                                 _InvokeResponse'body :: !Data.ByteString.ByteString,
                                 _InvokeResponse'resolvedSha256 :: !Data.ByteString.ByteString,
                                 _InvokeResponse'resolvedRevision :: !(Prelude.Maybe Data.Word.Word64),
                                 _InvokeResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InvokeResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InvokeResponse "status" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'status
           (\ x__ y__ -> x__ {_InvokeResponse'status = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeResponse "headers" [Header] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'headers
           (\ x__ y__ -> x__ {_InvokeResponse'headers = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField InvokeResponse "vec'headers" (Data.Vector.Vector Header) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'headers
           (\ x__ y__ -> x__ {_InvokeResponse'headers = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeResponse "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'body
           (\ x__ y__ -> x__ {_InvokeResponse'body = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeResponse "resolvedSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'resolvedSha256
           (\ x__ y__ -> x__ {_InvokeResponse'resolvedSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeResponse "resolvedRevision" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'resolvedRevision
           (\ x__ y__ -> x__ {_InvokeResponse'resolvedRevision = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField InvokeResponse "maybe'resolvedRevision" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeResponse'resolvedRevision
           (\ x__ y__ -> x__ {_InvokeResponse'resolvedRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Message InvokeResponse where
  messageName _ = Data.Text.pack "acyclic.workers.v1.InvokeResponse"
  packedMessageDescriptor _
    = "\n\
      \\SOInvokeResponse\DC2\SYN\n\
      \\ACKstatus\CAN\SOH \SOH(\rR\ACKstatus\DC24\n\
      \\aheaders\CAN\STX \ETX(\v2\SUB.acyclic.workers.v1.HeaderR\aheaders\DC2\DC2\n\
      \\EOTbody\CAN\ETX \SOH(\fR\EOTbody\DC2'\n\
      \\SIresolved_sha256\CAN\EOT \SOH(\fR\SOresolvedSha256\DC20\n\
      \\DC1resolved_revision\CAN\ENQ \SOH(\EOTH\NULR\DLEresolvedRevision\136\SOH\SOHB\DC4\n\
      \\DC2_resolved_revision"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        status__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "status"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"status")) ::
              Data.ProtoLens.FieldDescriptor InvokeResponse
        headers__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "headers"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Header)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"headers")) ::
              Data.ProtoLens.FieldDescriptor InvokeResponse
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"body")) ::
              Data.ProtoLens.FieldDescriptor InvokeResponse
        resolvedSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "resolved_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"resolvedSha256")) ::
              Data.ProtoLens.FieldDescriptor InvokeResponse
        resolvedRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "resolved_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'resolvedRevision")) ::
              Data.ProtoLens.FieldDescriptor InvokeResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, status__field_descriptor),
           (Data.ProtoLens.Tag 2, headers__field_descriptor),
           (Data.ProtoLens.Tag 3, body__field_descriptor),
           (Data.ProtoLens.Tag 4, resolvedSha256__field_descriptor),
           (Data.ProtoLens.Tag 5, resolvedRevision__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InvokeResponse'_unknownFields
        (\ x__ y__ -> x__ {_InvokeResponse'_unknownFields = y__})
  defMessage
    = InvokeResponse'_constructor
        {_InvokeResponse'status = Data.ProtoLens.fieldDefault,
         _InvokeResponse'headers = Data.Vector.Generic.empty,
         _InvokeResponse'body = Data.ProtoLens.fieldDefault,
         _InvokeResponse'resolvedSha256 = Data.ProtoLens.fieldDefault,
         _InvokeResponse'resolvedRevision = Prelude.Nothing,
         _InvokeResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InvokeResponse
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Header
             -> Data.ProtoLens.Encoding.Bytes.Parser InvokeResponse
        loop x mutable'headers
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'headers <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'headers)
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
                              (Data.ProtoLens.Field.field @"vec'headers") frozen'headers x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        8 -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "status"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"status") y x)
                                  mutable'headers
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "headers"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'headers y)
                                loop x v
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                                  mutable'headers
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "resolved_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"resolvedSha256") y x)
                                  mutable'headers
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "resolved_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"resolvedRevision") y x)
                                  mutable'headers
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'headers
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'headers <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'headers)
          "InvokeResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"status") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
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
                   (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'headers") _x))
                ((Data.Monoid.<>)
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"body") _x
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
                               (Data.ProtoLens.Field.field @"resolvedSha256") _x
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
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view
                                (Data.ProtoLens.Field.field @"maybe'resolvedRevision") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData InvokeResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InvokeResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InvokeResponse'status x__)
                (Control.DeepSeq.deepseq
                   (_InvokeResponse'headers x__)
                   (Control.DeepSeq.deepseq
                      (_InvokeResponse'body x__)
                      (Control.DeepSeq.deepseq
                         (_InvokeResponse'resolvedSha256 x__)
                         (Control.DeepSeq.deepseq
                            (_InvokeResponse'resolvedRevision x__) ())))))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.versionSha256' @:: Lens' InvokeVersionRequest Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.method' @:: Lens' InvokeVersionRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.url' @:: Lens' InvokeVersionRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.headers' @:: Lens' InvokeVersionRequest [Header]@
         * 'Proto.Workers.V1.Workers_Fields.vec'headers' @:: Lens' InvokeVersionRequest (Data.Vector.Vector Header)@
         * 'Proto.Workers.V1.Workers_Fields.body' @:: Lens' InvokeVersionRequest Data.ByteString.ByteString@ -}
data InvokeVersionRequest
  = InvokeVersionRequest'_constructor {_InvokeVersionRequest'versionSha256 :: !Data.ByteString.ByteString,
                                       _InvokeVersionRequest'method :: !Data.Text.Text,
                                       _InvokeVersionRequest'url :: !Data.Text.Text,
                                       _InvokeVersionRequest'headers :: !(Data.Vector.Vector Header),
                                       _InvokeVersionRequest'body :: !Data.ByteString.ByteString,
                                       _InvokeVersionRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InvokeVersionRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InvokeVersionRequest "versionSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeVersionRequest'versionSha256
           (\ x__ y__ -> x__ {_InvokeVersionRequest'versionSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeVersionRequest "method" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeVersionRequest'method
           (\ x__ y__ -> x__ {_InvokeVersionRequest'method = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeVersionRequest "url" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeVersionRequest'url
           (\ x__ y__ -> x__ {_InvokeVersionRequest'url = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeVersionRequest "headers" [Header] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeVersionRequest'headers
           (\ x__ y__ -> x__ {_InvokeVersionRequest'headers = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField InvokeVersionRequest "vec'headers" (Data.Vector.Vector Header) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeVersionRequest'headers
           (\ x__ y__ -> x__ {_InvokeVersionRequest'headers = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeVersionRequest "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeVersionRequest'body
           (\ x__ y__ -> x__ {_InvokeVersionRequest'body = y__}))
        Prelude.id
instance Data.ProtoLens.Message InvokeVersionRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.InvokeVersionRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC4InvokeVersionRequest\DC2%\n\
      \\SOversion_sha256\CAN\SOH \SOH(\fR\rversionSha256\DC2\SYN\n\
      \\ACKmethod\CAN\STX \SOH(\tR\ACKmethod\DC2\DLE\n\
      \\ETXurl\CAN\ETX \SOH(\tR\ETXurl\DC24\n\
      \\aheaders\CAN\EOT \ETX(\v2\SUB.acyclic.workers.v1.HeaderR\aheaders\DC2\DC2\n\
      \\EOTbody\CAN\ENQ \SOH(\fR\EOTbody"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        versionSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"versionSha256")) ::
              Data.ProtoLens.FieldDescriptor InvokeVersionRequest
        method__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "method"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"method")) ::
              Data.ProtoLens.FieldDescriptor InvokeVersionRequest
        url__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "url"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"url")) ::
              Data.ProtoLens.FieldDescriptor InvokeVersionRequest
        headers__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "headers"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Header)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"headers")) ::
              Data.ProtoLens.FieldDescriptor InvokeVersionRequest
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"body")) ::
              Data.ProtoLens.FieldDescriptor InvokeVersionRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, versionSha256__field_descriptor),
           (Data.ProtoLens.Tag 2, method__field_descriptor),
           (Data.ProtoLens.Tag 3, url__field_descriptor),
           (Data.ProtoLens.Tag 4, headers__field_descriptor),
           (Data.ProtoLens.Tag 5, body__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InvokeVersionRequest'_unknownFields
        (\ x__ y__ -> x__ {_InvokeVersionRequest'_unknownFields = y__})
  defMessage
    = InvokeVersionRequest'_constructor
        {_InvokeVersionRequest'versionSha256 = Data.ProtoLens.fieldDefault,
         _InvokeVersionRequest'method = Data.ProtoLens.fieldDefault,
         _InvokeVersionRequest'url = Data.ProtoLens.fieldDefault,
         _InvokeVersionRequest'headers = Data.Vector.Generic.empty,
         _InvokeVersionRequest'body = Data.ProtoLens.fieldDefault,
         _InvokeVersionRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InvokeVersionRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Header
             -> Data.ProtoLens.Encoding.Bytes.Parser InvokeVersionRequest
        loop x mutable'headers
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'headers <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'headers)
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
                              (Data.ProtoLens.Field.field @"vec'headers") frozen'headers x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "version_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"versionSha256") y x)
                                  mutable'headers
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "method"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"method") y x)
                                  mutable'headers
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "url"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"url") y x)
                                  mutable'headers
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "headers"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'headers y)
                                loop x v
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                                  mutable'headers
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'headers
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'headers <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'headers)
          "InvokeVersionRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"versionSha256") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"method") _x
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
                   (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"url") _x
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
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'headers") _x))
                      ((Data.Monoid.<>)
                         (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"body") _x
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
instance Control.DeepSeq.NFData InvokeVersionRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InvokeVersionRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InvokeVersionRequest'versionSha256 x__)
                (Control.DeepSeq.deepseq
                   (_InvokeVersionRequest'method x__)
                   (Control.DeepSeq.deepseq
                      (_InvokeVersionRequest'url x__)
                      (Control.DeepSeq.deepseq
                         (_InvokeVersionRequest'headers x__)
                         (Control.DeepSeq.deepseq (_InvokeVersionRequest'body x__) ())))))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.timeoutMillis' @:: Lens' JobLimits Data.Word.Word64@
         * 'Proto.Workers.V1.Workers_Fields.memoryBytes' @:: Lens' JobLimits Data.Word.Word64@
         * 'Proto.Workers.V1.Workers_Fields.outputBytes' @:: Lens' JobLimits Data.Word.Word64@ -}
data JobLimits
  = JobLimits'_constructor {_JobLimits'timeoutMillis :: !Data.Word.Word64,
                            _JobLimits'memoryBytes :: !Data.Word.Word64,
                            _JobLimits'outputBytes :: !Data.Word.Word64,
                            _JobLimits'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show JobLimits where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField JobLimits "timeoutMillis" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobLimits'timeoutMillis
           (\ x__ y__ -> x__ {_JobLimits'timeoutMillis = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobLimits "memoryBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobLimits'memoryBytes
           (\ x__ y__ -> x__ {_JobLimits'memoryBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobLimits "outputBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobLimits'outputBytes
           (\ x__ y__ -> x__ {_JobLimits'outputBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Message JobLimits where
  messageName _ = Data.Text.pack "acyclic.workers.v1.JobLimits"
  packedMessageDescriptor _
    = "\n\
      \\tJobLimits\DC2%\n\
      \\SOtimeout_millis\CAN\SOH \SOH(\EOTR\rtimeoutMillis\DC2!\n\
      \\fmemory_bytes\CAN\STX \SOH(\EOTR\vmemoryBytes\DC2!\n\
      \\foutput_bytes\CAN\ETX \SOH(\EOTR\voutputBytes"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        timeoutMillis__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "timeout_millis"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"timeoutMillis")) ::
              Data.ProtoLens.FieldDescriptor JobLimits
        memoryBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "memory_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"memoryBytes")) ::
              Data.ProtoLens.FieldDescriptor JobLimits
        outputBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "output_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"outputBytes")) ::
              Data.ProtoLens.FieldDescriptor JobLimits
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, timeoutMillis__field_descriptor),
           (Data.ProtoLens.Tag 2, memoryBytes__field_descriptor),
           (Data.ProtoLens.Tag 3, outputBytes__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _JobLimits'_unknownFields
        (\ x__ y__ -> x__ {_JobLimits'_unknownFields = y__})
  defMessage
    = JobLimits'_constructor
        {_JobLimits'timeoutMillis = Data.ProtoLens.fieldDefault,
         _JobLimits'memoryBytes = Data.ProtoLens.fieldDefault,
         _JobLimits'outputBytes = Data.ProtoLens.fieldDefault,
         _JobLimits'_unknownFields = []}
  parseMessage
    = let
        loop :: JobLimits -> Data.ProtoLens.Encoding.Bytes.Parser JobLimits
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
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "timeout_millis"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"timeoutMillis") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "memory_bytes"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"memoryBytes") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "output_bytes"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"outputBytes") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "JobLimits"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"timeoutMillis") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"memoryBytes") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"outputBytes") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData JobLimits where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_JobLimits'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_JobLimits'timeoutMillis x__)
                (Control.DeepSeq.deepseq
                   (_JobLimits'memoryBytes x__)
                   (Control.DeepSeq.deepseq (_JobLimits'outputBytes x__) ())))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.jobId' @:: Lens' JobObservation Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.state' @:: Lens' JobObservation JobState@
         * 'Proto.Workers.V1.Workers_Fields.resolvedSha256' @:: Lens' JobObservation Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.attempt' @:: Lens' JobObservation Data.Word.Word32@
         * 'Proto.Workers.V1.Workers_Fields.result' @:: Lens' JobObservation JobResult@
         * 'Proto.Workers.V1.Workers_Fields.maybe'result' @:: Lens' JobObservation (Prelude.Maybe JobResult)@
         * 'Proto.Workers.V1.Workers_Fields.failureCode' @:: Lens' JobObservation Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.cancellationRequested' @:: Lens' JobObservation Prelude.Bool@ -}
data JobObservation
  = JobObservation'_constructor {_JobObservation'jobId :: !Data.Text.Text,
                                 _JobObservation'state :: !JobState,
                                 _JobObservation'resolvedSha256 :: !Data.ByteString.ByteString,
                                 _JobObservation'attempt :: !Data.Word.Word32,
                                 _JobObservation'result :: !(Prelude.Maybe JobResult),
                                 _JobObservation'failureCode :: !Data.Text.Text,
                                 _JobObservation'cancellationRequested :: !Prelude.Bool,
                                 _JobObservation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show JobObservation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField JobObservation "jobId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'jobId
           (\ x__ y__ -> x__ {_JobObservation'jobId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobObservation "state" JobState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'state
           (\ x__ y__ -> x__ {_JobObservation'state = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobObservation "resolvedSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'resolvedSha256
           (\ x__ y__ -> x__ {_JobObservation'resolvedSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobObservation "attempt" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'attempt
           (\ x__ y__ -> x__ {_JobObservation'attempt = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobObservation "result" JobResult where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'result
           (\ x__ y__ -> x__ {_JobObservation'result = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField JobObservation "maybe'result" (Prelude.Maybe JobResult) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'result
           (\ x__ y__ -> x__ {_JobObservation'result = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobObservation "failureCode" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'failureCode
           (\ x__ y__ -> x__ {_JobObservation'failureCode = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobObservation "cancellationRequested" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobObservation'cancellationRequested
           (\ x__ y__ -> x__ {_JobObservation'cancellationRequested = y__}))
        Prelude.id
instance Data.ProtoLens.Message JobObservation where
  messageName _ = Data.Text.pack "acyclic.workers.v1.JobObservation"
  packedMessageDescriptor _
    = "\n\
      \\SOJobObservation\DC2\NAK\n\
      \\ACKjob_id\CAN\SOH \SOH(\tR\ENQjobId\DC22\n\
      \\ENQstate\CAN\STX \SOH(\SO2\FS.acyclic.workers.v1.JobStateR\ENQstate\DC2'\n\
      \\SIresolved_sha256\CAN\ETX \SOH(\fR\SOresolvedSha256\DC2\CAN\n\
      \\aattempt\CAN\EOT \SOH(\rR\aattempt\DC25\n\
      \\ACKresult\CAN\ENQ \SOH(\v2\GS.acyclic.workers.v1.JobResultR\ACKresult\DC2!\n\
      \\ffailure_code\CAN\ACK \SOH(\tR\vfailureCode\DC25\n\
      \\SYNcancellation_requested\CAN\a \SOH(\bR\NAKcancellationRequested"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        jobId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "job_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"jobId")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
        state__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor JobState)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"state")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
        resolvedSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "resolved_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"resolvedSha256")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
        attempt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "attempt"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"attempt")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
        result__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "result"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor JobResult)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'result")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
        failureCode__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "failure_code"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"failureCode")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
        cancellationRequested__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "cancellation_requested"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"cancellationRequested")) ::
              Data.ProtoLens.FieldDescriptor JobObservation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, jobId__field_descriptor),
           (Data.ProtoLens.Tag 2, state__field_descriptor),
           (Data.ProtoLens.Tag 3, resolvedSha256__field_descriptor),
           (Data.ProtoLens.Tag 4, attempt__field_descriptor),
           (Data.ProtoLens.Tag 5, result__field_descriptor),
           (Data.ProtoLens.Tag 6, failureCode__field_descriptor),
           (Data.ProtoLens.Tag 7, cancellationRequested__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _JobObservation'_unknownFields
        (\ x__ y__ -> x__ {_JobObservation'_unknownFields = y__})
  defMessage
    = JobObservation'_constructor
        {_JobObservation'jobId = Data.ProtoLens.fieldDefault,
         _JobObservation'state = Data.ProtoLens.fieldDefault,
         _JobObservation'resolvedSha256 = Data.ProtoLens.fieldDefault,
         _JobObservation'attempt = Data.ProtoLens.fieldDefault,
         _JobObservation'result = Prelude.Nothing,
         _JobObservation'failureCode = Data.ProtoLens.fieldDefault,
         _JobObservation'cancellationRequested = Data.ProtoLens.fieldDefault,
         _JobObservation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          JobObservation
          -> Data.ProtoLens.Encoding.Bytes.Parser JobObservation
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
                                       "job_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"jobId") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "state"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"state") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "resolved_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"resolvedSha256") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "attempt"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"attempt") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "result"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"result") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "failure_code"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"failureCode") y x)
                        56
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "cancellation_requested"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"cancellationRequested") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "JobObservation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"jobId") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"state") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         ((Prelude..)
                            ((Prelude..)
                               Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral)
                            Prelude.fromEnum _v))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"resolvedSha256") _x
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
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"attempt") _x
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
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'result") _x
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
                            (let
                               _v
                                 = Lens.Family2.view (Data.ProtoLens.Field.field @"failureCode") _x
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
                                        (Data.ProtoLens.Field.field @"cancellationRequested") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 56)
                                        ((Prelude..)
                                           Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (\ b -> if b then 1 else 0) _v))
                               (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                  (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))))
instance Control.DeepSeq.NFData JobObservation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_JobObservation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_JobObservation'jobId x__)
                (Control.DeepSeq.deepseq
                   (_JobObservation'state x__)
                   (Control.DeepSeq.deepseq
                      (_JobObservation'resolvedSha256 x__)
                      (Control.DeepSeq.deepseq
                         (_JobObservation'attempt x__)
                         (Control.DeepSeq.deepseq
                            (_JobObservation'result x__)
                            (Control.DeepSeq.deepseq
                               (_JobObservation'failureCode x__)
                               (Control.DeepSeq.deepseq
                                  (_JobObservation'cancellationRequested x__) ())))))))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.body' @:: Lens' JobResult Data.ByteString.ByteString@ -}
data JobResult
  = JobResult'_constructor {_JobResult'body :: !Data.ByteString.ByteString,
                            _JobResult'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show JobResult where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField JobResult "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobResult'body (\ x__ y__ -> x__ {_JobResult'body = y__}))
        Prelude.id
instance Data.ProtoLens.Message JobResult where
  messageName _ = Data.Text.pack "acyclic.workers.v1.JobResult"
  packedMessageDescriptor _
    = "\n\
      \\tJobResult\DC2\DC2\n\
      \\EOTbody\CAN\SOH \SOH(\fR\EOTbodyJ\EOT\b\STX\DLE\ETXR\SOobject_version"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"body")) ::
              Data.ProtoLens.FieldDescriptor JobResult
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, body__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _JobResult'_unknownFields
        (\ x__ y__ -> x__ {_JobResult'_unknownFields = y__})
  defMessage
    = JobResult'_constructor
        {_JobResult'body = Data.ProtoLens.fieldDefault,
         _JobResult'_unknownFields = []}
  parseMessage
    = let
        loop :: JobResult -> Data.ProtoLens.Encoding.Bytes.Parser JobResult
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
          (do loop Data.ProtoLens.defMessage) "JobResult"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"body") _x
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
instance Control.DeepSeq.NFData JobResult where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_JobResult'_unknownFields x__)
             (Control.DeepSeq.deepseq (_JobResult'body x__) ())
newtype JobState'UnrecognizedValue
  = JobState'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data JobState
  = JOB_STATE_UNSPECIFIED |
    JOB_STATE_ACCEPTED |
    JOB_STATE_RUNNING |
    JOB_STATE_SUCCEEDED |
    JOB_STATE_FAILED |
    JOB_STATE_CANCELLED |
    JobState'Unrecognized !JobState'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum JobState where
  maybeToEnum 0 = Prelude.Just JOB_STATE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just JOB_STATE_ACCEPTED
  maybeToEnum 2 = Prelude.Just JOB_STATE_RUNNING
  maybeToEnum 3 = Prelude.Just JOB_STATE_SUCCEEDED
  maybeToEnum 4 = Prelude.Just JOB_STATE_FAILED
  maybeToEnum 5 = Prelude.Just JOB_STATE_CANCELLED
  maybeToEnum k
    = Prelude.Just
        (JobState'Unrecognized
           (JobState'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum JOB_STATE_UNSPECIFIED = "JOB_STATE_UNSPECIFIED"
  showEnum JOB_STATE_ACCEPTED = "JOB_STATE_ACCEPTED"
  showEnum JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
  showEnum JOB_STATE_SUCCEEDED = "JOB_STATE_SUCCEEDED"
  showEnum JOB_STATE_FAILED = "JOB_STATE_FAILED"
  showEnum JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
  showEnum (JobState'Unrecognized (JobState'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "JOB_STATE_UNSPECIFIED"
    = Prelude.Just JOB_STATE_UNSPECIFIED
    | (Prelude.==) k "JOB_STATE_ACCEPTED"
    = Prelude.Just JOB_STATE_ACCEPTED
    | (Prelude.==) k "JOB_STATE_RUNNING"
    = Prelude.Just JOB_STATE_RUNNING
    | (Prelude.==) k "JOB_STATE_SUCCEEDED"
    = Prelude.Just JOB_STATE_SUCCEEDED
    | (Prelude.==) k "JOB_STATE_FAILED" = Prelude.Just JOB_STATE_FAILED
    | (Prelude.==) k "JOB_STATE_CANCELLED"
    = Prelude.Just JOB_STATE_CANCELLED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded JobState where
  minBound = JOB_STATE_UNSPECIFIED
  maxBound = JOB_STATE_CANCELLED
instance Prelude.Enum JobState where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum JobState: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum JOB_STATE_UNSPECIFIED = 0
  fromEnum JOB_STATE_ACCEPTED = 1
  fromEnum JOB_STATE_RUNNING = 2
  fromEnum JOB_STATE_SUCCEEDED = 3
  fromEnum JOB_STATE_FAILED = 4
  fromEnum JOB_STATE_CANCELLED = 5
  fromEnum (JobState'Unrecognized (JobState'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ JOB_STATE_CANCELLED
    = Prelude.error
        "JobState.succ: bad argument JOB_STATE_CANCELLED. This value would be out of bounds."
  succ JOB_STATE_UNSPECIFIED = JOB_STATE_ACCEPTED
  succ JOB_STATE_ACCEPTED = JOB_STATE_RUNNING
  succ JOB_STATE_RUNNING = JOB_STATE_SUCCEEDED
  succ JOB_STATE_SUCCEEDED = JOB_STATE_FAILED
  succ JOB_STATE_FAILED = JOB_STATE_CANCELLED
  succ (JobState'Unrecognized _)
    = Prelude.error "JobState.succ: bad argument: unrecognized value"
  pred JOB_STATE_UNSPECIFIED
    = Prelude.error
        "JobState.pred: bad argument JOB_STATE_UNSPECIFIED. This value would be out of bounds."
  pred JOB_STATE_ACCEPTED = JOB_STATE_UNSPECIFIED
  pred JOB_STATE_RUNNING = JOB_STATE_ACCEPTED
  pred JOB_STATE_SUCCEEDED = JOB_STATE_RUNNING
  pred JOB_STATE_FAILED = JOB_STATE_SUCCEEDED
  pred JOB_STATE_CANCELLED = JOB_STATE_FAILED
  pred (JobState'Unrecognized _)
    = Prelude.error "JobState.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault JobState where
  fieldDefault = JOB_STATE_UNSPECIFIED
instance Control.DeepSeq.NFData JobState where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.maybe'target' @:: Lens' JobTarget (Prelude.Maybe JobTarget'Target)@
         * 'Proto.Workers.V1.Workers_Fields.maybe'deploymentAlias' @:: Lens' JobTarget (Prelude.Maybe Data.Text.Text)@
         * 'Proto.Workers.V1.Workers_Fields.deploymentAlias' @:: Lens' JobTarget Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.maybe'versionSha256' @:: Lens' JobTarget (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Workers.V1.Workers_Fields.versionSha256' @:: Lens' JobTarget Data.ByteString.ByteString@ -}
data JobTarget
  = JobTarget'_constructor {_JobTarget'target :: !(Prelude.Maybe JobTarget'Target),
                            _JobTarget'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show JobTarget where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data JobTarget'Target
  = JobTarget'DeploymentAlias !Data.Text.Text |
    JobTarget'VersionSha256 !Data.ByteString.ByteString
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField JobTarget "maybe'target" (Prelude.Maybe JobTarget'Target) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobTarget'target (\ x__ y__ -> x__ {_JobTarget'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField JobTarget "maybe'deploymentAlias" (Prelude.Maybe Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobTarget'target (\ x__ y__ -> x__ {_JobTarget'target = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (JobTarget'DeploymentAlias x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap JobTarget'DeploymentAlias y__))
instance Data.ProtoLens.Field.HasField JobTarget "deploymentAlias" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobTarget'target (\ x__ y__ -> x__ {_JobTarget'target = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (JobTarget'DeploymentAlias x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap JobTarget'DeploymentAlias y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField JobTarget "maybe'versionSha256" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobTarget'target (\ x__ y__ -> x__ {_JobTarget'target = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (JobTarget'VersionSha256 x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap JobTarget'VersionSha256 y__))
instance Data.ProtoLens.Field.HasField JobTarget "versionSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _JobTarget'target (\ x__ y__ -> x__ {_JobTarget'target = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (JobTarget'VersionSha256 x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap JobTarget'VersionSha256 y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message JobTarget where
  messageName _ = Data.Text.pack "acyclic.workers.v1.JobTarget"
  packedMessageDescriptor _
    = "\n\
      \\tJobTarget\DC2+\n\
      \\DLEdeployment_alias\CAN\SOH \SOH(\tH\NULR\SIdeploymentAlias\DC2'\n\
      \\SOversion_sha256\CAN\STX \SOH(\fH\NULR\rversionSha256B\b\n\
      \\ACKtarget"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        deploymentAlias__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "deployment_alias"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'deploymentAlias")) ::
              Data.ProtoLens.FieldDescriptor JobTarget
        versionSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'versionSha256")) ::
              Data.ProtoLens.FieldDescriptor JobTarget
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, deploymentAlias__field_descriptor),
           (Data.ProtoLens.Tag 2, versionSha256__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _JobTarget'_unknownFields
        (\ x__ y__ -> x__ {_JobTarget'_unknownFields = y__})
  defMessage
    = JobTarget'_constructor
        {_JobTarget'target = Prelude.Nothing,
         _JobTarget'_unknownFields = []}
  parseMessage
    = let
        loop :: JobTarget -> Data.ProtoLens.Encoding.Bytes.Parser JobTarget
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
                                       "deployment_alias"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"deploymentAlias") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "version_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"versionSha256") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "JobTarget"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'target") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (JobTarget'DeploymentAlias v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.Text.Encoding.encodeUtf8 v)
                (Prelude.Just (JobTarget'VersionSha256 v))
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
instance Control.DeepSeq.NFData JobTarget where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_JobTarget'_unknownFields x__)
             (Control.DeepSeq.deepseq (_JobTarget'target x__) ())
instance Control.DeepSeq.NFData JobTarget'Target where
  rnf (JobTarget'DeploymentAlias x__) = Control.DeepSeq.rnf x__
  rnf (JobTarget'VersionSha256 x__) = Control.DeepSeq.rnf x__
_JobTarget'DeploymentAlias ::
  Data.ProtoLens.Prism.Prism' JobTarget'Target Data.Text.Text
_JobTarget'DeploymentAlias
  = Data.ProtoLens.Prism.prism'
      JobTarget'DeploymentAlias
      (\ p__
         -> case p__ of
              (JobTarget'DeploymentAlias p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_JobTarget'VersionSha256 ::
  Data.ProtoLens.Prism.Prism' JobTarget'Target Data.ByteString.ByteString
_JobTarget'VersionSha256
  = Data.ProtoLens.Prism.prism'
      JobTarget'VersionSha256
      (\ p__
         -> case p__ of
              (JobTarget'VersionSha256 p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.bucket' @:: Lens' ObjectRef Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.key' @:: Lens' ObjectRef Data.Text.Text@ -}
data ObjectRef
  = ObjectRef'_constructor {_ObjectRef'bucket :: !Data.Text.Text,
                            _ObjectRef'key :: !Data.Text.Text,
                            _ObjectRef'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ObjectRef where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ObjectRef "bucket" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectRef'bucket (\ x__ y__ -> x__ {_ObjectRef'bucket = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ObjectRef "key" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ObjectRef'key (\ x__ y__ -> x__ {_ObjectRef'key = y__}))
        Prelude.id
instance Data.ProtoLens.Message ObjectRef where
  messageName _ = Data.Text.pack "acyclic.workers.v1.ObjectRef"
  packedMessageDescriptor _
    = "\n\
      \\tObjectRef\DC2\SYN\n\
      \\ACKbucket\CAN\SOH \SOH(\tR\ACKbucket\DC2\DLE\n\
      \\ETXkey\CAN\STX \SOH(\tR\ETXkeyJ\EOT\b\ETX\DLE\EOTR\n\
      \version_id"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        bucket__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bucket"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"bucket")) ::
              Data.ProtoLens.FieldDescriptor ObjectRef
        key__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"key")) ::
              Data.ProtoLens.FieldDescriptor ObjectRef
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, bucket__field_descriptor),
           (Data.ProtoLens.Tag 2, key__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ObjectRef'_unknownFields
        (\ x__ y__ -> x__ {_ObjectRef'_unknownFields = y__})
  defMessage
    = ObjectRef'_constructor
        {_ObjectRef'bucket = Data.ProtoLens.fieldDefault,
         _ObjectRef'key = Data.ProtoLens.fieldDefault,
         _ObjectRef'_unknownFields = []}
  parseMessage
    = let
        loop :: ObjectRef -> Data.ProtoLens.Encoding.Bytes.Parser ObjectRef
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
                                       "bucket"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"bucket") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "key"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"key") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ObjectRef"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"bucket") _x
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"key") _x
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
instance Control.DeepSeq.NFData ObjectRef where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ObjectRef'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ObjectRef'bucket x__)
                (Control.DeepSeq.deepseq (_ObjectRef'key x__) ()))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.maybe'source' @:: Lens' Payload (Prelude.Maybe Payload'Source)@
         * 'Proto.Workers.V1.Workers_Fields.maybe'inlineBytes' @:: Lens' Payload (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Workers.V1.Workers_Fields.inlineBytes' @:: Lens' Payload Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.maybe'object' @:: Lens' Payload (Prelude.Maybe ObjectRef)@
         * 'Proto.Workers.V1.Workers_Fields.object' @:: Lens' Payload ObjectRef@ -}
data Payload
  = Payload'_constructor {_Payload'source :: !(Prelude.Maybe Payload'Source),
                          _Payload'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Payload where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data Payload'Source
  = Payload'InlineBytes !Data.ByteString.ByteString |
    Payload'Object !ObjectRef
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField Payload "maybe'source" (Prelude.Maybe Payload'Source) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Payload'source (\ x__ y__ -> x__ {_Payload'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Payload "maybe'inlineBytes" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Payload'source (\ x__ y__ -> x__ {_Payload'source = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Payload'InlineBytes x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Payload'InlineBytes y__))
instance Data.ProtoLens.Field.HasField Payload "inlineBytes" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Payload'source (\ x__ y__ -> x__ {_Payload'source = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Payload'InlineBytes x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Payload'InlineBytes y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField Payload "maybe'object" (Prelude.Maybe ObjectRef) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Payload'source (\ x__ y__ -> x__ {_Payload'source = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Payload'Object x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Payload'Object y__))
instance Data.ProtoLens.Field.HasField Payload "object" ObjectRef where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Payload'source (\ x__ y__ -> x__ {_Payload'source = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Payload'Object x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Payload'Object y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message Payload where
  messageName _ = Data.Text.pack "acyclic.workers.v1.Payload"
  packedMessageDescriptor _
    = "\n\
      \\aPayload\DC2#\n\
      \\finline_bytes\CAN\SOH \SOH(\fH\NULR\vinlineBytes\DC27\n\
      \\ACKobject\CAN\STX \SOH(\v2\GS.acyclic.workers.v1.ObjectRefH\NULR\ACKobjectB\b\n\
      \\ACKsource"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        inlineBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "inline_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'inlineBytes")) ::
              Data.ProtoLens.FieldDescriptor Payload
        object__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "object"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ObjectRef)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'object")) ::
              Data.ProtoLens.FieldDescriptor Payload
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, inlineBytes__field_descriptor),
           (Data.ProtoLens.Tag 2, object__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Payload'_unknownFields
        (\ x__ y__ -> x__ {_Payload'_unknownFields = y__})
  defMessage
    = Payload'_constructor
        {_Payload'source = Prelude.Nothing, _Payload'_unknownFields = []}
  parseMessage
    = let
        loop :: Payload -> Data.ProtoLens.Encoding.Bytes.Parser Payload
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
                                       "inline_bytes"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"inlineBytes") y x)
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
          (do loop Data.ProtoLens.defMessage) "Payload"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'source") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (Payload'InlineBytes v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((\ bs
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                   (Prelude.fromIntegral (Data.ByteString.length bs)))
                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          v)
                (Prelude.Just (Payload'Object v))
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
instance Control.DeepSeq.NFData Payload where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Payload'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Payload'source x__) ())
instance Control.DeepSeq.NFData Payload'Source where
  rnf (Payload'InlineBytes x__) = Control.DeepSeq.rnf x__
  rnf (Payload'Object x__) = Control.DeepSeq.rnf x__
_Payload'InlineBytes ::
  Data.ProtoLens.Prism.Prism' Payload'Source Data.ByteString.ByteString
_Payload'InlineBytes
  = Data.ProtoLens.Prism.prism'
      Payload'InlineBytes
      (\ p__
         -> case p__ of
              (Payload'InlineBytes p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Payload'Object ::
  Data.ProtoLens.Prism.Prism' Payload'Source ObjectRef
_Payload'Object
  = Data.ProtoLens.Prism.prism'
      Payload'Object
      (\ p__
         -> case p__ of
              (Payload'Object p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.javascriptModule' @:: Lens' PublishVersionRequest Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.expectedSha256' @:: Lens' PublishVersionRequest Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.idempotencyKey' @:: Lens' PublishVersionRequest Data.Text.Text@ -}
data PublishVersionRequest
  = PublishVersionRequest'_constructor {_PublishVersionRequest'javascriptModule :: !Data.ByteString.ByteString,
                                        _PublishVersionRequest'expectedSha256 :: !Data.ByteString.ByteString,
                                        _PublishVersionRequest'idempotencyKey :: !Data.Text.Text,
                                        _PublishVersionRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show PublishVersionRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField PublishVersionRequest "javascriptModule" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PublishVersionRequest'javascriptModule
           (\ x__ y__ -> x__ {_PublishVersionRequest'javascriptModule = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PublishVersionRequest "expectedSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PublishVersionRequest'expectedSha256
           (\ x__ y__ -> x__ {_PublishVersionRequest'expectedSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PublishVersionRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PublishVersionRequest'idempotencyKey
           (\ x__ y__ -> x__ {_PublishVersionRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message PublishVersionRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.PublishVersionRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKPublishVersionRequest\DC2+\n\
      \\DC1javascript_module\CAN\SOH \SOH(\fR\DLEjavascriptModule\DC2'\n\
      \\SIexpected_sha256\CAN\STX \SOH(\fR\SOexpectedSha256\DC2'\n\
      \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        javascriptModule__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "javascript_module"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"javascriptModule")) ::
              Data.ProtoLens.FieldDescriptor PublishVersionRequest
        expectedSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expected_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expectedSha256")) ::
              Data.ProtoLens.FieldDescriptor PublishVersionRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor PublishVersionRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, javascriptModule__field_descriptor),
           (Data.ProtoLens.Tag 2, expectedSha256__field_descriptor),
           (Data.ProtoLens.Tag 3, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _PublishVersionRequest'_unknownFields
        (\ x__ y__ -> x__ {_PublishVersionRequest'_unknownFields = y__})
  defMessage
    = PublishVersionRequest'_constructor
        {_PublishVersionRequest'javascriptModule = Data.ProtoLens.fieldDefault,
         _PublishVersionRequest'expectedSha256 = Data.ProtoLens.fieldDefault,
         _PublishVersionRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _PublishVersionRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          PublishVersionRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser PublishVersionRequest
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
                                       "javascript_module"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"javascriptModule") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "expected_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"expectedSha256") y x)
                        26
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
          (do loop Data.ProtoLens.defMessage) "PublishVersionRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"javascriptModule") _x
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
                         (Data.ProtoLens.Field.field @"expectedSha256") _x
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
                      _v
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"idempotencyKey") _x
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
instance Control.DeepSeq.NFData PublishVersionRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_PublishVersionRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_PublishVersionRequest'javascriptModule x__)
                (Control.DeepSeq.deepseq
                   (_PublishVersionRequest'expectedSha256 x__)
                   (Control.DeepSeq.deepseq
                      (_PublishVersionRequest'idempotencyKey x__) ())))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.version' @:: Lens' PublishVersionResponse CodeVersion@
         * 'Proto.Workers.V1.Workers_Fields.maybe'version' @:: Lens' PublishVersionResponse (Prelude.Maybe CodeVersion)@ -}
data PublishVersionResponse
  = PublishVersionResponse'_constructor {_PublishVersionResponse'version :: !(Prelude.Maybe CodeVersion),
                                         _PublishVersionResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show PublishVersionResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField PublishVersionResponse "version" CodeVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PublishVersionResponse'version
           (\ x__ y__ -> x__ {_PublishVersionResponse'version = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PublishVersionResponse "maybe'version" (Prelude.Maybe CodeVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PublishVersionResponse'version
           (\ x__ y__ -> x__ {_PublishVersionResponse'version = y__}))
        Prelude.id
instance Data.ProtoLens.Message PublishVersionResponse where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.PublishVersionResponse"
  packedMessageDescriptor _
    = "\n\
      \\SYNPublishVersionResponse\DC29\n\
      \\aversion\CAN\SOH \SOH(\v2\US.acyclic.workers.v1.CodeVersionR\aversion"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CodeVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'version")) ::
              Data.ProtoLens.FieldDescriptor PublishVersionResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, version__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _PublishVersionResponse'_unknownFields
        (\ x__ y__ -> x__ {_PublishVersionResponse'_unknownFields = y__})
  defMessage
    = PublishVersionResponse'_constructor
        {_PublishVersionResponse'version = Prelude.Nothing,
         _PublishVersionResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          PublishVersionResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser PublishVersionResponse
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
          (do loop Data.ProtoLens.defMessage) "PublishVersionResponse"
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
instance Control.DeepSeq.NFData PublishVersionResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_PublishVersionResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_PublishVersionResponse'version x__) ())
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.maxAttempts' @:: Lens' RetryPolicy Data.Word.Word32@
         * 'Proto.Workers.V1.Workers_Fields.backoffMillis' @:: Lens' RetryPolicy Data.Word.Word64@ -}
data RetryPolicy
  = RetryPolicy'_constructor {_RetryPolicy'maxAttempts :: !Data.Word.Word32,
                              _RetryPolicy'backoffMillis :: !Data.Word.Word64,
                              _RetryPolicy'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RetryPolicy where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RetryPolicy "maxAttempts" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetryPolicy'maxAttempts
           (\ x__ y__ -> x__ {_RetryPolicy'maxAttempts = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetryPolicy "backoffMillis" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetryPolicy'backoffMillis
           (\ x__ y__ -> x__ {_RetryPolicy'backoffMillis = y__}))
        Prelude.id
instance Data.ProtoLens.Message RetryPolicy where
  messageName _ = Data.Text.pack "acyclic.workers.v1.RetryPolicy"
  packedMessageDescriptor _
    = "\n\
      \\vRetryPolicy\DC2!\n\
      \\fmax_attempts\CAN\SOH \SOH(\rR\vmaxAttempts\DC2%\n\
      \\SObackoff_millis\CAN\STX \SOH(\EOTR\rbackoffMillis"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        maxAttempts__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "max_attempts"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maxAttempts")) ::
              Data.ProtoLens.FieldDescriptor RetryPolicy
        backoffMillis__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "backoff_millis"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"backoffMillis")) ::
              Data.ProtoLens.FieldDescriptor RetryPolicy
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, maxAttempts__field_descriptor),
           (Data.ProtoLens.Tag 2, backoffMillis__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RetryPolicy'_unknownFields
        (\ x__ y__ -> x__ {_RetryPolicy'_unknownFields = y__})
  defMessage
    = RetryPolicy'_constructor
        {_RetryPolicy'maxAttempts = Data.ProtoLens.fieldDefault,
         _RetryPolicy'backoffMillis = Data.ProtoLens.fieldDefault,
         _RetryPolicy'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RetryPolicy -> Data.ProtoLens.Encoding.Bytes.Parser RetryPolicy
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
                                       "max_attempts"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"maxAttempts") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "backoff_millis"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"backoffMillis") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RetryPolicy"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"maxAttempts") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"backoffMillis") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData RetryPolicy where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RetryPolicy'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RetryPolicy'maxAttempts x__)
                (Control.DeepSeq.deepseq (_RetryPolicy'backoffMillis x__) ()))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.alias' @:: Lens' SelectDeploymentRequest Data.Text.Text@
         * 'Proto.Workers.V1.Workers_Fields.versionSha256' @:: Lens' SelectDeploymentRequest Data.ByteString.ByteString@
         * 'Proto.Workers.V1.Workers_Fields.expectedRevision' @:: Lens' SelectDeploymentRequest Data.Word.Word64@
         * 'Proto.Workers.V1.Workers_Fields.maybe'expectedRevision' @:: Lens' SelectDeploymentRequest (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Workers.V1.Workers_Fields.idempotencyKey' @:: Lens' SelectDeploymentRequest Data.Text.Text@ -}
data SelectDeploymentRequest
  = SelectDeploymentRequest'_constructor {_SelectDeploymentRequest'alias :: !Data.Text.Text,
                                          _SelectDeploymentRequest'versionSha256 :: !Data.ByteString.ByteString,
                                          _SelectDeploymentRequest'expectedRevision :: !(Prelude.Maybe Data.Word.Word64),
                                          _SelectDeploymentRequest'idempotencyKey :: !Data.Text.Text,
                                          _SelectDeploymentRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SelectDeploymentRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SelectDeploymentRequest "alias" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentRequest'alias
           (\ x__ y__ -> x__ {_SelectDeploymentRequest'alias = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SelectDeploymentRequest "versionSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentRequest'versionSha256
           (\ x__ y__ -> x__ {_SelectDeploymentRequest'versionSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SelectDeploymentRequest "expectedRevision" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentRequest'expectedRevision
           (\ x__ y__
              -> x__ {_SelectDeploymentRequest'expectedRevision = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField SelectDeploymentRequest "maybe'expectedRevision" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentRequest'expectedRevision
           (\ x__ y__
              -> x__ {_SelectDeploymentRequest'expectedRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SelectDeploymentRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentRequest'idempotencyKey
           (\ x__ y__ -> x__ {_SelectDeploymentRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message SelectDeploymentRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.SelectDeploymentRequest"
  packedMessageDescriptor _
    = "\n\
      \\ETBSelectDeploymentRequest\DC2\DC4\n\
      \\ENQalias\CAN\SOH \SOH(\tR\ENQalias\DC2%\n\
      \\SOversion_sha256\CAN\STX \SOH(\fR\rversionSha256\DC20\n\
      \\DC1expected_revision\CAN\ETX \SOH(\EOTH\NULR\DLEexpectedRevision\136\SOH\SOH\DC2'\n\
      \\SIidempotency_key\CAN\EOT \SOH(\tR\SOidempotencyKeyB\DC4\n\
      \\DC2_expected_revision"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        alias__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "alias"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"alias")) ::
              Data.ProtoLens.FieldDescriptor SelectDeploymentRequest
        versionSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"versionSha256")) ::
              Data.ProtoLens.FieldDescriptor SelectDeploymentRequest
        expectedRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expected_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'expectedRevision")) ::
              Data.ProtoLens.FieldDescriptor SelectDeploymentRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor SelectDeploymentRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, alias__field_descriptor),
           (Data.ProtoLens.Tag 2, versionSha256__field_descriptor),
           (Data.ProtoLens.Tag 3, expectedRevision__field_descriptor),
           (Data.ProtoLens.Tag 4, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SelectDeploymentRequest'_unknownFields
        (\ x__ y__ -> x__ {_SelectDeploymentRequest'_unknownFields = y__})
  defMessage
    = SelectDeploymentRequest'_constructor
        {_SelectDeploymentRequest'alias = Data.ProtoLens.fieldDefault,
         _SelectDeploymentRequest'versionSha256 = Data.ProtoLens.fieldDefault,
         _SelectDeploymentRequest'expectedRevision = Prelude.Nothing,
         _SelectDeploymentRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _SelectDeploymentRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SelectDeploymentRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser SelectDeploymentRequest
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
                                       "alias"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"alias") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "version_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"versionSha256") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "expected_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"expectedRevision") y x)
                        34
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
          (do loop Data.ProtoLens.defMessage) "SelectDeploymentRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"alias") _x
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
                         (Data.ProtoLens.Field.field @"versionSha256") _x
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
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'expectedRevision") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
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
instance Control.DeepSeq.NFData SelectDeploymentRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SelectDeploymentRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SelectDeploymentRequest'alias x__)
                (Control.DeepSeq.deepseq
                   (_SelectDeploymentRequest'versionSha256 x__)
                   (Control.DeepSeq.deepseq
                      (_SelectDeploymentRequest'expectedRevision x__)
                      (Control.DeepSeq.deepseq
                         (_SelectDeploymentRequest'idempotencyKey x__) ()))))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.deployment' @:: Lens' SelectDeploymentResponse Deployment@
         * 'Proto.Workers.V1.Workers_Fields.maybe'deployment' @:: Lens' SelectDeploymentResponse (Prelude.Maybe Deployment)@ -}
data SelectDeploymentResponse
  = SelectDeploymentResponse'_constructor {_SelectDeploymentResponse'deployment :: !(Prelude.Maybe Deployment),
                                           _SelectDeploymentResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SelectDeploymentResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SelectDeploymentResponse "deployment" Deployment where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentResponse'deployment
           (\ x__ y__ -> x__ {_SelectDeploymentResponse'deployment = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SelectDeploymentResponse "maybe'deployment" (Prelude.Maybe Deployment) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SelectDeploymentResponse'deployment
           (\ x__ y__ -> x__ {_SelectDeploymentResponse'deployment = y__}))
        Prelude.id
instance Data.ProtoLens.Message SelectDeploymentResponse where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.SelectDeploymentResponse"
  packedMessageDescriptor _
    = "\n\
      \\CANSelectDeploymentResponse\DC2>\n\
      \\n\
      \deployment\CAN\SOH \SOH(\v2\RS.acyclic.workers.v1.DeploymentR\n\
      \deployment"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        deployment__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "deployment"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Deployment)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'deployment")) ::
              Data.ProtoLens.FieldDescriptor SelectDeploymentResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, deployment__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SelectDeploymentResponse'_unknownFields
        (\ x__ y__ -> x__ {_SelectDeploymentResponse'_unknownFields = y__})
  defMessage
    = SelectDeploymentResponse'_constructor
        {_SelectDeploymentResponse'deployment = Prelude.Nothing,
         _SelectDeploymentResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SelectDeploymentResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser SelectDeploymentResponse
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
                                       "deployment"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"deployment") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SelectDeploymentResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'deployment") _x
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
instance Control.DeepSeq.NFData SelectDeploymentResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SelectDeploymentResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SelectDeploymentResponse'deployment x__) ())
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.target' @:: Lens' SubmitJobRequest JobTarget@
         * 'Proto.Workers.V1.Workers_Fields.maybe'target' @:: Lens' SubmitJobRequest (Prelude.Maybe JobTarget)@
         * 'Proto.Workers.V1.Workers_Fields.input' @:: Lens' SubmitJobRequest Payload@
         * 'Proto.Workers.V1.Workers_Fields.maybe'input' @:: Lens' SubmitJobRequest (Prelude.Maybe Payload)@
         * 'Proto.Workers.V1.Workers_Fields.limits' @:: Lens' SubmitJobRequest JobLimits@
         * 'Proto.Workers.V1.Workers_Fields.maybe'limits' @:: Lens' SubmitJobRequest (Prelude.Maybe JobLimits)@
         * 'Proto.Workers.V1.Workers_Fields.retry' @:: Lens' SubmitJobRequest RetryPolicy@
         * 'Proto.Workers.V1.Workers_Fields.maybe'retry' @:: Lens' SubmitJobRequest (Prelude.Maybe RetryPolicy)@
         * 'Proto.Workers.V1.Workers_Fields.idempotencyKey' @:: Lens' SubmitJobRequest Data.Text.Text@ -}
data SubmitJobRequest
  = SubmitJobRequest'_constructor {_SubmitJobRequest'target :: !(Prelude.Maybe JobTarget),
                                   _SubmitJobRequest'input :: !(Prelude.Maybe Payload),
                                   _SubmitJobRequest'limits :: !(Prelude.Maybe JobLimits),
                                   _SubmitJobRequest'retry :: !(Prelude.Maybe RetryPolicy),
                                   _SubmitJobRequest'idempotencyKey :: !Data.Text.Text,
                                   _SubmitJobRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SubmitJobRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SubmitJobRequest "target" JobTarget where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'target
           (\ x__ y__ -> x__ {_SubmitJobRequest'target = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SubmitJobRequest "maybe'target" (Prelude.Maybe JobTarget) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'target
           (\ x__ y__ -> x__ {_SubmitJobRequest'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubmitJobRequest "input" Payload where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'input
           (\ x__ y__ -> x__ {_SubmitJobRequest'input = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SubmitJobRequest "maybe'input" (Prelude.Maybe Payload) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'input
           (\ x__ y__ -> x__ {_SubmitJobRequest'input = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubmitJobRequest "limits" JobLimits where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'limits
           (\ x__ y__ -> x__ {_SubmitJobRequest'limits = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SubmitJobRequest "maybe'limits" (Prelude.Maybe JobLimits) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'limits
           (\ x__ y__ -> x__ {_SubmitJobRequest'limits = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubmitJobRequest "retry" RetryPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'retry
           (\ x__ y__ -> x__ {_SubmitJobRequest'retry = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SubmitJobRequest "maybe'retry" (Prelude.Maybe RetryPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'retry
           (\ x__ y__ -> x__ {_SubmitJobRequest'retry = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubmitJobRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobRequest'idempotencyKey
           (\ x__ y__ -> x__ {_SubmitJobRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message SubmitJobRequest where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.SubmitJobRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLESubmitJobRequest\DC25\n\
      \\ACKtarget\CAN\SOH \SOH(\v2\GS.acyclic.workers.v1.JobTargetR\ACKtarget\DC21\n\
      \\ENQinput\CAN\STX \SOH(\v2\ESC.acyclic.workers.v1.PayloadR\ENQinput\DC25\n\
      \\ACKlimits\CAN\ETX \SOH(\v2\GS.acyclic.workers.v1.JobLimitsR\ACKlimits\DC25\n\
      \\ENQretry\CAN\EOT \SOH(\v2\US.acyclic.workers.v1.RetryPolicyR\ENQretry\DC2'\n\
      \\SIidempotency_key\CAN\ENQ \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        target__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "target"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor JobTarget)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'target")) ::
              Data.ProtoLens.FieldDescriptor SubmitJobRequest
        input__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "input"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Payload)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'input")) ::
              Data.ProtoLens.FieldDescriptor SubmitJobRequest
        limits__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limits"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor JobLimits)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'limits")) ::
              Data.ProtoLens.FieldDescriptor SubmitJobRequest
        retry__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "retry"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RetryPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'retry")) ::
              Data.ProtoLens.FieldDescriptor SubmitJobRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor SubmitJobRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, target__field_descriptor),
           (Data.ProtoLens.Tag 2, input__field_descriptor),
           (Data.ProtoLens.Tag 3, limits__field_descriptor),
           (Data.ProtoLens.Tag 4, retry__field_descriptor),
           (Data.ProtoLens.Tag 5, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SubmitJobRequest'_unknownFields
        (\ x__ y__ -> x__ {_SubmitJobRequest'_unknownFields = y__})
  defMessage
    = SubmitJobRequest'_constructor
        {_SubmitJobRequest'target = Prelude.Nothing,
         _SubmitJobRequest'input = Prelude.Nothing,
         _SubmitJobRequest'limits = Prelude.Nothing,
         _SubmitJobRequest'retry = Prelude.Nothing,
         _SubmitJobRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _SubmitJobRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SubmitJobRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser SubmitJobRequest
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "input"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"input") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "limits"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"limits") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "retry"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"retry") y x)
                        42
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
          (do loop Data.ProtoLens.defMessage) "SubmitJobRequest"
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'input") _x
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
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'limits") _x
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
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'retry") _x
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
                         (let
                            _v
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"idempotencyKey") _x
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
instance Control.DeepSeq.NFData SubmitJobRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SubmitJobRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SubmitJobRequest'target x__)
                (Control.DeepSeq.deepseq
                   (_SubmitJobRequest'input x__)
                   (Control.DeepSeq.deepseq
                      (_SubmitJobRequest'limits x__)
                      (Control.DeepSeq.deepseq
                         (_SubmitJobRequest'retry x__)
                         (Control.DeepSeq.deepseq
                            (_SubmitJobRequest'idempotencyKey x__) ())))))
{- | Fields :
     
         * 'Proto.Workers.V1.Workers_Fields.job' @:: Lens' SubmitJobResponse JobObservation@
         * 'Proto.Workers.V1.Workers_Fields.maybe'job' @:: Lens' SubmitJobResponse (Prelude.Maybe JobObservation)@ -}
data SubmitJobResponse
  = SubmitJobResponse'_constructor {_SubmitJobResponse'job :: !(Prelude.Maybe JobObservation),
                                    _SubmitJobResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SubmitJobResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SubmitJobResponse "job" JobObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobResponse'job
           (\ x__ y__ -> x__ {_SubmitJobResponse'job = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SubmitJobResponse "maybe'job" (Prelude.Maybe JobObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubmitJobResponse'job
           (\ x__ y__ -> x__ {_SubmitJobResponse'job = y__}))
        Prelude.id
instance Data.ProtoLens.Message SubmitJobResponse where
  messageName _
    = Data.Text.pack "acyclic.workers.v1.SubmitJobResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1SubmitJobResponse\DC24\n\
      \\ETXjob\CAN\SOH \SOH(\v2\".acyclic.workers.v1.JobObservationR\ETXjob"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        job__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "job"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor JobObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'job")) ::
              Data.ProtoLens.FieldDescriptor SubmitJobResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, job__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SubmitJobResponse'_unknownFields
        (\ x__ y__ -> x__ {_SubmitJobResponse'_unknownFields = y__})
  defMessage
    = SubmitJobResponse'_constructor
        {_SubmitJobResponse'job = Prelude.Nothing,
         _SubmitJobResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SubmitJobResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser SubmitJobResponse
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
                                       "job"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"job") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SubmitJobResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'job") _x
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
instance Control.DeepSeq.NFData SubmitJobResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SubmitJobResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_SubmitJobResponse'job x__) ())
data WorkersService = WorkersService {}
instance Data.ProtoLens.Service.Types.Service WorkersService where
  type ServiceName WorkersService = "WorkersService"
  type ServicePackage WorkersService = "acyclic.workers.v1"
  type ServiceMethods WorkersService = '["cancelJob",
                                         "inspectJob",
                                         "invokeDeployment",
                                         "invokeVersion",
                                         "publishVersion",
                                         "selectDeployment",
                                         "submitJob"]
  packedServiceDescriptor _
    = "\n\
      \\SOWorkersService\DC2g\n\
      \\SOPublishVersion\DC2).acyclic.workers.v1.PublishVersionRequest\SUB*.acyclic.workers.v1.PublishVersionResponse\DC2m\n\
      \\DLESelectDeployment\DC2+.acyclic.workers.v1.SelectDeploymentRequest\SUB,.acyclic.workers.v1.SelectDeploymentResponse\DC2X\n\
      \\tSubmitJob\DC2$.acyclic.workers.v1.SubmitJobRequest\SUB%.acyclic.workers.v1.SubmitJobResponse\DC2[\n\
      \\n\
      \InspectJob\DC2%.acyclic.workers.v1.InspectJobRequest\SUB&.acyclic.workers.v1.InspectJobResponse\DC2X\n\
      \\tCancelJob\DC2$.acyclic.workers.v1.CancelJobRequest\SUB%.acyclic.workers.v1.CancelJobResponse\DC2]\n\
      \\rInvokeVersion\DC2(.acyclic.workers.v1.InvokeVersionRequest\SUB\".acyclic.workers.v1.InvokeResponse\DC2c\n\
      \\DLEInvokeDeployment\DC2+.acyclic.workers.v1.InvokeDeploymentRequest\SUB\".acyclic.workers.v1.InvokeResponse"
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "publishVersion" where
  type MethodName WorkersService "publishVersion" = "PublishVersion"
  type MethodInput WorkersService "publishVersion" = PublishVersionRequest
  type MethodOutput WorkersService "publishVersion" = PublishVersionResponse
  type MethodStreamingType WorkersService "publishVersion" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "selectDeployment" where
  type MethodName WorkersService "selectDeployment" = "SelectDeployment"
  type MethodInput WorkersService "selectDeployment" = SelectDeploymentRequest
  type MethodOutput WorkersService "selectDeployment" = SelectDeploymentResponse
  type MethodStreamingType WorkersService "selectDeployment" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "submitJob" where
  type MethodName WorkersService "submitJob" = "SubmitJob"
  type MethodInput WorkersService "submitJob" = SubmitJobRequest
  type MethodOutput WorkersService "submitJob" = SubmitJobResponse
  type MethodStreamingType WorkersService "submitJob" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "inspectJob" where
  type MethodName WorkersService "inspectJob" = "InspectJob"
  type MethodInput WorkersService "inspectJob" = InspectJobRequest
  type MethodOutput WorkersService "inspectJob" = InspectJobResponse
  type MethodStreamingType WorkersService "inspectJob" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "cancelJob" where
  type MethodName WorkersService "cancelJob" = "CancelJob"
  type MethodInput WorkersService "cancelJob" = CancelJobRequest
  type MethodOutput WorkersService "cancelJob" = CancelJobResponse
  type MethodStreamingType WorkersService "cancelJob" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "invokeVersion" where
  type MethodName WorkersService "invokeVersion" = "InvokeVersion"
  type MethodInput WorkersService "invokeVersion" = InvokeVersionRequest
  type MethodOutput WorkersService "invokeVersion" = InvokeResponse
  type MethodStreamingType WorkersService "invokeVersion" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WorkersService "invokeDeployment" where
  type MethodName WorkersService "invokeDeployment" = "InvokeDeployment"
  type MethodInput WorkersService "invokeDeployment" = InvokeDeploymentRequest
  type MethodOutput WorkersService "invokeDeployment" = InvokeResponse
  type MethodStreamingType WorkersService "invokeDeployment" = 'Data.ProtoLens.Service.Types.NonStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\CANworkers/v1/workers.proto\DC2\DC2acyclic.workers.v1\"D\n\
    \\vCodeVersion\DC2\SYN\n\
    \\ACKsha256\CAN\SOH \SOH(\fR\ACKsha256\DC2\GS\n\
    \\n\
    \size_bytes\CAN\STX \SOH(\EOTR\tsizeBytes\"y\n\
    \\n\
    \Deployment\DC2\DC4\n\
    \\ENQalias\CAN\SOH \SOH(\tR\ENQalias\DC29\n\
    \\aversion\CAN\STX \SOH(\v2\US.acyclic.workers.v1.CodeVersionR\aversion\DC2\SUB\n\
    \\brevision\CAN\ETX \SOH(\EOTR\brevision\"\150\SOH\n\
    \\NAKPublishVersionRequest\DC2+\n\
    \\DC1javascript_module\CAN\SOH \SOH(\fR\DLEjavascriptModule\DC2'\n\
    \\SIexpected_sha256\CAN\STX \SOH(\fR\SOexpectedSha256\DC2'\n\
    \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey\"S\n\
    \\SYNPublishVersionResponse\DC29\n\
    \\aversion\CAN\SOH \SOH(\v2\US.acyclic.workers.v1.CodeVersionR\aversion\"\199\SOH\n\
    \\ETBSelectDeploymentRequest\DC2\DC4\n\
    \\ENQalias\CAN\SOH \SOH(\tR\ENQalias\DC2%\n\
    \\SOversion_sha256\CAN\STX \SOH(\fR\rversionSha256\DC20\n\
    \\DC1expected_revision\CAN\ETX \SOH(\EOTH\NULR\DLEexpectedRevision\136\SOH\SOH\DC2'\n\
    \\SIidempotency_key\CAN\EOT \SOH(\tR\SOidempotencyKeyB\DC4\n\
    \\DC2_expected_revision\"Z\n\
    \\CANSelectDeploymentResponse\DC2>\n\
    \\n\
    \deployment\CAN\SOH \SOH(\v2\RS.acyclic.workers.v1.DeploymentR\n\
    \deployment\"G\n\
    \\tObjectRef\DC2\SYN\n\
    \\ACKbucket\CAN\SOH \SOH(\tR\ACKbucket\DC2\DLE\n\
    \\ETXkey\CAN\STX \SOH(\tR\ETXkeyJ\EOT\b\ETX\DLE\EOTR\n\
    \version_id\"q\n\
    \\aPayload\DC2#\n\
    \\finline_bytes\CAN\SOH \SOH(\fH\NULR\vinlineBytes\DC27\n\
    \\ACKobject\CAN\STX \SOH(\v2\GS.acyclic.workers.v1.ObjectRefH\NULR\ACKobjectB\b\n\
    \\ACKsource\"5\n\
    \\tJobResult\DC2\DC2\n\
    \\EOTbody\CAN\SOH \SOH(\fR\EOTbodyJ\EOT\b\STX\DLE\ETXR\SOobject_version\"x\n\
    \\tJobLimits\DC2%\n\
    \\SOtimeout_millis\CAN\SOH \SOH(\EOTR\rtimeoutMillis\DC2!\n\
    \\fmemory_bytes\CAN\STX \SOH(\EOTR\vmemoryBytes\DC2!\n\
    \\foutput_bytes\CAN\ETX \SOH(\EOTR\voutputBytes\"W\n\
    \\vRetryPolicy\DC2!\n\
    \\fmax_attempts\CAN\SOH \SOH(\rR\vmaxAttempts\DC2%\n\
    \\SObackoff_millis\CAN\STX \SOH(\EOTR\rbackoffMillis\"k\n\
    \\tJobTarget\DC2+\n\
    \\DLEdeployment_alias\CAN\SOH \SOH(\tH\NULR\SIdeploymentAlias\DC2'\n\
    \\SOversion_sha256\CAN\STX \SOH(\fH\NULR\rversionSha256B\b\n\
    \\ACKtarget\"\147\STX\n\
    \\DLESubmitJobRequest\DC25\n\
    \\ACKtarget\CAN\SOH \SOH(\v2\GS.acyclic.workers.v1.JobTargetR\ACKtarget\DC21\n\
    \\ENQinput\CAN\STX \SOH(\v2\ESC.acyclic.workers.v1.PayloadR\ENQinput\DC25\n\
    \\ACKlimits\CAN\ETX \SOH(\v2\GS.acyclic.workers.v1.JobLimitsR\ACKlimits\DC25\n\
    \\ENQretry\CAN\EOT \SOH(\v2\US.acyclic.workers.v1.RetryPolicyR\ENQretry\DC2'\n\
    \\SIidempotency_key\CAN\ENQ \SOH(\tR\SOidempotencyKey\"I\n\
    \\DC1SubmitJobResponse\DC24\n\
    \\ETXjob\CAN\SOH \SOH(\v2\".acyclic.workers.v1.JobObservationR\ETXjob\"\175\STX\n\
    \\SOJobObservation\DC2\NAK\n\
    \\ACKjob_id\CAN\SOH \SOH(\tR\ENQjobId\DC22\n\
    \\ENQstate\CAN\STX \SOH(\SO2\FS.acyclic.workers.v1.JobStateR\ENQstate\DC2'\n\
    \\SIresolved_sha256\CAN\ETX \SOH(\fR\SOresolvedSha256\DC2\CAN\n\
    \\aattempt\CAN\EOT \SOH(\rR\aattempt\DC25\n\
    \\ACKresult\CAN\ENQ \SOH(\v2\GS.acyclic.workers.v1.JobResultR\ACKresult\DC2!\n\
    \\ffailure_code\CAN\ACK \SOH(\tR\vfailureCode\DC25\n\
    \\SYNcancellation_requested\CAN\a \SOH(\bR\NAKcancellationRequested\"*\n\
    \\DC1InspectJobRequest\DC2\NAK\n\
    \\ACKjob_id\CAN\SOH \SOH(\tR\ENQjobId\"J\n\
    \\DC2InspectJobResponse\DC24\n\
    \\ETXjob\CAN\SOH \SOH(\v2\".acyclic.workers.v1.JobObservationR\ETXjob\"R\n\
    \\DLECancelJobRequest\DC2\NAK\n\
    \\ACKjob_id\CAN\SOH \SOH(\tR\ENQjobId\DC2'\n\
    \\SIidempotency_key\CAN\STX \SOH(\tR\SOidempotencyKey\"I\n\
    \\DC1CancelJobResponse\DC24\n\
    \\ETXjob\CAN\SOH \SOH(\v2\".acyclic.workers.v1.JobObservationR\ETXjob\"2\n\
    \\ACKHeader\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\DC4\n\
    \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue\"\177\SOH\n\
    \\DC4InvokeVersionRequest\DC2%\n\
    \\SOversion_sha256\CAN\SOH \SOH(\fR\rversionSha256\DC2\SYN\n\
    \\ACKmethod\CAN\STX \SOH(\tR\ACKmethod\DC2\DLE\n\
    \\ETXurl\CAN\ETX \SOH(\tR\ETXurl\DC24\n\
    \\aheaders\CAN\EOT \ETX(\v2\SUB.acyclic.workers.v1.HeaderR\aheaders\DC2\DC2\n\
    \\EOTbody\CAN\ENQ \SOH(\fR\EOTbody\"\163\SOH\n\
    \\ETBInvokeDeploymentRequest\DC2\DC4\n\
    \\ENQalias\CAN\SOH \SOH(\tR\ENQalias\DC2\SYN\n\
    \\ACKmethod\CAN\STX \SOH(\tR\ACKmethod\DC2\DLE\n\
    \\ETXurl\CAN\ETX \SOH(\tR\ETXurl\DC24\n\
    \\aheaders\CAN\EOT \ETX(\v2\SUB.acyclic.workers.v1.HeaderR\aheaders\DC2\DC2\n\
    \\EOTbody\CAN\ENQ \SOH(\fR\EOTbody\"\227\SOH\n\
    \\SOInvokeResponse\DC2\SYN\n\
    \\ACKstatus\CAN\SOH \SOH(\rR\ACKstatus\DC24\n\
    \\aheaders\CAN\STX \ETX(\v2\SUB.acyclic.workers.v1.HeaderR\aheaders\DC2\DC2\n\
    \\EOTbody\CAN\ETX \SOH(\fR\EOTbody\DC2'\n\
    \\SIresolved_sha256\CAN\EOT \SOH(\fR\SOresolvedSha256\DC20\n\
    \\DC1resolved_revision\CAN\ENQ \SOH(\EOTH\NULR\DLEresolvedRevision\136\SOH\SOHB\DC4\n\
    \\DC2_resolved_revision\"T\n\
    \\ENQError\DC21\n\
    \\EOTcode\CAN\SOH \SOH(\SO2\GS.acyclic.workers.v1.ErrorCodeR\EOTcode\DC2\CAN\n\
    \\amessage\CAN\STX \SOH(\tR\amessage*\156\SOH\n\
    \\bJobState\DC2\EM\n\
    \\NAKJOB_STATE_UNSPECIFIED\DLE\NUL\DC2\SYN\n\
    \\DC2JOB_STATE_ACCEPTED\DLE\SOH\DC2\NAK\n\
    \\DC1JOB_STATE_RUNNING\DLE\STX\DC2\ETB\n\
    \\DC3JOB_STATE_SUCCEEDED\DLE\ETX\DC2\DC4\n\
    \\DLEJOB_STATE_FAILED\DLE\EOT\DC2\ETB\n\
    \\DC3JOB_STATE_CANCELLED\DLE\ENQ*\249\STX\n\
    \\tErrorCode\DC2\SUB\n\
    \\SYNERROR_CODE_UNSPECIFIED\DLE\NUL\DC2\US\n\
    \\ESCERROR_CODE_INVALID_ARGUMENT\DLE\SOH\DC2 \n\
    \\FSERROR_CODE_CAPABILITY_DENIED\DLE\STX\DC2!\n\
    \\GSERROR_CODE_CAPABILITY_EXPIRED\DLE\ETX\DC2 \n\
    \\FSERROR_CODE_VERSION_NOT_FOUND\DLE\EOT\DC2#\n\
    \\USERROR_CODE_DEPLOYMENT_NOT_FOUND\DLE\ENQ\DC2\FS\n\
    \\CANERROR_CODE_JOB_NOT_FOUND\DLE\ACK\DC2#\n\
    \\USERROR_CODE_IDEMPOTENCY_MISMATCH\DLE\a\DC2 \n\
    \\FSERROR_CODE_REVISION_CONFLICT\DLE\b\DC2\EM\n\
    \\NAKERROR_CODE_OVERLOADED\DLE\t\DC2#\n\
    \\USERROR_CODE_TERMINAL_JOB_FAILURE\DLE\n\
    \2\189\ENQ\n\
    \\SOWorkersService\DC2g\n\
    \\SOPublishVersion\DC2).acyclic.workers.v1.PublishVersionRequest\SUB*.acyclic.workers.v1.PublishVersionResponse\DC2m\n\
    \\DLESelectDeployment\DC2+.acyclic.workers.v1.SelectDeploymentRequest\SUB,.acyclic.workers.v1.SelectDeploymentResponse\DC2X\n\
    \\tSubmitJob\DC2$.acyclic.workers.v1.SubmitJobRequest\SUB%.acyclic.workers.v1.SubmitJobResponse\DC2[\n\
    \\n\
    \InspectJob\DC2%.acyclic.workers.v1.InspectJobRequest\SUB&.acyclic.workers.v1.InspectJobResponse\DC2X\n\
    \\tCancelJob\DC2$.acyclic.workers.v1.CancelJobRequest\SUB%.acyclic.workers.v1.CancelJobResponse\DC2]\n\
    \\rInvokeVersion\DC2(.acyclic.workers.v1.InvokeVersionRequest\SUB\".acyclic.workers.v1.InvokeResponse\DC2c\n\
    \\DLEInvokeDeployment\DC2+.acyclic.workers.v1.InvokeDeploymentRequest\SUB\".acyclic.workers.v1.InvokeResponseB9Z7github.com/acyclic-labs/sdk/go/gen/workers/v1;workersv1J\217\&2\n\
    \\a\DC2\ENQ\NUL\NUL\172\SOH\SOH\n\
    \\b\n\
    \\SOH\f\DC2\ETX\NUL\NUL\DC2\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\SOH\NUL\ESC\n\
    \\b\n\
    \\SOH\b\DC2\ETX\ETX\NULN\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\ETX\NULN\n\
    \J\n\
    \\STX\EOT\NUL\DC2\EOT\ACK\NUL\t\SOH\SUB> A digest identifies exact immutable JavaScript module bytes.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\ACK\b\DC3\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\a\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\a\STX\a\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\a\b\SO\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\a\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\SOH\DC2\ETX\b\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ENQ\DC2\ETX\b\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\SOH\DC2\ETX\b\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ETX\DC2\ETX\b\SYN\ETB\n\
    \S\n\
    \\STX\EOT\SOH\DC2\EOT\f\NUL\DLE\SOH\SUBG A deployment alias may change; revision increases on every selection.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\f\b\DC2\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\r\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ENQ\DC2\ETX\r\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\r\t\SO\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\r\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\SOH\DC2\ETX\SO\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ACK\DC2\ETX\SO\STX\r\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\SOH\DC2\ETX\SO\SO\NAK\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ETX\DC2\ETX\SO\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\STX\DC2\ETX\SI\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\ENQ\DC2\ETX\SI\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\SOH\DC2\ETX\SI\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\ETX\DC2\ETX\SI\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\DC2\NUL\SYN\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\DC2\b\GS\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\DC3\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ENQ\DC2\ETX\DC3\STX\a\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\DC3\b\EM\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\DC3\FS\GS\n\
    \\v\n\
    \\EOT\EOT\STX\STX\SOH\DC2\ETX\DC4\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ENQ\DC2\ETX\DC4\STX\a\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\SOH\DC2\ETX\DC4\b\ETB\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ETX\DC2\ETX\DC4\SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\STX\STX\STX\DC2\ETX\NAK\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ENQ\DC2\ETX\NAK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\SOH\DC2\ETX\NAK\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ETX\DC2\ETX\NAK\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT\ETB\NUL\EM\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX\ETB\b\RS\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX\CAN\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ACK\DC2\ETX\CAN\STX\r\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX\CAN\SO\NAK\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX\CAN\CAN\EM\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT\ESC\NUL\"\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX\ESC\b\US\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX\FS\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ENQ\DC2\ETX\FS\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX\FS\t\SO\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX\FS\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\SOH\DC2\ETX\GS\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ENQ\DC2\ETX\GS\STX\a\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\SOH\DC2\ETX\GS\b\SYN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ETX\DC2\ETX\GS\EM\SUB\n\
    \\169\SOH\n\
    \\EOT\EOT\EOT\STX\STX\DC2\ETX \STX(\SUB\155\SOH Omitted means create only if absent. A present positive value selects only\n\
    \ when it matches the current revision; every successful selection advances it.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\EOT\DC2\ETX \STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ENQ\DC2\ETX \v\DC1\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\SOH\DC2\ETX \DC2#\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ETX\DC2\ETX &'\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\ETX\DC2\ETX!\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\ENQ\DC2\ETX!\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\SOH\DC2\ETX!\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\ETX\DC2\ETX!\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\ENQ\DC2\EOT#\NUL%\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETX#\b \n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETX$\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ACK\DC2\ETX$\STX\f\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETX$\r\ETB\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETX$\SUB\ESC\n\
    \\166\SOH\n\
    \\STX\EOT\ACK\DC2\EOT)\NUL.\SOH\SUB\153\SOH Logical S3 object selected and privately retained at durable job acceptance.\n\
    \ Retries read the same retained bytes even if this public key is replaced.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETX)\b\DC1\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX*\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ENQ\DC2\ETX*\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX*\t\SI\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX*\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\SOH\DC2\ETX+\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ENQ\DC2\ETX+\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\SOH\DC2\ETX+\t\f\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ETX\DC2\ETX+\SI\DLE\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\t\DC2\ETX,\STX\r\n\
    \\v\n\
    \\EOT\EOT\ACK\t\NUL\DC2\ETX,\v\f\n\
    \\f\n\
    \\ENQ\EOT\ACK\t\NUL\SOH\DC2\ETX,\v\f\n\
    \\f\n\
    \\ENQ\EOT\ACK\t\NUL\STX\DC2\ETX,\v\f\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\n\
    \\DC2\ETX-\STX\CAN\n\
    \\v\n\
    \\EOT\EOT\ACK\n\
    \\NUL\DC2\ETX-\v\ETB\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOT/\NUL4\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETX/\b\SI\n\
    \\f\n\
    \\EOT\EOT\a\b\NUL\DC2\EOT0\STX3\ETX\n\
    \\f\n\
    \\ENQ\EOT\a\b\NUL\SOH\DC2\ETX0\b\SO\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETX1\EOT\ESC\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ENQ\DC2\ETX1\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETX1\n\
    \\SYN\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETX1\EM\SUB\n\
    \\v\n\
    \\EOT\EOT\a\STX\SOH\DC2\ETX2\EOT\EM\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ACK\DC2\ETX2\EOT\r\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\SOH\DC2\ETX2\SO\DC4\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ETX\DC2\ETX2\ETB\CAN\n\
    \\167\SOH\n\
    \\STX\EOT\b\DC2\EOT7\NUL;\SOH\SUB\154\SOH Exact accepted job output, bounded by JobLimits.output_bytes. Storage and\n\
    \ retention are service-owned; no replaceable public Object pointer is exposed.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETX7\b\DC1\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETX8\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ENQ\DC2\ETX8\STX\a\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETX8\b\f\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETX8\SI\DLE\n\
    \\n\
    \\n\
    \\ETX\EOT\b\t\DC2\ETX9\STX\r\n\
    \\v\n\
    \\EOT\EOT\b\t\NUL\DC2\ETX9\v\f\n\
    \\f\n\
    \\ENQ\EOT\b\t\NUL\SOH\DC2\ETX9\v\f\n\
    \\f\n\
    \\ENQ\EOT\b\t\NUL\STX\DC2\ETX9\v\f\n\
    \\n\
    \\n\
    \\ETX\EOT\b\n\
    \\DC2\ETX:\STX\FS\n\
    \\v\n\
    \\EOT\EOT\b\n\
    \\NUL\DC2\ETX:\v\ESC\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOT<\NUL@\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETX<\b\DC1\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETX=\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ENQ\DC2\ETX=\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETX=\t\ETB\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETX=\SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\t\STX\SOH\DC2\ETX>\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ENQ\DC2\ETX>\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\SOH\DC2\ETX>\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ETX\DC2\ETX>\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\t\STX\STX\DC2\ETX?\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\t\STX\STX\ENQ\DC2\ETX?\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\STX\SOH\DC2\ETX?\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\t\STX\STX\ETX\DC2\ETX?\CAN\EM\n\
    \\n\
    \\n\
    \\STX\EOT\n\
    \\DC2\EOTA\NULD\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\n\
    \\SOH\DC2\ETXA\b\DC3\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\ETXB\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ENQ\DC2\ETXB\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\ETXB\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\ETXB\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\SOH\DC2\ETXC\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ENQ\DC2\ETXC\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\SOH\DC2\ETXC\t\ETB\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ETX\DC2\ETXC\SUB\ESC\n\
    \\n\
    \\n\
    \\STX\EOT\v\DC2\EOTE\NULJ\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\v\SOH\DC2\ETXE\b\DC1\n\
    \\f\n\
    \\EOT\EOT\v\b\NUL\DC2\EOTF\STXI\ETX\n\
    \\f\n\
    \\ENQ\EOT\v\b\NUL\SOH\DC2\ETXF\b\SO\n\
    \\v\n\
    \\EOT\EOT\v\STX\NUL\DC2\ETXG\EOT \n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ENQ\DC2\ETXG\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\ETXG\v\ESC\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\ETXG\RS\US\n\
    \\v\n\
    \\EOT\EOT\v\STX\SOH\DC2\ETXH\EOT\GS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ENQ\DC2\ETXH\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\SOH\DC2\ETXH\n\
    \\CAN\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ETX\DC2\ETXH\ESC\FS\n\
    \\158\SOH\n\
    \\STX\EOT\f\DC2\EOTM\NULS\SOH\SUB\145\SOH Accepted input is delivered to default.run, never to default.fetch.\n\
    \ The same job ID and input recur on retry; attempt numbering starts at one.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\f\SOH\DC2\ETXM\b\CAN\n\
    \\v\n\
    \\EOT\EOT\f\STX\NUL\DC2\ETXN\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ACK\DC2\ETXN\STX\v\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\ETXN\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\ETXN\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\f\STX\SOH\DC2\ETXO\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ACK\DC2\ETXO\STX\t\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\ETXO\n\
    \\SI\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\ETXO\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\f\STX\STX\DC2\ETXP\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\f\STX\STX\ACK\DC2\ETXP\STX\v\n\
    \\f\n\
    \\ENQ\EOT\f\STX\STX\SOH\DC2\ETXP\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\f\STX\STX\ETX\DC2\ETXP\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\f\STX\ETX\DC2\ETXQ\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\f\STX\ETX\ACK\DC2\ETXQ\STX\r\n\
    \\f\n\
    \\ENQ\EOT\f\STX\ETX\SOH\DC2\ETXQ\SO\DC3\n\
    \\f\n\
    \\ENQ\EOT\f\STX\ETX\ETX\DC2\ETXQ\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\f\STX\EOT\DC2\ETXR\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\f\STX\EOT\ENQ\DC2\ETXR\STX\b\n\
    \\f\n\
    \\ENQ\EOT\f\STX\EOT\SOH\DC2\ETXR\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\f\STX\EOT\ETX\DC2\ETXR\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\r\DC2\EOTT\NULV\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\r\SOH\DC2\ETXT\b\EM\n\
    \\v\n\
    \\EOT\EOT\r\STX\NUL\DC2\ETXU\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ACK\DC2\ETXU\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\ETXU\DC1\DC4\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\ETXU\ETB\CAN\n\
    \\n\
    \\n\
    \\STX\ENQ\NUL\DC2\EOTX\NUL_\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETXX\ENQ\r\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETXY\STX\FS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETXY\STX\ETB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETXY\SUB\ESC\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETXZ\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETXZ\STX\DC4\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETXZ\ETB\CAN\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\STX\DC2\ETX[\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\SOH\DC2\ETX[\STX\DC3\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\STX\DC2\ETX[\SYN\ETB\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ETX\DC2\ETX\\\STX\SUB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\SOH\DC2\ETX\\\STX\NAK\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\STX\DC2\ETX\\\CAN\EM\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\EOT\DC2\ETX]\STX\ETB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\SOH\DC2\ETX]\STX\DC2\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\STX\DC2\ETX]\NAK\SYN\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ENQ\DC2\ETX^\STX\SUB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ENQ\SOH\DC2\ETX^\STX\NAK\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ENQ\STX\DC2\ETX^\CAN\EM\n\
    \\n\
    \\n\
    \\STX\EOT\SO\DC2\EOT`\NULh\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SO\SOH\DC2\ETX`\b\SYN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\NUL\DC2\ETXa\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ENQ\DC2\ETXa\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\ETXa\t\SI\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\ETXa\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\SO\STX\SOH\DC2\ETXb\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ACK\DC2\ETXb\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\SOH\DC2\ETXb\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ETX\DC2\ETXb\DC3\DC4\n\
    \\v\n\
    \\EOT\EOT\SO\STX\STX\DC2\ETXc\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ENQ\DC2\ETXc\STX\a\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\SOH\DC2\ETXc\b\ETB\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ETX\DC2\ETXc\SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ETX\DC2\ETXd\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\ENQ\DC2\ETXd\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\SOH\DC2\ETXd\t\DLE\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ETX\ETX\DC2\ETXd\DC3\DC4\n\
    \\v\n\
    \\EOT\EOT\SO\STX\EOT\DC2\ETXe\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\ACK\DC2\ETXe\STX\v\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\SOH\DC2\ETXe\f\DC2\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\EOT\ETX\DC2\ETXe\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ENQ\DC2\ETXf\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ENQ\ENQ\DC2\ETXf\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ENQ\SOH\DC2\ETXf\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ENQ\ETX\DC2\ETXf\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\SO\STX\ACK\DC2\ETXg\STX\"\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ACK\ENQ\DC2\ETXg\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ACK\SOH\DC2\ETXg\a\GS\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\ACK\ETX\DC2\ETXg !\n\
    \\n\
    \\n\
    \\STX\EOT\SI\DC2\EOTi\NULk\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SI\SOH\DC2\ETXi\b\EM\n\
    \\v\n\
    \\EOT\EOT\SI\STX\NUL\DC2\ETXj\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ENQ\DC2\ETXj\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\ETXj\t\SI\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\ETXj\DC2\DC3\n\
    \\n\
    \\n\
    \\STX\EOT\DLE\DC2\EOTl\NULn\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DLE\SOH\DC2\ETXl\b\SUB\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\ETXm\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ACK\DC2\ETXm\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\ETXm\DC1\DC4\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\ETXm\ETB\CAN\n\
    \\n\
    \\n\
    \\STX\EOT\DC1\DC2\EOTo\NULr\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC1\SOH\DC2\ETXo\b\CAN\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\ETXp\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ENQ\DC2\ETXp\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\ETXp\t\SI\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\ETXp\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\SOH\DC2\ETXq\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\ENQ\DC2\ETXq\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\SOH\DC2\ETXq\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\SOH\ETX\DC2\ETXq\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\DC2\DC2\EOTs\NULu\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC2\SOH\DC2\ETXs\b\EM\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\ETXt\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ACK\DC2\ETXt\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\ETXt\DC1\DC4\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\ETXt\ETB\CAN\n\
    \\n\
    \\n\
    \\STX\EOT\DC3\DC2\EOTw\NULz\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC3\SOH\DC2\ETXw\b\SO\n\
    \\v\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\ETXx\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\ENQ\DC2\ETXx\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\ETXx\t\r\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\ETXx\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\DC3\STX\SOH\DC2\ETXy\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\SOH\ENQ\DC2\ETXy\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\SOH\SOH\DC2\ETXy\t\SO\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\SOH\ETX\DC2\ETXy\DC1\DC2\n\
    \L\n\
    \\STX\EOT\DC4\DC2\ENQ|\NUL\130\SOH\SOH\SUB? Invocation is ordinary HTTP work, not durable job acceptance.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\DC4\SOH\DC2\ETX|\b\FS\n\
    \\v\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\ETX}\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\NUL\ENQ\DC2\ETX}\STX\a\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\ETX}\b\SYN\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\ETX}\EM\SUB\n\
    \\v\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\ETX~\STX\DC4\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\SOH\ENQ\DC2\ETX~\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\ETX~\t\SI\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\ETX~\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\DC4\STX\STX\DC2\ETX\DEL\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\STX\ENQ\DC2\ETX\DEL\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\STX\SOH\DC2\ETX\DEL\t\f\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\STX\ETX\DC2\ETX\DEL\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\ETX\DC2\EOT\128\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\EOT\DC2\EOT\128\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\ACK\DC2\EOT\128\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\SOH\DC2\EOT\128\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\ETX\ETX\DC2\EOT\128\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\EOT\DC2\EOT\129\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\EOT\ENQ\DC2\EOT\129\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\EOT\SOH\DC2\EOT\129\SOH\b\f\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\EOT\ETX\DC2\EOT\129\SOH\SI\DLE\n\
    \\f\n\
    \\STX\EOT\NAK\DC2\ACK\131\SOH\NUL\137\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\131\SOH\b\US\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\132\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ENQ\DC2\EOT\132\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\132\SOH\t\SO\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\132\SOH\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\SOH\DC2\EOT\133\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ENQ\DC2\EOT\133\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\SOH\DC2\EOT\133\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ETX\DC2\EOT\133\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\STX\DC2\EOT\134\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ENQ\DC2\EOT\134\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\SOH\DC2\EOT\134\SOH\t\f\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ETX\DC2\EOT\134\SOH\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\ETX\DC2\EOT\135\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\EOT\DC2\EOT\135\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ACK\DC2\EOT\135\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\SOH\DC2\EOT\135\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ETX\DC2\EOT\135\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\EOT\DC2\EOT\136\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\ENQ\DC2\EOT\136\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\SOH\DC2\EOT\136\SOH\b\f\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\ETX\DC2\EOT\136\SOH\SI\DLE\n\
    \\f\n\
    \\STX\EOT\SYN\DC2\ACK\138\SOH\NUL\144\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\138\SOH\b\SYN\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\139\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ENQ\DC2\EOT\139\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\139\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\139\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\140\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\EOT\DC2\EOT\140\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ACK\DC2\EOT\140\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\140\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\140\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\STX\DC2\EOT\141\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ENQ\DC2\EOT\141\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\SOH\DC2\EOT\141\SOH\b\f\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ETX\DC2\EOT\141\SOH\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\ETX\DC2\EOT\142\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\ETX\ENQ\DC2\EOT\142\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\ETX\SOH\DC2\EOT\142\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\ETX\ETX\DC2\EOT\142\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\EOT\DC2\EOT\143\SOH\STX(\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\EOT\DC2\EOT\143\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\ENQ\DC2\EOT\143\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\SOH\DC2\EOT\143\SOH\DC2#\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\EOT\ETX\DC2\EOT\143\SOH&'\n\
    \\f\n\
    \\STX\ENQ\SOH\DC2\ACK\146\SOH\NUL\158\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\SOH\SOH\DC2\EOT\146\SOH\ENQ\SO\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\NUL\DC2\EOT\147\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\SOH\DC2\EOT\147\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\STX\DC2\EOT\147\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\SOH\DC2\EOT\148\SOH\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\SOH\DC2\EOT\148\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\STX\DC2\EOT\148\SOH !\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\STX\DC2\EOT\149\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\SOH\DC2\EOT\149\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\STX\DC2\EOT\149\SOH!\"\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ETX\DC2\EOT\150\SOH\STX$\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ETX\SOH\DC2\EOT\150\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ETX\STX\DC2\EOT\150\SOH\"#\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\EOT\DC2\EOT\151\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\EOT\SOH\DC2\EOT\151\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\EOT\STX\DC2\EOT\151\SOH!\"\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ENQ\DC2\EOT\152\SOH\STX&\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ENQ\SOH\DC2\EOT\152\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ENQ\STX\DC2\EOT\152\SOH$%\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ACK\DC2\EOT\153\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ACK\SOH\DC2\EOT\153\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ACK\STX\DC2\EOT\153\SOH\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\a\DC2\EOT\154\SOH\STX&\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\a\SOH\DC2\EOT\154\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\a\STX\DC2\EOT\154\SOH$%\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\b\DC2\EOT\155\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\b\SOH\DC2\EOT\155\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\b\STX\DC2\EOT\155\SOH!\"\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\t\DC2\EOT\156\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\t\SOH\DC2\EOT\156\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\t\STX\DC2\EOT\156\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\n\
    \\DC2\EOT\157\SOH\STX'\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\n\
    \\SOH\DC2\EOT\157\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\n\
    \\STX\DC2\EOT\157\SOH$&\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\159\SOH\NUL\162\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\159\SOH\b\r\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\160\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ACK\DC2\EOT\160\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\160\SOH\f\DLE\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\160\SOH\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\161\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ENQ\DC2\EOT\161\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\161\SOH\t\DLE\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\161\SOH\DC3\DC4\n\
    \\f\n\
    \\STX\ACK\NUL\DC2\ACK\164\SOH\NUL\172\SOH\SOH\n\
    \\v\n\
    \\ETX\ACK\NUL\SOH\DC2\EOT\164\SOH\b\SYN\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\EOT\165\SOH\STXM\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\EOT\165\SOH\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\EOT\165\SOH\NAK*\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\EOT\165\SOH5K\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\SOH\DC2\EOT\166\SOH\STXS\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\SOH\DC2\EOT\166\SOH\ACK\SYN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\STX\DC2\EOT\166\SOH\ETB.\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\ETX\DC2\EOT\166\SOH9Q\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\STX\DC2\EOT\167\SOH\STX>\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\SOH\DC2\EOT\167\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\STX\DC2\EOT\167\SOH\DLE \n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\ETX\DC2\EOT\167\SOH+<\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ETX\DC2\EOT\168\SOH\STXA\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\SOH\DC2\EOT\168\SOH\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\STX\DC2\EOT\168\SOH\DC1\"\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\ETX\DC2\EOT\168\SOH-?\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\EOT\DC2\EOT\169\SOH\STX>\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\SOH\DC2\EOT\169\SOH\ACK\SI\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\STX\DC2\EOT\169\SOH\DLE \n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\ETX\DC2\EOT\169\SOH+<\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ENQ\DC2\EOT\170\SOH\STXC\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\SOH\DC2\EOT\170\SOH\ACK\DC3\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\STX\DC2\EOT\170\SOH\DC4(\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\ETX\DC2\EOT\170\SOH3A\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ACK\DC2\EOT\171\SOH\STXI\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\SOH\DC2\EOT\171\SOH\ACK\SYN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\STX\DC2\EOT\171\SOH\ETB.\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\ETX\DC2\EOT\171\SOH9Gb\ACKproto3"