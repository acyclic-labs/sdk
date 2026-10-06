{- This file was auto-generated from actors/v1/actors.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Actors.V1.Actors (
        ActorsService(..), ActorLimits(), ActorObservation(),
        ActorState(..), ActorState(), ActorState'UnrecognizedValue,
        AddSubscriptionRequest(), AddSubscriptionResponse(), Binding(),
        CheckpointActorRequest(), CheckpointActorResponse(),
        CreateActorRequest(), CreateActorResponse(), Error(),
        ErrorCode(..), ErrorCode(), ErrorCode'UnrecognizedValue, Header(),
        InspectActorRequest(), InspectActorResponse(),
        InvokeActorRequest(), InvokeActorResponse(),
        RemoveSubscriptionRequest(), RemoveSubscriptionResponse(),
        ResumeSubscriptionRequest(), ResumeSubscriptionResponse(),
        SubscriptionObservation(), SubscriptionSpec(), SubscriptionStart(),
        SubscriptionStart'Start(..), _SubscriptionStart'Cursor,
        _SubscriptionStart'CurrentHead, SubscriptionState(..),
        SubscriptionState(), SubscriptionState'UnrecognizedValue,
        UpdateActorRequest(), UpdateActorResponse()
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
     
         * 'Proto.Actors.V1.Actors_Fields.handlerTimeoutMillis' @:: Lens' ActorLimits Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.memoryBytes' @:: Lens' ActorLimits Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.checkpointBytes' @:: Lens' ActorLimits Data.Word.Word64@ -}
data ActorLimits
  = ActorLimits'_constructor {_ActorLimits'handlerTimeoutMillis :: !Data.Word.Word64,
                              _ActorLimits'memoryBytes :: !Data.Word.Word64,
                              _ActorLimits'checkpointBytes :: !Data.Word.Word64,
                              _ActorLimits'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ActorLimits where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ActorLimits "handlerTimeoutMillis" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorLimits'handlerTimeoutMillis
           (\ x__ y__ -> x__ {_ActorLimits'handlerTimeoutMillis = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorLimits "memoryBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorLimits'memoryBytes
           (\ x__ y__ -> x__ {_ActorLimits'memoryBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorLimits "checkpointBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorLimits'checkpointBytes
           (\ x__ y__ -> x__ {_ActorLimits'checkpointBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Message ActorLimits where
  messageName _ = Data.Text.pack "acyclic.actors.v1.ActorLimits"
  packedMessageDescriptor _
    = "\n\
      \\vActorLimits\DC24\n\
      \\SYNhandler_timeout_millis\CAN\SOH \SOH(\EOTR\DC4handlerTimeoutMillis\DC2!\n\
      \\fmemory_bytes\CAN\STX \SOH(\EOTR\vmemoryBytes\DC2)\n\
      \\DLEcheckpoint_bytes\CAN\ETX \SOH(\EOTR\SIcheckpointBytes"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        handlerTimeoutMillis__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "handler_timeout_millis"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"handlerTimeoutMillis")) ::
              Data.ProtoLens.FieldDescriptor ActorLimits
        memoryBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "memory_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"memoryBytes")) ::
              Data.ProtoLens.FieldDescriptor ActorLimits
        checkpointBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"checkpointBytes")) ::
              Data.ProtoLens.FieldDescriptor ActorLimits
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, handlerTimeoutMillis__field_descriptor),
           (Data.ProtoLens.Tag 2, memoryBytes__field_descriptor),
           (Data.ProtoLens.Tag 3, checkpointBytes__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ActorLimits'_unknownFields
        (\ x__ y__ -> x__ {_ActorLimits'_unknownFields = y__})
  defMessage
    = ActorLimits'_constructor
        {_ActorLimits'handlerTimeoutMillis = Data.ProtoLens.fieldDefault,
         _ActorLimits'memoryBytes = Data.ProtoLens.fieldDefault,
         _ActorLimits'checkpointBytes = Data.ProtoLens.fieldDefault,
         _ActorLimits'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ActorLimits -> Data.ProtoLens.Encoding.Bytes.Parser ActorLimits
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
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "handler_timeout_millis"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"handlerTimeoutMillis") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "memory_bytes"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"memoryBytes") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "checkpoint_bytes"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"checkpointBytes") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ActorLimits"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"handlerTimeoutMillis") _x
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
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"checkpointBytes") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ActorLimits where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ActorLimits'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ActorLimits'handlerTimeoutMillis x__)
                (Control.DeepSeq.deepseq
                   (_ActorLimits'memoryBytes x__)
                   (Control.DeepSeq.deepseq (_ActorLimits'checkpointBytes x__) ())))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' ActorObservation Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.codeSha256' @:: Lens' ActorObservation Data.ByteString.ByteString@
         * 'Proto.Actors.V1.Actors_Fields.homeRegion' @:: Lens' ActorObservation Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.state' @:: Lens' ActorObservation ActorState@
         * 'Proto.Actors.V1.Actors_Fields.subscriptions' @:: Lens' ActorObservation [SubscriptionObservation]@
         * 'Proto.Actors.V1.Actors_Fields.vec'subscriptions' @:: Lens' ActorObservation (Data.Vector.Vector SubscriptionObservation)@
         * 'Proto.Actors.V1.Actors_Fields.checkpointUnixMillis' @:: Lens' ActorObservation Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.maybe'checkpointUnixMillis' @:: Lens' ActorObservation (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Actors.V1.Actors_Fields.checkpointEpoch' @:: Lens' ActorObservation Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.configurationRevision' @:: Lens' ActorObservation Data.Word.Word64@ -}
data ActorObservation
  = ActorObservation'_constructor {_ActorObservation'actorId :: !Data.Text.Text,
                                   _ActorObservation'codeSha256 :: !Data.ByteString.ByteString,
                                   _ActorObservation'homeRegion :: !Data.Text.Text,
                                   _ActorObservation'state :: !ActorState,
                                   _ActorObservation'subscriptions :: !(Data.Vector.Vector SubscriptionObservation),
                                   _ActorObservation'checkpointUnixMillis :: !(Prelude.Maybe Data.Word.Word64),
                                   _ActorObservation'checkpointEpoch :: !Data.Word.Word64,
                                   _ActorObservation'configurationRevision :: !Data.Word.Word64,
                                   _ActorObservation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ActorObservation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ActorObservation "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'actorId
           (\ x__ y__ -> x__ {_ActorObservation'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "codeSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'codeSha256
           (\ x__ y__ -> x__ {_ActorObservation'codeSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "homeRegion" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'homeRegion
           (\ x__ y__ -> x__ {_ActorObservation'homeRegion = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "state" ActorState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'state
           (\ x__ y__ -> x__ {_ActorObservation'state = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "subscriptions" [SubscriptionObservation] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'subscriptions
           (\ x__ y__ -> x__ {_ActorObservation'subscriptions = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ActorObservation "vec'subscriptions" (Data.Vector.Vector SubscriptionObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'subscriptions
           (\ x__ y__ -> x__ {_ActorObservation'subscriptions = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "checkpointUnixMillis" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'checkpointUnixMillis
           (\ x__ y__ -> x__ {_ActorObservation'checkpointUnixMillis = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ActorObservation "maybe'checkpointUnixMillis" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'checkpointUnixMillis
           (\ x__ y__ -> x__ {_ActorObservation'checkpointUnixMillis = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "checkpointEpoch" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'checkpointEpoch
           (\ x__ y__ -> x__ {_ActorObservation'checkpointEpoch = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ActorObservation "configurationRevision" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ActorObservation'configurationRevision
           (\ x__ y__ -> x__ {_ActorObservation'configurationRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Message ActorObservation where
  messageName _ = Data.Text.pack "acyclic.actors.v1.ActorObservation"
  packedMessageDescriptor _
    = "\n\
      \\DLEActorObservation\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2\US\n\
      \\vcode_sha256\CAN\STX \SOH(\fR\n\
      \codeSha256\DC2\US\n\
      \\vhome_region\CAN\ETX \SOH(\tR\n\
      \homeRegion\DC23\n\
      \\ENQstate\CAN\EOT \SOH(\SO2\GS.acyclic.actors.v1.ActorStateR\ENQstate\DC2P\n\
      \\rsubscriptions\CAN\ENQ \ETX(\v2*.acyclic.actors.v1.SubscriptionObservationR\rsubscriptions\DC29\n\
      \\SYNcheckpoint_unix_millis\CAN\ACK \SOH(\EOTH\NULR\DC4checkpointUnixMillis\136\SOH\SOH\DC2)\n\
      \\DLEcheckpoint_epoch\CAN\a \SOH(\EOTR\SIcheckpointEpoch\DC25\n\
      \\SYNconfiguration_revision\CAN\b \SOH(\EOTR\NAKconfigurationRevisionB\EM\n\
      \\ETB_checkpoint_unix_millis"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        codeSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "code_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"codeSha256")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        homeRegion__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "home_region"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"homeRegion")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        state__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ActorState)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"state")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        subscriptions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscriptions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SubscriptionObservation)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"subscriptions")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        checkpointUnixMillis__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint_unix_millis"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpointUnixMillis")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        checkpointEpoch__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint_epoch"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"checkpointEpoch")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
        configurationRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "configuration_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"configurationRevision")) ::
              Data.ProtoLens.FieldDescriptor ActorObservation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, codeSha256__field_descriptor),
           (Data.ProtoLens.Tag 3, homeRegion__field_descriptor),
           (Data.ProtoLens.Tag 4, state__field_descriptor),
           (Data.ProtoLens.Tag 5, subscriptions__field_descriptor),
           (Data.ProtoLens.Tag 6, checkpointUnixMillis__field_descriptor),
           (Data.ProtoLens.Tag 7, checkpointEpoch__field_descriptor),
           (Data.ProtoLens.Tag 8, configurationRevision__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ActorObservation'_unknownFields
        (\ x__ y__ -> x__ {_ActorObservation'_unknownFields = y__})
  defMessage
    = ActorObservation'_constructor
        {_ActorObservation'actorId = Data.ProtoLens.fieldDefault,
         _ActorObservation'codeSha256 = Data.ProtoLens.fieldDefault,
         _ActorObservation'homeRegion = Data.ProtoLens.fieldDefault,
         _ActorObservation'state = Data.ProtoLens.fieldDefault,
         _ActorObservation'subscriptions = Data.Vector.Generic.empty,
         _ActorObservation'checkpointUnixMillis = Prelude.Nothing,
         _ActorObservation'checkpointEpoch = Data.ProtoLens.fieldDefault,
         _ActorObservation'configurationRevision = Data.ProtoLens.fieldDefault,
         _ActorObservation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ActorObservation
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld SubscriptionObservation
             -> Data.ProtoLens.Encoding.Bytes.Parser ActorObservation
        loop x mutable'subscriptions
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'subscriptions <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                   mutable'subscriptions)
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
                              (Data.ProtoLens.Field.field @"vec'subscriptions")
                              frozen'subscriptions x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "actor_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
                                  mutable'subscriptions
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "code_sha256"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"codeSha256") y x)
                                  mutable'subscriptions
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "home_region"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"homeRegion") y x)
                                  mutable'subscriptions
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "state"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"state") y x)
                                  mutable'subscriptions
                        42
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "subscriptions"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'subscriptions y)
                                loop x v
                        48
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "checkpoint_unix_millis"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"checkpointUnixMillis") y x)
                                  mutable'subscriptions
                        56
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "checkpoint_epoch"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"checkpointEpoch") y x)
                                  mutable'subscriptions
                        64
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "configuration_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"configurationRevision") y x)
                                  mutable'subscriptions
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'subscriptions
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'subscriptions <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                         Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'subscriptions)
          "ActorObservation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"codeSha256") _x
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
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"homeRegion") _x
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
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"state") _x
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
                            (Lens.Family2.view
                               (Data.ProtoLens.Field.field @"vec'subscriptions") _x))
                         ((Data.Monoid.<>)
                            (case
                                 Lens.Family2.view
                                   (Data.ProtoLens.Field.field @"maybe'checkpointUnixMillis") _x
                             of
                               Prelude.Nothing -> Data.Monoid.mempty
                               (Prelude.Just _v)
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt 48)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                            ((Data.Monoid.<>)
                               (let
                                  _v
                                    = Lens.Family2.view
                                        (Data.ProtoLens.Field.field @"checkpointEpoch") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 56)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                               ((Data.Monoid.<>)
                                  (let
                                     _v
                                       = Lens.Family2.view
                                           (Data.ProtoLens.Field.field @"configurationRevision") _x
                                   in
                                     if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                         Data.Monoid.mempty
                                     else
                                         (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt 64)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                                  (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                     (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))))
instance Control.DeepSeq.NFData ActorObservation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ActorObservation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ActorObservation'actorId x__)
                (Control.DeepSeq.deepseq
                   (_ActorObservation'codeSha256 x__)
                   (Control.DeepSeq.deepseq
                      (_ActorObservation'homeRegion x__)
                      (Control.DeepSeq.deepseq
                         (_ActorObservation'state x__)
                         (Control.DeepSeq.deepseq
                            (_ActorObservation'subscriptions x__)
                            (Control.DeepSeq.deepseq
                               (_ActorObservation'checkpointUnixMillis x__)
                               (Control.DeepSeq.deepseq
                                  (_ActorObservation'checkpointEpoch x__)
                                  (Control.DeepSeq.deepseq
                                     (_ActorObservation'configurationRevision x__) ()))))))))
newtype ActorState'UnrecognizedValue
  = ActorState'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ActorState
  = ACTOR_STATE_UNSPECIFIED |
    ACTOR_STATE_ACTIVE |
    ACTOR_STATE_HIBERNATED |
    ACTOR_STATE_PAUSED |
    ActorState'Unrecognized !ActorState'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ActorState where
  maybeToEnum 0 = Prelude.Just ACTOR_STATE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just ACTOR_STATE_ACTIVE
  maybeToEnum 2 = Prelude.Just ACTOR_STATE_HIBERNATED
  maybeToEnum 3 = Prelude.Just ACTOR_STATE_PAUSED
  maybeToEnum k
    = Prelude.Just
        (ActorState'Unrecognized
           (ActorState'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum ACTOR_STATE_UNSPECIFIED = "ACTOR_STATE_UNSPECIFIED"
  showEnum ACTOR_STATE_ACTIVE = "ACTOR_STATE_ACTIVE"
  showEnum ACTOR_STATE_HIBERNATED = "ACTOR_STATE_HIBERNATED"
  showEnum ACTOR_STATE_PAUSED = "ACTOR_STATE_PAUSED"
  showEnum (ActorState'Unrecognized (ActorState'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "ACTOR_STATE_UNSPECIFIED"
    = Prelude.Just ACTOR_STATE_UNSPECIFIED
    | (Prelude.==) k "ACTOR_STATE_ACTIVE"
    = Prelude.Just ACTOR_STATE_ACTIVE
    | (Prelude.==) k "ACTOR_STATE_HIBERNATED"
    = Prelude.Just ACTOR_STATE_HIBERNATED
    | (Prelude.==) k "ACTOR_STATE_PAUSED"
    = Prelude.Just ACTOR_STATE_PAUSED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ActorState where
  minBound = ACTOR_STATE_UNSPECIFIED
  maxBound = ACTOR_STATE_PAUSED
instance Prelude.Enum ActorState where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ActorState: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum ACTOR_STATE_UNSPECIFIED = 0
  fromEnum ACTOR_STATE_ACTIVE = 1
  fromEnum ACTOR_STATE_HIBERNATED = 2
  fromEnum ACTOR_STATE_PAUSED = 3
  fromEnum (ActorState'Unrecognized (ActorState'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ ACTOR_STATE_PAUSED
    = Prelude.error
        "ActorState.succ: bad argument ACTOR_STATE_PAUSED. This value would be out of bounds."
  succ ACTOR_STATE_UNSPECIFIED = ACTOR_STATE_ACTIVE
  succ ACTOR_STATE_ACTIVE = ACTOR_STATE_HIBERNATED
  succ ACTOR_STATE_HIBERNATED = ACTOR_STATE_PAUSED
  succ (ActorState'Unrecognized _)
    = Prelude.error "ActorState.succ: bad argument: unrecognized value"
  pred ACTOR_STATE_UNSPECIFIED
    = Prelude.error
        "ActorState.pred: bad argument ACTOR_STATE_UNSPECIFIED. This value would be out of bounds."
  pred ACTOR_STATE_ACTIVE = ACTOR_STATE_UNSPECIFIED
  pred ACTOR_STATE_HIBERNATED = ACTOR_STATE_ACTIVE
  pred ACTOR_STATE_PAUSED = ACTOR_STATE_HIBERNATED
  pred (ActorState'Unrecognized _)
    = Prelude.error "ActorState.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ActorState where
  fieldDefault = ACTOR_STATE_UNSPECIFIED
instance Control.DeepSeq.NFData ActorState where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' AddSubscriptionRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.subscription' @:: Lens' AddSubscriptionRequest SubscriptionSpec@
         * 'Proto.Actors.V1.Actors_Fields.maybe'subscription' @:: Lens' AddSubscriptionRequest (Prelude.Maybe SubscriptionSpec)@
         * 'Proto.Actors.V1.Actors_Fields.idempotencyKey' @:: Lens' AddSubscriptionRequest Data.Text.Text@ -}
data AddSubscriptionRequest
  = AddSubscriptionRequest'_constructor {_AddSubscriptionRequest'actorId :: !Data.Text.Text,
                                         _AddSubscriptionRequest'subscription :: !(Prelude.Maybe SubscriptionSpec),
                                         _AddSubscriptionRequest'idempotencyKey :: !Data.Text.Text,
                                         _AddSubscriptionRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AddSubscriptionRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AddSubscriptionRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AddSubscriptionRequest'actorId
           (\ x__ y__ -> x__ {_AddSubscriptionRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AddSubscriptionRequest "subscription" SubscriptionSpec where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AddSubscriptionRequest'subscription
           (\ x__ y__ -> x__ {_AddSubscriptionRequest'subscription = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField AddSubscriptionRequest "maybe'subscription" (Prelude.Maybe SubscriptionSpec) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AddSubscriptionRequest'subscription
           (\ x__ y__ -> x__ {_AddSubscriptionRequest'subscription = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField AddSubscriptionRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AddSubscriptionRequest'idempotencyKey
           (\ x__ y__ -> x__ {_AddSubscriptionRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message AddSubscriptionRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.AddSubscriptionRequest"
  packedMessageDescriptor _
    = "\n\
      \\SYNAddSubscriptionRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2G\n\
      \\fsubscription\CAN\STX \SOH(\v2#.acyclic.actors.v1.SubscriptionSpecR\fsubscription\DC2'\n\
      \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor AddSubscriptionRequest
        subscription__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscription"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SubscriptionSpec)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'subscription")) ::
              Data.ProtoLens.FieldDescriptor AddSubscriptionRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor AddSubscriptionRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, subscription__field_descriptor),
           (Data.ProtoLens.Tag 3, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AddSubscriptionRequest'_unknownFields
        (\ x__ y__ -> x__ {_AddSubscriptionRequest'_unknownFields = y__})
  defMessage
    = AddSubscriptionRequest'_constructor
        {_AddSubscriptionRequest'actorId = Data.ProtoLens.fieldDefault,
         _AddSubscriptionRequest'subscription = Prelude.Nothing,
         _AddSubscriptionRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _AddSubscriptionRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AddSubscriptionRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser AddSubscriptionRequest
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
                                       "actor_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "subscription"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"subscription") y x)
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
          (do loop Data.ProtoLens.defMessage) "AddSubscriptionRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
                     Lens.Family2.view
                       (Data.ProtoLens.Field.field @"maybe'subscription") _x
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
instance Control.DeepSeq.NFData AddSubscriptionRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AddSubscriptionRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_AddSubscriptionRequest'actorId x__)
                (Control.DeepSeq.deepseq
                   (_AddSubscriptionRequest'subscription x__)
                   (Control.DeepSeq.deepseq
                      (_AddSubscriptionRequest'idempotencyKey x__) ())))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' AddSubscriptionResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' AddSubscriptionResponse (Prelude.Maybe ActorObservation)@ -}
data AddSubscriptionResponse
  = AddSubscriptionResponse'_constructor {_AddSubscriptionResponse'actor :: !(Prelude.Maybe ActorObservation),
                                          _AddSubscriptionResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show AddSubscriptionResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField AddSubscriptionResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AddSubscriptionResponse'actor
           (\ x__ y__ -> x__ {_AddSubscriptionResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField AddSubscriptionResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _AddSubscriptionResponse'actor
           (\ x__ y__ -> x__ {_AddSubscriptionResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message AddSubscriptionResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.AddSubscriptionResponse"
  packedMessageDescriptor _
    = "\n\
      \\ETBAddSubscriptionResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor AddSubscriptionResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _AddSubscriptionResponse'_unknownFields
        (\ x__ y__ -> x__ {_AddSubscriptionResponse'_unknownFields = y__})
  defMessage
    = AddSubscriptionResponse'_constructor
        {_AddSubscriptionResponse'actor = Prelude.Nothing,
         _AddSubscriptionResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          AddSubscriptionResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser AddSubscriptionResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "AddSubscriptionResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData AddSubscriptionResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_AddSubscriptionResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_AddSubscriptionResponse'actor x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.name' @:: Lens' Binding Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.capability' @:: Lens' Binding Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.resource' @:: Lens' Binding Data.Text.Text@ -}
data Binding
  = Binding'_constructor {_Binding'name :: !Data.Text.Text,
                          _Binding'capability :: !Data.Text.Text,
                          _Binding'resource :: !Data.Text.Text,
                          _Binding'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Binding where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Binding "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Binding'name (\ x__ y__ -> x__ {_Binding'name = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Binding "capability" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Binding'capability (\ x__ y__ -> x__ {_Binding'capability = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Binding "resource" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Binding'resource (\ x__ y__ -> x__ {_Binding'resource = y__}))
        Prelude.id
instance Data.ProtoLens.Message Binding where
  messageName _ = Data.Text.pack "acyclic.actors.v1.Binding"
  packedMessageDescriptor _
    = "\n\
      \\aBinding\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\RS\n\
      \\n\
      \capability\CAN\STX \SOH(\tR\n\
      \capability\DC2\SUB\n\
      \\bresource\CAN\ETX \SOH(\tR\bresource"
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
              Data.ProtoLens.FieldDescriptor Binding
        capability__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "capability"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"capability")) ::
              Data.ProtoLens.FieldDescriptor Binding
        resource__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "resource"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"resource")) ::
              Data.ProtoLens.FieldDescriptor Binding
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, name__field_descriptor),
           (Data.ProtoLens.Tag 2, capability__field_descriptor),
           (Data.ProtoLens.Tag 3, resource__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Binding'_unknownFields
        (\ x__ y__ -> x__ {_Binding'_unknownFields = y__})
  defMessage
    = Binding'_constructor
        {_Binding'name = Data.ProtoLens.fieldDefault,
         _Binding'capability = Data.ProtoLens.fieldDefault,
         _Binding'resource = Data.ProtoLens.fieldDefault,
         _Binding'_unknownFields = []}
  parseMessage
    = let
        loop :: Binding -> Data.ProtoLens.Encoding.Bytes.Parser Binding
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
                                       "capability"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"capability") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "resource"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"resource") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Binding"
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
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"capability") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"resource") _x
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
instance Control.DeepSeq.NFData Binding where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Binding'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Binding'name x__)
                (Control.DeepSeq.deepseq
                   (_Binding'capability x__)
                   (Control.DeepSeq.deepseq (_Binding'resource x__) ())))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' CheckpointActorRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.idempotencyKey' @:: Lens' CheckpointActorRequest Data.Text.Text@ -}
data CheckpointActorRequest
  = CheckpointActorRequest'_constructor {_CheckpointActorRequest'actorId :: !Data.Text.Text,
                                         _CheckpointActorRequest'idempotencyKey :: !Data.Text.Text,
                                         _CheckpointActorRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointActorRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointActorRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointActorRequest'actorId
           (\ x__ y__ -> x__ {_CheckpointActorRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointActorRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointActorRequest'idempotencyKey
           (\ x__ y__ -> x__ {_CheckpointActorRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointActorRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.CheckpointActorRequest"
  packedMessageDescriptor _
    = "\n\
      \\SYNCheckpointActorRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2'\n\
      \\SIidempotency_key\CAN\STX \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor CheckpointActorRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CheckpointActorRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointActorRequest'_unknownFields
        (\ x__ y__ -> x__ {_CheckpointActorRequest'_unknownFields = y__})
  defMessage
    = CheckpointActorRequest'_constructor
        {_CheckpointActorRequest'actorId = Data.ProtoLens.fieldDefault,
         _CheckpointActorRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _CheckpointActorRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointActorRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointActorRequest
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
                                       "actor_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
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
          (do loop Data.ProtoLens.defMessage) "CheckpointActorRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
instance Control.DeepSeq.NFData CheckpointActorRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointActorRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CheckpointActorRequest'actorId x__)
                (Control.DeepSeq.deepseq
                   (_CheckpointActorRequest'idempotencyKey x__) ()))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' CheckpointActorResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' CheckpointActorResponse (Prelude.Maybe ActorObservation)@ -}
data CheckpointActorResponse
  = CheckpointActorResponse'_constructor {_CheckpointActorResponse'actor :: !(Prelude.Maybe ActorObservation),
                                          _CheckpointActorResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointActorResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointActorResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointActorResponse'actor
           (\ x__ y__ -> x__ {_CheckpointActorResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointActorResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointActorResponse'actor
           (\ x__ y__ -> x__ {_CheckpointActorResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointActorResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.CheckpointActorResponse"
  packedMessageDescriptor _
    = "\n\
      \\ETBCheckpointActorResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor CheckpointActorResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointActorResponse'_unknownFields
        (\ x__ y__ -> x__ {_CheckpointActorResponse'_unknownFields = y__})
  defMessage
    = CheckpointActorResponse'_constructor
        {_CheckpointActorResponse'actor = Prelude.Nothing,
         _CheckpointActorResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointActorResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointActorResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CheckpointActorResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData CheckpointActorResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointActorResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CheckpointActorResponse'actor x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.codeSha256' @:: Lens' CreateActorRequest Data.ByteString.ByteString@
         * 'Proto.Actors.V1.Actors_Fields.homeRegion' @:: Lens' CreateActorRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.bindings' @:: Lens' CreateActorRequest [Binding]@
         * 'Proto.Actors.V1.Actors_Fields.vec'bindings' @:: Lens' CreateActorRequest (Data.Vector.Vector Binding)@
         * 'Proto.Actors.V1.Actors_Fields.limits' @:: Lens' CreateActorRequest ActorLimits@
         * 'Proto.Actors.V1.Actors_Fields.maybe'limits' @:: Lens' CreateActorRequest (Prelude.Maybe ActorLimits)@
         * 'Proto.Actors.V1.Actors_Fields.subscriptions' @:: Lens' CreateActorRequest [SubscriptionSpec]@
         * 'Proto.Actors.V1.Actors_Fields.vec'subscriptions' @:: Lens' CreateActorRequest (Data.Vector.Vector SubscriptionSpec)@
         * 'Proto.Actors.V1.Actors_Fields.idempotencyKey' @:: Lens' CreateActorRequest Data.Text.Text@ -}
data CreateActorRequest
  = CreateActorRequest'_constructor {_CreateActorRequest'codeSha256 :: !Data.ByteString.ByteString,
                                     _CreateActorRequest'homeRegion :: !Data.Text.Text,
                                     _CreateActorRequest'bindings :: !(Data.Vector.Vector Binding),
                                     _CreateActorRequest'limits :: !(Prelude.Maybe ActorLimits),
                                     _CreateActorRequest'subscriptions :: !(Data.Vector.Vector SubscriptionSpec),
                                     _CreateActorRequest'idempotencyKey :: !Data.Text.Text,
                                     _CreateActorRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateActorRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateActorRequest "codeSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'codeSha256
           (\ x__ y__ -> x__ {_CreateActorRequest'codeSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateActorRequest "homeRegion" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'homeRegion
           (\ x__ y__ -> x__ {_CreateActorRequest'homeRegion = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateActorRequest "bindings" [Binding] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'bindings
           (\ x__ y__ -> x__ {_CreateActorRequest'bindings = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CreateActorRequest "vec'bindings" (Data.Vector.Vector Binding) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'bindings
           (\ x__ y__ -> x__ {_CreateActorRequest'bindings = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateActorRequest "limits" ActorLimits where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'limits
           (\ x__ y__ -> x__ {_CreateActorRequest'limits = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateActorRequest "maybe'limits" (Prelude.Maybe ActorLimits) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'limits
           (\ x__ y__ -> x__ {_CreateActorRequest'limits = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateActorRequest "subscriptions" [SubscriptionSpec] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'subscriptions
           (\ x__ y__ -> x__ {_CreateActorRequest'subscriptions = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CreateActorRequest "vec'subscriptions" (Data.Vector.Vector SubscriptionSpec) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'subscriptions
           (\ x__ y__ -> x__ {_CreateActorRequest'subscriptions = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateActorRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorRequest'idempotencyKey
           (\ x__ y__ -> x__ {_CreateActorRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateActorRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.CreateActorRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2CreateActorRequest\DC2\US\n\
      \\vcode_sha256\CAN\SOH \SOH(\fR\n\
      \codeSha256\DC2\US\n\
      \\vhome_region\CAN\STX \SOH(\tR\n\
      \homeRegion\DC26\n\
      \\bbindings\CAN\ETX \ETX(\v2\SUB.acyclic.actors.v1.BindingR\bbindings\DC26\n\
      \\ACKlimits\CAN\EOT \SOH(\v2\RS.acyclic.actors.v1.ActorLimitsR\ACKlimits\DC2I\n\
      \\rsubscriptions\CAN\ENQ \ETX(\v2#.acyclic.actors.v1.SubscriptionSpecR\rsubscriptions\DC2'\n\
      \\SIidempotency_key\CAN\ACK \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        codeSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "code_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"codeSha256")) ::
              Data.ProtoLens.FieldDescriptor CreateActorRequest
        homeRegion__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "home_region"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"homeRegion")) ::
              Data.ProtoLens.FieldDescriptor CreateActorRequest
        bindings__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bindings"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Binding)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"bindings")) ::
              Data.ProtoLens.FieldDescriptor CreateActorRequest
        limits__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limits"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorLimits)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'limits")) ::
              Data.ProtoLens.FieldDescriptor CreateActorRequest
        subscriptions__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscriptions"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SubscriptionSpec)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"subscriptions")) ::
              Data.ProtoLens.FieldDescriptor CreateActorRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CreateActorRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, codeSha256__field_descriptor),
           (Data.ProtoLens.Tag 2, homeRegion__field_descriptor),
           (Data.ProtoLens.Tag 3, bindings__field_descriptor),
           (Data.ProtoLens.Tag 4, limits__field_descriptor),
           (Data.ProtoLens.Tag 5, subscriptions__field_descriptor),
           (Data.ProtoLens.Tag 6, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateActorRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateActorRequest'_unknownFields = y__})
  defMessage
    = CreateActorRequest'_constructor
        {_CreateActorRequest'codeSha256 = Data.ProtoLens.fieldDefault,
         _CreateActorRequest'homeRegion = Data.ProtoLens.fieldDefault,
         _CreateActorRequest'bindings = Data.Vector.Generic.empty,
         _CreateActorRequest'limits = Prelude.Nothing,
         _CreateActorRequest'subscriptions = Data.Vector.Generic.empty,
         _CreateActorRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _CreateActorRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateActorRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Binding
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld SubscriptionSpec
                -> Data.ProtoLens.Encoding.Bytes.Parser CreateActorRequest
        loop x mutable'bindings mutable'subscriptions
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'bindings <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'bindings)
                      frozen'subscriptions <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                   mutable'subscriptions)
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
                              (Data.ProtoLens.Field.field @"vec'bindings") frozen'bindings
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'subscriptions")
                                 frozen'subscriptions x)))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "code_sha256"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"codeSha256") y x)
                                  mutable'bindings mutable'subscriptions
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "home_region"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"homeRegion") y x)
                                  mutable'bindings mutable'subscriptions
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "bindings"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'bindings y)
                                loop x v mutable'subscriptions
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "limits"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"limits") y x)
                                  mutable'bindings mutable'subscriptions
                        42
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "subscriptions"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'subscriptions y)
                                loop x mutable'bindings v
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                                  mutable'bindings mutable'subscriptions
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'bindings mutable'subscriptions
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'bindings <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              mutable'subscriptions <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                         Data.ProtoLens.Encoding.Growing.new
              loop
                Data.ProtoLens.defMessage mutable'bindings mutable'subscriptions)
          "CreateActorRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"codeSha256") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"homeRegion") _x
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
                                 Data.ProtoLens.encodeMessage _v))
                      (Lens.Family2.view
                         (Data.ProtoLens.Field.field @"vec'bindings") _x))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'limits") _x
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
                            (Lens.Family2.view
                               (Data.ProtoLens.Field.field @"vec'subscriptions") _x))
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
instance Control.DeepSeq.NFData CreateActorRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateActorRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateActorRequest'codeSha256 x__)
                (Control.DeepSeq.deepseq
                   (_CreateActorRequest'homeRegion x__)
                   (Control.DeepSeq.deepseq
                      (_CreateActorRequest'bindings x__)
                      (Control.DeepSeq.deepseq
                         (_CreateActorRequest'limits x__)
                         (Control.DeepSeq.deepseq
                            (_CreateActorRequest'subscriptions x__)
                            (Control.DeepSeq.deepseq
                               (_CreateActorRequest'idempotencyKey x__) ()))))))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' CreateActorResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' CreateActorResponse (Prelude.Maybe ActorObservation)@ -}
data CreateActorResponse
  = CreateActorResponse'_constructor {_CreateActorResponse'actor :: !(Prelude.Maybe ActorObservation),
                                      _CreateActorResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateActorResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateActorResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorResponse'actor
           (\ x__ y__ -> x__ {_CreateActorResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateActorResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateActorResponse'actor
           (\ x__ y__ -> x__ {_CreateActorResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateActorResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.CreateActorResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC3CreateActorResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor CreateActorResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateActorResponse'_unknownFields
        (\ x__ y__ -> x__ {_CreateActorResponse'_unknownFields = y__})
  defMessage
    = CreateActorResponse'_constructor
        {_CreateActorResponse'actor = Prelude.Nothing,
         _CreateActorResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateActorResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser CreateActorResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CreateActorResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData CreateActorResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateActorResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CreateActorResponse'actor x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.code' @:: Lens' Error ErrorCode@
         * 'Proto.Actors.V1.Actors_Fields.message' @:: Lens' Error Data.Text.Text@ -}
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
  messageName _ = Data.Text.pack "acyclic.actors.v1.Error"
  packedMessageDescriptor _
    = "\n\
      \\ENQError\DC20\n\
      \\EOTcode\CAN\SOH \SOH(\SO2\FS.acyclic.actors.v1.ErrorCodeR\EOTcode\DC2\CAN\n\
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
    ERROR_CODE_ACTOR_NOT_FOUND |
    ERROR_CODE_SUBSCRIPTION_NOT_FOUND |
    ERROR_CODE_IDEMPOTENCY_MISMATCH |
    ERROR_CODE_CONFLICT |
    ERROR_CODE_ADMISSION_DENIED |
    ERROR_CODE_CHECKPOINT_FAILED |
    ERROR_CODE_DEPENDENCY_UNAVAILABLE |
    ErrorCode'Unrecognized !ErrorCode'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ErrorCode where
  maybeToEnum 0 = Prelude.Just ERROR_CODE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just ERROR_CODE_INVALID_ARGUMENT
  maybeToEnum 2 = Prelude.Just ERROR_CODE_CAPABILITY_DENIED
  maybeToEnum 3 = Prelude.Just ERROR_CODE_CAPABILITY_EXPIRED
  maybeToEnum 4 = Prelude.Just ERROR_CODE_ACTOR_NOT_FOUND
  maybeToEnum 5 = Prelude.Just ERROR_CODE_SUBSCRIPTION_NOT_FOUND
  maybeToEnum 6 = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
  maybeToEnum 7 = Prelude.Just ERROR_CODE_CONFLICT
  maybeToEnum 8 = Prelude.Just ERROR_CODE_ADMISSION_DENIED
  maybeToEnum 9 = Prelude.Just ERROR_CODE_CHECKPOINT_FAILED
  maybeToEnum 10 = Prelude.Just ERROR_CODE_DEPENDENCY_UNAVAILABLE
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
  showEnum ERROR_CODE_ACTOR_NOT_FOUND = "ERROR_CODE_ACTOR_NOT_FOUND"
  showEnum ERROR_CODE_SUBSCRIPTION_NOT_FOUND
    = "ERROR_CODE_SUBSCRIPTION_NOT_FOUND"
  showEnum ERROR_CODE_IDEMPOTENCY_MISMATCH
    = "ERROR_CODE_IDEMPOTENCY_MISMATCH"
  showEnum ERROR_CODE_CONFLICT = "ERROR_CODE_CONFLICT"
  showEnum ERROR_CODE_ADMISSION_DENIED
    = "ERROR_CODE_ADMISSION_DENIED"
  showEnum ERROR_CODE_CHECKPOINT_FAILED
    = "ERROR_CODE_CHECKPOINT_FAILED"
  showEnum ERROR_CODE_DEPENDENCY_UNAVAILABLE
    = "ERROR_CODE_DEPENDENCY_UNAVAILABLE"
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
    | (Prelude.==) k "ERROR_CODE_ACTOR_NOT_FOUND"
    = Prelude.Just ERROR_CODE_ACTOR_NOT_FOUND
    | (Prelude.==) k "ERROR_CODE_SUBSCRIPTION_NOT_FOUND"
    = Prelude.Just ERROR_CODE_SUBSCRIPTION_NOT_FOUND
    | (Prelude.==) k "ERROR_CODE_IDEMPOTENCY_MISMATCH"
    = Prelude.Just ERROR_CODE_IDEMPOTENCY_MISMATCH
    | (Prelude.==) k "ERROR_CODE_CONFLICT"
    = Prelude.Just ERROR_CODE_CONFLICT
    | (Prelude.==) k "ERROR_CODE_ADMISSION_DENIED"
    = Prelude.Just ERROR_CODE_ADMISSION_DENIED
    | (Prelude.==) k "ERROR_CODE_CHECKPOINT_FAILED"
    = Prelude.Just ERROR_CODE_CHECKPOINT_FAILED
    | (Prelude.==) k "ERROR_CODE_DEPENDENCY_UNAVAILABLE"
    = Prelude.Just ERROR_CODE_DEPENDENCY_UNAVAILABLE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ErrorCode where
  minBound = ERROR_CODE_UNSPECIFIED
  maxBound = ERROR_CODE_DEPENDENCY_UNAVAILABLE
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
  fromEnum ERROR_CODE_ACTOR_NOT_FOUND = 4
  fromEnum ERROR_CODE_SUBSCRIPTION_NOT_FOUND = 5
  fromEnum ERROR_CODE_IDEMPOTENCY_MISMATCH = 6
  fromEnum ERROR_CODE_CONFLICT = 7
  fromEnum ERROR_CODE_ADMISSION_DENIED = 8
  fromEnum ERROR_CODE_CHECKPOINT_FAILED = 9
  fromEnum ERROR_CODE_DEPENDENCY_UNAVAILABLE = 10
  fromEnum (ErrorCode'Unrecognized (ErrorCode'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ ERROR_CODE_DEPENDENCY_UNAVAILABLE
    = Prelude.error
        "ErrorCode.succ: bad argument ERROR_CODE_DEPENDENCY_UNAVAILABLE. This value would be out of bounds."
  succ ERROR_CODE_UNSPECIFIED = ERROR_CODE_INVALID_ARGUMENT
  succ ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_CAPABILITY_DENIED
  succ ERROR_CODE_CAPABILITY_DENIED = ERROR_CODE_CAPABILITY_EXPIRED
  succ ERROR_CODE_CAPABILITY_EXPIRED = ERROR_CODE_ACTOR_NOT_FOUND
  succ ERROR_CODE_ACTOR_NOT_FOUND = ERROR_CODE_SUBSCRIPTION_NOT_FOUND
  succ ERROR_CODE_SUBSCRIPTION_NOT_FOUND
    = ERROR_CODE_IDEMPOTENCY_MISMATCH
  succ ERROR_CODE_IDEMPOTENCY_MISMATCH = ERROR_CODE_CONFLICT
  succ ERROR_CODE_CONFLICT = ERROR_CODE_ADMISSION_DENIED
  succ ERROR_CODE_ADMISSION_DENIED = ERROR_CODE_CHECKPOINT_FAILED
  succ ERROR_CODE_CHECKPOINT_FAILED
    = ERROR_CODE_DEPENDENCY_UNAVAILABLE
  succ (ErrorCode'Unrecognized _)
    = Prelude.error "ErrorCode.succ: bad argument: unrecognized value"
  pred ERROR_CODE_UNSPECIFIED
    = Prelude.error
        "ErrorCode.pred: bad argument ERROR_CODE_UNSPECIFIED. This value would be out of bounds."
  pred ERROR_CODE_INVALID_ARGUMENT = ERROR_CODE_UNSPECIFIED
  pred ERROR_CODE_CAPABILITY_DENIED = ERROR_CODE_INVALID_ARGUMENT
  pred ERROR_CODE_CAPABILITY_EXPIRED = ERROR_CODE_CAPABILITY_DENIED
  pred ERROR_CODE_ACTOR_NOT_FOUND = ERROR_CODE_CAPABILITY_EXPIRED
  pred ERROR_CODE_SUBSCRIPTION_NOT_FOUND = ERROR_CODE_ACTOR_NOT_FOUND
  pred ERROR_CODE_IDEMPOTENCY_MISMATCH
    = ERROR_CODE_SUBSCRIPTION_NOT_FOUND
  pred ERROR_CODE_CONFLICT = ERROR_CODE_IDEMPOTENCY_MISMATCH
  pred ERROR_CODE_ADMISSION_DENIED = ERROR_CODE_CONFLICT
  pred ERROR_CODE_CHECKPOINT_FAILED = ERROR_CODE_ADMISSION_DENIED
  pred ERROR_CODE_DEPENDENCY_UNAVAILABLE
    = ERROR_CODE_CHECKPOINT_FAILED
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
     
         * 'Proto.Actors.V1.Actors_Fields.name' @:: Lens' Header Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.value' @:: Lens' Header Data.Text.Text@ -}
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
  messageName _ = Data.Text.pack "acyclic.actors.v1.Header"
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
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' InspectActorRequest Data.Text.Text@ -}
data InspectActorRequest
  = InspectActorRequest'_constructor {_InspectActorRequest'actorId :: !Data.Text.Text,
                                      _InspectActorRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectActorRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectActorRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectActorRequest'actorId
           (\ x__ y__ -> x__ {_InspectActorRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectActorRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.InspectActorRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3InspectActorRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor InspectActorRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectActorRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectActorRequest'_unknownFields = y__})
  defMessage
    = InspectActorRequest'_constructor
        {_InspectActorRequest'actorId = Data.ProtoLens.fieldDefault,
         _InspectActorRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectActorRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectActorRequest
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
                                       "actor_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectActorRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
instance Control.DeepSeq.NFData InspectActorRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectActorRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectActorRequest'actorId x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' InspectActorResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' InspectActorResponse (Prelude.Maybe ActorObservation)@ -}
data InspectActorResponse
  = InspectActorResponse'_constructor {_InspectActorResponse'actor :: !(Prelude.Maybe ActorObservation),
                                       _InspectActorResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectActorResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectActorResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectActorResponse'actor
           (\ x__ y__ -> x__ {_InspectActorResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectActorResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectActorResponse'actor
           (\ x__ y__ -> x__ {_InspectActorResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectActorResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.InspectActorResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC4InspectActorResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor InspectActorResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectActorResponse'_unknownFields
        (\ x__ y__ -> x__ {_InspectActorResponse'_unknownFields = y__})
  defMessage
    = InspectActorResponse'_constructor
        {_InspectActorResponse'actor = Prelude.Nothing,
         _InspectActorResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectActorResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectActorResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectActorResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData InspectActorResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectActorResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectActorResponse'actor x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' InvokeActorRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.method' @:: Lens' InvokeActorRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.url' @:: Lens' InvokeActorRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.body' @:: Lens' InvokeActorRequest Data.ByteString.ByteString@
         * 'Proto.Actors.V1.Actors_Fields.headers' @:: Lens' InvokeActorRequest [Header]@
         * 'Proto.Actors.V1.Actors_Fields.vec'headers' @:: Lens' InvokeActorRequest (Data.Vector.Vector Header)@ -}
data InvokeActorRequest
  = InvokeActorRequest'_constructor {_InvokeActorRequest'actorId :: !Data.Text.Text,
                                     _InvokeActorRequest'method :: !Data.Text.Text,
                                     _InvokeActorRequest'url :: !Data.Text.Text,
                                     _InvokeActorRequest'body :: !Data.ByteString.ByteString,
                                     _InvokeActorRequest'headers :: !(Data.Vector.Vector Header),
                                     _InvokeActorRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InvokeActorRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InvokeActorRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorRequest'actorId
           (\ x__ y__ -> x__ {_InvokeActorRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeActorRequest "method" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorRequest'method
           (\ x__ y__ -> x__ {_InvokeActorRequest'method = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeActorRequest "url" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorRequest'url
           (\ x__ y__ -> x__ {_InvokeActorRequest'url = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeActorRequest "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorRequest'body
           (\ x__ y__ -> x__ {_InvokeActorRequest'body = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeActorRequest "headers" [Header] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorRequest'headers
           (\ x__ y__ -> x__ {_InvokeActorRequest'headers = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField InvokeActorRequest "vec'headers" (Data.Vector.Vector Header) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorRequest'headers
           (\ x__ y__ -> x__ {_InvokeActorRequest'headers = y__}))
        Prelude.id
instance Data.ProtoLens.Message InvokeActorRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.InvokeActorRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2InvokeActorRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2\SYN\n\
      \\ACKmethod\CAN\STX \SOH(\tR\ACKmethod\DC2\DLE\n\
      \\ETXurl\CAN\ETX \SOH(\tR\ETXurl\DC2\DC2\n\
      \\EOTbody\CAN\EOT \SOH(\fR\EOTbody\DC23\n\
      \\aheaders\CAN\ENQ \ETX(\v2\EM.acyclic.actors.v1.HeaderR\aheaders"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorRequest
        method__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "method"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"method")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorRequest
        url__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "url"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"url")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorRequest
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"body")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorRequest
        headers__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "headers"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Header)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"headers")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, method__field_descriptor),
           (Data.ProtoLens.Tag 3, url__field_descriptor),
           (Data.ProtoLens.Tag 4, body__field_descriptor),
           (Data.ProtoLens.Tag 5, headers__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InvokeActorRequest'_unknownFields
        (\ x__ y__ -> x__ {_InvokeActorRequest'_unknownFields = y__})
  defMessage
    = InvokeActorRequest'_constructor
        {_InvokeActorRequest'actorId = Data.ProtoLens.fieldDefault,
         _InvokeActorRequest'method = Data.ProtoLens.fieldDefault,
         _InvokeActorRequest'url = Data.ProtoLens.fieldDefault,
         _InvokeActorRequest'body = Data.ProtoLens.fieldDefault,
         _InvokeActorRequest'headers = Data.Vector.Generic.empty,
         _InvokeActorRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InvokeActorRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Header
             -> Data.ProtoLens.Encoding.Bytes.Parser InvokeActorRequest
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
                                       "actor_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
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
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                                  mutable'headers
                        42
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "headers"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'headers y)
                                loop x v
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
          "InvokeActorRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"body") _x
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
                            (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'headers") _x))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData InvokeActorRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InvokeActorRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InvokeActorRequest'actorId x__)
                (Control.DeepSeq.deepseq
                   (_InvokeActorRequest'method x__)
                   (Control.DeepSeq.deepseq
                      (_InvokeActorRequest'url x__)
                      (Control.DeepSeq.deepseq
                         (_InvokeActorRequest'body x__)
                         (Control.DeepSeq.deepseq (_InvokeActorRequest'headers x__) ())))))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.status' @:: Lens' InvokeActorResponse Data.Word.Word32@
         * 'Proto.Actors.V1.Actors_Fields.body' @:: Lens' InvokeActorResponse Data.ByteString.ByteString@
         * 'Proto.Actors.V1.Actors_Fields.headers' @:: Lens' InvokeActorResponse [Header]@
         * 'Proto.Actors.V1.Actors_Fields.vec'headers' @:: Lens' InvokeActorResponse (Data.Vector.Vector Header)@ -}
data InvokeActorResponse
  = InvokeActorResponse'_constructor {_InvokeActorResponse'status :: !Data.Word.Word32,
                                      _InvokeActorResponse'body :: !Data.ByteString.ByteString,
                                      _InvokeActorResponse'headers :: !(Data.Vector.Vector Header),
                                      _InvokeActorResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InvokeActorResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InvokeActorResponse "status" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorResponse'status
           (\ x__ y__ -> x__ {_InvokeActorResponse'status = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeActorResponse "body" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorResponse'body
           (\ x__ y__ -> x__ {_InvokeActorResponse'body = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InvokeActorResponse "headers" [Header] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorResponse'headers
           (\ x__ y__ -> x__ {_InvokeActorResponse'headers = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField InvokeActorResponse "vec'headers" (Data.Vector.Vector Header) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InvokeActorResponse'headers
           (\ x__ y__ -> x__ {_InvokeActorResponse'headers = y__}))
        Prelude.id
instance Data.ProtoLens.Message InvokeActorResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.InvokeActorResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC3InvokeActorResponse\DC2\SYN\n\
      \\ACKstatus\CAN\SOH \SOH(\rR\ACKstatus\DC2\DC2\n\
      \\EOTbody\CAN\STX \SOH(\fR\EOTbody\DC23\n\
      \\aheaders\CAN\ETX \ETX(\v2\EM.acyclic.actors.v1.HeaderR\aheaders"
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
              Data.ProtoLens.FieldDescriptor InvokeActorResponse
        body__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "body"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"body")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorResponse
        headers__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "headers"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Header)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"headers")) ::
              Data.ProtoLens.FieldDescriptor InvokeActorResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, status__field_descriptor),
           (Data.ProtoLens.Tag 2, body__field_descriptor),
           (Data.ProtoLens.Tag 3, headers__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InvokeActorResponse'_unknownFields
        (\ x__ y__ -> x__ {_InvokeActorResponse'_unknownFields = y__})
  defMessage
    = InvokeActorResponse'_constructor
        {_InvokeActorResponse'status = Data.ProtoLens.fieldDefault,
         _InvokeActorResponse'body = Data.ProtoLens.fieldDefault,
         _InvokeActorResponse'headers = Data.Vector.Generic.empty,
         _InvokeActorResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InvokeActorResponse
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Header
             -> Data.ProtoLens.Encoding.Bytes.Parser InvokeActorResponse
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
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "body"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"body") y x)
                                  mutable'headers
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "headers"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'headers y)
                                loop x v
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
          "InvokeActorResponse"
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"body") _x
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
                                 Data.ProtoLens.encodeMessage _v))
                      (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'headers") _x))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData InvokeActorResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InvokeActorResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InvokeActorResponse'status x__)
                (Control.DeepSeq.deepseq
                   (_InvokeActorResponse'body x__)
                   (Control.DeepSeq.deepseq (_InvokeActorResponse'headers x__) ())))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' RemoveSubscriptionRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.subscriptionId' @:: Lens' RemoveSubscriptionRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.idempotencyKey' @:: Lens' RemoveSubscriptionRequest Data.Text.Text@ -}
data RemoveSubscriptionRequest
  = RemoveSubscriptionRequest'_constructor {_RemoveSubscriptionRequest'actorId :: !Data.Text.Text,
                                            _RemoveSubscriptionRequest'subscriptionId :: !Data.Text.Text,
                                            _RemoveSubscriptionRequest'idempotencyKey :: !Data.Text.Text,
                                            _RemoveSubscriptionRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RemoveSubscriptionRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RemoveSubscriptionRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RemoveSubscriptionRequest'actorId
           (\ x__ y__ -> x__ {_RemoveSubscriptionRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RemoveSubscriptionRequest "subscriptionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RemoveSubscriptionRequest'subscriptionId
           (\ x__ y__
              -> x__ {_RemoveSubscriptionRequest'subscriptionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RemoveSubscriptionRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RemoveSubscriptionRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_RemoveSubscriptionRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message RemoveSubscriptionRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.RemoveSubscriptionRequest"
  packedMessageDescriptor _
    = "\n\
      \\EMRemoveSubscriptionRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2'\n\
      \\SIsubscription_id\CAN\STX \SOH(\tR\SOsubscriptionId\DC2'\n\
      \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor RemoveSubscriptionRequest
        subscriptionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscription_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"subscriptionId")) ::
              Data.ProtoLens.FieldDescriptor RemoveSubscriptionRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor RemoveSubscriptionRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, subscriptionId__field_descriptor),
           (Data.ProtoLens.Tag 3, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RemoveSubscriptionRequest'_unknownFields
        (\ x__ y__
           -> x__ {_RemoveSubscriptionRequest'_unknownFields = y__})
  defMessage
    = RemoveSubscriptionRequest'_constructor
        {_RemoveSubscriptionRequest'actorId = Data.ProtoLens.fieldDefault,
         _RemoveSubscriptionRequest'subscriptionId = Data.ProtoLens.fieldDefault,
         _RemoveSubscriptionRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _RemoveSubscriptionRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RemoveSubscriptionRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser RemoveSubscriptionRequest
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
                                       "actor_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "subscription_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"subscriptionId") y x)
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
          (do loop Data.ProtoLens.defMessage) "RemoveSubscriptionRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
                         (Data.ProtoLens.Field.field @"subscriptionId") _x
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
instance Control.DeepSeq.NFData RemoveSubscriptionRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RemoveSubscriptionRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RemoveSubscriptionRequest'actorId x__)
                (Control.DeepSeq.deepseq
                   (_RemoveSubscriptionRequest'subscriptionId x__)
                   (Control.DeepSeq.deepseq
                      (_RemoveSubscriptionRequest'idempotencyKey x__) ())))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' RemoveSubscriptionResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' RemoveSubscriptionResponse (Prelude.Maybe ActorObservation)@ -}
data RemoveSubscriptionResponse
  = RemoveSubscriptionResponse'_constructor {_RemoveSubscriptionResponse'actor :: !(Prelude.Maybe ActorObservation),
                                             _RemoveSubscriptionResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RemoveSubscriptionResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RemoveSubscriptionResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RemoveSubscriptionResponse'actor
           (\ x__ y__ -> x__ {_RemoveSubscriptionResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RemoveSubscriptionResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RemoveSubscriptionResponse'actor
           (\ x__ y__ -> x__ {_RemoveSubscriptionResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message RemoveSubscriptionResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.RemoveSubscriptionResponse"
  packedMessageDescriptor _
    = "\n\
      \\SUBRemoveSubscriptionResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor RemoveSubscriptionResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RemoveSubscriptionResponse'_unknownFields
        (\ x__ y__
           -> x__ {_RemoveSubscriptionResponse'_unknownFields = y__})
  defMessage
    = RemoveSubscriptionResponse'_constructor
        {_RemoveSubscriptionResponse'actor = Prelude.Nothing,
         _RemoveSubscriptionResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RemoveSubscriptionResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser RemoveSubscriptionResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RemoveSubscriptionResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData RemoveSubscriptionResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RemoveSubscriptionResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RemoveSubscriptionResponse'actor x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' ResumeSubscriptionRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.subscriptionId' @:: Lens' ResumeSubscriptionRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.idempotencyKey' @:: Lens' ResumeSubscriptionRequest Data.Text.Text@ -}
data ResumeSubscriptionRequest
  = ResumeSubscriptionRequest'_constructor {_ResumeSubscriptionRequest'actorId :: !Data.Text.Text,
                                            _ResumeSubscriptionRequest'subscriptionId :: !Data.Text.Text,
                                            _ResumeSubscriptionRequest'idempotencyKey :: !Data.Text.Text,
                                            _ResumeSubscriptionRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ResumeSubscriptionRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ResumeSubscriptionRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ResumeSubscriptionRequest'actorId
           (\ x__ y__ -> x__ {_ResumeSubscriptionRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ResumeSubscriptionRequest "subscriptionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ResumeSubscriptionRequest'subscriptionId
           (\ x__ y__
              -> x__ {_ResumeSubscriptionRequest'subscriptionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ResumeSubscriptionRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ResumeSubscriptionRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_ResumeSubscriptionRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message ResumeSubscriptionRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.ResumeSubscriptionRequest"
  packedMessageDescriptor _
    = "\n\
      \\EMResumeSubscriptionRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2'\n\
      \\SIsubscription_id\CAN\STX \SOH(\tR\SOsubscriptionId\DC2'\n\
      \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor ResumeSubscriptionRequest
        subscriptionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscription_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"subscriptionId")) ::
              Data.ProtoLens.FieldDescriptor ResumeSubscriptionRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor ResumeSubscriptionRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, subscriptionId__field_descriptor),
           (Data.ProtoLens.Tag 3, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ResumeSubscriptionRequest'_unknownFields
        (\ x__ y__
           -> x__ {_ResumeSubscriptionRequest'_unknownFields = y__})
  defMessage
    = ResumeSubscriptionRequest'_constructor
        {_ResumeSubscriptionRequest'actorId = Data.ProtoLens.fieldDefault,
         _ResumeSubscriptionRequest'subscriptionId = Data.ProtoLens.fieldDefault,
         _ResumeSubscriptionRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _ResumeSubscriptionRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ResumeSubscriptionRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ResumeSubscriptionRequest
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
                                       "actor_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "subscription_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"subscriptionId") y x)
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
          (do loop Data.ProtoLens.defMessage) "ResumeSubscriptionRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
                         (Data.ProtoLens.Field.field @"subscriptionId") _x
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
instance Control.DeepSeq.NFData ResumeSubscriptionRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ResumeSubscriptionRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ResumeSubscriptionRequest'actorId x__)
                (Control.DeepSeq.deepseq
                   (_ResumeSubscriptionRequest'subscriptionId x__)
                   (Control.DeepSeq.deepseq
                      (_ResumeSubscriptionRequest'idempotencyKey x__) ())))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' ResumeSubscriptionResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' ResumeSubscriptionResponse (Prelude.Maybe ActorObservation)@ -}
data ResumeSubscriptionResponse
  = ResumeSubscriptionResponse'_constructor {_ResumeSubscriptionResponse'actor :: !(Prelude.Maybe ActorObservation),
                                             _ResumeSubscriptionResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ResumeSubscriptionResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ResumeSubscriptionResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ResumeSubscriptionResponse'actor
           (\ x__ y__ -> x__ {_ResumeSubscriptionResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ResumeSubscriptionResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ResumeSubscriptionResponse'actor
           (\ x__ y__ -> x__ {_ResumeSubscriptionResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message ResumeSubscriptionResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.ResumeSubscriptionResponse"
  packedMessageDescriptor _
    = "\n\
      \\SUBResumeSubscriptionResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor ResumeSubscriptionResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ResumeSubscriptionResponse'_unknownFields
        (\ x__ y__
           -> x__ {_ResumeSubscriptionResponse'_unknownFields = y__})
  defMessage
    = ResumeSubscriptionResponse'_constructor
        {_ResumeSubscriptionResponse'actor = Prelude.Nothing,
         _ResumeSubscriptionResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ResumeSubscriptionResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser ResumeSubscriptionResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ResumeSubscriptionResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData ResumeSubscriptionResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ResumeSubscriptionResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ResumeSubscriptionResponse'actor x__) ())
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.subscriptionId' @:: Lens' SubscriptionObservation Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.streamPath' @:: Lens' SubscriptionObservation Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.state' @:: Lens' SubscriptionObservation SubscriptionState@
         * 'Proto.Actors.V1.Actors_Fields.deliveredCursor' @:: Lens' SubscriptionObservation Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.completedCursor' @:: Lens' SubscriptionObservation Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.recoverableCursor' @:: Lens' SubscriptionObservation Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.placementAnchor' @:: Lens' SubscriptionObservation Prelude.Bool@
         * 'Proto.Actors.V1.Actors_Fields.retryCount' @:: Lens' SubscriptionObservation Data.Word.Word32@
         * 'Proto.Actors.V1.Actors_Fields.failureCode' @:: Lens' SubscriptionObservation Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.failedCursor' @:: Lens' SubscriptionObservation Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.maybe'failedCursor' @:: Lens' SubscriptionObservation (Prelude.Maybe Data.Word.Word64)@ -}
data SubscriptionObservation
  = SubscriptionObservation'_constructor {_SubscriptionObservation'subscriptionId :: !Data.Text.Text,
                                          _SubscriptionObservation'streamPath :: !Data.Text.Text,
                                          _SubscriptionObservation'state :: !SubscriptionState,
                                          _SubscriptionObservation'deliveredCursor :: !Data.Word.Word64,
                                          _SubscriptionObservation'completedCursor :: !Data.Word.Word64,
                                          _SubscriptionObservation'recoverableCursor :: !Data.Word.Word64,
                                          _SubscriptionObservation'placementAnchor :: !Prelude.Bool,
                                          _SubscriptionObservation'retryCount :: !Data.Word.Word32,
                                          _SubscriptionObservation'failureCode :: !Data.Text.Text,
                                          _SubscriptionObservation'failedCursor :: !(Prelude.Maybe Data.Word.Word64),
                                          _SubscriptionObservation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SubscriptionObservation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SubscriptionObservation "subscriptionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'subscriptionId
           (\ x__ y__ -> x__ {_SubscriptionObservation'subscriptionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "streamPath" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'streamPath
           (\ x__ y__ -> x__ {_SubscriptionObservation'streamPath = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "state" SubscriptionState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'state
           (\ x__ y__ -> x__ {_SubscriptionObservation'state = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "deliveredCursor" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'deliveredCursor
           (\ x__ y__
              -> x__ {_SubscriptionObservation'deliveredCursor = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "completedCursor" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'completedCursor
           (\ x__ y__
              -> x__ {_SubscriptionObservation'completedCursor = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "recoverableCursor" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'recoverableCursor
           (\ x__ y__
              -> x__ {_SubscriptionObservation'recoverableCursor = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "placementAnchor" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'placementAnchor
           (\ x__ y__
              -> x__ {_SubscriptionObservation'placementAnchor = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "retryCount" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'retryCount
           (\ x__ y__ -> x__ {_SubscriptionObservation'retryCount = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "failureCode" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'failureCode
           (\ x__ y__ -> x__ {_SubscriptionObservation'failureCode = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionObservation "failedCursor" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'failedCursor
           (\ x__ y__ -> x__ {_SubscriptionObservation'failedCursor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField SubscriptionObservation "maybe'failedCursor" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionObservation'failedCursor
           (\ x__ y__ -> x__ {_SubscriptionObservation'failedCursor = y__}))
        Prelude.id
instance Data.ProtoLens.Message SubscriptionObservation where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.SubscriptionObservation"
  packedMessageDescriptor _
    = "\n\
      \\ETBSubscriptionObservation\DC2'\n\
      \\SIsubscription_id\CAN\SOH \SOH(\tR\SOsubscriptionId\DC2\US\n\
      \\vstream_path\CAN\STX \SOH(\tR\n\
      \streamPath\DC2:\n\
      \\ENQstate\CAN\ETX \SOH(\SO2$.acyclic.actors.v1.SubscriptionStateR\ENQstate\DC2)\n\
      \\DLEdelivered_cursor\CAN\EOT \SOH(\EOTR\SIdeliveredCursor\DC2)\n\
      \\DLEcompleted_cursor\CAN\ENQ \SOH(\EOTR\SIcompletedCursor\DC2-\n\
      \\DC2recoverable_cursor\CAN\ACK \SOH(\EOTR\DC1recoverableCursor\DC2)\n\
      \\DLEplacement_anchor\CAN\a \SOH(\bR\SIplacementAnchor\DC2\US\n\
      \\vretry_count\CAN\b \SOH(\rR\n\
      \retryCount\DC2!\n\
      \\ffailure_code\CAN\t \SOH(\tR\vfailureCode\DC2(\n\
      \\rfailed_cursor\CAN\n\
      \ \SOH(\EOTH\NULR\ffailedCursor\136\SOH\SOHB\DLE\n\
      \\SO_failed_cursor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        subscriptionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscription_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"subscriptionId")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        streamPath__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "stream_path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"streamPath")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        state__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor SubscriptionState)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"state")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        deliveredCursor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "delivered_cursor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"deliveredCursor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        completedCursor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "completed_cursor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"completedCursor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        recoverableCursor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "recoverable_cursor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"recoverableCursor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        placementAnchor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "placement_anchor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"placementAnchor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        retryCount__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "retry_count"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"retryCount")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        failureCode__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "failure_code"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"failureCode")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
        failedCursor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "failed_cursor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'failedCursor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionObservation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, subscriptionId__field_descriptor),
           (Data.ProtoLens.Tag 2, streamPath__field_descriptor),
           (Data.ProtoLens.Tag 3, state__field_descriptor),
           (Data.ProtoLens.Tag 4, deliveredCursor__field_descriptor),
           (Data.ProtoLens.Tag 5, completedCursor__field_descriptor),
           (Data.ProtoLens.Tag 6, recoverableCursor__field_descriptor),
           (Data.ProtoLens.Tag 7, placementAnchor__field_descriptor),
           (Data.ProtoLens.Tag 8, retryCount__field_descriptor),
           (Data.ProtoLens.Tag 9, failureCode__field_descriptor),
           (Data.ProtoLens.Tag 10, failedCursor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SubscriptionObservation'_unknownFields
        (\ x__ y__ -> x__ {_SubscriptionObservation'_unknownFields = y__})
  defMessage
    = SubscriptionObservation'_constructor
        {_SubscriptionObservation'subscriptionId = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'streamPath = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'state = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'deliveredCursor = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'completedCursor = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'recoverableCursor = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'placementAnchor = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'retryCount = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'failureCode = Data.ProtoLens.fieldDefault,
         _SubscriptionObservation'failedCursor = Prelude.Nothing,
         _SubscriptionObservation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SubscriptionObservation
          -> Data.ProtoLens.Encoding.Bytes.Parser SubscriptionObservation
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
                                       "subscription_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"subscriptionId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "stream_path"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"streamPath") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "state"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"state") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "delivered_cursor"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"deliveredCursor") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "completed_cursor"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"completedCursor") y x)
                        48
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "recoverable_cursor"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"recoverableCursor") y x)
                        56
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "placement_anchor"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"placementAnchor") y x)
                        64
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "retry_count"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"retryCount") y x)
                        74
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "failure_code"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"failureCode") y x)
                        80
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "failed_cursor"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"failedCursor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SubscriptionObservation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"subscriptionId") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"streamPath") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"state") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            ((Prelude..)
                               ((Prelude..)
                                  Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral)
                               Prelude.fromEnum _v))
                   ((Data.Monoid.<>)
                      (let
                         _v
                           = Lens.Family2.view
                               (Data.ProtoLens.Field.field @"deliveredCursor") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (let
                            _v
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"completedCursor") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         ((Data.Monoid.<>)
                            (let
                               _v
                                 = Lens.Family2.view
                                     (Data.ProtoLens.Field.field @"recoverableCursor") _x
                             in
                               if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                   Data.Monoid.mempty
                               else
                                   (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt 48)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                            ((Data.Monoid.<>)
                               (let
                                  _v
                                    = Lens.Family2.view
                                        (Data.ProtoLens.Field.field @"placementAnchor") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 56)
                                        ((Prelude..)
                                           Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (\ b -> if b then 1 else 0) _v))
                               ((Data.Monoid.<>)
                                  (let
                                     _v
                                       = Lens.Family2.view
                                           (Data.ProtoLens.Field.field @"retryCount") _x
                                   in
                                     if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                         Data.Monoid.mempty
                                     else
                                         (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt 64)
                                           ((Prelude..)
                                              Data.ProtoLens.Encoding.Bytes.putVarInt
                                              Prelude.fromIntegral _v))
                                  ((Data.Monoid.<>)
                                     (let
                                        _v
                                          = Lens.Family2.view
                                              (Data.ProtoLens.Field.field @"failureCode") _x
                                      in
                                        if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                            Data.Monoid.mempty
                                        else
                                            (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt 74)
                                              ((Prelude..)
                                                 (\ bs
                                                    -> (Data.Monoid.<>)
                                                         (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                            (Prelude.fromIntegral
                                                               (Data.ByteString.length bs)))
                                                         (Data.ProtoLens.Encoding.Bytes.putBytes
                                                            bs))
                                                 Data.Text.Encoding.encodeUtf8 _v))
                                     ((Data.Monoid.<>)
                                        (case
                                             Lens.Family2.view
                                               (Data.ProtoLens.Field.field @"maybe'failedCursor") _x
                                         of
                                           Prelude.Nothing -> Data.Monoid.mempty
                                           (Prelude.Just _v)
                                             -> (Data.Monoid.<>)
                                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 80)
                                                  (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                                        (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                           (Lens.Family2.view
                                              Data.ProtoLens.unknownFields _x)))))))))))
instance Control.DeepSeq.NFData SubscriptionObservation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SubscriptionObservation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SubscriptionObservation'subscriptionId x__)
                (Control.DeepSeq.deepseq
                   (_SubscriptionObservation'streamPath x__)
                   (Control.DeepSeq.deepseq
                      (_SubscriptionObservation'state x__)
                      (Control.DeepSeq.deepseq
                         (_SubscriptionObservation'deliveredCursor x__)
                         (Control.DeepSeq.deepseq
                            (_SubscriptionObservation'completedCursor x__)
                            (Control.DeepSeq.deepseq
                               (_SubscriptionObservation'recoverableCursor x__)
                               (Control.DeepSeq.deepseq
                                  (_SubscriptionObservation'placementAnchor x__)
                                  (Control.DeepSeq.deepseq
                                     (_SubscriptionObservation'retryCount x__)
                                     (Control.DeepSeq.deepseq
                                        (_SubscriptionObservation'failureCode x__)
                                        (Control.DeepSeq.deepseq
                                           (_SubscriptionObservation'failedCursor x__) ()))))))))))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.subscriptionId' @:: Lens' SubscriptionSpec Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.streamPath' @:: Lens' SubscriptionSpec Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.start' @:: Lens' SubscriptionSpec SubscriptionStart@
         * 'Proto.Actors.V1.Actors_Fields.maybe'start' @:: Lens' SubscriptionSpec (Prelude.Maybe SubscriptionStart)@
         * 'Proto.Actors.V1.Actors_Fields.placementAnchor' @:: Lens' SubscriptionSpec Prelude.Bool@ -}
data SubscriptionSpec
  = SubscriptionSpec'_constructor {_SubscriptionSpec'subscriptionId :: !Data.Text.Text,
                                   _SubscriptionSpec'streamPath :: !Data.Text.Text,
                                   _SubscriptionSpec'start :: !(Prelude.Maybe SubscriptionStart),
                                   _SubscriptionSpec'placementAnchor :: !Prelude.Bool,
                                   _SubscriptionSpec'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SubscriptionSpec where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SubscriptionSpec "subscriptionId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionSpec'subscriptionId
           (\ x__ y__ -> x__ {_SubscriptionSpec'subscriptionId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionSpec "streamPath" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionSpec'streamPath
           (\ x__ y__ -> x__ {_SubscriptionSpec'streamPath = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionSpec "start" SubscriptionStart where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionSpec'start
           (\ x__ y__ -> x__ {_SubscriptionSpec'start = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SubscriptionSpec "maybe'start" (Prelude.Maybe SubscriptionStart) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionSpec'start
           (\ x__ y__ -> x__ {_SubscriptionSpec'start = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionSpec "placementAnchor" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionSpec'placementAnchor
           (\ x__ y__ -> x__ {_SubscriptionSpec'placementAnchor = y__}))
        Prelude.id
instance Data.ProtoLens.Message SubscriptionSpec where
  messageName _ = Data.Text.pack "acyclic.actors.v1.SubscriptionSpec"
  packedMessageDescriptor _
    = "\n\
      \\DLESubscriptionSpec\DC2'\n\
      \\SIsubscription_id\CAN\SOH \SOH(\tR\SOsubscriptionId\DC2\US\n\
      \\vstream_path\CAN\STX \SOH(\tR\n\
      \streamPath\DC2:\n\
      \\ENQstart\CAN\ETX \SOH(\v2$.acyclic.actors.v1.SubscriptionStartR\ENQstart\DC2)\n\
      \\DLEplacement_anchor\CAN\EOT \SOH(\bR\SIplacementAnchor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        subscriptionId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "subscription_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"subscriptionId")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionSpec
        streamPath__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "stream_path"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"streamPath")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionSpec
        start__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "start"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SubscriptionStart)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'start")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionSpec
        placementAnchor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "placement_anchor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"placementAnchor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionSpec
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, subscriptionId__field_descriptor),
           (Data.ProtoLens.Tag 2, streamPath__field_descriptor),
           (Data.ProtoLens.Tag 3, start__field_descriptor),
           (Data.ProtoLens.Tag 4, placementAnchor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SubscriptionSpec'_unknownFields
        (\ x__ y__ -> x__ {_SubscriptionSpec'_unknownFields = y__})
  defMessage
    = SubscriptionSpec'_constructor
        {_SubscriptionSpec'subscriptionId = Data.ProtoLens.fieldDefault,
         _SubscriptionSpec'streamPath = Data.ProtoLens.fieldDefault,
         _SubscriptionSpec'start = Prelude.Nothing,
         _SubscriptionSpec'placementAnchor = Data.ProtoLens.fieldDefault,
         _SubscriptionSpec'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SubscriptionSpec
          -> Data.ProtoLens.Encoding.Bytes.Parser SubscriptionSpec
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
                                       "subscription_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"subscriptionId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "stream_path"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"streamPath") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "start"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"start") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "placement_anchor"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"placementAnchor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SubscriptionSpec"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"subscriptionId") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"streamPath") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'start") _x
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
                         _v
                           = Lens.Family2.view
                               (Data.ProtoLens.Field.field @"placementAnchor") _x
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
instance Control.DeepSeq.NFData SubscriptionSpec where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SubscriptionSpec'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SubscriptionSpec'subscriptionId x__)
                (Control.DeepSeq.deepseq
                   (_SubscriptionSpec'streamPath x__)
                   (Control.DeepSeq.deepseq
                      (_SubscriptionSpec'start x__)
                      (Control.DeepSeq.deepseq
                         (_SubscriptionSpec'placementAnchor x__) ()))))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.maybe'start' @:: Lens' SubscriptionStart (Prelude.Maybe SubscriptionStart'Start)@
         * 'Proto.Actors.V1.Actors_Fields.maybe'cursor' @:: Lens' SubscriptionStart (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Actors.V1.Actors_Fields.cursor' @:: Lens' SubscriptionStart Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.maybe'currentHead' @:: Lens' SubscriptionStart (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Actors.V1.Actors_Fields.currentHead' @:: Lens' SubscriptionStart Prelude.Bool@ -}
data SubscriptionStart
  = SubscriptionStart'_constructor {_SubscriptionStart'start :: !(Prelude.Maybe SubscriptionStart'Start),
                                    _SubscriptionStart'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SubscriptionStart where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data SubscriptionStart'Start
  = SubscriptionStart'Cursor !Data.Word.Word64 |
    SubscriptionStart'CurrentHead !Prelude.Bool
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField SubscriptionStart "maybe'start" (Prelude.Maybe SubscriptionStart'Start) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionStart'start
           (\ x__ y__ -> x__ {_SubscriptionStart'start = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SubscriptionStart "maybe'cursor" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionStart'start
           (\ x__ y__ -> x__ {_SubscriptionStart'start = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (SubscriptionStart'Cursor x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap SubscriptionStart'Cursor y__))
instance Data.ProtoLens.Field.HasField SubscriptionStart "cursor" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionStart'start
           (\ x__ y__ -> x__ {_SubscriptionStart'start = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (SubscriptionStart'Cursor x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap SubscriptionStart'Cursor y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField SubscriptionStart "maybe'currentHead" (Prelude.Maybe Prelude.Bool) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionStart'start
           (\ x__ y__ -> x__ {_SubscriptionStart'start = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (SubscriptionStart'CurrentHead x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap SubscriptionStart'CurrentHead y__))
instance Data.ProtoLens.Field.HasField SubscriptionStart "currentHead" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SubscriptionStart'start
           (\ x__ y__ -> x__ {_SubscriptionStart'start = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (SubscriptionStart'CurrentHead x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap SubscriptionStart'CurrentHead y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message SubscriptionStart where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.SubscriptionStart"
  packedMessageDescriptor _
    = "\n\
      \\DC1SubscriptionStart\DC2\CAN\n\
      \\ACKcursor\CAN\SOH \SOH(\EOTH\NULR\ACKcursor\DC2#\n\
      \\fcurrent_head\CAN\STX \SOH(\bH\NULR\vcurrentHeadB\a\n\
      \\ENQstart"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        cursor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "cursor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'cursor")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionStart
        currentHead__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "current_head"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'currentHead")) ::
              Data.ProtoLens.FieldDescriptor SubscriptionStart
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, cursor__field_descriptor),
           (Data.ProtoLens.Tag 2, currentHead__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SubscriptionStart'_unknownFields
        (\ x__ y__ -> x__ {_SubscriptionStart'_unknownFields = y__})
  defMessage
    = SubscriptionStart'_constructor
        {_SubscriptionStart'start = Prelude.Nothing,
         _SubscriptionStart'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SubscriptionStart
          -> Data.ProtoLens.Encoding.Bytes.Parser SubscriptionStart
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
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "cursor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"cursor") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "current_head"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"currentHead") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SubscriptionStart"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'start") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (SubscriptionStart'Cursor v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt v)
                (Prelude.Just (SubscriptionStart'CurrentHead v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                       ((Prelude..)
                          Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                          v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData SubscriptionStart where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SubscriptionStart'_unknownFields x__)
             (Control.DeepSeq.deepseq (_SubscriptionStart'start x__) ())
instance Control.DeepSeq.NFData SubscriptionStart'Start where
  rnf (SubscriptionStart'Cursor x__) = Control.DeepSeq.rnf x__
  rnf (SubscriptionStart'CurrentHead x__) = Control.DeepSeq.rnf x__
_SubscriptionStart'Cursor ::
  Data.ProtoLens.Prism.Prism' SubscriptionStart'Start Data.Word.Word64
_SubscriptionStart'Cursor
  = Data.ProtoLens.Prism.prism'
      SubscriptionStart'Cursor
      (\ p__
         -> case p__ of
              (SubscriptionStart'Cursor p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_SubscriptionStart'CurrentHead ::
  Data.ProtoLens.Prism.Prism' SubscriptionStart'Start Prelude.Bool
_SubscriptionStart'CurrentHead
  = Data.ProtoLens.Prism.prism'
      SubscriptionStart'CurrentHead
      (\ p__
         -> case p__ of
              (SubscriptionStart'CurrentHead p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
newtype SubscriptionState'UnrecognizedValue
  = SubscriptionState'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data SubscriptionState
  = SUBSCRIPTION_STATE_UNSPECIFIED |
    SUBSCRIPTION_STATE_ACTIVE |
    SUBSCRIPTION_STATE_PAUSED |
    SubscriptionState'Unrecognized !SubscriptionState'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum SubscriptionState where
  maybeToEnum 0 = Prelude.Just SUBSCRIPTION_STATE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just SUBSCRIPTION_STATE_ACTIVE
  maybeToEnum 2 = Prelude.Just SUBSCRIPTION_STATE_PAUSED
  maybeToEnum k
    = Prelude.Just
        (SubscriptionState'Unrecognized
           (SubscriptionState'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum SUBSCRIPTION_STATE_UNSPECIFIED
    = "SUBSCRIPTION_STATE_UNSPECIFIED"
  showEnum SUBSCRIPTION_STATE_ACTIVE = "SUBSCRIPTION_STATE_ACTIVE"
  showEnum SUBSCRIPTION_STATE_PAUSED = "SUBSCRIPTION_STATE_PAUSED"
  showEnum
    (SubscriptionState'Unrecognized (SubscriptionState'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "SUBSCRIPTION_STATE_UNSPECIFIED"
    = Prelude.Just SUBSCRIPTION_STATE_UNSPECIFIED
    | (Prelude.==) k "SUBSCRIPTION_STATE_ACTIVE"
    = Prelude.Just SUBSCRIPTION_STATE_ACTIVE
    | (Prelude.==) k "SUBSCRIPTION_STATE_PAUSED"
    = Prelude.Just SUBSCRIPTION_STATE_PAUSED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded SubscriptionState where
  minBound = SUBSCRIPTION_STATE_UNSPECIFIED
  maxBound = SUBSCRIPTION_STATE_PAUSED
instance Prelude.Enum SubscriptionState where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum SubscriptionState: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum SUBSCRIPTION_STATE_UNSPECIFIED = 0
  fromEnum SUBSCRIPTION_STATE_ACTIVE = 1
  fromEnum SUBSCRIPTION_STATE_PAUSED = 2
  fromEnum
    (SubscriptionState'Unrecognized (SubscriptionState'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ SUBSCRIPTION_STATE_PAUSED
    = Prelude.error
        "SubscriptionState.succ: bad argument SUBSCRIPTION_STATE_PAUSED. This value would be out of bounds."
  succ SUBSCRIPTION_STATE_UNSPECIFIED = SUBSCRIPTION_STATE_ACTIVE
  succ SUBSCRIPTION_STATE_ACTIVE = SUBSCRIPTION_STATE_PAUSED
  succ (SubscriptionState'Unrecognized _)
    = Prelude.error
        "SubscriptionState.succ: bad argument: unrecognized value"
  pred SUBSCRIPTION_STATE_UNSPECIFIED
    = Prelude.error
        "SubscriptionState.pred: bad argument SUBSCRIPTION_STATE_UNSPECIFIED. This value would be out of bounds."
  pred SUBSCRIPTION_STATE_ACTIVE = SUBSCRIPTION_STATE_UNSPECIFIED
  pred SUBSCRIPTION_STATE_PAUSED = SUBSCRIPTION_STATE_ACTIVE
  pred (SubscriptionState'Unrecognized _)
    = Prelude.error
        "SubscriptionState.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault SubscriptionState where
  fieldDefault = SUBSCRIPTION_STATE_UNSPECIFIED
instance Control.DeepSeq.NFData SubscriptionState where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actorId' @:: Lens' UpdateActorRequest Data.Text.Text@
         * 'Proto.Actors.V1.Actors_Fields.codeSha256' @:: Lens' UpdateActorRequest Data.ByteString.ByteString@
         * 'Proto.Actors.V1.Actors_Fields.bindings' @:: Lens' UpdateActorRequest [Binding]@
         * 'Proto.Actors.V1.Actors_Fields.vec'bindings' @:: Lens' UpdateActorRequest (Data.Vector.Vector Binding)@
         * 'Proto.Actors.V1.Actors_Fields.limits' @:: Lens' UpdateActorRequest ActorLimits@
         * 'Proto.Actors.V1.Actors_Fields.maybe'limits' @:: Lens' UpdateActorRequest (Prelude.Maybe ActorLimits)@
         * 'Proto.Actors.V1.Actors_Fields.expectedConfigurationRevision' @:: Lens' UpdateActorRequest Data.Word.Word64@
         * 'Proto.Actors.V1.Actors_Fields.idempotencyKey' @:: Lens' UpdateActorRequest Data.Text.Text@ -}
data UpdateActorRequest
  = UpdateActorRequest'_constructor {_UpdateActorRequest'actorId :: !Data.Text.Text,
                                     _UpdateActorRequest'codeSha256 :: !Data.ByteString.ByteString,
                                     _UpdateActorRequest'bindings :: !(Data.Vector.Vector Binding),
                                     _UpdateActorRequest'limits :: !(Prelude.Maybe ActorLimits),
                                     _UpdateActorRequest'expectedConfigurationRevision :: !Data.Word.Word64,
                                     _UpdateActorRequest'idempotencyKey :: !Data.Text.Text,
                                     _UpdateActorRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UpdateActorRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UpdateActorRequest "actorId" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'actorId
           (\ x__ y__ -> x__ {_UpdateActorRequest'actorId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UpdateActorRequest "codeSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'codeSha256
           (\ x__ y__ -> x__ {_UpdateActorRequest'codeSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UpdateActorRequest "bindings" [Binding] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'bindings
           (\ x__ y__ -> x__ {_UpdateActorRequest'bindings = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField UpdateActorRequest "vec'bindings" (Data.Vector.Vector Binding) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'bindings
           (\ x__ y__ -> x__ {_UpdateActorRequest'bindings = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UpdateActorRequest "limits" ActorLimits where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'limits
           (\ x__ y__ -> x__ {_UpdateActorRequest'limits = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UpdateActorRequest "maybe'limits" (Prelude.Maybe ActorLimits) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'limits
           (\ x__ y__ -> x__ {_UpdateActorRequest'limits = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UpdateActorRequest "expectedConfigurationRevision" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'expectedConfigurationRevision
           (\ x__ y__
              -> x__ {_UpdateActorRequest'expectedConfigurationRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UpdateActorRequest "idempotencyKey" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorRequest'idempotencyKey
           (\ x__ y__ -> x__ {_UpdateActorRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message UpdateActorRequest where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.UpdateActorRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2UpdateActorRequest\DC2\EM\n\
      \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2\US\n\
      \\vcode_sha256\CAN\STX \SOH(\fR\n\
      \codeSha256\DC26\n\
      \\bbindings\CAN\ETX \ETX(\v2\SUB.acyclic.actors.v1.BindingR\bbindings\DC26\n\
      \\ACKlimits\CAN\EOT \SOH(\v2\RS.acyclic.actors.v1.ActorLimitsR\ACKlimits\DC2F\n\
      \\USexpected_configuration_revision\CAN\ENQ \SOH(\EOTR\GSexpectedConfigurationRevision\DC2'\n\
      \\SIidempotency_key\CAN\ACK \SOH(\tR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actorId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"actorId")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorRequest
        codeSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "code_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"codeSha256")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorRequest
        bindings__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "bindings"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Binding)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"bindings")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorRequest
        limits__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limits"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorLimits)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'limits")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorRequest
        expectedConfigurationRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expected_configuration_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expectedConfigurationRevision")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, actorId__field_descriptor),
           (Data.ProtoLens.Tag 2, codeSha256__field_descriptor),
           (Data.ProtoLens.Tag 3, bindings__field_descriptor),
           (Data.ProtoLens.Tag 4, limits__field_descriptor),
           (Data.ProtoLens.Tag 5, 
            expectedConfigurationRevision__field_descriptor),
           (Data.ProtoLens.Tag 6, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UpdateActorRequest'_unknownFields
        (\ x__ y__ -> x__ {_UpdateActorRequest'_unknownFields = y__})
  defMessage
    = UpdateActorRequest'_constructor
        {_UpdateActorRequest'actorId = Data.ProtoLens.fieldDefault,
         _UpdateActorRequest'codeSha256 = Data.ProtoLens.fieldDefault,
         _UpdateActorRequest'bindings = Data.Vector.Generic.empty,
         _UpdateActorRequest'limits = Prelude.Nothing,
         _UpdateActorRequest'expectedConfigurationRevision = Data.ProtoLens.fieldDefault,
         _UpdateActorRequest'idempotencyKey = Data.ProtoLens.fieldDefault,
         _UpdateActorRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UpdateActorRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Binding
             -> Data.ProtoLens.Encoding.Bytes.Parser UpdateActorRequest
        loop x mutable'bindings
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'bindings <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'bindings)
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
                              (Data.ProtoLens.Field.field @"vec'bindings") frozen'bindings x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "actor_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"actorId") y x)
                                  mutable'bindings
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "code_sha256"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"codeSha256") y x)
                                  mutable'bindings
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "bindings"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'bindings y)
                                loop x v
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "limits"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"limits") y x)
                                  mutable'bindings
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "expected_configuration_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"expectedConfigurationRevision") y
                                     x)
                                  mutable'bindings
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                                  mutable'bindings
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'bindings
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'bindings <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'bindings)
          "UpdateActorRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"actorId") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"codeSha256") _x
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
                                 Data.ProtoLens.encodeMessage _v))
                      (Lens.Family2.view
                         (Data.ProtoLens.Field.field @"vec'bindings") _x))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'limits") _x
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
                                  (Data.ProtoLens.Field.field @"expectedConfigurationRevision") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
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
instance Control.DeepSeq.NFData UpdateActorRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UpdateActorRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_UpdateActorRequest'actorId x__)
                (Control.DeepSeq.deepseq
                   (_UpdateActorRequest'codeSha256 x__)
                   (Control.DeepSeq.deepseq
                      (_UpdateActorRequest'bindings x__)
                      (Control.DeepSeq.deepseq
                         (_UpdateActorRequest'limits x__)
                         (Control.DeepSeq.deepseq
                            (_UpdateActorRequest'expectedConfigurationRevision x__)
                            (Control.DeepSeq.deepseq
                               (_UpdateActorRequest'idempotencyKey x__) ()))))))
{- | Fields :
     
         * 'Proto.Actors.V1.Actors_Fields.actor' @:: Lens' UpdateActorResponse ActorObservation@
         * 'Proto.Actors.V1.Actors_Fields.maybe'actor' @:: Lens' UpdateActorResponse (Prelude.Maybe ActorObservation)@ -}
data UpdateActorResponse
  = UpdateActorResponse'_constructor {_UpdateActorResponse'actor :: !(Prelude.Maybe ActorObservation),
                                      _UpdateActorResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UpdateActorResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UpdateActorResponse "actor" ActorObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorResponse'actor
           (\ x__ y__ -> x__ {_UpdateActorResponse'actor = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UpdateActorResponse "maybe'actor" (Prelude.Maybe ActorObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UpdateActorResponse'actor
           (\ x__ y__ -> x__ {_UpdateActorResponse'actor = y__}))
        Prelude.id
instance Data.ProtoLens.Message UpdateActorResponse where
  messageName _
    = Data.Text.pack "acyclic.actors.v1.UpdateActorResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC3UpdateActorResponse\DC29\n\
      \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        actor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "actor"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ActorObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'actor")) ::
              Data.ProtoLens.FieldDescriptor UpdateActorResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, actor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UpdateActorResponse'_unknownFields
        (\ x__ y__ -> x__ {_UpdateActorResponse'_unknownFields = y__})
  defMessage
    = UpdateActorResponse'_constructor
        {_UpdateActorResponse'actor = Prelude.Nothing,
         _UpdateActorResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UpdateActorResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser UpdateActorResponse
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
                                       "actor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"actor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UpdateActorResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'actor") _x
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
instance Control.DeepSeq.NFData UpdateActorResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UpdateActorResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_UpdateActorResponse'actor x__) ())
data ActorsService = ActorsService {}
instance Data.ProtoLens.Service.Types.Service ActorsService where
  type ServiceName ActorsService = "ActorsService"
  type ServicePackage ActorsService = "acyclic.actors.v1"
  type ServiceMethods ActorsService = '["addSubscription",
                                        "checkpointActor",
                                        "createActor",
                                        "inspectActor",
                                        "invokeActor",
                                        "removeSubscription",
                                        "resumeSubscription",
                                        "updateActor"]
  packedServiceDescriptor _
    = "\n\
      \\rActorsService\DC2\\\n\
      \\vCreateActor\DC2%.acyclic.actors.v1.CreateActorRequest\SUB&.acyclic.actors.v1.CreateActorResponse\DC2\\\n\
      \\vUpdateActor\DC2%.acyclic.actors.v1.UpdateActorRequest\SUB&.acyclic.actors.v1.UpdateActorResponse\DC2_\n\
      \\fInspectActor\DC2&.acyclic.actors.v1.InspectActorRequest\SUB'.acyclic.actors.v1.InspectActorResponse\DC2h\n\
      \\SIAddSubscription\DC2).acyclic.actors.v1.AddSubscriptionRequest\SUB*.acyclic.actors.v1.AddSubscriptionResponse\DC2q\n\
      \\DC2RemoveSubscription\DC2,.acyclic.actors.v1.RemoveSubscriptionRequest\SUB-.acyclic.actors.v1.RemoveSubscriptionResponse\DC2q\n\
      \\DC2ResumeSubscription\DC2,.acyclic.actors.v1.ResumeSubscriptionRequest\SUB-.acyclic.actors.v1.ResumeSubscriptionResponse\DC2h\n\
      \\SICheckpointActor\DC2).acyclic.actors.v1.CheckpointActorRequest\SUB*.acyclic.actors.v1.CheckpointActorResponse\DC2\\\n\
      \\vInvokeActor\DC2%.acyclic.actors.v1.InvokeActorRequest\SUB&.acyclic.actors.v1.InvokeActorResponse"
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "createActor" where
  type MethodName ActorsService "createActor" = "CreateActor"
  type MethodInput ActorsService "createActor" = CreateActorRequest
  type MethodOutput ActorsService "createActor" = CreateActorResponse
  type MethodStreamingType ActorsService "createActor" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "updateActor" where
  type MethodName ActorsService "updateActor" = "UpdateActor"
  type MethodInput ActorsService "updateActor" = UpdateActorRequest
  type MethodOutput ActorsService "updateActor" = UpdateActorResponse
  type MethodStreamingType ActorsService "updateActor" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "inspectActor" where
  type MethodName ActorsService "inspectActor" = "InspectActor"
  type MethodInput ActorsService "inspectActor" = InspectActorRequest
  type MethodOutput ActorsService "inspectActor" = InspectActorResponse
  type MethodStreamingType ActorsService "inspectActor" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "addSubscription" where
  type MethodName ActorsService "addSubscription" = "AddSubscription"
  type MethodInput ActorsService "addSubscription" = AddSubscriptionRequest
  type MethodOutput ActorsService "addSubscription" = AddSubscriptionResponse
  type MethodStreamingType ActorsService "addSubscription" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "removeSubscription" where
  type MethodName ActorsService "removeSubscription" = "RemoveSubscription"
  type MethodInput ActorsService "removeSubscription" = RemoveSubscriptionRequest
  type MethodOutput ActorsService "removeSubscription" = RemoveSubscriptionResponse
  type MethodStreamingType ActorsService "removeSubscription" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "resumeSubscription" where
  type MethodName ActorsService "resumeSubscription" = "ResumeSubscription"
  type MethodInput ActorsService "resumeSubscription" = ResumeSubscriptionRequest
  type MethodOutput ActorsService "resumeSubscription" = ResumeSubscriptionResponse
  type MethodStreamingType ActorsService "resumeSubscription" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "checkpointActor" where
  type MethodName ActorsService "checkpointActor" = "CheckpointActor"
  type MethodInput ActorsService "checkpointActor" = CheckpointActorRequest
  type MethodOutput ActorsService "checkpointActor" = CheckpointActorResponse
  type MethodStreamingType ActorsService "checkpointActor" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ActorsService "invokeActor" where
  type MethodName ActorsService "invokeActor" = "InvokeActor"
  type MethodInput ActorsService "invokeActor" = InvokeActorRequest
  type MethodOutput ActorsService "invokeActor" = InvokeActorResponse
  type MethodStreamingType ActorsService "invokeActor" = 'Data.ProtoLens.Service.Types.NonStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\SYNactors/v1/actors.proto\DC2\DC1acyclic.actors.v1\"Y\n\
    \\aBinding\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\RS\n\
    \\n\
    \capability\CAN\STX \SOH(\tR\n\
    \capability\DC2\SUB\n\
    \\bresource\CAN\ETX \SOH(\tR\bresource\"\145\SOH\n\
    \\vActorLimits\DC24\n\
    \\SYNhandler_timeout_millis\CAN\SOH \SOH(\EOTR\DC4handlerTimeoutMillis\DC2!\n\
    \\fmemory_bytes\CAN\STX \SOH(\EOTR\vmemoryBytes\DC2)\n\
    \\DLEcheckpoint_bytes\CAN\ETX \SOH(\EOTR\SIcheckpointBytes\"[\n\
    \\DC1SubscriptionStart\DC2\CAN\n\
    \\ACKcursor\CAN\SOH \SOH(\EOTH\NULR\ACKcursor\DC2#\n\
    \\fcurrent_head\CAN\STX \SOH(\bH\NULR\vcurrentHeadB\a\n\
    \\ENQstart\"\195\SOH\n\
    \\DLESubscriptionSpec\DC2'\n\
    \\SIsubscription_id\CAN\SOH \SOH(\tR\SOsubscriptionId\DC2\US\n\
    \\vstream_path\CAN\STX \SOH(\tR\n\
    \streamPath\DC2:\n\
    \\ENQstart\CAN\ETX \SOH(\v2$.acyclic.actors.v1.SubscriptionStartR\ENQstart\DC2)\n\
    \\DLEplacement_anchor\CAN\EOT \SOH(\bR\SIplacementAnchor\"\207\ETX\n\
    \\ETBSubscriptionObservation\DC2'\n\
    \\SIsubscription_id\CAN\SOH \SOH(\tR\SOsubscriptionId\DC2\US\n\
    \\vstream_path\CAN\STX \SOH(\tR\n\
    \streamPath\DC2:\n\
    \\ENQstate\CAN\ETX \SOH(\SO2$.acyclic.actors.v1.SubscriptionStateR\ENQstate\DC2)\n\
    \\DLEdelivered_cursor\CAN\EOT \SOH(\EOTR\SIdeliveredCursor\DC2)\n\
    \\DLEcompleted_cursor\CAN\ENQ \SOH(\EOTR\SIcompletedCursor\DC2-\n\
    \\DC2recoverable_cursor\CAN\ACK \SOH(\EOTR\DC1recoverableCursor\DC2)\n\
    \\DLEplacement_anchor\CAN\a \SOH(\bR\SIplacementAnchor\DC2\US\n\
    \\vretry_count\CAN\b \SOH(\rR\n\
    \retryCount\DC2!\n\
    \\ffailure_code\CAN\t \SOH(\tR\vfailureCode\DC2(\n\
    \\rfailed_cursor\CAN\n\
    \ \SOH(\EOTH\NULR\ffailedCursor\136\SOH\SOHB\DLE\n\
    \\SO_failed_cursor\"\174\ETX\n\
    \\DLEActorObservation\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2\US\n\
    \\vcode_sha256\CAN\STX \SOH(\fR\n\
    \codeSha256\DC2\US\n\
    \\vhome_region\CAN\ETX \SOH(\tR\n\
    \homeRegion\DC23\n\
    \\ENQstate\CAN\EOT \SOH(\SO2\GS.acyclic.actors.v1.ActorStateR\ENQstate\DC2P\n\
    \\rsubscriptions\CAN\ENQ \ETX(\v2*.acyclic.actors.v1.SubscriptionObservationR\rsubscriptions\DC29\n\
    \\SYNcheckpoint_unix_millis\CAN\ACK \SOH(\EOTH\NULR\DC4checkpointUnixMillis\136\SOH\SOH\DC2)\n\
    \\DLEcheckpoint_epoch\CAN\a \SOH(\EOTR\SIcheckpointEpoch\DC25\n\
    \\SYNconfiguration_revision\CAN\b \SOH(\EOTR\NAKconfigurationRevisionB\EM\n\
    \\ETB_checkpoint_unix_millis\"\186\STX\n\
    \\DC2CreateActorRequest\DC2\US\n\
    \\vcode_sha256\CAN\SOH \SOH(\fR\n\
    \codeSha256\DC2\US\n\
    \\vhome_region\CAN\STX \SOH(\tR\n\
    \homeRegion\DC26\n\
    \\bbindings\CAN\ETX \ETX(\v2\SUB.acyclic.actors.v1.BindingR\bbindings\DC26\n\
    \\ACKlimits\CAN\EOT \SOH(\v2\RS.acyclic.actors.v1.ActorLimitsR\ACKlimits\DC2I\n\
    \\rsubscriptions\CAN\ENQ \ETX(\v2#.acyclic.actors.v1.SubscriptionSpecR\rsubscriptions\DC2'\n\
    \\SIidempotency_key\CAN\ACK \SOH(\tR\SOidempotencyKey\"P\n\
    \\DC3CreateActorResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"\177\STX\n\
    \\DC2UpdateActorRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2\US\n\
    \\vcode_sha256\CAN\STX \SOH(\fR\n\
    \codeSha256\DC26\n\
    \\bbindings\CAN\ETX \ETX(\v2\SUB.acyclic.actors.v1.BindingR\bbindings\DC26\n\
    \\ACKlimits\CAN\EOT \SOH(\v2\RS.acyclic.actors.v1.ActorLimitsR\ACKlimits\DC2F\n\
    \\USexpected_configuration_revision\CAN\ENQ \SOH(\EOTR\GSexpectedConfigurationRevision\DC2'\n\
    \\SIidempotency_key\CAN\ACK \SOH(\tR\SOidempotencyKey\"P\n\
    \\DC3UpdateActorResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"0\n\
    \\DC3InspectActorRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\"Q\n\
    \\DC4InspectActorResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"\165\SOH\n\
    \\SYNAddSubscriptionRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2G\n\
    \\fsubscription\CAN\STX \SOH(\v2#.acyclic.actors.v1.SubscriptionSpecR\fsubscription\DC2'\n\
    \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey\"T\n\
    \\ETBAddSubscriptionResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"\136\SOH\n\
    \\EMRemoveSubscriptionRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2'\n\
    \\SIsubscription_id\CAN\STX \SOH(\tR\SOsubscriptionId\DC2'\n\
    \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey\"W\n\
    \\SUBRemoveSubscriptionResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"\136\SOH\n\
    \\EMResumeSubscriptionRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2'\n\
    \\SIsubscription_id\CAN\STX \SOH(\tR\SOsubscriptionId\DC2'\n\
    \\SIidempotency_key\CAN\ETX \SOH(\tR\SOidempotencyKey\"W\n\
    \\SUBResumeSubscriptionResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"\\\n\
    \\SYNCheckpointActorRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2'\n\
    \\SIidempotency_key\CAN\STX \SOH(\tR\SOidempotencyKey\"T\n\
    \\ETBCheckpointActorResponse\DC29\n\
    \\ENQactor\CAN\SOH \SOH(\v2#.acyclic.actors.v1.ActorObservationR\ENQactor\"2\n\
    \\ACKHeader\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\DC4\n\
    \\ENQvalue\CAN\STX \SOH(\tR\ENQvalue\"\162\SOH\n\
    \\DC2InvokeActorRequest\DC2\EM\n\
    \\bactor_id\CAN\SOH \SOH(\tR\aactorId\DC2\SYN\n\
    \\ACKmethod\CAN\STX \SOH(\tR\ACKmethod\DC2\DLE\n\
    \\ETXurl\CAN\ETX \SOH(\tR\ETXurl\DC2\DC2\n\
    \\EOTbody\CAN\EOT \SOH(\fR\EOTbody\DC23\n\
    \\aheaders\CAN\ENQ \ETX(\v2\EM.acyclic.actors.v1.HeaderR\aheaders\"v\n\
    \\DC3InvokeActorResponse\DC2\SYN\n\
    \\ACKstatus\CAN\SOH \SOH(\rR\ACKstatus\DC2\DC2\n\
    \\EOTbody\CAN\STX \SOH(\fR\EOTbody\DC23\n\
    \\aheaders\CAN\ETX \ETX(\v2\EM.acyclic.actors.v1.HeaderR\aheaders\"S\n\
    \\ENQError\DC20\n\
    \\EOTcode\CAN\SOH \SOH(\SO2\FS.acyclic.actors.v1.ErrorCodeR\EOTcode\DC2\CAN\n\
    \\amessage\CAN\STX \SOH(\tR\amessage*u\n\
    \\DC1SubscriptionState\DC2\"\n\
    \\RSSUBSCRIPTION_STATE_UNSPECIFIED\DLE\NUL\DC2\GS\n\
    \\EMSUBSCRIPTION_STATE_ACTIVE\DLE\SOH\DC2\GS\n\
    \\EMSUBSCRIPTION_STATE_PAUSED\DLE\STX*u\n\
    \\n\
    \ActorState\DC2\ESC\n\
    \\ETBACTOR_STATE_UNSPECIFIED\DLE\NUL\DC2\SYN\n\
    \\DC2ACTOR_STATE_ACTIVE\DLE\SOH\DC2\SUB\n\
    \\SYNACTOR_STATE_HIBERNATED\DLE\STX\DC2\SYN\n\
    \\DC2ACTOR_STATE_PAUSED\DLE\ETX*\252\STX\n\
    \\tErrorCode\DC2\SUB\n\
    \\SYNERROR_CODE_UNSPECIFIED\DLE\NUL\DC2\US\n\
    \\ESCERROR_CODE_INVALID_ARGUMENT\DLE\SOH\DC2 \n\
    \\FSERROR_CODE_CAPABILITY_DENIED\DLE\STX\DC2!\n\
    \\GSERROR_CODE_CAPABILITY_EXPIRED\DLE\ETX\DC2\RS\n\
    \\SUBERROR_CODE_ACTOR_NOT_FOUND\DLE\EOT\DC2%\n\
    \!ERROR_CODE_SUBSCRIPTION_NOT_FOUND\DLE\ENQ\DC2#\n\
    \\USERROR_CODE_IDEMPOTENCY_MISMATCH\DLE\ACK\DC2\ETB\n\
    \\DC3ERROR_CODE_CONFLICT\DLE\a\DC2\US\n\
    \\ESCERROR_CODE_ADMISSION_DENIED\DLE\b\DC2 \n\
    \\FSERROR_CODE_CHECKPOINT_FAILED\DLE\t\DC2%\n\
    \!ERROR_CODE_DEPENDENCY_UNAVAILABLE\DLE\n\
    \2\196\ACK\n\
    \\rActorsService\DC2\\\n\
    \\vCreateActor\DC2%.acyclic.actors.v1.CreateActorRequest\SUB&.acyclic.actors.v1.CreateActorResponse\DC2\\\n\
    \\vUpdateActor\DC2%.acyclic.actors.v1.UpdateActorRequest\SUB&.acyclic.actors.v1.UpdateActorResponse\DC2_\n\
    \\fInspectActor\DC2&.acyclic.actors.v1.InspectActorRequest\SUB'.acyclic.actors.v1.InspectActorResponse\DC2h\n\
    \\SIAddSubscription\DC2).acyclic.actors.v1.AddSubscriptionRequest\SUB*.acyclic.actors.v1.AddSubscriptionResponse\DC2q\n\
    \\DC2RemoveSubscription\DC2,.acyclic.actors.v1.RemoveSubscriptionRequest\SUB-.acyclic.actors.v1.RemoveSubscriptionResponse\DC2q\n\
    \\DC2ResumeSubscription\DC2,.acyclic.actors.v1.ResumeSubscriptionRequest\SUB-.acyclic.actors.v1.ResumeSubscriptionResponse\DC2h\n\
    \\SICheckpointActor\DC2).acyclic.actors.v1.CheckpointActorRequest\SUB*.acyclic.actors.v1.CheckpointActorResponse\DC2\\\n\
    \\vInvokeActor\DC2%.acyclic.actors.v1.InvokeActorRequest\SUB&.acyclic.actors.v1.InvokeActorResponseB7Z5github.com/acyclic-labs/sdk/go/gen/actors/v1;actorsv1J\170\&3\n\
    \\a\DC2\ENQ\NUL\NUL\167\SOH\SOH\n\
    \\b\n\
    \\SOH\f\DC2\ETX\NUL\NUL\DC2\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\SOH\NUL\SUB\n\
    \\b\n\
    \\SOH\b\DC2\ETX\ETX\NULL\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\ETX\NULL\n\
    \\n\
    \\n\
    \\STX\EOT\NUL\DC2\EOT\ENQ\NUL\t\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\ENQ\b\SI\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\ACK\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\ACK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\ACK\t\r\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\ACK\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\SOH\DC2\ETX\a\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ENQ\DC2\ETX\a\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\SOH\DC2\ETX\a\t\DC3\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ETX\DC2\ETX\a\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\STX\DC2\ETX\b\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\STX\ENQ\DC2\ETX\b\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\STX\SOH\DC2\ETX\b\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\STX\ETX\DC2\ETX\b\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\n\
    \\NUL\SO\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\n\
    \\b\DC3\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\v\STX$\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ENQ\DC2\ETX\v\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\v\t\US\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\v\"#\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\SOH\DC2\ETX\f\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ENQ\DC2\ETX\f\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\SOH\DC2\ETX\f\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ETX\DC2\ETX\f\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\STX\DC2\ETX\r\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\ENQ\DC2\ETX\r\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\SOH\DC2\ETX\r\t\EM\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\STX\ETX\DC2\ETX\r\FS\GS\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\SI\NUL\DC4\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\SI\b\EM\n\
    \\f\n\
    \\EOT\EOT\STX\b\NUL\DC2\EOT\DLE\STX\DC3\ETX\n\
    \\f\n\
    \\ENQ\EOT\STX\b\NUL\SOH\DC2\ETX\DLE\b\r\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\DC1\EOT\SYN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ENQ\DC2\ETX\DC1\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\DC1\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\DC1\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\STX\STX\SOH\DC2\ETX\DC2\EOT\SUB\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ENQ\DC2\ETX\DC2\EOT\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\SOH\DC2\ETX\DC2\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ETX\DC2\ETX\DC2\CAN\EM\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT\NAK\NUL\SUB\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX\NAK\b\CAN\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX\SYN\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ENQ\DC2\ETX\SYN\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX\SYN\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX\SYN\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\SOH\DC2\ETX\ETB\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ENQ\DC2\ETX\ETB\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\SOH\DC2\ETX\ETB\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ETX\DC2\ETX\ETB\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\STX\DC2\ETX\CAN\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\ACK\DC2\ETX\CAN\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\SOH\DC2\ETX\CAN\DC4\EM\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\ETX\DC2\ETX\CAN\FS\GS\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\ETX\DC2\ETX\EM\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ETX\ENQ\DC2\ETX\EM\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ETX\SOH\DC2\ETX\EM\a\ETB\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\ETX\ETX\DC2\ETX\EM\SUB\ESC\n\
    \\n\
    \\n\
    \\STX\ENQ\NUL\DC2\EOT\ESC\NUL\US\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETX\ESC\ENQ\SYN\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETX\FS\STX%\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETX\FS\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETX\FS#$\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETX\GS\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETX\GS\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETX\GS\RS\US\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\STX\DC2\ETX\RS\STX \n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\SOH\DC2\ETX\RS\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\STX\DC2\ETX\RS\RS\US\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT \NUL+\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX \b\US\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX!\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ENQ\DC2\ETX!\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX!\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX!\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\SOH\DC2\ETX\"\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ENQ\DC2\ETX\"\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\SOH\DC2\ETX\"\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ETX\DC2\ETX\"\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\STX\DC2\ETX#\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ACK\DC2\ETX#\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\SOH\DC2\ETX#\DC4\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ETX\DC2\ETX#\FS\GS\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\ETX\DC2\ETX$\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\ENQ\DC2\ETX$\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\SOH\DC2\ETX$\t\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\ETX\DC2\ETX$\FS\GS\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\EOT\DC2\ETX%\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\EOT\ENQ\DC2\ETX%\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\EOT\SOH\DC2\ETX%\t\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\EOT\ETX\DC2\ETX%\FS\GS\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\ENQ\DC2\ETX&\STX \n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ENQ\ENQ\DC2\ETX&\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ENQ\SOH\DC2\ETX&\t\ESC\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ENQ\ETX\DC2\ETX&\RS\US\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\ACK\DC2\ETX'\STX\FS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ACK\ENQ\DC2\ETX'\STX\ACK\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ACK\SOH\DC2\ETX'\a\ETB\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ACK\ETX\DC2\ETX'\SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\a\DC2\ETX(\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\a\ENQ\DC2\ETX(\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\a\SOH\DC2\ETX(\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\a\ETX\DC2\ETX(\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\b\DC2\ETX)\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\b\ENQ\DC2\ETX)\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\b\SOH\DC2\ETX)\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\b\ETX\DC2\ETX)\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\t\DC2\ETX*\STX%\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\t\EOT\DC2\ETX*\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\t\ENQ\DC2\ETX*\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\t\SOH\DC2\ETX*\DC2\US\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\t\ETX\DC2\ETX*\"$\n\
    \\n\
    \\n\
    \\STX\ENQ\SOH\DC2\EOT,\NUL1\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\SOH\SOH\DC2\ETX,\ENQ\SI\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\NUL\DC2\ETX-\STX\RS\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\NUL\SOH\DC2\ETX-\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\NUL\STX\DC2\ETX-\FS\GS\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\SOH\DC2\ETX.\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\SOH\SOH\DC2\ETX.\STX\DC4\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\SOH\STX\DC2\ETX.\ETB\CAN\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\STX\DC2\ETX/\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\STX\SOH\DC2\ETX/\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\STX\STX\DC2\ETX/\ESC\FS\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\ETX\DC2\ETX0\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ETX\SOH\DC2\ETX0\STX\DC4\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ETX\STX\DC2\ETX0\ETB\CAN\n\
    \\n\
    \\n\
    \\STX\EOT\ENQ\DC2\EOT2\NUL;\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETX2\b\CAN\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETX3\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ENQ\DC2\ETX3\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETX3\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETX3\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\SOH\DC2\ETX4\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ENQ\DC2\ETX4\STX\a\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\SOH\DC2\ETX4\b\DC3\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ETX\DC2\ETX4\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\STX\DC2\ETX5\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\ENQ\DC2\ETX5\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\SOH\DC2\ETX5\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\ETX\DC2\ETX5\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\ETX\DC2\ETX6\STX\ETB\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\ACK\DC2\ETX6\STX\f\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\SOH\DC2\ETX6\r\DC2\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\ETX\DC2\ETX6\NAK\SYN\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\EOT\DC2\ETX7\STX5\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\EOT\EOT\DC2\ETX7\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\EOT\ACK\DC2\ETX7\v\"\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\EOT\SOH\DC2\ETX7#0\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\EOT\ETX\DC2\ETX734\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\ENQ\DC2\ETX8\STX-\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ENQ\EOT\DC2\ETX8\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ENQ\ENQ\DC2\ETX8\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ENQ\SOH\DC2\ETX8\DC2(\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ENQ\ETX\DC2\ETX8+,\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\ACK\DC2\ETX9\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ACK\ENQ\DC2\ETX9\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ACK\SOH\DC2\ETX9\t\EM\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ACK\ETX\DC2\ETX9\FS\GS\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\a\DC2\ETX:\STX$\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\a\ENQ\DC2\ETX:\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\a\SOH\DC2\ETX:\t\US\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\a\ETX\DC2\ETX:\"#\n\
    \\n\
    \\n\
    \\STX\EOT\ACK\DC2\EOT<\NULC\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETX<\b\SUB\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX=\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ENQ\DC2\ETX=\STX\a\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX=\b\DC3\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX=\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\SOH\DC2\ETX>\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ENQ\DC2\ETX>\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\SOH\DC2\ETX>\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ETX\DC2\ETX>\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\STX\DC2\ETX?\STX \n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\EOT\DC2\ETX?\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\ACK\DC2\ETX?\v\DC2\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\SOH\DC2\ETX?\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\ETX\DC2\ETX?\RS\US\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\ETX\DC2\ETX@\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\ACK\DC2\ETX@\STX\r\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\SOH\DC2\ETX@\SO\DC4\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\ETX\DC2\ETX@\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\EOT\DC2\ETXA\STX.\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\EOT\EOT\DC2\ETXA\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\EOT\ACK\DC2\ETXA\v\ESC\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\EOT\SOH\DC2\ETXA\FS)\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\EOT\ETX\DC2\ETXA,-\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\ENQ\DC2\ETXB\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ENQ\ENQ\DC2\ETXB\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ENQ\SOH\DC2\ETXB\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ENQ\ETX\DC2\ETXB\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOTD\NULF\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETXD\b\ESC\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETXE\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ACK\DC2\ETXE\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETXE\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETXE\ESC\FS\n\
    \\242\SOH\n\
    \\STX\EOT\b\DC2\EOTJ\NULQ\SOH\SUB\229\SOH Full configuration replacement with CAS. The new code's checkpoint schema\n\
    \ must be compatible or explicitly migrated before activation; failure keeps\n\
    \ the previous version active. Paused subscriptions stay paused until resumed.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETXJ\b\SUB\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETXK\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ENQ\DC2\ETXK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETXK\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETXK\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\b\STX\SOH\DC2\ETXL\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ENQ\DC2\ETXL\STX\a\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\SOH\DC2\ETXL\b\DC3\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ETX\DC2\ETXL\SYN\ETB\n\
    \\v\n\
    \\EOT\EOT\b\STX\STX\DC2\ETXM\STX \n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\EOT\DC2\ETXM\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\ACK\DC2\ETXM\v\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\SOH\DC2\ETXM\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\ETX\DC2\ETXM\RS\US\n\
    \\v\n\
    \\EOT\EOT\b\STX\ETX\DC2\ETXN\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\ACK\DC2\ETXN\STX\r\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\SOH\DC2\ETXN\SO\DC4\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\ETX\DC2\ETXN\ETB\CAN\n\
    \\v\n\
    \\EOT\EOT\b\STX\EOT\DC2\ETXO\STX-\n\
    \\f\n\
    \\ENQ\EOT\b\STX\EOT\ENQ\DC2\ETXO\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\EOT\SOH\DC2\ETXO\t(\n\
    \\f\n\
    \\ENQ\EOT\b\STX\EOT\ETX\DC2\ETXO+,\n\
    \\v\n\
    \\EOT\EOT\b\STX\ENQ\DC2\ETXP\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ENQ\ENQ\DC2\ETXP\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ENQ\SOH\DC2\ETXP\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ENQ\ETX\DC2\ETXP\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOTR\NULT\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETXR\b\ESC\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETXS\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ACK\DC2\ETXS\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETXS\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETXS\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\n\
    \\DC2\EOTU\NULW\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\n\
    \\SOH\DC2\ETXU\b\ESC\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\ETXV\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ENQ\DC2\ETXV\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\ETXV\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\ETXV\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\v\DC2\EOTX\NULZ\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\v\SOH\DC2\ETXX\b\FS\n\
    \\v\n\
    \\EOT\EOT\v\STX\NUL\DC2\ETXY\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ACK\DC2\ETXY\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\ETXY\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\ETXY\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\f\DC2\EOT[\NUL_\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\f\SOH\DC2\ETX[\b\RS\n\
    \\v\n\
    \\EOT\EOT\f\STX\NUL\DC2\ETX\\\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ENQ\DC2\ETX\\\STX\b\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\ETX\\\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\ETX\\\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\f\STX\SOH\DC2\ETX]\STX$\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ACK\DC2\ETX]\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\ETX]\DC3\US\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\ETX]\"#\n\
    \\v\n\
    \\EOT\EOT\f\STX\STX\DC2\ETX^\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\f\STX\STX\ENQ\DC2\ETX^\STX\b\n\
    \\f\n\
    \\ENQ\EOT\f\STX\STX\SOH\DC2\ETX^\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\f\STX\STX\ETX\DC2\ETX^\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\r\DC2\EOT`\NULb\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\r\SOH\DC2\ETX`\b\US\n\
    \\v\n\
    \\EOT\EOT\r\STX\NUL\DC2\ETXa\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ACK\DC2\ETXa\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\ETXa\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\ETXa\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\SO\DC2\EOTc\NULg\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SO\SOH\DC2\ETXc\b!\n\
    \\v\n\
    \\EOT\EOT\SO\STX\NUL\DC2\ETXd\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ENQ\DC2\ETXd\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\ETXd\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\ETXd\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\SO\STX\SOH\DC2\ETXe\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ENQ\DC2\ETXe\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\SOH\DC2\ETXe\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ETX\DC2\ETXe\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\SO\STX\STX\DC2\ETXf\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ENQ\DC2\ETXf\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\SOH\DC2\ETXf\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ETX\DC2\ETXf\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\SI\DC2\EOTh\NULj\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SI\SOH\DC2\ETXh\b\"\n\
    \\v\n\
    \\EOT\EOT\SI\STX\NUL\DC2\ETXi\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ACK\DC2\ETXi\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\ETXi\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\ETXi\ESC\FS\n\
    \a\n\
    \\STX\EOT\DLE\DC2\EOTl\NULp\SOH\SUBU Resumption may replay a previously delivered record and duplicate external effects.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\DLE\SOH\DC2\ETXl\b!\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\ETXm\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ENQ\DC2\ETXm\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\ETXm\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\ETXm\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\SOH\DC2\ETXn\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\ENQ\DC2\ETXn\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\SOH\DC2\ETXn\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\SOH\ETX\DC2\ETXn\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\STX\DC2\ETXo\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\ENQ\DC2\ETXo\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\SOH\DC2\ETXo\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\STX\ETX\DC2\ETXo\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\DC1\DC2\EOTq\NULs\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC1\SOH\DC2\ETXq\b\"\n\
    \\v\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\ETXr\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ACK\DC2\ETXr\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\ETXr\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\ETXr\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\DC2\DC2\EOTt\NULw\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC2\SOH\DC2\ETXt\b\RS\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\ETXu\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ENQ\DC2\ETXu\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\ETXu\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\ETXu\DC4\NAK\n\
    \\v\n\
    \\EOT\EOT\DC2\STX\SOH\DC2\ETXv\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\SOH\ENQ\DC2\ETXv\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\SOH\SOH\DC2\ETXv\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\DC2\STX\SOH\ETX\DC2\ETXv\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\DC3\DC2\EOTx\NULz\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC3\SOH\DC2\ETXx\b\US\n\
    \\v\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\ETXy\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\ACK\DC2\ETXy\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\ETXy\DC3\CAN\n\
    \\f\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\ETXy\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\DC4\DC2\EOT{\NUL~\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DC4\SOH\DC2\ETX{\b\SO\n\
    \\v\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\ETX|\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\NUL\ENQ\DC2\ETX|\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\ETX|\t\r\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\ETX|\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\ETX}\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\SOH\ENQ\DC2\ETX}\STX\b\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\ETX}\t\SO\n\
    \\f\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\ETX}\DC1\DC2\n\
    \U\n\
    \\STX\EOT\NAK\DC2\ACK\128\SOH\NUL\134\SOH\SOH\SUBG Invocation is not an implicit Stream append or persistence guarantee.\n\
    \\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\128\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\129\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ENQ\DC2\EOT\129\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\129\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\129\SOH\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\SOH\DC2\EOT\130\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ENQ\DC2\EOT\130\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\SOH\DC2\EOT\130\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ETX\DC2\EOT\130\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\STX\DC2\EOT\131\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ENQ\DC2\EOT\131\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\SOH\DC2\EOT\131\SOH\t\f\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ETX\DC2\EOT\131\SOH\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\ETX\DC2\EOT\132\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ENQ\DC2\EOT\132\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\SOH\DC2\EOT\132\SOH\b\f\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ETX\DC2\EOT\132\SOH\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\EOT\DC2\EOT\133\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\EOT\DC2\EOT\133\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\ACK\DC2\EOT\133\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\SOH\DC2\EOT\133\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\ETX\DC2\EOT\133\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\SYN\DC2\ACK\135\SOH\NUL\139\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\135\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\136\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ENQ\DC2\EOT\136\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\136\SOH\t\SI\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\136\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\137\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ENQ\DC2\EOT\137\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\137\SOH\b\f\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\137\SOH\SI\DLE\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\STX\DC2\EOT\138\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\EOT\DC2\EOT\138\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ACK\DC2\EOT\138\SOH\v\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\SOH\DC2\EOT\138\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ETX\DC2\EOT\138\SOH\FS\GS\n\
    \\f\n\
    \\STX\ENQ\STX\DC2\ACK\140\SOH\NUL\152\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\STX\SOH\DC2\EOT\140\SOH\ENQ\SO\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\NUL\DC2\EOT\141\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\NUL\SOH\DC2\EOT\141\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\NUL\STX\DC2\EOT\141\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\SOH\DC2\EOT\142\SOH\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\SOH\SOH\DC2\EOT\142\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\SOH\STX\DC2\EOT\142\SOH !\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\STX\DC2\EOT\143\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\STX\SOH\DC2\EOT\143\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\STX\STX\DC2\EOT\143\SOH!\"\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ETX\DC2\EOT\144\SOH\STX$\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ETX\SOH\DC2\EOT\144\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ETX\STX\DC2\EOT\144\SOH\"#\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\EOT\DC2\EOT\145\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\EOT\SOH\DC2\EOT\145\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\EOT\STX\DC2\EOT\145\SOH\US \n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ENQ\DC2\EOT\146\SOH\STX(\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ENQ\SOH\DC2\EOT\146\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ENQ\STX\DC2\EOT\146\SOH&'\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ACK\DC2\EOT\147\SOH\STX&\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ACK\SOH\DC2\EOT\147\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ACK\STX\DC2\EOT\147\SOH$%\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\a\DC2\EOT\148\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\a\SOH\DC2\EOT\148\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\a\STX\DC2\EOT\148\SOH\CAN\EM\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\b\DC2\EOT\149\SOH\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\b\SOH\DC2\EOT\149\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\b\STX\DC2\EOT\149\SOH !\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\t\DC2\EOT\150\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\t\SOH\DC2\EOT\150\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\t\STX\DC2\EOT\150\SOH!\"\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\n\
    \\DC2\EOT\151\SOH\STX)\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\n\
    \\SOH\DC2\EOT\151\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\n\
    \\STX\DC2\EOT\151\SOH&(\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\153\SOH\NUL\156\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\153\SOH\b\r\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\154\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ACK\DC2\EOT\154\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\154\SOH\f\DLE\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\154\SOH\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\155\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ENQ\DC2\EOT\155\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\155\SOH\t\DLE\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\155\SOH\DC3\DC4\n\
    \\f\n\
    \\STX\ACK\NUL\DC2\ACK\158\SOH\NUL\167\SOH\SOH\n\
    \\v\n\
    \\ETX\ACK\NUL\SOH\DC2\EOT\158\SOH\b\NAK\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\EOT\159\SOH\STXD\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\EOT\159\SOH\ACK\DC1\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\EOT\159\SOH\DC2$\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\EOT\159\SOH/B\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\SOH\DC2\EOT\160\SOH\STXD\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\SOH\DC2\EOT\160\SOH\ACK\DC1\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\STX\DC2\EOT\160\SOH\DC2$\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\ETX\DC2\EOT\160\SOH/B\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\STX\DC2\EOT\161\SOH\STXG\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\SOH\DC2\EOT\161\SOH\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\STX\DC2\EOT\161\SOH\DC3&\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\ETX\DC2\EOT\161\SOH1E\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ETX\DC2\EOT\162\SOH\STXP\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\SOH\DC2\EOT\162\SOH\ACK\NAK\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\STX\DC2\EOT\162\SOH\SYN,\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\ETX\DC2\EOT\162\SOH7N\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\EOT\DC2\EOT\163\SOH\STXY\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\SOH\DC2\EOT\163\SOH\ACK\CAN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\STX\DC2\EOT\163\SOH\EM2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\ETX\DC2\EOT\163\SOH=W\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ENQ\DC2\EOT\164\SOH\STXY\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\SOH\DC2\EOT\164\SOH\ACK\CAN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\STX\DC2\EOT\164\SOH\EM2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\ETX\DC2\EOT\164\SOH=W\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ACK\DC2\EOT\165\SOH\STXP\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\SOH\DC2\EOT\165\SOH\ACK\NAK\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\STX\DC2\EOT\165\SOH\SYN,\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\ETX\DC2\EOT\165\SOH7N\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\a\DC2\EOT\166\SOH\STXD\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\SOH\DC2\EOT\166\SOH\ACK\DC1\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\STX\DC2\EOT\166\SOH\DC2$\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\ETX\DC2\EOT\166\SOH/Bb\ACKproto3"