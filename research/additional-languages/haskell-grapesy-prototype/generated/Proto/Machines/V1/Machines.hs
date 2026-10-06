{- This file was auto-generated from machines/v1/machines.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Machines.V1.Machines (
        MachinesService(..), Budgets(), Capability(..), Capability(),
        Capability'UnrecognizedValue, CheckpointAdmission(),
        CheckpointId(), CheckpointMachineRequest(),
        CheckpointMutationRequest(), CheckpointState(),
        CompatibilityMode(..), CompatibilityMode(),
        CompatibilityMode'UnrecognizedValue, CompatibilityPolicy(),
        CreateMachineRequest(), Endpoint(), EventKind(..), EventKind(),
        EventKind'UnrecognizedValue, EventPage(), EventsRequest(),
        ExpirationKind(..), ExpirationKind(),
        ExpirationKind'UnrecognizedValue, ExpirationPolicy(),
        ForkAdmission(), ForkCheckpointRequest(), ForkFidelity(..),
        ForkFidelity(), ForkFidelity'UnrecognizedValue,
        ForkMachineAdmission(), ForkMachineRequest(), ForkedLiveMachines(),
        ForkedMachines(), IdempotencyKey(), Image(),
        Image'ImmutableReference(..), _Image'ManagedDigest,
        _Image'CustomDigest, _Image'Checkpoint, ImageKind(..), ImageKind(),
        ImageKind'UnrecognizedValue, ImageQualification(),
        InspectCheckpointRequest(), InspectMachineRequest(),
        ListMachinesRequest(), MachineAdmission(), MachineContract(),
        MachineEvent(), MachineId(), MachineMutationRequest(),
        MachinePage(), MachineState(), MachineStatus(..), MachineStatus(),
        MachineStatus'UnrecognizedValue, MutationAdmission(),
        MutationOutcome(), MutationOutcome'Result(..),
        _MutationOutcome'Created, _MutationOutcome'Checkpointed,
        _MutationOutcome'Forked, _MutationOutcome'Suspended,
        _MutationOutcome'Woken, _MutationOutcome'SuspensionPolicySet,
        _MutationOutcome'MachineDestroyed,
        _MutationOutcome'CheckpointDestroyed,
        _MutationOutcome'MachineForked, OperationId(), OperationPage(),
        OperationRequest(), OperationState(), OperationStatus(..),
        OperationStatus(), OperationStatus'UnrecognizedValue,
        PolicyAdmission(), PolicySet(), PressureKind(..), PressureKind(),
        PressureKind'UnrecognizedValue, ProtocolVersion(),
        QualifyImageRequest(), RecoverRequest(), RecoveredAdmission(),
        RecoveredAdmission'Result(..), _RecoveredAdmission'Create,
        _RecoveredAdmission'Checkpoint, _RecoveredAdmission'Fork,
        _RecoveredAdmission'Suspend, _RecoveredAdmission'Wake,
        _RecoveredAdmission'DestroyMachine,
        _RecoveredAdmission'SetSuspensionPolicy,
        _RecoveredAdmission'DestroyCheckpoint,
        _RecoveredAdmission'ForkMachine, SetSuspensionPolicyRequest(),
        SuspensionPolicy(), SuspensionPolicy'Policy(..),
        _SuspensionPolicy'Manual, _SuspensionPolicy'AfterIdleMs,
        UsageReceipt(), UsageRequest()
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
     
         * 'Proto.Machines.V1.Machines_Fields.spendMicros' @:: Lens' Budgets Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.concurrency' @:: Lens' Budgets Data.Word.Word32@ -}
data Budgets
  = Budgets'_constructor {_Budgets'spendMicros :: !Data.Word.Word64,
                          _Budgets'concurrency :: !Data.Word.Word32,
                          _Budgets'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Budgets where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Budgets "spendMicros" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Budgets'spendMicros
           (\ x__ y__ -> x__ {_Budgets'spendMicros = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Budgets "concurrency" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Budgets'concurrency
           (\ x__ y__ -> x__ {_Budgets'concurrency = y__}))
        Prelude.id
instance Data.ProtoLens.Message Budgets where
  messageName _ = Data.Text.pack "acyclic.machines.v1.Budgets"
  packedMessageDescriptor _
    = "\n\
      \\aBudgets\DC2!\n\
      \\fspend_micros\CAN\SOH \SOH(\EOTR\vspendMicros\DC2 \n\
      \\vconcurrency\CAN\STX \SOH(\rR\vconcurrency"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        spendMicros__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "spend_micros"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"spendMicros")) ::
              Data.ProtoLens.FieldDescriptor Budgets
        concurrency__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "concurrency"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"concurrency")) ::
              Data.ProtoLens.FieldDescriptor Budgets
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, spendMicros__field_descriptor),
           (Data.ProtoLens.Tag 2, concurrency__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Budgets'_unknownFields
        (\ x__ y__ -> x__ {_Budgets'_unknownFields = y__})
  defMessage
    = Budgets'_constructor
        {_Budgets'spendMicros = Data.ProtoLens.fieldDefault,
         _Budgets'concurrency = Data.ProtoLens.fieldDefault,
         _Budgets'_unknownFields = []}
  parseMessage
    = let
        loop :: Budgets -> Data.ProtoLens.Encoding.Bytes.Parser Budgets
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
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "spend_micros"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"spendMicros") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "concurrency"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"concurrency") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Budgets"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"spendMicros") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"concurrency") _x
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
instance Control.DeepSeq.NFData Budgets where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Budgets'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Budgets'spendMicros x__)
                (Control.DeepSeq.deepseq (_Budgets'concurrency x__) ()))
newtype Capability'UnrecognizedValue
  = Capability'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data Capability
  = CAPABILITY_UNSPECIFIED |
    CAPABILITY_ELASTIC_CPU |
    CAPABILITY_ELASTIC_MEMORY |
    CAPABILITY_LIVE_CHECKPOINT |
    CAPABILITY_LIVE_FORK |
    CAPABILITY_SUSPEND_RESUME |
    CAPABILITY_LIVE_MOVEMENT |
    CAPABILITY_DISK_FORK |
    Capability'Unrecognized !Capability'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum Capability where
  maybeToEnum 0 = Prelude.Just CAPABILITY_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just CAPABILITY_ELASTIC_CPU
  maybeToEnum 2 = Prelude.Just CAPABILITY_ELASTIC_MEMORY
  maybeToEnum 3 = Prelude.Just CAPABILITY_LIVE_CHECKPOINT
  maybeToEnum 4 = Prelude.Just CAPABILITY_LIVE_FORK
  maybeToEnum 5 = Prelude.Just CAPABILITY_SUSPEND_RESUME
  maybeToEnum 6 = Prelude.Just CAPABILITY_LIVE_MOVEMENT
  maybeToEnum 7 = Prelude.Just CAPABILITY_DISK_FORK
  maybeToEnum k
    = Prelude.Just
        (Capability'Unrecognized
           (Capability'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum CAPABILITY_UNSPECIFIED = "CAPABILITY_UNSPECIFIED"
  showEnum CAPABILITY_ELASTIC_CPU = "CAPABILITY_ELASTIC_CPU"
  showEnum CAPABILITY_ELASTIC_MEMORY = "CAPABILITY_ELASTIC_MEMORY"
  showEnum CAPABILITY_LIVE_CHECKPOINT = "CAPABILITY_LIVE_CHECKPOINT"
  showEnum CAPABILITY_LIVE_FORK = "CAPABILITY_LIVE_FORK"
  showEnum CAPABILITY_SUSPEND_RESUME = "CAPABILITY_SUSPEND_RESUME"
  showEnum CAPABILITY_LIVE_MOVEMENT = "CAPABILITY_LIVE_MOVEMENT"
  showEnum CAPABILITY_DISK_FORK = "CAPABILITY_DISK_FORK"
  showEnum (Capability'Unrecognized (Capability'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "CAPABILITY_UNSPECIFIED"
    = Prelude.Just CAPABILITY_UNSPECIFIED
    | (Prelude.==) k "CAPABILITY_ELASTIC_CPU"
    = Prelude.Just CAPABILITY_ELASTIC_CPU
    | (Prelude.==) k "CAPABILITY_ELASTIC_MEMORY"
    = Prelude.Just CAPABILITY_ELASTIC_MEMORY
    | (Prelude.==) k "CAPABILITY_LIVE_CHECKPOINT"
    = Prelude.Just CAPABILITY_LIVE_CHECKPOINT
    | (Prelude.==) k "CAPABILITY_LIVE_FORK"
    = Prelude.Just CAPABILITY_LIVE_FORK
    | (Prelude.==) k "CAPABILITY_SUSPEND_RESUME"
    = Prelude.Just CAPABILITY_SUSPEND_RESUME
    | (Prelude.==) k "CAPABILITY_LIVE_MOVEMENT"
    = Prelude.Just CAPABILITY_LIVE_MOVEMENT
    | (Prelude.==) k "CAPABILITY_DISK_FORK"
    = Prelude.Just CAPABILITY_DISK_FORK
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded Capability where
  minBound = CAPABILITY_UNSPECIFIED
  maxBound = CAPABILITY_DISK_FORK
instance Prelude.Enum Capability where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum Capability: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum CAPABILITY_UNSPECIFIED = 0
  fromEnum CAPABILITY_ELASTIC_CPU = 1
  fromEnum CAPABILITY_ELASTIC_MEMORY = 2
  fromEnum CAPABILITY_LIVE_CHECKPOINT = 3
  fromEnum CAPABILITY_LIVE_FORK = 4
  fromEnum CAPABILITY_SUSPEND_RESUME = 5
  fromEnum CAPABILITY_LIVE_MOVEMENT = 6
  fromEnum CAPABILITY_DISK_FORK = 7
  fromEnum (Capability'Unrecognized (Capability'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ CAPABILITY_DISK_FORK
    = Prelude.error
        "Capability.succ: bad argument CAPABILITY_DISK_FORK. This value would be out of bounds."
  succ CAPABILITY_UNSPECIFIED = CAPABILITY_ELASTIC_CPU
  succ CAPABILITY_ELASTIC_CPU = CAPABILITY_ELASTIC_MEMORY
  succ CAPABILITY_ELASTIC_MEMORY = CAPABILITY_LIVE_CHECKPOINT
  succ CAPABILITY_LIVE_CHECKPOINT = CAPABILITY_LIVE_FORK
  succ CAPABILITY_LIVE_FORK = CAPABILITY_SUSPEND_RESUME
  succ CAPABILITY_SUSPEND_RESUME = CAPABILITY_LIVE_MOVEMENT
  succ CAPABILITY_LIVE_MOVEMENT = CAPABILITY_DISK_FORK
  succ (Capability'Unrecognized _)
    = Prelude.error "Capability.succ: bad argument: unrecognized value"
  pred CAPABILITY_UNSPECIFIED
    = Prelude.error
        "Capability.pred: bad argument CAPABILITY_UNSPECIFIED. This value would be out of bounds."
  pred CAPABILITY_ELASTIC_CPU = CAPABILITY_UNSPECIFIED
  pred CAPABILITY_ELASTIC_MEMORY = CAPABILITY_ELASTIC_CPU
  pred CAPABILITY_LIVE_CHECKPOINT = CAPABILITY_ELASTIC_MEMORY
  pred CAPABILITY_LIVE_FORK = CAPABILITY_LIVE_CHECKPOINT
  pred CAPABILITY_SUSPEND_RESUME = CAPABILITY_LIVE_FORK
  pred CAPABILITY_LIVE_MOVEMENT = CAPABILITY_SUSPEND_RESUME
  pred CAPABILITY_DISK_FORK = CAPABILITY_LIVE_MOVEMENT
  pred (Capability'Unrecognized _)
    = Prelude.error "Capability.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault Capability where
  fieldDefault = CAPABILITY_UNSPECIFIED
instance Control.DeepSeq.NFData Capability where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' CheckpointAdmission CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' CheckpointAdmission (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.source' @:: Lens' CheckpointAdmission MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'source' @:: Lens' CheckpointAdmission (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' CheckpointAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' CheckpointAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.contract' @:: Lens' CheckpointAdmission MachineContract@
         * 'Proto.Machines.V1.Machines_Fields.maybe'contract' @:: Lens' CheckpointAdmission (Prelude.Maybe MachineContract)@ -}
data CheckpointAdmission
  = CheckpointAdmission'_constructor {_CheckpointAdmission'checkpoint :: !(Prelude.Maybe CheckpointId),
                                      _CheckpointAdmission'source :: !(Prelude.Maybe MachineId),
                                      _CheckpointAdmission'operation :: !(Prelude.Maybe OperationId),
                                      _CheckpointAdmission'contract :: !(Prelude.Maybe MachineContract),
                                      _CheckpointAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointAdmission "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'checkpoint
           (\ x__ y__ -> x__ {_CheckpointAdmission'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointAdmission "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'checkpoint
           (\ x__ y__ -> x__ {_CheckpointAdmission'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointAdmission "source" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'source
           (\ x__ y__ -> x__ {_CheckpointAdmission'source = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointAdmission "maybe'source" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'source
           (\ x__ y__ -> x__ {_CheckpointAdmission'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'operation
           (\ x__ y__ -> x__ {_CheckpointAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'operation
           (\ x__ y__ -> x__ {_CheckpointAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointAdmission "contract" MachineContract where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'contract
           (\ x__ y__ -> x__ {_CheckpointAdmission'contract = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointAdmission "maybe'contract" (Prelude.Maybe MachineContract) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointAdmission'contract
           (\ x__ y__ -> x__ {_CheckpointAdmission'contract = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointAdmission where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.CheckpointAdmission"
  packedMessageDescriptor _
    = "\n\
      \\DC3CheckpointAdmission\DC2A\n\
      \\n\
      \checkpoint\CAN\SOH \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint\DC26\n\
      \\ACKsource\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2>\n\
      \\toperation\CAN\ETX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
      \\bcontract\CAN\EOT \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor CheckpointAdmission
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'source")) ::
              Data.ProtoLens.FieldDescriptor CheckpointAdmission
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor CheckpointAdmission
        contract__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "contract"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineContract)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contract")) ::
              Data.ProtoLens.FieldDescriptor CheckpointAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, checkpoint__field_descriptor),
           (Data.ProtoLens.Tag 2, source__field_descriptor),
           (Data.ProtoLens.Tag 3, operation__field_descriptor),
           (Data.ProtoLens.Tag 4, contract__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointAdmission'_unknownFields
        (\ x__ y__ -> x__ {_CheckpointAdmission'_unknownFields = y__})
  defMessage
    = CheckpointAdmission'_constructor
        {_CheckpointAdmission'checkpoint = Prelude.Nothing,
         _CheckpointAdmission'source = Prelude.Nothing,
         _CheckpointAdmission'operation = Prelude.Nothing,
         _CheckpointAdmission'contract = Prelude.Nothing,
         _CheckpointAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointAdmission
          -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointAdmission
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
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "contract"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contract") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CheckpointAdmission"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'source") _x
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
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'contract") _x
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
instance Control.DeepSeq.NFData CheckpointAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CheckpointAdmission'checkpoint x__)
                (Control.DeepSeq.deepseq
                   (_CheckpointAdmission'source x__)
                   (Control.DeepSeq.deepseq
                      (_CheckpointAdmission'operation x__)
                      (Control.DeepSeq.deepseq (_CheckpointAdmission'contract x__) ()))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.value' @:: Lens' CheckpointId Data.ByteString.ByteString@ -}
data CheckpointId
  = CheckpointId'_constructor {_CheckpointId'value :: !Data.ByteString.ByteString,
                               _CheckpointId'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointId where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointId "value" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointId'value (\ x__ y__ -> x__ {_CheckpointId'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointId where
  messageName _ = Data.Text.pack "acyclic.machines.v1.CheckpointId"
  packedMessageDescriptor _
    = "\n\
      \\fCheckpointId\DC2\DC4\n\
      \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor CheckpointId
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointId'_unknownFields
        (\ x__ y__ -> x__ {_CheckpointId'_unknownFields = y__})
  defMessage
    = CheckpointId'_constructor
        {_CheckpointId'value = Data.ProtoLens.fieldDefault,
         _CheckpointId'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointId -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointId
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
          (do loop Data.ProtoLens.defMessage) "CheckpointId"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"value") _x
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
instance Control.DeepSeq.NFData CheckpointId where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointId'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CheckpointId'value x__) ())
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' CheckpointMachineRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' CheckpointMachineRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' CheckpointMachineRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' CheckpointMachineRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' CheckpointMachineRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' CheckpointMachineRequest (Prelude.Maybe MachineId)@ -}
data CheckpointMachineRequest
  = CheckpointMachineRequest'_constructor {_CheckpointMachineRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                           _CheckpointMachineRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                           _CheckpointMachineRequest'machine :: !(Prelude.Maybe MachineId),
                                           _CheckpointMachineRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointMachineRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointMachineRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMachineRequest'protocol
           (\ x__ y__ -> x__ {_CheckpointMachineRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointMachineRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMachineRequest'protocol
           (\ x__ y__ -> x__ {_CheckpointMachineRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointMachineRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMachineRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_CheckpointMachineRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointMachineRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMachineRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_CheckpointMachineRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointMachineRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMachineRequest'machine
           (\ x__ y__ -> x__ {_CheckpointMachineRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointMachineRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMachineRequest'machine
           (\ x__ y__ -> x__ {_CheckpointMachineRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointMachineRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.CheckpointMachineRequest"
  packedMessageDescriptor _
    = "\n\
      \\CANCheckpointMachineRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
      \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor CheckpointMachineRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CheckpointMachineRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor CheckpointMachineRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, machine__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointMachineRequest'_unknownFields
        (\ x__ y__ -> x__ {_CheckpointMachineRequest'_unknownFields = y__})
  defMessage
    = CheckpointMachineRequest'_constructor
        {_CheckpointMachineRequest'protocol = Prelude.Nothing,
         _CheckpointMachineRequest'idempotencyKey = Prelude.Nothing,
         _CheckpointMachineRequest'machine = Prelude.Nothing,
         _CheckpointMachineRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointMachineRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointMachineRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CheckpointMachineRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
instance Control.DeepSeq.NFData CheckpointMachineRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointMachineRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CheckpointMachineRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_CheckpointMachineRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_CheckpointMachineRequest'machine x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' CheckpointMutationRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' CheckpointMutationRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' CheckpointMutationRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' CheckpointMutationRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' CheckpointMutationRequest CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' CheckpointMutationRequest (Prelude.Maybe CheckpointId)@ -}
data CheckpointMutationRequest
  = CheckpointMutationRequest'_constructor {_CheckpointMutationRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                            _CheckpointMutationRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                            _CheckpointMutationRequest'checkpoint :: !(Prelude.Maybe CheckpointId),
                                            _CheckpointMutationRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointMutationRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointMutationRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMutationRequest'protocol
           (\ x__ y__ -> x__ {_CheckpointMutationRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointMutationRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMutationRequest'protocol
           (\ x__ y__ -> x__ {_CheckpointMutationRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointMutationRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMutationRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_CheckpointMutationRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointMutationRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMutationRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_CheckpointMutationRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointMutationRequest "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMutationRequest'checkpoint
           (\ x__ y__ -> x__ {_CheckpointMutationRequest'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointMutationRequest "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointMutationRequest'checkpoint
           (\ x__ y__ -> x__ {_CheckpointMutationRequest'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointMutationRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.CheckpointMutationRequest"
  packedMessageDescriptor _
    = "\n\
      \\EMCheckpointMutationRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC2A\n\
      \\n\
      \checkpoint\CAN\ETX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor CheckpointMutationRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CheckpointMutationRequest
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor CheckpointMutationRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, checkpoint__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointMutationRequest'_unknownFields
        (\ x__ y__
           -> x__ {_CheckpointMutationRequest'_unknownFields = y__})
  defMessage
    = CheckpointMutationRequest'_constructor
        {_CheckpointMutationRequest'protocol = Prelude.Nothing,
         _CheckpointMutationRequest'idempotencyKey = Prelude.Nothing,
         _CheckpointMutationRequest'checkpoint = Prelude.Nothing,
         _CheckpointMutationRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointMutationRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointMutationRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CheckpointMutationRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
instance Control.DeepSeq.NFData CheckpointMutationRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointMutationRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CheckpointMutationRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_CheckpointMutationRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_CheckpointMutationRequest'checkpoint x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' CheckpointState CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' CheckpointState (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.source' @:: Lens' CheckpointState MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'source' @:: Lens' CheckpointState (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.contract' @:: Lens' CheckpointState MachineContract@
         * 'Proto.Machines.V1.Machines_Fields.maybe'contract' @:: Lens' CheckpointState (Prelude.Maybe MachineContract)@
         * 'Proto.Machines.V1.Machines_Fields.forkable' @:: Lens' CheckpointState Prelude.Bool@
         * 'Proto.Machines.V1.Machines_Fields.createdAtUnixMs' @:: Lens' CheckpointState Data.Word.Word64@ -}
data CheckpointState
  = CheckpointState'_constructor {_CheckpointState'checkpoint :: !(Prelude.Maybe CheckpointId),
                                  _CheckpointState'source :: !(Prelude.Maybe MachineId),
                                  _CheckpointState'contract :: !(Prelude.Maybe MachineContract),
                                  _CheckpointState'forkable :: !Prelude.Bool,
                                  _CheckpointState'createdAtUnixMs :: !Data.Word.Word64,
                                  _CheckpointState'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CheckpointState where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CheckpointState "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'checkpoint
           (\ x__ y__ -> x__ {_CheckpointState'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointState "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'checkpoint
           (\ x__ y__ -> x__ {_CheckpointState'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointState "source" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'source
           (\ x__ y__ -> x__ {_CheckpointState'source = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointState "maybe'source" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'source
           (\ x__ y__ -> x__ {_CheckpointState'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointState "contract" MachineContract where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'contract
           (\ x__ y__ -> x__ {_CheckpointState'contract = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CheckpointState "maybe'contract" (Prelude.Maybe MachineContract) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'contract
           (\ x__ y__ -> x__ {_CheckpointState'contract = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointState "forkable" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'forkable
           (\ x__ y__ -> x__ {_CheckpointState'forkable = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CheckpointState "createdAtUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CheckpointState'createdAtUnixMs
           (\ x__ y__ -> x__ {_CheckpointState'createdAtUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message CheckpointState where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.CheckpointState"
  packedMessageDescriptor _
    = "\n\
      \\SICheckpointState\DC2A\n\
      \\n\
      \checkpoint\CAN\SOH \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint\DC26\n\
      \\ACKsource\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2@\n\
      \\bcontract\CAN\ETX \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\DC2\SUB\n\
      \\bforkable\CAN\EOT \SOH(\bR\bforkable\DC2+\n\
      \\DC2created_at_unix_ms\CAN\ENQ \SOH(\EOTR\SIcreatedAtUnixMs"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor CheckpointState
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'source")) ::
              Data.ProtoLens.FieldDescriptor CheckpointState
        contract__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "contract"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineContract)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contract")) ::
              Data.ProtoLens.FieldDescriptor CheckpointState
        forkable__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "forkable"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"forkable")) ::
              Data.ProtoLens.FieldDescriptor CheckpointState
        createdAtUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created_at_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"createdAtUnixMs")) ::
              Data.ProtoLens.FieldDescriptor CheckpointState
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, checkpoint__field_descriptor),
           (Data.ProtoLens.Tag 2, source__field_descriptor),
           (Data.ProtoLens.Tag 3, contract__field_descriptor),
           (Data.ProtoLens.Tag 4, forkable__field_descriptor),
           (Data.ProtoLens.Tag 5, createdAtUnixMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CheckpointState'_unknownFields
        (\ x__ y__ -> x__ {_CheckpointState'_unknownFields = y__})
  defMessage
    = CheckpointState'_constructor
        {_CheckpointState'checkpoint = Prelude.Nothing,
         _CheckpointState'source = Prelude.Nothing,
         _CheckpointState'contract = Prelude.Nothing,
         _CheckpointState'forkable = Data.ProtoLens.fieldDefault,
         _CheckpointState'createdAtUnixMs = Data.ProtoLens.fieldDefault,
         _CheckpointState'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CheckpointState
          -> Data.ProtoLens.Encoding.Bytes.Parser CheckpointState
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
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "contract"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contract") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "forkable"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"forkable") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "created_at_unix_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"createdAtUnixMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CheckpointState"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'source") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'contract") _x
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
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"forkable") _x
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
                         (let
                            _v
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"createdAtUnixMs") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData CheckpointState where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CheckpointState'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CheckpointState'checkpoint x__)
                (Control.DeepSeq.deepseq
                   (_CheckpointState'source x__)
                   (Control.DeepSeq.deepseq
                      (_CheckpointState'contract x__)
                      (Control.DeepSeq.deepseq
                         (_CheckpointState'forkable x__)
                         (Control.DeepSeq.deepseq
                            (_CheckpointState'createdAtUnixMs x__) ())))))
newtype CompatibilityMode'UnrecognizedValue
  = CompatibilityMode'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data CompatibilityMode
  = COMPATIBILITY_MODE_UNSPECIFIED |
    COMPATIBILITY_MODE_BEST_EFFORT |
    COMPATIBILITY_MODE_REQUIRE |
    CompatibilityMode'Unrecognized !CompatibilityMode'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum CompatibilityMode where
  maybeToEnum 0 = Prelude.Just COMPATIBILITY_MODE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just COMPATIBILITY_MODE_BEST_EFFORT
  maybeToEnum 2 = Prelude.Just COMPATIBILITY_MODE_REQUIRE
  maybeToEnum k
    = Prelude.Just
        (CompatibilityMode'Unrecognized
           (CompatibilityMode'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum COMPATIBILITY_MODE_UNSPECIFIED
    = "COMPATIBILITY_MODE_UNSPECIFIED"
  showEnum COMPATIBILITY_MODE_BEST_EFFORT
    = "COMPATIBILITY_MODE_BEST_EFFORT"
  showEnum COMPATIBILITY_MODE_REQUIRE = "COMPATIBILITY_MODE_REQUIRE"
  showEnum
    (CompatibilityMode'Unrecognized (CompatibilityMode'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "COMPATIBILITY_MODE_UNSPECIFIED"
    = Prelude.Just COMPATIBILITY_MODE_UNSPECIFIED
    | (Prelude.==) k "COMPATIBILITY_MODE_BEST_EFFORT"
    = Prelude.Just COMPATIBILITY_MODE_BEST_EFFORT
    | (Prelude.==) k "COMPATIBILITY_MODE_REQUIRE"
    = Prelude.Just COMPATIBILITY_MODE_REQUIRE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded CompatibilityMode where
  minBound = COMPATIBILITY_MODE_UNSPECIFIED
  maxBound = COMPATIBILITY_MODE_REQUIRE
instance Prelude.Enum CompatibilityMode where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum CompatibilityMode: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum COMPATIBILITY_MODE_UNSPECIFIED = 0
  fromEnum COMPATIBILITY_MODE_BEST_EFFORT = 1
  fromEnum COMPATIBILITY_MODE_REQUIRE = 2
  fromEnum
    (CompatibilityMode'Unrecognized (CompatibilityMode'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ COMPATIBILITY_MODE_REQUIRE
    = Prelude.error
        "CompatibilityMode.succ: bad argument COMPATIBILITY_MODE_REQUIRE. This value would be out of bounds."
  succ COMPATIBILITY_MODE_UNSPECIFIED
    = COMPATIBILITY_MODE_BEST_EFFORT
  succ COMPATIBILITY_MODE_BEST_EFFORT = COMPATIBILITY_MODE_REQUIRE
  succ (CompatibilityMode'Unrecognized _)
    = Prelude.error
        "CompatibilityMode.succ: bad argument: unrecognized value"
  pred COMPATIBILITY_MODE_UNSPECIFIED
    = Prelude.error
        "CompatibilityMode.pred: bad argument COMPATIBILITY_MODE_UNSPECIFIED. This value would be out of bounds."
  pred COMPATIBILITY_MODE_BEST_EFFORT
    = COMPATIBILITY_MODE_UNSPECIFIED
  pred COMPATIBILITY_MODE_REQUIRE = COMPATIBILITY_MODE_BEST_EFFORT
  pred (CompatibilityMode'Unrecognized _)
    = Prelude.error
        "CompatibilityMode.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault CompatibilityMode where
  fieldDefault = COMPATIBILITY_MODE_UNSPECIFIED
instance Control.DeepSeq.NFData CompatibilityMode where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.mode' @:: Lens' CompatibilityPolicy CompatibilityMode@
         * 'Proto.Machines.V1.Machines_Fields.required' @:: Lens' CompatibilityPolicy [Capability]@
         * 'Proto.Machines.V1.Machines_Fields.vec'required' @:: Lens' CompatibilityPolicy (Data.Vector.Vector Capability)@ -}
data CompatibilityPolicy
  = CompatibilityPolicy'_constructor {_CompatibilityPolicy'mode :: !CompatibilityMode,
                                      _CompatibilityPolicy'required :: !(Data.Vector.Vector Capability),
                                      _CompatibilityPolicy'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CompatibilityPolicy where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CompatibilityPolicy "mode" CompatibilityMode where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompatibilityPolicy'mode
           (\ x__ y__ -> x__ {_CompatibilityPolicy'mode = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CompatibilityPolicy "required" [Capability] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompatibilityPolicy'required
           (\ x__ y__ -> x__ {_CompatibilityPolicy'required = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CompatibilityPolicy "vec'required" (Data.Vector.Vector Capability) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CompatibilityPolicy'required
           (\ x__ y__ -> x__ {_CompatibilityPolicy'required = y__}))
        Prelude.id
instance Data.ProtoLens.Message CompatibilityPolicy where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.CompatibilityPolicy"
  packedMessageDescriptor _
    = "\n\
      \\DC3CompatibilityPolicy\DC2:\n\
      \\EOTmode\CAN\SOH \SOH(\SO2&.acyclic.machines.v1.CompatibilityModeR\EOTmode\DC2;\n\
      \\brequired\CAN\STX \ETX(\SO2\US.acyclic.machines.v1.CapabilityR\brequired"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        mode__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "mode"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor CompatibilityMode)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"mode")) ::
              Data.ProtoLens.FieldDescriptor CompatibilityPolicy
        required__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "required"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor Capability)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Packed (Data.ProtoLens.Field.field @"required")) ::
              Data.ProtoLens.FieldDescriptor CompatibilityPolicy
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, mode__field_descriptor),
           (Data.ProtoLens.Tag 2, required__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CompatibilityPolicy'_unknownFields
        (\ x__ y__ -> x__ {_CompatibilityPolicy'_unknownFields = y__})
  defMessage
    = CompatibilityPolicy'_constructor
        {_CompatibilityPolicy'mode = Data.ProtoLens.fieldDefault,
         _CompatibilityPolicy'required = Data.Vector.Generic.empty,
         _CompatibilityPolicy'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CompatibilityPolicy
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Capability
             -> Data.ProtoLens.Encoding.Bytes.Parser CompatibilityPolicy
        loop x mutable'required
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'required <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'required)
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
                              (Data.ProtoLens.Field.field @"vec'required") frozen'required x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        8 -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "mode"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mode") y x)
                                  mutable'required
                        16
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (Prelude.fmap
                                           Prelude.toEnum
                                           (Prelude.fmap
                                              Prelude.fromIntegral
                                              Data.ProtoLens.Encoding.Bytes.getVarInt))
                                        "required"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'required y)
                                loop x v
                        18
                          -> do y <- do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                        Data.ProtoLens.Encoding.Bytes.isolate
                                          (Prelude.fromIntegral len)
                                          ((let
                                              ploop qs
                                                = do packedEnd <- Data.ProtoLens.Encoding.Bytes.atEnd
                                                     if packedEnd then
                                                         Prelude.return qs
                                                     else
                                                         do !q <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                                                    (Prelude.fmap
                                                                       Prelude.toEnum
                                                                       (Prelude.fmap
                                                                          Prelude.fromIntegral
                                                                          Data.ProtoLens.Encoding.Bytes.getVarInt))
                                                                    "required"
                                                            qs' <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                                     (Data.ProtoLens.Encoding.Growing.append
                                                                        qs q)
                                                            ploop qs'
                                            in ploop)
                                             mutable'required)
                                loop x y
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'required
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'required <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'required)
          "CompatibilityPolicy"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"mode") _x
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
                   p = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"vec'required") _x
                 in
                   if Data.Vector.Generic.null p then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            (Data.ProtoLens.Encoding.Bytes.runBuilder
                               (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                                  ((Prelude..)
                                     ((Prelude..)
                                        Data.ProtoLens.Encoding.Bytes.putVarInt
                                        Prelude.fromIntegral)
                                     Prelude.fromEnum)
                                  p))))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData CompatibilityPolicy where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CompatibilityPolicy'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CompatibilityPolicy'mode x__)
                (Control.DeepSeq.deepseq (_CompatibilityPolicy'required x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' CreateMachineRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' CreateMachineRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' CreateMachineRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' CreateMachineRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.image' @:: Lens' CreateMachineRequest Image@
         * 'Proto.Machines.V1.Machines_Fields.maybe'image' @:: Lens' CreateMachineRequest (Prelude.Maybe Image)@
         * 'Proto.Machines.V1.Machines_Fields.compatibility' @:: Lens' CreateMachineRequest CompatibilityPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'compatibility' @:: Lens' CreateMachineRequest (Prelude.Maybe CompatibilityPolicy)@
         * 'Proto.Machines.V1.Machines_Fields.suspension' @:: Lens' CreateMachineRequest SuspensionPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'suspension' @:: Lens' CreateMachineRequest (Prelude.Maybe SuspensionPolicy)@
         * 'Proto.Machines.V1.Machines_Fields.expiration' @:: Lens' CreateMachineRequest ExpirationPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'expiration' @:: Lens' CreateMachineRequest (Prelude.Maybe ExpirationPolicy)@
         * 'Proto.Machines.V1.Machines_Fields.networkPolicyDigest' @:: Lens' CreateMachineRequest Data.ByteString.ByteString@
         * 'Proto.Machines.V1.Machines_Fields.budgets' @:: Lens' CreateMachineRequest Budgets@
         * 'Proto.Machines.V1.Machines_Fields.maybe'budgets' @:: Lens' CreateMachineRequest (Prelude.Maybe Budgets)@ -}
data CreateMachineRequest
  = CreateMachineRequest'_constructor {_CreateMachineRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                       _CreateMachineRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                       _CreateMachineRequest'image :: !(Prelude.Maybe Image),
                                       _CreateMachineRequest'compatibility :: !(Prelude.Maybe CompatibilityPolicy),
                                       _CreateMachineRequest'suspension :: !(Prelude.Maybe SuspensionPolicy),
                                       _CreateMachineRequest'expiration :: !(Prelude.Maybe ExpirationPolicy),
                                       _CreateMachineRequest'networkPolicyDigest :: !Data.ByteString.ByteString,
                                       _CreateMachineRequest'budgets :: !(Prelude.Maybe Budgets),
                                       _CreateMachineRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateMachineRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateMachineRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'protocol
           (\ x__ y__ -> x__ {_CreateMachineRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'protocol
           (\ x__ y__ -> x__ {_CreateMachineRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'idempotencyKey
           (\ x__ y__ -> x__ {_CreateMachineRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'idempotencyKey
           (\ x__ y__ -> x__ {_CreateMachineRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "image" Image where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'image
           (\ x__ y__ -> x__ {_CreateMachineRequest'image = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'image" (Prelude.Maybe Image) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'image
           (\ x__ y__ -> x__ {_CreateMachineRequest'image = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "compatibility" CompatibilityPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'compatibility
           (\ x__ y__ -> x__ {_CreateMachineRequest'compatibility = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'compatibility" (Prelude.Maybe CompatibilityPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'compatibility
           (\ x__ y__ -> x__ {_CreateMachineRequest'compatibility = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "suspension" SuspensionPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'suspension
           (\ x__ y__ -> x__ {_CreateMachineRequest'suspension = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'suspension" (Prelude.Maybe SuspensionPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'suspension
           (\ x__ y__ -> x__ {_CreateMachineRequest'suspension = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "expiration" ExpirationPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'expiration
           (\ x__ y__ -> x__ {_CreateMachineRequest'expiration = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'expiration" (Prelude.Maybe ExpirationPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'expiration
           (\ x__ y__ -> x__ {_CreateMachineRequest'expiration = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "networkPolicyDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'networkPolicyDigest
           (\ x__ y__
              -> x__ {_CreateMachineRequest'networkPolicyDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateMachineRequest "budgets" Budgets where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'budgets
           (\ x__ y__ -> x__ {_CreateMachineRequest'budgets = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateMachineRequest "maybe'budgets" (Prelude.Maybe Budgets) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateMachineRequest'budgets
           (\ x__ y__ -> x__ {_CreateMachineRequest'budgets = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateMachineRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.CreateMachineRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC4CreateMachineRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC20\n\
      \\ENQimage\CAN\ETX \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\DC2N\n\
      \\rcompatibility\CAN\EOT \SOH(\v2(.acyclic.machines.v1.CompatibilityPolicyR\rcompatibility\DC2E\n\
      \\n\
      \suspension\CAN\ACK \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\n\
      \suspension\DC2E\n\
      \\n\
      \expiration\CAN\a \SOH(\v2%.acyclic.machines.v1.ExpirationPolicyR\n\
      \expiration\DC22\n\
      \\NAKnetwork_policy_digest\CAN\b \SOH(\fR\DC3networkPolicyDigest\DC26\n\
      \\abudgets\CAN\t \SOH(\v2\FS.acyclic.machines.v1.BudgetsR\abudgetsJ\EOT\b\ENQ\DLE\ACKR\vperformance"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        image__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "image"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Image)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'image")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        compatibility__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "compatibility"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CompatibilityPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'compatibility")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        suspension__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suspension"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SuspensionPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suspension")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        expiration__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expiration"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ExpirationPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'expiration")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        networkPolicyDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "network_policy_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"networkPolicyDigest")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
        budgets__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "budgets"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Budgets)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'budgets")) ::
              Data.ProtoLens.FieldDescriptor CreateMachineRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, image__field_descriptor),
           (Data.ProtoLens.Tag 4, compatibility__field_descriptor),
           (Data.ProtoLens.Tag 6, suspension__field_descriptor),
           (Data.ProtoLens.Tag 7, expiration__field_descriptor),
           (Data.ProtoLens.Tag 8, networkPolicyDigest__field_descriptor),
           (Data.ProtoLens.Tag 9, budgets__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateMachineRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateMachineRequest'_unknownFields = y__})
  defMessage
    = CreateMachineRequest'_constructor
        {_CreateMachineRequest'protocol = Prelude.Nothing,
         _CreateMachineRequest'idempotencyKey = Prelude.Nothing,
         _CreateMachineRequest'image = Prelude.Nothing,
         _CreateMachineRequest'compatibility = Prelude.Nothing,
         _CreateMachineRequest'suspension = Prelude.Nothing,
         _CreateMachineRequest'expiration = Prelude.Nothing,
         _CreateMachineRequest'networkPolicyDigest = Data.ProtoLens.fieldDefault,
         _CreateMachineRequest'budgets = Prelude.Nothing,
         _CreateMachineRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateMachineRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CreateMachineRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "image"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"image") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "compatibility"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"compatibility") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "suspension"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"suspension") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "expiration"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expiration") y x)
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "network_policy_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"networkPolicyDigest") y x)
                        74
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "budgets"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"budgets") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CreateMachineRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'image") _x
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
                             (Data.ProtoLens.Field.field @"maybe'compatibility") _x
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
                              Lens.Family2.view
                                (Data.ProtoLens.Field.field @"maybe'suspension") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
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
                                   (Data.ProtoLens.Field.field @"maybe'expiration") _x
                             of
                               Prelude.Nothing -> Data.Monoid.mempty
                               (Prelude.Just _v)
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                                      ((Prelude..)
                                         (\ bs
                                            -> (Data.Monoid.<>)
                                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                    (Prelude.fromIntegral
                                                       (Data.ByteString.length bs)))
                                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                         Data.ProtoLens.encodeMessage _v))
                            ((Data.Monoid.<>)
                               (let
                                  _v
                                    = Lens.Family2.view
                                        (Data.ProtoLens.Field.field @"networkPolicyDigest") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 66)
                                        ((\ bs
                                            -> (Data.Monoid.<>)
                                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                    (Prelude.fromIntegral
                                                       (Data.ByteString.length bs)))
                                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                           _v))
                               ((Data.Monoid.<>)
                                  (case
                                       Lens.Family2.view
                                         (Data.ProtoLens.Field.field @"maybe'budgets") _x
                                   of
                                     Prelude.Nothing -> Data.Monoid.mempty
                                     (Prelude.Just _v)
                                       -> (Data.Monoid.<>)
                                            (Data.ProtoLens.Encoding.Bytes.putVarInt 74)
                                            ((Prelude..)
                                               (\ bs
                                                  -> (Data.Monoid.<>)
                                                       (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                          (Prelude.fromIntegral
                                                             (Data.ByteString.length bs)))
                                                       (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                               Data.ProtoLens.encodeMessage _v))
                                  (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                     (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))))
instance Control.DeepSeq.NFData CreateMachineRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateMachineRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateMachineRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_CreateMachineRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_CreateMachineRequest'image x__)
                      (Control.DeepSeq.deepseq
                         (_CreateMachineRequest'compatibility x__)
                         (Control.DeepSeq.deepseq
                            (_CreateMachineRequest'suspension x__)
                            (Control.DeepSeq.deepseq
                               (_CreateMachineRequest'expiration x__)
                               (Control.DeepSeq.deepseq
                                  (_CreateMachineRequest'networkPolicyDigest x__)
                                  (Control.DeepSeq.deepseq
                                     (_CreateMachineRequest'budgets x__) ()))))))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.name' @:: Lens' Endpoint Data.Text.Text@
         * 'Proto.Machines.V1.Machines_Fields.uri' @:: Lens' Endpoint Data.Text.Text@ -}
data Endpoint
  = Endpoint'_constructor {_Endpoint'name :: !Data.Text.Text,
                           _Endpoint'uri :: !Data.Text.Text,
                           _Endpoint'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Endpoint where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Endpoint "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Endpoint'name (\ x__ y__ -> x__ {_Endpoint'name = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Endpoint "uri" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Endpoint'uri (\ x__ y__ -> x__ {_Endpoint'uri = y__}))
        Prelude.id
instance Data.ProtoLens.Message Endpoint where
  messageName _ = Data.Text.pack "acyclic.machines.v1.Endpoint"
  packedMessageDescriptor _
    = "\n\
      \\bEndpoint\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\DLE\n\
      \\ETXuri\CAN\STX \SOH(\tR\ETXuri"
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
              Data.ProtoLens.FieldDescriptor Endpoint
        uri__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "uri"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"uri")) ::
              Data.ProtoLens.FieldDescriptor Endpoint
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, name__field_descriptor),
           (Data.ProtoLens.Tag 2, uri__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Endpoint'_unknownFields
        (\ x__ y__ -> x__ {_Endpoint'_unknownFields = y__})
  defMessage
    = Endpoint'_constructor
        {_Endpoint'name = Data.ProtoLens.fieldDefault,
         _Endpoint'uri = Data.ProtoLens.fieldDefault,
         _Endpoint'_unknownFields = []}
  parseMessage
    = let
        loop :: Endpoint -> Data.ProtoLens.Encoding.Bytes.Parser Endpoint
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
                                       "uri"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"uri") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Endpoint"
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"uri") _x
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
instance Control.DeepSeq.NFData Endpoint where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Endpoint'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Endpoint'name x__)
                (Control.DeepSeq.deepseq (_Endpoint'uri x__) ()))
newtype EventKind'UnrecognizedValue
  = EventKind'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data EventKind
  = EVENT_KIND_UNSPECIFIED |
    EVENT_KIND_STATE |
    EVENT_KIND_PRESSURE |
    EVENT_KIND_CAPACITY |
    EventKind'Unrecognized !EventKind'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum EventKind where
  maybeToEnum 0 = Prelude.Just EVENT_KIND_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just EVENT_KIND_STATE
  maybeToEnum 2 = Prelude.Just EVENT_KIND_PRESSURE
  maybeToEnum 3 = Prelude.Just EVENT_KIND_CAPACITY
  maybeToEnum k
    = Prelude.Just
        (EventKind'Unrecognized
           (EventKind'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum EVENT_KIND_UNSPECIFIED = "EVENT_KIND_UNSPECIFIED"
  showEnum EVENT_KIND_STATE = "EVENT_KIND_STATE"
  showEnum EVENT_KIND_PRESSURE = "EVENT_KIND_PRESSURE"
  showEnum EVENT_KIND_CAPACITY = "EVENT_KIND_CAPACITY"
  showEnum (EventKind'Unrecognized (EventKind'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "EVENT_KIND_UNSPECIFIED"
    = Prelude.Just EVENT_KIND_UNSPECIFIED
    | (Prelude.==) k "EVENT_KIND_STATE" = Prelude.Just EVENT_KIND_STATE
    | (Prelude.==) k "EVENT_KIND_PRESSURE"
    = Prelude.Just EVENT_KIND_PRESSURE
    | (Prelude.==) k "EVENT_KIND_CAPACITY"
    = Prelude.Just EVENT_KIND_CAPACITY
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded EventKind where
  minBound = EVENT_KIND_UNSPECIFIED
  maxBound = EVENT_KIND_CAPACITY
instance Prelude.Enum EventKind where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum EventKind: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum EVENT_KIND_UNSPECIFIED = 0
  fromEnum EVENT_KIND_STATE = 1
  fromEnum EVENT_KIND_PRESSURE = 2
  fromEnum EVENT_KIND_CAPACITY = 3
  fromEnum (EventKind'Unrecognized (EventKind'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ EVENT_KIND_CAPACITY
    = Prelude.error
        "EventKind.succ: bad argument EVENT_KIND_CAPACITY. This value would be out of bounds."
  succ EVENT_KIND_UNSPECIFIED = EVENT_KIND_STATE
  succ EVENT_KIND_STATE = EVENT_KIND_PRESSURE
  succ EVENT_KIND_PRESSURE = EVENT_KIND_CAPACITY
  succ (EventKind'Unrecognized _)
    = Prelude.error "EventKind.succ: bad argument: unrecognized value"
  pred EVENT_KIND_UNSPECIFIED
    = Prelude.error
        "EventKind.pred: bad argument EVENT_KIND_UNSPECIFIED. This value would be out of bounds."
  pred EVENT_KIND_STATE = EVENT_KIND_UNSPECIFIED
  pred EVENT_KIND_PRESSURE = EVENT_KIND_STATE
  pred EVENT_KIND_CAPACITY = EVENT_KIND_PRESSURE
  pred (EventKind'Unrecognized _)
    = Prelude.error "EventKind.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault EventKind where
  fieldDefault = EVENT_KIND_UNSPECIFIED
instance Control.DeepSeq.NFData EventKind where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.events' @:: Lens' EventPage [MachineEvent]@
         * 'Proto.Machines.V1.Machines_Fields.vec'events' @:: Lens' EventPage (Data.Vector.Vector MachineEvent)@
         * 'Proto.Machines.V1.Machines_Fields.nextSequence' @:: Lens' EventPage Data.Word.Word64@ -}
data EventPage
  = EventPage'_constructor {_EventPage'events :: !(Data.Vector.Vector MachineEvent),
                            _EventPage'nextSequence :: !Data.Word.Word64,
                            _EventPage'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EventPage where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EventPage "events" [MachineEvent] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventPage'events (\ x__ y__ -> x__ {_EventPage'events = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EventPage "vec'events" (Data.Vector.Vector MachineEvent) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventPage'events (\ x__ y__ -> x__ {_EventPage'events = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EventPage "nextSequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventPage'nextSequence
           (\ x__ y__ -> x__ {_EventPage'nextSequence = y__}))
        Prelude.id
instance Data.ProtoLens.Message EventPage where
  messageName _ = Data.Text.pack "acyclic.machines.v1.EventPage"
  packedMessageDescriptor _
    = "\n\
      \\tEventPage\DC29\n\
      \\ACKevents\CAN\SOH \ETX(\v2!.acyclic.machines.v1.MachineEventR\ACKevents\DC2#\n\
      \\rnext_sequence\CAN\STX \SOH(\EOTR\fnextSequence"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        events__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "events"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineEvent)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"events")) ::
              Data.ProtoLens.FieldDescriptor EventPage
        nextSequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "next_sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"nextSequence")) ::
              Data.ProtoLens.FieldDescriptor EventPage
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, events__field_descriptor),
           (Data.ProtoLens.Tag 2, nextSequence__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EventPage'_unknownFields
        (\ x__ y__ -> x__ {_EventPage'_unknownFields = y__})
  defMessage
    = EventPage'_constructor
        {_EventPage'events = Data.Vector.Generic.empty,
         _EventPage'nextSequence = Data.ProtoLens.fieldDefault,
         _EventPage'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EventPage
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld MachineEvent
             -> Data.ProtoLens.Encoding.Bytes.Parser EventPage
        loop x mutable'events
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'events <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                         (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                            mutable'events)
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
                              (Data.ProtoLens.Field.field @"vec'events") frozen'events x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "events"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'events y)
                                loop x v
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "next_sequence"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"nextSequence") y x)
                                  mutable'events
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'events
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'events <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                  Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'events)
          "EventPage"
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
                (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'events") _x))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"nextSequence") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData EventPage where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EventPage'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EventPage'events x__)
                (Control.DeepSeq.deepseq (_EventPage'nextSequence x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' EventsRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' EventsRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' EventsRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' EventsRequest (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.afterSequence' @:: Lens' EventsRequest Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.limit' @:: Lens' EventsRequest Data.Word.Word32@ -}
data EventsRequest
  = EventsRequest'_constructor {_EventsRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                _EventsRequest'machine :: !(Prelude.Maybe MachineId),
                                _EventsRequest'afterSequence :: !Data.Word.Word64,
                                _EventsRequest'limit :: !Data.Word.Word32,
                                _EventsRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EventsRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EventsRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventsRequest'protocol
           (\ x__ y__ -> x__ {_EventsRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EventsRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventsRequest'protocol
           (\ x__ y__ -> x__ {_EventsRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EventsRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventsRequest'machine
           (\ x__ y__ -> x__ {_EventsRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EventsRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventsRequest'machine
           (\ x__ y__ -> x__ {_EventsRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EventsRequest "afterSequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventsRequest'afterSequence
           (\ x__ y__ -> x__ {_EventsRequest'afterSequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EventsRequest "limit" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EventsRequest'limit
           (\ x__ y__ -> x__ {_EventsRequest'limit = y__}))
        Prelude.id
instance Data.ProtoLens.Message EventsRequest where
  messageName _ = Data.Text.pack "acyclic.machines.v1.EventsRequest"
  packedMessageDescriptor _
    = "\n\
      \\rEventsRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC28\n\
      \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2%\n\
      \\SOafter_sequence\CAN\ETX \SOH(\EOTR\rafterSequence\DC2\DC4\n\
      \\ENQlimit\CAN\EOT \SOH(\rR\ENQlimit"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor EventsRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor EventsRequest
        afterSequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "after_sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"afterSequence")) ::
              Data.ProtoLens.FieldDescriptor EventsRequest
        limit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limit"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"limit")) ::
              Data.ProtoLens.FieldDescriptor EventsRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, machine__field_descriptor),
           (Data.ProtoLens.Tag 3, afterSequence__field_descriptor),
           (Data.ProtoLens.Tag 4, limit__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EventsRequest'_unknownFields
        (\ x__ y__ -> x__ {_EventsRequest'_unknownFields = y__})
  defMessage
    = EventsRequest'_constructor
        {_EventsRequest'protocol = Prelude.Nothing,
         _EventsRequest'machine = Prelude.Nothing,
         _EventsRequest'afterSequence = Data.ProtoLens.fieldDefault,
         _EventsRequest'limit = Data.ProtoLens.fieldDefault,
         _EventsRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EventsRequest -> Data.ProtoLens.Encoding.Bytes.Parser EventsRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "after_sequence"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"afterSequence") y x)
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
          (do loop Data.ProtoLens.defMessage) "EventsRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                            (Data.ProtoLens.Field.field @"afterSequence") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
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
instance Control.DeepSeq.NFData EventsRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EventsRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EventsRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_EventsRequest'machine x__)
                   (Control.DeepSeq.deepseq
                      (_EventsRequest'afterSequence x__)
                      (Control.DeepSeq.deepseq (_EventsRequest'limit x__) ()))))
newtype ExpirationKind'UnrecognizedValue
  = ExpirationKind'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ExpirationKind
  = EXPIRATION_KIND_UNSPECIFIED |
    EXPIRATION_KIND_NEVER |
    EXPIRATION_KIND_MAX_AGE |
    EXPIRATION_KIND_AT |
    EXPIRATION_KIND_IDLE |
    ExpirationKind'Unrecognized !ExpirationKind'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ExpirationKind where
  maybeToEnum 0 = Prelude.Just EXPIRATION_KIND_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just EXPIRATION_KIND_NEVER
  maybeToEnum 2 = Prelude.Just EXPIRATION_KIND_MAX_AGE
  maybeToEnum 3 = Prelude.Just EXPIRATION_KIND_AT
  maybeToEnum 4 = Prelude.Just EXPIRATION_KIND_IDLE
  maybeToEnum k
    = Prelude.Just
        (ExpirationKind'Unrecognized
           (ExpirationKind'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum EXPIRATION_KIND_UNSPECIFIED
    = "EXPIRATION_KIND_UNSPECIFIED"
  showEnum EXPIRATION_KIND_NEVER = "EXPIRATION_KIND_NEVER"
  showEnum EXPIRATION_KIND_MAX_AGE = "EXPIRATION_KIND_MAX_AGE"
  showEnum EXPIRATION_KIND_AT = "EXPIRATION_KIND_AT"
  showEnum EXPIRATION_KIND_IDLE = "EXPIRATION_KIND_IDLE"
  showEnum
    (ExpirationKind'Unrecognized (ExpirationKind'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "EXPIRATION_KIND_UNSPECIFIED"
    = Prelude.Just EXPIRATION_KIND_UNSPECIFIED
    | (Prelude.==) k "EXPIRATION_KIND_NEVER"
    = Prelude.Just EXPIRATION_KIND_NEVER
    | (Prelude.==) k "EXPIRATION_KIND_MAX_AGE"
    = Prelude.Just EXPIRATION_KIND_MAX_AGE
    | (Prelude.==) k "EXPIRATION_KIND_AT"
    = Prelude.Just EXPIRATION_KIND_AT
    | (Prelude.==) k "EXPIRATION_KIND_IDLE"
    = Prelude.Just EXPIRATION_KIND_IDLE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ExpirationKind where
  minBound = EXPIRATION_KIND_UNSPECIFIED
  maxBound = EXPIRATION_KIND_IDLE
instance Prelude.Enum ExpirationKind where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ExpirationKind: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum EXPIRATION_KIND_UNSPECIFIED = 0
  fromEnum EXPIRATION_KIND_NEVER = 1
  fromEnum EXPIRATION_KIND_MAX_AGE = 2
  fromEnum EXPIRATION_KIND_AT = 3
  fromEnum EXPIRATION_KIND_IDLE = 4
  fromEnum
    (ExpirationKind'Unrecognized (ExpirationKind'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ EXPIRATION_KIND_IDLE
    = Prelude.error
        "ExpirationKind.succ: bad argument EXPIRATION_KIND_IDLE. This value would be out of bounds."
  succ EXPIRATION_KIND_UNSPECIFIED = EXPIRATION_KIND_NEVER
  succ EXPIRATION_KIND_NEVER = EXPIRATION_KIND_MAX_AGE
  succ EXPIRATION_KIND_MAX_AGE = EXPIRATION_KIND_AT
  succ EXPIRATION_KIND_AT = EXPIRATION_KIND_IDLE
  succ (ExpirationKind'Unrecognized _)
    = Prelude.error
        "ExpirationKind.succ: bad argument: unrecognized value"
  pred EXPIRATION_KIND_UNSPECIFIED
    = Prelude.error
        "ExpirationKind.pred: bad argument EXPIRATION_KIND_UNSPECIFIED. This value would be out of bounds."
  pred EXPIRATION_KIND_NEVER = EXPIRATION_KIND_UNSPECIFIED
  pred EXPIRATION_KIND_MAX_AGE = EXPIRATION_KIND_NEVER
  pred EXPIRATION_KIND_AT = EXPIRATION_KIND_MAX_AGE
  pred EXPIRATION_KIND_IDLE = EXPIRATION_KIND_AT
  pred (ExpirationKind'Unrecognized _)
    = Prelude.error
        "ExpirationKind.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ExpirationKind where
  fieldDefault = EXPIRATION_KIND_UNSPECIFIED
instance Control.DeepSeq.NFData ExpirationKind where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.kind' @:: Lens' ExpirationPolicy ExpirationKind@
         * 'Proto.Machines.V1.Machines_Fields.valueMs' @:: Lens' ExpirationPolicy Data.Word.Word64@ -}
data ExpirationPolicy
  = ExpirationPolicy'_constructor {_ExpirationPolicy'kind :: !ExpirationKind,
                                   _ExpirationPolicy'valueMs :: !Data.Word.Word64,
                                   _ExpirationPolicy'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ExpirationPolicy where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ExpirationPolicy "kind" ExpirationKind where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ExpirationPolicy'kind
           (\ x__ y__ -> x__ {_ExpirationPolicy'kind = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ExpirationPolicy "valueMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ExpirationPolicy'valueMs
           (\ x__ y__ -> x__ {_ExpirationPolicy'valueMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message ExpirationPolicy where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ExpirationPolicy"
  packedMessageDescriptor _
    = "\n\
      \\DLEExpirationPolicy\DC27\n\
      \\EOTkind\CAN\SOH \SOH(\SO2#.acyclic.machines.v1.ExpirationKindR\EOTkind\DC2\EM\n\
      \\bvalue_ms\CAN\STX \SOH(\EOTR\avalueMs"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        kind__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "kind"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ExpirationKind)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"kind")) ::
              Data.ProtoLens.FieldDescriptor ExpirationPolicy
        valueMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"valueMs")) ::
              Data.ProtoLens.FieldDescriptor ExpirationPolicy
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, kind__field_descriptor),
           (Data.ProtoLens.Tag 2, valueMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ExpirationPolicy'_unknownFields
        (\ x__ y__ -> x__ {_ExpirationPolicy'_unknownFields = y__})
  defMessage
    = ExpirationPolicy'_constructor
        {_ExpirationPolicy'kind = Data.ProtoLens.fieldDefault,
         _ExpirationPolicy'valueMs = Data.ProtoLens.fieldDefault,
         _ExpirationPolicy'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ExpirationPolicy
          -> Data.ProtoLens.Encoding.Bytes.Parser ExpirationPolicy
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
                                       "kind"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"kind") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "value_ms"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"valueMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ExpirationPolicy"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"kind") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"valueMs") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ExpirationPolicy where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ExpirationPolicy'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ExpirationPolicy'kind x__)
                (Control.DeepSeq.deepseq (_ExpirationPolicy'valueMs x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' ForkAdmission CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' ForkAdmission (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.children' @:: Lens' ForkAdmission [MachineId]@
         * 'Proto.Machines.V1.Machines_Fields.vec'children' @:: Lens' ForkAdmission (Data.Vector.Vector MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' ForkAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' ForkAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.contract' @:: Lens' ForkAdmission MachineContract@
         * 'Proto.Machines.V1.Machines_Fields.maybe'contract' @:: Lens' ForkAdmission (Prelude.Maybe MachineContract)@ -}
data ForkAdmission
  = ForkAdmission'_constructor {_ForkAdmission'checkpoint :: !(Prelude.Maybe CheckpointId),
                                _ForkAdmission'children :: !(Data.Vector.Vector MachineId),
                                _ForkAdmission'operation :: !(Prelude.Maybe OperationId),
                                _ForkAdmission'contract :: !(Prelude.Maybe MachineContract),
                                _ForkAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkAdmission "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'checkpoint
           (\ x__ y__ -> x__ {_ForkAdmission'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkAdmission "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'checkpoint
           (\ x__ y__ -> x__ {_ForkAdmission'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkAdmission "children" [MachineId] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'children
           (\ x__ y__ -> x__ {_ForkAdmission'children = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ForkAdmission "vec'children" (Data.Vector.Vector MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'children
           (\ x__ y__ -> x__ {_ForkAdmission'children = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'operation
           (\ x__ y__ -> x__ {_ForkAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'operation
           (\ x__ y__ -> x__ {_ForkAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkAdmission "contract" MachineContract where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'contract
           (\ x__ y__ -> x__ {_ForkAdmission'contract = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkAdmission "maybe'contract" (Prelude.Maybe MachineContract) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkAdmission'contract
           (\ x__ y__ -> x__ {_ForkAdmission'contract = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkAdmission where
  messageName _ = Data.Text.pack "acyclic.machines.v1.ForkAdmission"
  packedMessageDescriptor _
    = "\n\
      \\rForkAdmission\DC2A\n\
      \\n\
      \checkpoint\CAN\SOH \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint\DC2:\n\
      \\bchildren\CAN\STX \ETX(\v2\RS.acyclic.machines.v1.MachineIdR\bchildren\DC2>\n\
      \\toperation\CAN\ETX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
      \\bcontract\CAN\EOT \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor ForkAdmission
        children__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "children"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"children")) ::
              Data.ProtoLens.FieldDescriptor ForkAdmission
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor ForkAdmission
        contract__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "contract"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineContract)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contract")) ::
              Data.ProtoLens.FieldDescriptor ForkAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, checkpoint__field_descriptor),
           (Data.ProtoLens.Tag 2, children__field_descriptor),
           (Data.ProtoLens.Tag 3, operation__field_descriptor),
           (Data.ProtoLens.Tag 4, contract__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkAdmission'_unknownFields
        (\ x__ y__ -> x__ {_ForkAdmission'_unknownFields = y__})
  defMessage
    = ForkAdmission'_constructor
        {_ForkAdmission'checkpoint = Prelude.Nothing,
         _ForkAdmission'children = Data.Vector.Generic.empty,
         _ForkAdmission'operation = Prelude.Nothing,
         _ForkAdmission'contract = Prelude.Nothing,
         _ForkAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkAdmission
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld MachineId
             -> Data.ProtoLens.Encoding.Bytes.Parser ForkAdmission
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                                  mutable'children
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "contract"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contract") y x)
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
          "ForkAdmission"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
                          (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'contract") _x
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
instance Control.DeepSeq.NFData ForkAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkAdmission'checkpoint x__)
                (Control.DeepSeq.deepseq
                   (_ForkAdmission'children x__)
                   (Control.DeepSeq.deepseq
                      (_ForkAdmission'operation x__)
                      (Control.DeepSeq.deepseq (_ForkAdmission'contract x__) ()))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' ForkCheckpointRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' ForkCheckpointRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' ForkCheckpointRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' ForkCheckpointRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' ForkCheckpointRequest CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' ForkCheckpointRequest (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.count' @:: Lens' ForkCheckpointRequest Data.Word.Word32@ -}
data ForkCheckpointRequest
  = ForkCheckpointRequest'_constructor {_ForkCheckpointRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                        _ForkCheckpointRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                        _ForkCheckpointRequest'checkpoint :: !(Prelude.Maybe CheckpointId),
                                        _ForkCheckpointRequest'count :: !Data.Word.Word32,
                                        _ForkCheckpointRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkCheckpointRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'protocol
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'protocol
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'idempotencyKey
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'idempotencyKey
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'checkpoint
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'checkpoint
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkCheckpointRequest "count" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkCheckpointRequest'count
           (\ x__ y__ -> x__ {_ForkCheckpointRequest'count = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkCheckpointRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ForkCheckpointRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKForkCheckpointRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC2A\n\
      \\n\
      \checkpoint\CAN\ETX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint\DC2\DC4\n\
      \\ENQcount\CAN\EOT \SOH(\rR\ENQcountJ\EOT\b\ENQ\DLE\ACKR\vperformance"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor ForkCheckpointRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor ForkCheckpointRequest
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor ForkCheckpointRequest
        count__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "count"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"count")) ::
              Data.ProtoLens.FieldDescriptor ForkCheckpointRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, checkpoint__field_descriptor),
           (Data.ProtoLens.Tag 4, count__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkCheckpointRequest'_unknownFields
        (\ x__ y__ -> x__ {_ForkCheckpointRequest'_unknownFields = y__})
  defMessage
    = ForkCheckpointRequest'_constructor
        {_ForkCheckpointRequest'protocol = Prelude.Nothing,
         _ForkCheckpointRequest'idempotencyKey = Prelude.Nothing,
         _ForkCheckpointRequest'checkpoint = Prelude.Nothing,
         _ForkCheckpointRequest'count = Data.ProtoLens.fieldDefault,
         _ForkCheckpointRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkCheckpointRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ForkCheckpointRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "count"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"count") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ForkCheckpointRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"count") _x
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
instance Control.DeepSeq.NFData ForkCheckpointRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkCheckpointRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkCheckpointRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_ForkCheckpointRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_ForkCheckpointRequest'checkpoint x__)
                      (Control.DeepSeq.deepseq (_ForkCheckpointRequest'count x__) ()))))
newtype ForkFidelity'UnrecognizedValue
  = ForkFidelity'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ForkFidelity
  = FORK_FIDELITY_UNSPECIFIED |
    FORK_FIDELITY_MEMORY_AND_DISK |
    FORK_FIDELITY_DISK_ONLY |
    ForkFidelity'Unrecognized !ForkFidelity'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ForkFidelity where
  maybeToEnum 0 = Prelude.Just FORK_FIDELITY_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just FORK_FIDELITY_MEMORY_AND_DISK
  maybeToEnum 2 = Prelude.Just FORK_FIDELITY_DISK_ONLY
  maybeToEnum k
    = Prelude.Just
        (ForkFidelity'Unrecognized
           (ForkFidelity'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum FORK_FIDELITY_UNSPECIFIED = "FORK_FIDELITY_UNSPECIFIED"
  showEnum FORK_FIDELITY_MEMORY_AND_DISK
    = "FORK_FIDELITY_MEMORY_AND_DISK"
  showEnum FORK_FIDELITY_DISK_ONLY = "FORK_FIDELITY_DISK_ONLY"
  showEnum
    (ForkFidelity'Unrecognized (ForkFidelity'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "FORK_FIDELITY_UNSPECIFIED"
    = Prelude.Just FORK_FIDELITY_UNSPECIFIED
    | (Prelude.==) k "FORK_FIDELITY_MEMORY_AND_DISK"
    = Prelude.Just FORK_FIDELITY_MEMORY_AND_DISK
    | (Prelude.==) k "FORK_FIDELITY_DISK_ONLY"
    = Prelude.Just FORK_FIDELITY_DISK_ONLY
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ForkFidelity where
  minBound = FORK_FIDELITY_UNSPECIFIED
  maxBound = FORK_FIDELITY_DISK_ONLY
instance Prelude.Enum ForkFidelity where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ForkFidelity: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum FORK_FIDELITY_UNSPECIFIED = 0
  fromEnum FORK_FIDELITY_MEMORY_AND_DISK = 1
  fromEnum FORK_FIDELITY_DISK_ONLY = 2
  fromEnum
    (ForkFidelity'Unrecognized (ForkFidelity'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ FORK_FIDELITY_DISK_ONLY
    = Prelude.error
        "ForkFidelity.succ: bad argument FORK_FIDELITY_DISK_ONLY. This value would be out of bounds."
  succ FORK_FIDELITY_UNSPECIFIED = FORK_FIDELITY_MEMORY_AND_DISK
  succ FORK_FIDELITY_MEMORY_AND_DISK = FORK_FIDELITY_DISK_ONLY
  succ (ForkFidelity'Unrecognized _)
    = Prelude.error
        "ForkFidelity.succ: bad argument: unrecognized value"
  pred FORK_FIDELITY_UNSPECIFIED
    = Prelude.error
        "ForkFidelity.pred: bad argument FORK_FIDELITY_UNSPECIFIED. This value would be out of bounds."
  pred FORK_FIDELITY_MEMORY_AND_DISK = FORK_FIDELITY_UNSPECIFIED
  pred FORK_FIDELITY_DISK_ONLY = FORK_FIDELITY_MEMORY_AND_DISK
  pred (ForkFidelity'Unrecognized _)
    = Prelude.error
        "ForkFidelity.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ForkFidelity where
  fieldDefault = FORK_FIDELITY_UNSPECIFIED
instance Control.DeepSeq.NFData ForkFidelity where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.source' @:: Lens' ForkMachineAdmission MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'source' @:: Lens' ForkMachineAdmission (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.children' @:: Lens' ForkMachineAdmission [MachineId]@
         * 'Proto.Machines.V1.Machines_Fields.vec'children' @:: Lens' ForkMachineAdmission (Data.Vector.Vector MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' ForkMachineAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' ForkMachineAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.contract' @:: Lens' ForkMachineAdmission MachineContract@
         * 'Proto.Machines.V1.Machines_Fields.maybe'contract' @:: Lens' ForkMachineAdmission (Prelude.Maybe MachineContract)@
         * 'Proto.Machines.V1.Machines_Fields.fidelity' @:: Lens' ForkMachineAdmission ForkFidelity@ -}
data ForkMachineAdmission
  = ForkMachineAdmission'_constructor {_ForkMachineAdmission'source :: !(Prelude.Maybe MachineId),
                                       _ForkMachineAdmission'children :: !(Data.Vector.Vector MachineId),
                                       _ForkMachineAdmission'operation :: !(Prelude.Maybe OperationId),
                                       _ForkMachineAdmission'contract :: !(Prelude.Maybe MachineContract),
                                       _ForkMachineAdmission'fidelity :: !ForkFidelity,
                                       _ForkMachineAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkMachineAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "source" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'source
           (\ x__ y__ -> x__ {_ForkMachineAdmission'source = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "maybe'source" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'source
           (\ x__ y__ -> x__ {_ForkMachineAdmission'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "children" [MachineId] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'children
           (\ x__ y__ -> x__ {_ForkMachineAdmission'children = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "vec'children" (Data.Vector.Vector MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'children
           (\ x__ y__ -> x__ {_ForkMachineAdmission'children = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'operation
           (\ x__ y__ -> x__ {_ForkMachineAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'operation
           (\ x__ y__ -> x__ {_ForkMachineAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "contract" MachineContract where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'contract
           (\ x__ y__ -> x__ {_ForkMachineAdmission'contract = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "maybe'contract" (Prelude.Maybe MachineContract) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'contract
           (\ x__ y__ -> x__ {_ForkMachineAdmission'contract = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineAdmission "fidelity" ForkFidelity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineAdmission'fidelity
           (\ x__ y__ -> x__ {_ForkMachineAdmission'fidelity = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkMachineAdmission where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ForkMachineAdmission"
  packedMessageDescriptor _
    = "\n\
      \\DC4ForkMachineAdmission\DC26\n\
      \\ACKsource\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2:\n\
      \\bchildren\CAN\STX \ETX(\v2\RS.acyclic.machines.v1.MachineIdR\bchildren\DC2>\n\
      \\toperation\CAN\ETX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
      \\bcontract\CAN\EOT \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\DC2=\n\
      \\bfidelity\CAN\ENQ \SOH(\SO2!.acyclic.machines.v1.ForkFidelityR\bfidelity"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'source")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineAdmission
        children__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "children"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"children")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineAdmission
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineAdmission
        contract__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "contract"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineContract)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contract")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineAdmission
        fidelity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fidelity"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ForkFidelity)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"fidelity")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, children__field_descriptor),
           (Data.ProtoLens.Tag 3, operation__field_descriptor),
           (Data.ProtoLens.Tag 4, contract__field_descriptor),
           (Data.ProtoLens.Tag 5, fidelity__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkMachineAdmission'_unknownFields
        (\ x__ y__ -> x__ {_ForkMachineAdmission'_unknownFields = y__})
  defMessage
    = ForkMachineAdmission'_constructor
        {_ForkMachineAdmission'source = Prelude.Nothing,
         _ForkMachineAdmission'children = Data.Vector.Generic.empty,
         _ForkMachineAdmission'operation = Prelude.Nothing,
         _ForkMachineAdmission'contract = Prelude.Nothing,
         _ForkMachineAdmission'fidelity = Data.ProtoLens.fieldDefault,
         _ForkMachineAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkMachineAdmission
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld MachineId
             -> Data.ProtoLens.Encoding.Bytes.Parser ForkMachineAdmission
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "source"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                                  mutable'children
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "contract"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contract") y x)
                                  mutable'children
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "fidelity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"fidelity") y x)
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
          "ForkMachineAdmission"
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
                          (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'contract") _x
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
                            _v = Lens.Family2.view (Data.ProtoLens.Field.field @"fidelity") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  ((Prelude..)
                                     ((Prelude..)
                                        Data.ProtoLens.Encoding.Bytes.putVarInt
                                        Prelude.fromIntegral)
                                     Prelude.fromEnum _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData ForkMachineAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkMachineAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkMachineAdmission'source x__)
                (Control.DeepSeq.deepseq
                   (_ForkMachineAdmission'children x__)
                   (Control.DeepSeq.deepseq
                      (_ForkMachineAdmission'operation x__)
                      (Control.DeepSeq.deepseq
                         (_ForkMachineAdmission'contract x__)
                         (Control.DeepSeq.deepseq
                            (_ForkMachineAdmission'fidelity x__) ())))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' ForkMachineRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' ForkMachineRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' ForkMachineRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' ForkMachineRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' ForkMachineRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' ForkMachineRequest (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.count' @:: Lens' ForkMachineRequest Data.Word.Word32@ -}
data ForkMachineRequest
  = ForkMachineRequest'_constructor {_ForkMachineRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                     _ForkMachineRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                     _ForkMachineRequest'machine :: !(Prelude.Maybe MachineId),
                                     _ForkMachineRequest'count :: !Data.Word.Word32,
                                     _ForkMachineRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkMachineRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkMachineRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'protocol
           (\ x__ y__ -> x__ {_ForkMachineRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkMachineRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'protocol
           (\ x__ y__ -> x__ {_ForkMachineRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'idempotencyKey
           (\ x__ y__ -> x__ {_ForkMachineRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkMachineRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'idempotencyKey
           (\ x__ y__ -> x__ {_ForkMachineRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'machine
           (\ x__ y__ -> x__ {_ForkMachineRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkMachineRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'machine
           (\ x__ y__ -> x__ {_ForkMachineRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkMachineRequest "count" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkMachineRequest'count
           (\ x__ y__ -> x__ {_ForkMachineRequest'count = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkMachineRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ForkMachineRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2ForkMachineRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
      \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\DC4\n\
      \\ENQcount\CAN\EOT \SOH(\rR\ENQcount"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineRequest
        count__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "count"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"count")) ::
              Data.ProtoLens.FieldDescriptor ForkMachineRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, machine__field_descriptor),
           (Data.ProtoLens.Tag 4, count__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkMachineRequest'_unknownFields
        (\ x__ y__ -> x__ {_ForkMachineRequest'_unknownFields = y__})
  defMessage
    = ForkMachineRequest'_constructor
        {_ForkMachineRequest'protocol = Prelude.Nothing,
         _ForkMachineRequest'idempotencyKey = Prelude.Nothing,
         _ForkMachineRequest'machine = Prelude.Nothing,
         _ForkMachineRequest'count = Data.ProtoLens.fieldDefault,
         _ForkMachineRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkMachineRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ForkMachineRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "count"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"count") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ForkMachineRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"count") _x
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
instance Control.DeepSeq.NFData ForkMachineRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkMachineRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkMachineRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_ForkMachineRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_ForkMachineRequest'machine x__)
                      (Control.DeepSeq.deepseq (_ForkMachineRequest'count x__) ()))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.source' @:: Lens' ForkedLiveMachines MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'source' @:: Lens' ForkedLiveMachines (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.fidelity' @:: Lens' ForkedLiveMachines ForkFidelity@
         * 'Proto.Machines.V1.Machines_Fields.children' @:: Lens' ForkedLiveMachines [MachineState]@
         * 'Proto.Machines.V1.Machines_Fields.vec'children' @:: Lens' ForkedLiveMachines (Data.Vector.Vector MachineState)@ -}
data ForkedLiveMachines
  = ForkedLiveMachines'_constructor {_ForkedLiveMachines'source :: !(Prelude.Maybe MachineId),
                                     _ForkedLiveMachines'fidelity :: !ForkFidelity,
                                     _ForkedLiveMachines'children :: !(Data.Vector.Vector MachineState),
                                     _ForkedLiveMachines'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkedLiveMachines where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkedLiveMachines "source" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedLiveMachines'source
           (\ x__ y__ -> x__ {_ForkedLiveMachines'source = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ForkedLiveMachines "maybe'source" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedLiveMachines'source
           (\ x__ y__ -> x__ {_ForkedLiveMachines'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkedLiveMachines "fidelity" ForkFidelity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedLiveMachines'fidelity
           (\ x__ y__ -> x__ {_ForkedLiveMachines'fidelity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ForkedLiveMachines "children" [MachineState] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedLiveMachines'children
           (\ x__ y__ -> x__ {_ForkedLiveMachines'children = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ForkedLiveMachines "vec'children" (Data.Vector.Vector MachineState) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedLiveMachines'children
           (\ x__ y__ -> x__ {_ForkedLiveMachines'children = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkedLiveMachines where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ForkedLiveMachines"
  packedMessageDescriptor _
    = "\n\
      \\DC2ForkedLiveMachines\DC26\n\
      \\ACKsource\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2=\n\
      \\bfidelity\CAN\STX \SOH(\SO2!.acyclic.machines.v1.ForkFidelityR\bfidelity\DC2=\n\
      \\bchildren\CAN\ETX \ETX(\v2!.acyclic.machines.v1.MachineStateR\bchildren"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'source")) ::
              Data.ProtoLens.FieldDescriptor ForkedLiveMachines
        fidelity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fidelity"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ForkFidelity)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"fidelity")) ::
              Data.ProtoLens.FieldDescriptor ForkedLiveMachines
        children__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "children"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineState)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"children")) ::
              Data.ProtoLens.FieldDescriptor ForkedLiveMachines
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, fidelity__field_descriptor),
           (Data.ProtoLens.Tag 3, children__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkedLiveMachines'_unknownFields
        (\ x__ y__ -> x__ {_ForkedLiveMachines'_unknownFields = y__})
  defMessage
    = ForkedLiveMachines'_constructor
        {_ForkedLiveMachines'source = Prelude.Nothing,
         _ForkedLiveMachines'fidelity = Data.ProtoLens.fieldDefault,
         _ForkedLiveMachines'children = Data.Vector.Generic.empty,
         _ForkedLiveMachines'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkedLiveMachines
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld MachineState
             -> Data.ProtoLens.Encoding.Bytes.Parser ForkedLiveMachines
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
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "source"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                                  mutable'children
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "fidelity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"fidelity") y x)
                                  mutable'children
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "children"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'children y)
                                loop x v
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
          "ForkedLiveMachines"
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"fidelity") _x
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
                         (Data.ProtoLens.Field.field @"vec'children") _x))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ForkedLiveMachines where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkedLiveMachines'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ForkedLiveMachines'source x__)
                (Control.DeepSeq.deepseq
                   (_ForkedLiveMachines'fidelity x__)
                   (Control.DeepSeq.deepseq (_ForkedLiveMachines'children x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machines' @:: Lens' ForkedMachines [MachineState]@
         * 'Proto.Machines.V1.Machines_Fields.vec'machines' @:: Lens' ForkedMachines (Data.Vector.Vector MachineState)@ -}
data ForkedMachines
  = ForkedMachines'_constructor {_ForkedMachines'machines :: !(Data.Vector.Vector MachineState),
                                 _ForkedMachines'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ForkedMachines where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ForkedMachines "machines" [MachineState] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedMachines'machines
           (\ x__ y__ -> x__ {_ForkedMachines'machines = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ForkedMachines "vec'machines" (Data.Vector.Vector MachineState) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ForkedMachines'machines
           (\ x__ y__ -> x__ {_ForkedMachines'machines = y__}))
        Prelude.id
instance Data.ProtoLens.Message ForkedMachines where
  messageName _ = Data.Text.pack "acyclic.machines.v1.ForkedMachines"
  packedMessageDescriptor _
    = "\n\
      \\SOForkedMachines\DC2=\n\
      \\bmachines\CAN\SOH \ETX(\v2!.acyclic.machines.v1.MachineStateR\bmachines"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machines__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machines"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineState)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"machines")) ::
              Data.ProtoLens.FieldDescriptor ForkedMachines
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machines__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ForkedMachines'_unknownFields
        (\ x__ y__ -> x__ {_ForkedMachines'_unknownFields = y__})
  defMessage
    = ForkedMachines'_constructor
        {_ForkedMachines'machines = Data.Vector.Generic.empty,
         _ForkedMachines'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ForkedMachines
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld MachineState
             -> Data.ProtoLens.Encoding.Bytes.Parser ForkedMachines
        loop x mutable'machines
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'machines <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'machines)
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
                              (Data.ProtoLens.Field.field @"vec'machines") frozen'machines x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "machines"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'machines y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'machines
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'machines <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'machines)
          "ForkedMachines"
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
                   (Data.ProtoLens.Field.field @"vec'machines") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ForkedMachines where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ForkedMachines'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ForkedMachines'machines x__) ())
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.value' @:: Lens' IdempotencyKey Data.ByteString.ByteString@ -}
data IdempotencyKey
  = IdempotencyKey'_constructor {_IdempotencyKey'value :: !Data.ByteString.ByteString,
                                 _IdempotencyKey'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show IdempotencyKey where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField IdempotencyKey "value" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdempotencyKey'value
           (\ x__ y__ -> x__ {_IdempotencyKey'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message IdempotencyKey where
  messageName _ = Data.Text.pack "acyclic.machines.v1.IdempotencyKey"
  packedMessageDescriptor _
    = "\n\
      \\SOIdempotencyKey\DC2\DC4\n\
      \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor IdempotencyKey
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _IdempotencyKey'_unknownFields
        (\ x__ y__ -> x__ {_IdempotencyKey'_unknownFields = y__})
  defMessage
    = IdempotencyKey'_constructor
        {_IdempotencyKey'value = Data.ProtoLens.fieldDefault,
         _IdempotencyKey'_unknownFields = []}
  parseMessage
    = let
        loop ::
          IdempotencyKey
          -> Data.ProtoLens.Encoding.Bytes.Parser IdempotencyKey
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
          (do loop Data.ProtoLens.defMessage) "IdempotencyKey"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"value") _x
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
instance Control.DeepSeq.NFData IdempotencyKey where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_IdempotencyKey'_unknownFields x__)
             (Control.DeepSeq.deepseq (_IdempotencyKey'value x__) ())
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.kind' @:: Lens' Image ImageKind@
         * 'Proto.Machines.V1.Machines_Fields.maybe'immutableReference' @:: Lens' Image (Prelude.Maybe Image'ImmutableReference)@
         * 'Proto.Machines.V1.Machines_Fields.maybe'managedDigest' @:: Lens' Image (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Machines.V1.Machines_Fields.managedDigest' @:: Lens' Image Data.ByteString.ByteString@
         * 'Proto.Machines.V1.Machines_Fields.maybe'customDigest' @:: Lens' Image (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Machines.V1.Machines_Fields.customDigest' @:: Lens' Image Data.ByteString.ByteString@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' Image (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' Image CheckpointId@ -}
data Image
  = Image'_constructor {_Image'kind :: !ImageKind,
                        _Image'immutableReference :: !(Prelude.Maybe Image'ImmutableReference),
                        _Image'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Image where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data Image'ImmutableReference
  = Image'ManagedDigest !Data.ByteString.ByteString |
    Image'CustomDigest !Data.ByteString.ByteString |
    Image'Checkpoint !CheckpointId
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField Image "kind" ImageKind where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'kind (\ x__ y__ -> x__ {_Image'kind = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Image "maybe'immutableReference" (Prelude.Maybe Image'ImmutableReference) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Image "maybe'managedDigest" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Image'ManagedDigest x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Image'ManagedDigest y__))
instance Data.ProtoLens.Field.HasField Image "managedDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Image'ManagedDigest x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Image'ManagedDigest y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField Image "maybe'customDigest" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Image'CustomDigest x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Image'CustomDigest y__))
instance Data.ProtoLens.Field.HasField Image "customDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Image'CustomDigest x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Image'CustomDigest y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField Image "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Image'Checkpoint x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Image'Checkpoint y__))
instance Data.ProtoLens.Field.HasField Image "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Image'immutableReference
           (\ x__ y__ -> x__ {_Image'immutableReference = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Image'Checkpoint x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Image'Checkpoint y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message Image where
  messageName _ = Data.Text.pack "acyclic.machines.v1.Image"
  packedMessageDescriptor _
    = "\n\
      \\ENQImage\DC22\n\
      \\EOTkind\CAN\SOH \SOH(\SO2\RS.acyclic.machines.v1.ImageKindR\EOTkind\DC2'\n\
      \\SOmanaged_digest\CAN\STX \SOH(\fH\NULR\rmanagedDigest\DC2%\n\
      \\rcustom_digest\CAN\ETX \SOH(\fH\NULR\fcustomDigest\DC2C\n\
      \\n\
      \checkpoint\CAN\EOT \SOH(\v2!.acyclic.machines.v1.CheckpointIdH\NULR\n\
      \checkpointB\NAK\n\
      \\DC3immutable_reference"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        kind__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "kind"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ImageKind)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"kind")) ::
              Data.ProtoLens.FieldDescriptor Image
        managedDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "managed_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'managedDigest")) ::
              Data.ProtoLens.FieldDescriptor Image
        customDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "custom_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'customDigest")) ::
              Data.ProtoLens.FieldDescriptor Image
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor Image
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, kind__field_descriptor),
           (Data.ProtoLens.Tag 2, managedDigest__field_descriptor),
           (Data.ProtoLens.Tag 3, customDigest__field_descriptor),
           (Data.ProtoLens.Tag 4, checkpoint__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Image'_unknownFields
        (\ x__ y__ -> x__ {_Image'_unknownFields = y__})
  defMessage
    = Image'_constructor
        {_Image'kind = Data.ProtoLens.fieldDefault,
         _Image'immutableReference = Prelude.Nothing,
         _Image'_unknownFields = []}
  parseMessage
    = let
        loop :: Image -> Data.ProtoLens.Encoding.Bytes.Parser Image
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
                                       "kind"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"kind") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "managed_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"managedDigest") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "custom_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"customDigest") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Image"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"kind") _x
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
                (case
                     Lens.Family2.view
                       (Data.ProtoLens.Field.field @"maybe'immutableReference") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just (Image'ManagedDigest v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                          ((\ bs
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                      (Prelude.fromIntegral (Data.ByteString.length bs)))
                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             v)
                   (Prelude.Just (Image'CustomDigest v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                          ((\ bs
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                      (Prelude.fromIntegral (Data.ByteString.length bs)))
                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             v)
                   (Prelude.Just (Image'Checkpoint v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData Image where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Image'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Image'kind x__)
                (Control.DeepSeq.deepseq (_Image'immutableReference x__) ()))
instance Control.DeepSeq.NFData Image'ImmutableReference where
  rnf (Image'ManagedDigest x__) = Control.DeepSeq.rnf x__
  rnf (Image'CustomDigest x__) = Control.DeepSeq.rnf x__
  rnf (Image'Checkpoint x__) = Control.DeepSeq.rnf x__
_Image'ManagedDigest ::
  Data.ProtoLens.Prism.Prism' Image'ImmutableReference Data.ByteString.ByteString
_Image'ManagedDigest
  = Data.ProtoLens.Prism.prism'
      Image'ManagedDigest
      (\ p__
         -> case p__ of
              (Image'ManagedDigest p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Image'CustomDigest ::
  Data.ProtoLens.Prism.Prism' Image'ImmutableReference Data.ByteString.ByteString
_Image'CustomDigest
  = Data.ProtoLens.Prism.prism'
      Image'CustomDigest
      (\ p__
         -> case p__ of
              (Image'CustomDigest p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Image'Checkpoint ::
  Data.ProtoLens.Prism.Prism' Image'ImmutableReference CheckpointId
_Image'Checkpoint
  = Data.ProtoLens.Prism.prism'
      Image'Checkpoint
      (\ p__
         -> case p__ of
              (Image'Checkpoint p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
newtype ImageKind'UnrecognizedValue
  = ImageKind'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ImageKind
  = IMAGE_KIND_UNSPECIFIED |
    IMAGE_KIND_MANAGED_OCI |
    IMAGE_KIND_CUSTOM |
    IMAGE_KIND_CHECKPOINT |
    ImageKind'Unrecognized !ImageKind'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ImageKind where
  maybeToEnum 0 = Prelude.Just IMAGE_KIND_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just IMAGE_KIND_MANAGED_OCI
  maybeToEnum 2 = Prelude.Just IMAGE_KIND_CUSTOM
  maybeToEnum 3 = Prelude.Just IMAGE_KIND_CHECKPOINT
  maybeToEnum k
    = Prelude.Just
        (ImageKind'Unrecognized
           (ImageKind'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum IMAGE_KIND_UNSPECIFIED = "IMAGE_KIND_UNSPECIFIED"
  showEnum IMAGE_KIND_MANAGED_OCI = "IMAGE_KIND_MANAGED_OCI"
  showEnum IMAGE_KIND_CUSTOM = "IMAGE_KIND_CUSTOM"
  showEnum IMAGE_KIND_CHECKPOINT = "IMAGE_KIND_CHECKPOINT"
  showEnum (ImageKind'Unrecognized (ImageKind'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "IMAGE_KIND_UNSPECIFIED"
    = Prelude.Just IMAGE_KIND_UNSPECIFIED
    | (Prelude.==) k "IMAGE_KIND_MANAGED_OCI"
    = Prelude.Just IMAGE_KIND_MANAGED_OCI
    | (Prelude.==) k "IMAGE_KIND_CUSTOM"
    = Prelude.Just IMAGE_KIND_CUSTOM
    | (Prelude.==) k "IMAGE_KIND_CHECKPOINT"
    = Prelude.Just IMAGE_KIND_CHECKPOINT
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ImageKind where
  minBound = IMAGE_KIND_UNSPECIFIED
  maxBound = IMAGE_KIND_CHECKPOINT
instance Prelude.Enum ImageKind where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ImageKind: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum IMAGE_KIND_UNSPECIFIED = 0
  fromEnum IMAGE_KIND_MANAGED_OCI = 1
  fromEnum IMAGE_KIND_CUSTOM = 2
  fromEnum IMAGE_KIND_CHECKPOINT = 3
  fromEnum (ImageKind'Unrecognized (ImageKind'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ IMAGE_KIND_CHECKPOINT
    = Prelude.error
        "ImageKind.succ: bad argument IMAGE_KIND_CHECKPOINT. This value would be out of bounds."
  succ IMAGE_KIND_UNSPECIFIED = IMAGE_KIND_MANAGED_OCI
  succ IMAGE_KIND_MANAGED_OCI = IMAGE_KIND_CUSTOM
  succ IMAGE_KIND_CUSTOM = IMAGE_KIND_CHECKPOINT
  succ (ImageKind'Unrecognized _)
    = Prelude.error "ImageKind.succ: bad argument: unrecognized value"
  pred IMAGE_KIND_UNSPECIFIED
    = Prelude.error
        "ImageKind.pred: bad argument IMAGE_KIND_UNSPECIFIED. This value would be out of bounds."
  pred IMAGE_KIND_MANAGED_OCI = IMAGE_KIND_UNSPECIFIED
  pred IMAGE_KIND_CUSTOM = IMAGE_KIND_MANAGED_OCI
  pred IMAGE_KIND_CHECKPOINT = IMAGE_KIND_CUSTOM
  pred (ImageKind'Unrecognized _)
    = Prelude.error "ImageKind.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ImageKind where
  fieldDefault = IMAGE_KIND_UNSPECIFIED
instance Control.DeepSeq.NFData ImageKind where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.image' @:: Lens' ImageQualification Image@
         * 'Proto.Machines.V1.Machines_Fields.maybe'image' @:: Lens' ImageQualification (Prelude.Maybe Image)@
         * 'Proto.Machines.V1.Machines_Fields.capabilities' @:: Lens' ImageQualification [Capability]@
         * 'Proto.Machines.V1.Machines_Fields.vec'capabilities' @:: Lens' ImageQualification (Data.Vector.Vector Capability)@
         * 'Proto.Machines.V1.Machines_Fields.compatibilityRevision' @:: Lens' ImageQualification Data.ByteString.ByteString@ -}
data ImageQualification
  = ImageQualification'_constructor {_ImageQualification'image :: !(Prelude.Maybe Image),
                                     _ImageQualification'capabilities :: !(Data.Vector.Vector Capability),
                                     _ImageQualification'compatibilityRevision :: !Data.ByteString.ByteString,
                                     _ImageQualification'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ImageQualification where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ImageQualification "image" Image where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ImageQualification'image
           (\ x__ y__ -> x__ {_ImageQualification'image = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ImageQualification "maybe'image" (Prelude.Maybe Image) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ImageQualification'image
           (\ x__ y__ -> x__ {_ImageQualification'image = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ImageQualification "capabilities" [Capability] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ImageQualification'capabilities
           (\ x__ y__ -> x__ {_ImageQualification'capabilities = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ImageQualification "vec'capabilities" (Data.Vector.Vector Capability) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ImageQualification'capabilities
           (\ x__ y__ -> x__ {_ImageQualification'capabilities = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ImageQualification "compatibilityRevision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ImageQualification'compatibilityRevision
           (\ x__ y__
              -> x__ {_ImageQualification'compatibilityRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Message ImageQualification where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ImageQualification"
  packedMessageDescriptor _
    = "\n\
      \\DC2ImageQualification\DC20\n\
      \\ENQimage\CAN\SOH \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\DC2C\n\
      \\fcapabilities\CAN\STX \ETX(\SO2\US.acyclic.machines.v1.CapabilityR\fcapabilities\DC25\n\
      \\SYNcompatibility_revision\CAN\ETX \SOH(\fR\NAKcompatibilityRevision"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        image__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "image"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Image)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'image")) ::
              Data.ProtoLens.FieldDescriptor ImageQualification
        capabilities__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "capabilities"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor Capability)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Packed
                 (Data.ProtoLens.Field.field @"capabilities")) ::
              Data.ProtoLens.FieldDescriptor ImageQualification
        compatibilityRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "compatibility_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"compatibilityRevision")) ::
              Data.ProtoLens.FieldDescriptor ImageQualification
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, image__field_descriptor),
           (Data.ProtoLens.Tag 2, capabilities__field_descriptor),
           (Data.ProtoLens.Tag 3, compatibilityRevision__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ImageQualification'_unknownFields
        (\ x__ y__ -> x__ {_ImageQualification'_unknownFields = y__})
  defMessage
    = ImageQualification'_constructor
        {_ImageQualification'image = Prelude.Nothing,
         _ImageQualification'capabilities = Data.Vector.Generic.empty,
         _ImageQualification'compatibilityRevision = Data.ProtoLens.fieldDefault,
         _ImageQualification'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ImageQualification
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Capability
             -> Data.ProtoLens.Encoding.Bytes.Parser ImageQualification
        loop x mutable'capabilities
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'capabilities <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                               (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                  mutable'capabilities)
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
                              (Data.ProtoLens.Field.field @"vec'capabilities")
                              frozen'capabilities x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "image"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"image") y x)
                                  mutable'capabilities
                        16
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (Prelude.fmap
                                           Prelude.toEnum
                                           (Prelude.fmap
                                              Prelude.fromIntegral
                                              Data.ProtoLens.Encoding.Bytes.getVarInt))
                                        "capabilities"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'capabilities y)
                                loop x v
                        18
                          -> do y <- do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                        Data.ProtoLens.Encoding.Bytes.isolate
                                          (Prelude.fromIntegral len)
                                          ((let
                                              ploop qs
                                                = do packedEnd <- Data.ProtoLens.Encoding.Bytes.atEnd
                                                     if packedEnd then
                                                         Prelude.return qs
                                                     else
                                                         do !q <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                                                    (Prelude.fmap
                                                                       Prelude.toEnum
                                                                       (Prelude.fmap
                                                                          Prelude.fromIntegral
                                                                          Data.ProtoLens.Encoding.Bytes.getVarInt))
                                                                    "capabilities"
                                                            qs' <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                                     (Data.ProtoLens.Encoding.Growing.append
                                                                        qs q)
                                                            ploop qs'
                                            in ploop)
                                             mutable'capabilities)
                                loop x y
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "compatibility_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"compatibilityRevision") y x)
                                  mutable'capabilities
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'capabilities
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'capabilities <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'capabilities)
          "ImageQualification"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'image") _x
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
                   p = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"vec'capabilities") _x
                 in
                   if Data.Vector.Generic.null p then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            (Data.ProtoLens.Encoding.Bytes.runBuilder
                               (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                                  ((Prelude..)
                                     ((Prelude..)
                                        Data.ProtoLens.Encoding.Bytes.putVarInt
                                        Prelude.fromIntegral)
                                     Prelude.fromEnum)
                                  p))))
                ((Data.Monoid.<>)
                   (let
                      _v
                        = Lens.Family2.view
                            (Data.ProtoLens.Field.field @"compatibilityRevision") _x
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
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData ImageQualification where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ImageQualification'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ImageQualification'image x__)
                (Control.DeepSeq.deepseq
                   (_ImageQualification'capabilities x__)
                   (Control.DeepSeq.deepseq
                      (_ImageQualification'compatibilityRevision x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' InspectCheckpointRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' InspectCheckpointRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' InspectCheckpointRequest CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' InspectCheckpointRequest (Prelude.Maybe CheckpointId)@ -}
data InspectCheckpointRequest
  = InspectCheckpointRequest'_constructor {_InspectCheckpointRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                           _InspectCheckpointRequest'checkpoint :: !(Prelude.Maybe CheckpointId),
                                           _InspectCheckpointRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectCheckpointRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectCheckpointRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectCheckpointRequest'protocol
           (\ x__ y__ -> x__ {_InspectCheckpointRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectCheckpointRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectCheckpointRequest'protocol
           (\ x__ y__ -> x__ {_InspectCheckpointRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InspectCheckpointRequest "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectCheckpointRequest'checkpoint
           (\ x__ y__ -> x__ {_InspectCheckpointRequest'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectCheckpointRequest "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectCheckpointRequest'checkpoint
           (\ x__ y__ -> x__ {_InspectCheckpointRequest'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectCheckpointRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.InspectCheckpointRequest"
  packedMessageDescriptor _
    = "\n\
      \\CANInspectCheckpointRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2A\n\
      \\n\
      \checkpoint\CAN\STX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor InspectCheckpointRequest
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor InspectCheckpointRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, checkpoint__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectCheckpointRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectCheckpointRequest'_unknownFields = y__})
  defMessage
    = InspectCheckpointRequest'_constructor
        {_InspectCheckpointRequest'protocol = Prelude.Nothing,
         _InspectCheckpointRequest'checkpoint = Prelude.Nothing,
         _InspectCheckpointRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectCheckpointRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectCheckpointRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectCheckpointRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
instance Control.DeepSeq.NFData InspectCheckpointRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectCheckpointRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InspectCheckpointRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_InspectCheckpointRequest'checkpoint x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' InspectMachineRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' InspectMachineRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' InspectMachineRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' InspectMachineRequest (Prelude.Maybe MachineId)@ -}
data InspectMachineRequest
  = InspectMachineRequest'_constructor {_InspectMachineRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                        _InspectMachineRequest'machine :: !(Prelude.Maybe MachineId),
                                        _InspectMachineRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectMachineRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectMachineRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectMachineRequest'protocol
           (\ x__ y__ -> x__ {_InspectMachineRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectMachineRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectMachineRequest'protocol
           (\ x__ y__ -> x__ {_InspectMachineRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField InspectMachineRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectMachineRequest'machine
           (\ x__ y__ -> x__ {_InspectMachineRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField InspectMachineRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectMachineRequest'machine
           (\ x__ y__ -> x__ {_InspectMachineRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectMachineRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.InspectMachineRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKInspectMachineRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC28\n\
      \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor InspectMachineRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor InspectMachineRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, machine__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectMachineRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectMachineRequest'_unknownFields = y__})
  defMessage
    = InspectMachineRequest'_constructor
        {_InspectMachineRequest'protocol = Prelude.Nothing,
         _InspectMachineRequest'machine = Prelude.Nothing,
         _InspectMachineRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectMachineRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectMachineRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectMachineRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
instance Control.DeepSeq.NFData InspectMachineRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectMachineRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InspectMachineRequest'protocol x__)
                (Control.DeepSeq.deepseq (_InspectMachineRequest'machine x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' ListMachinesRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' ListMachinesRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.after' @:: Lens' ListMachinesRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'after' @:: Lens' ListMachinesRequest (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.limit' @:: Lens' ListMachinesRequest Data.Word.Word32@ -}
data ListMachinesRequest
  = ListMachinesRequest'_constructor {_ListMachinesRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                      _ListMachinesRequest'after :: !(Prelude.Maybe MachineId),
                                      _ListMachinesRequest'limit :: !Data.Word.Word32,
                                      _ListMachinesRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListMachinesRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListMachinesRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListMachinesRequest'protocol
           (\ x__ y__ -> x__ {_ListMachinesRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListMachinesRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListMachinesRequest'protocol
           (\ x__ y__ -> x__ {_ListMachinesRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListMachinesRequest "after" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListMachinesRequest'after
           (\ x__ y__ -> x__ {_ListMachinesRequest'after = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ListMachinesRequest "maybe'after" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListMachinesRequest'after
           (\ x__ y__ -> x__ {_ListMachinesRequest'after = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ListMachinesRequest "limit" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListMachinesRequest'limit
           (\ x__ y__ -> x__ {_ListMachinesRequest'limit = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListMachinesRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ListMachinesRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3ListMachinesRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC24\n\
      \\ENQafter\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ENQafter\DC2\DC4\n\
      \\ENQlimit\CAN\ETX \SOH(\rR\ENQlimit"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor ListMachinesRequest
        after__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "after"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'after")) ::
              Data.ProtoLens.FieldDescriptor ListMachinesRequest
        limit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "limit"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"limit")) ::
              Data.ProtoLens.FieldDescriptor ListMachinesRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, after__field_descriptor),
           (Data.ProtoLens.Tag 3, limit__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListMachinesRequest'_unknownFields
        (\ x__ y__ -> x__ {_ListMachinesRequest'_unknownFields = y__})
  defMessage
    = ListMachinesRequest'_constructor
        {_ListMachinesRequest'protocol = Prelude.Nothing,
         _ListMachinesRequest'after = Prelude.Nothing,
         _ListMachinesRequest'limit = Data.ProtoLens.fieldDefault,
         _ListMachinesRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListMachinesRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ListMachinesRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "after"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"after") y x)
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
          (do loop Data.ProtoLens.defMessage) "ListMachinesRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                             Data.ProtoLens.encodeMessage _v))
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
instance Control.DeepSeq.NFData ListMachinesRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListMachinesRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ListMachinesRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_ListMachinesRequest'after x__)
                   (Control.DeepSeq.deepseq (_ListMachinesRequest'limit x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' MachineAdmission MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' MachineAdmission (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' MachineAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' MachineAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.contract' @:: Lens' MachineAdmission MachineContract@
         * 'Proto.Machines.V1.Machines_Fields.maybe'contract' @:: Lens' MachineAdmission (Prelude.Maybe MachineContract)@ -}
data MachineAdmission
  = MachineAdmission'_constructor {_MachineAdmission'machine :: !(Prelude.Maybe MachineId),
                                   _MachineAdmission'operation :: !(Prelude.Maybe OperationId),
                                   _MachineAdmission'contract :: !(Prelude.Maybe MachineContract),
                                   _MachineAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachineAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachineAdmission "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineAdmission'machine
           (\ x__ y__ -> x__ {_MachineAdmission'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineAdmission "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineAdmission'machine
           (\ x__ y__ -> x__ {_MachineAdmission'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineAdmission'operation
           (\ x__ y__ -> x__ {_MachineAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineAdmission'operation
           (\ x__ y__ -> x__ {_MachineAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineAdmission "contract" MachineContract where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineAdmission'contract
           (\ x__ y__ -> x__ {_MachineAdmission'contract = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineAdmission "maybe'contract" (Prelude.Maybe MachineContract) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineAdmission'contract
           (\ x__ y__ -> x__ {_MachineAdmission'contract = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachineAdmission where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.MachineAdmission"
  packedMessageDescriptor _
    = "\n\
      \\DLEMachineAdmission\DC28\n\
      \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2>\n\
      \\toperation\CAN\STX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
      \\bcontract\CAN\ETX \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor MachineAdmission
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor MachineAdmission
        contract__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "contract"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineContract)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contract")) ::
              Data.ProtoLens.FieldDescriptor MachineAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machine__field_descriptor),
           (Data.ProtoLens.Tag 2, operation__field_descriptor),
           (Data.ProtoLens.Tag 3, contract__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachineAdmission'_unknownFields
        (\ x__ y__ -> x__ {_MachineAdmission'_unknownFields = y__})
  defMessage
    = MachineAdmission'_constructor
        {_MachineAdmission'machine = Prelude.Nothing,
         _MachineAdmission'operation = Prelude.Nothing,
         _MachineAdmission'contract = Prelude.Nothing,
         _MachineAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MachineAdmission
          -> Data.ProtoLens.Encoding.Bytes.Parser MachineAdmission
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
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "contract"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contract") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MachineAdmission"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                       (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'contract") _x
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
instance Control.DeepSeq.NFData MachineAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachineAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MachineAdmission'machine x__)
                (Control.DeepSeq.deepseq
                   (_MachineAdmission'operation x__)
                   (Control.DeepSeq.deepseq (_MachineAdmission'contract x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.image' @:: Lens' MachineContract Image@
         * 'Proto.Machines.V1.Machines_Fields.maybe'image' @:: Lens' MachineContract (Prelude.Maybe Image)@
         * 'Proto.Machines.V1.Machines_Fields.capabilities' @:: Lens' MachineContract [Capability]@
         * 'Proto.Machines.V1.Machines_Fields.vec'capabilities' @:: Lens' MachineContract (Data.Vector.Vector Capability)@
         * 'Proto.Machines.V1.Machines_Fields.compatibility' @:: Lens' MachineContract CompatibilityPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'compatibility' @:: Lens' MachineContract (Prelude.Maybe CompatibilityPolicy)@
         * 'Proto.Machines.V1.Machines_Fields.compatibilityRevision' @:: Lens' MachineContract Data.ByteString.ByteString@
         * 'Proto.Machines.V1.Machines_Fields.suspension' @:: Lens' MachineContract SuspensionPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'suspension' @:: Lens' MachineContract (Prelude.Maybe SuspensionPolicy)@
         * 'Proto.Machines.V1.Machines_Fields.expiration' @:: Lens' MachineContract ExpirationPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'expiration' @:: Lens' MachineContract (Prelude.Maybe ExpirationPolicy)@
         * 'Proto.Machines.V1.Machines_Fields.networkPolicyDigest' @:: Lens' MachineContract Data.ByteString.ByteString@
         * 'Proto.Machines.V1.Machines_Fields.budgets' @:: Lens' MachineContract Budgets@
         * 'Proto.Machines.V1.Machines_Fields.maybe'budgets' @:: Lens' MachineContract (Prelude.Maybe Budgets)@ -}
data MachineContract
  = MachineContract'_constructor {_MachineContract'image :: !(Prelude.Maybe Image),
                                  _MachineContract'capabilities :: !(Data.Vector.Vector Capability),
                                  _MachineContract'compatibility :: !(Prelude.Maybe CompatibilityPolicy),
                                  _MachineContract'compatibilityRevision :: !Data.ByteString.ByteString,
                                  _MachineContract'suspension :: !(Prelude.Maybe SuspensionPolicy),
                                  _MachineContract'expiration :: !(Prelude.Maybe ExpirationPolicy),
                                  _MachineContract'networkPolicyDigest :: !Data.ByteString.ByteString,
                                  _MachineContract'budgets :: !(Prelude.Maybe Budgets),
                                  _MachineContract'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachineContract where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachineContract "image" Image where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'image
           (\ x__ y__ -> x__ {_MachineContract'image = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineContract "maybe'image" (Prelude.Maybe Image) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'image
           (\ x__ y__ -> x__ {_MachineContract'image = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "capabilities" [Capability] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'capabilities
           (\ x__ y__ -> x__ {_MachineContract'capabilities = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField MachineContract "vec'capabilities" (Data.Vector.Vector Capability) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'capabilities
           (\ x__ y__ -> x__ {_MachineContract'capabilities = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "compatibility" CompatibilityPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'compatibility
           (\ x__ y__ -> x__ {_MachineContract'compatibility = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineContract "maybe'compatibility" (Prelude.Maybe CompatibilityPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'compatibility
           (\ x__ y__ -> x__ {_MachineContract'compatibility = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "compatibilityRevision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'compatibilityRevision
           (\ x__ y__ -> x__ {_MachineContract'compatibilityRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "suspension" SuspensionPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'suspension
           (\ x__ y__ -> x__ {_MachineContract'suspension = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineContract "maybe'suspension" (Prelude.Maybe SuspensionPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'suspension
           (\ x__ y__ -> x__ {_MachineContract'suspension = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "expiration" ExpirationPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'expiration
           (\ x__ y__ -> x__ {_MachineContract'expiration = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineContract "maybe'expiration" (Prelude.Maybe ExpirationPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'expiration
           (\ x__ y__ -> x__ {_MachineContract'expiration = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "networkPolicyDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'networkPolicyDigest
           (\ x__ y__ -> x__ {_MachineContract'networkPolicyDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineContract "budgets" Budgets where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'budgets
           (\ x__ y__ -> x__ {_MachineContract'budgets = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineContract "maybe'budgets" (Prelude.Maybe Budgets) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineContract'budgets
           (\ x__ y__ -> x__ {_MachineContract'budgets = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachineContract where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.MachineContract"
  packedMessageDescriptor _
    = "\n\
      \\SIMachineContract\DC20\n\
      \\ENQimage\CAN\SOH \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\DC2C\n\
      \\fcapabilities\CAN\STX \ETX(\SO2\US.acyclic.machines.v1.CapabilityR\fcapabilities\DC2N\n\
      \\rcompatibility\CAN\ETX \SOH(\v2(.acyclic.machines.v1.CompatibilityPolicyR\rcompatibility\DC25\n\
      \\SYNcompatibility_revision\CAN\EOT \SOH(\fR\NAKcompatibilityRevision\DC2E\n\
      \\n\
      \suspension\CAN\ACK \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\n\
      \suspension\DC2E\n\
      \\n\
      \expiration\CAN\a \SOH(\v2%.acyclic.machines.v1.ExpirationPolicyR\n\
      \expiration\DC22\n\
      \\NAKnetwork_policy_digest\CAN\b \SOH(\fR\DC3networkPolicyDigest\DC26\n\
      \\abudgets\CAN\t \SOH(\v2\FS.acyclic.machines.v1.BudgetsR\abudgetsJ\EOT\b\ENQ\DLE\ACKR\vperformance"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        image__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "image"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Image)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'image")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        capabilities__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "capabilities"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor Capability)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Packed
                 (Data.ProtoLens.Field.field @"capabilities")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        compatibility__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "compatibility"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CompatibilityPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'compatibility")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        compatibilityRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "compatibility_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"compatibilityRevision")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        suspension__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suspension"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SuspensionPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suspension")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        expiration__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expiration"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ExpirationPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'expiration")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        networkPolicyDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "network_policy_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"networkPolicyDigest")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
        budgets__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "budgets"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Budgets)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'budgets")) ::
              Data.ProtoLens.FieldDescriptor MachineContract
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, image__field_descriptor),
           (Data.ProtoLens.Tag 2, capabilities__field_descriptor),
           (Data.ProtoLens.Tag 3, compatibility__field_descriptor),
           (Data.ProtoLens.Tag 4, compatibilityRevision__field_descriptor),
           (Data.ProtoLens.Tag 6, suspension__field_descriptor),
           (Data.ProtoLens.Tag 7, expiration__field_descriptor),
           (Data.ProtoLens.Tag 8, networkPolicyDigest__field_descriptor),
           (Data.ProtoLens.Tag 9, budgets__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachineContract'_unknownFields
        (\ x__ y__ -> x__ {_MachineContract'_unknownFields = y__})
  defMessage
    = MachineContract'_constructor
        {_MachineContract'image = Prelude.Nothing,
         _MachineContract'capabilities = Data.Vector.Generic.empty,
         _MachineContract'compatibility = Prelude.Nothing,
         _MachineContract'compatibilityRevision = Data.ProtoLens.fieldDefault,
         _MachineContract'suspension = Prelude.Nothing,
         _MachineContract'expiration = Prelude.Nothing,
         _MachineContract'networkPolicyDigest = Data.ProtoLens.fieldDefault,
         _MachineContract'budgets = Prelude.Nothing,
         _MachineContract'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MachineContract
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Capability
             -> Data.ProtoLens.Encoding.Bytes.Parser MachineContract
        loop x mutable'capabilities
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'capabilities <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                               (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                  mutable'capabilities)
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
                              (Data.ProtoLens.Field.field @"vec'capabilities")
                              frozen'capabilities x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "image"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"image") y x)
                                  mutable'capabilities
                        16
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (Prelude.fmap
                                           Prelude.toEnum
                                           (Prelude.fmap
                                              Prelude.fromIntegral
                                              Data.ProtoLens.Encoding.Bytes.getVarInt))
                                        "capabilities"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'capabilities y)
                                loop x v
                        18
                          -> do y <- do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                        Data.ProtoLens.Encoding.Bytes.isolate
                                          (Prelude.fromIntegral len)
                                          ((let
                                              ploop qs
                                                = do packedEnd <- Data.ProtoLens.Encoding.Bytes.atEnd
                                                     if packedEnd then
                                                         Prelude.return qs
                                                     else
                                                         do !q <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                                                    (Prelude.fmap
                                                                       Prelude.toEnum
                                                                       (Prelude.fmap
                                                                          Prelude.fromIntegral
                                                                          Data.ProtoLens.Encoding.Bytes.getVarInt))
                                                                    "capabilities"
                                                            qs' <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                                     (Data.ProtoLens.Encoding.Growing.append
                                                                        qs q)
                                                            ploop qs'
                                            in ploop)
                                             mutable'capabilities)
                                loop x y
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "compatibility"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"compatibility") y x)
                                  mutable'capabilities
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "compatibility_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"compatibilityRevision") y x)
                                  mutable'capabilities
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "suspension"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"suspension") y x)
                                  mutable'capabilities
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "expiration"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expiration") y x)
                                  mutable'capabilities
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "network_policy_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"networkPolicyDigest") y x)
                                  mutable'capabilities
                        74
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "budgets"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"budgets") y x)
                                  mutable'capabilities
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'capabilities
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'capabilities <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'capabilities)
          "MachineContract"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'image") _x
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
                   p = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"vec'capabilities") _x
                 in
                   if Data.Vector.Generic.null p then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                         ((\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                            (Data.ProtoLens.Encoding.Bytes.runBuilder
                               (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                                  ((Prelude..)
                                     ((Prelude..)
                                        Data.ProtoLens.Encoding.Bytes.putVarInt
                                        Prelude.fromIntegral)
                                     Prelude.fromEnum)
                                  p))))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'compatibility") _x
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
                               (Data.ProtoLens.Field.field @"compatibilityRevision") _x
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
                                (Data.ProtoLens.Field.field @"maybe'suspension") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
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
                                   (Data.ProtoLens.Field.field @"maybe'expiration") _x
                             of
                               Prelude.Nothing -> Data.Monoid.mempty
                               (Prelude.Just _v)
                                 -> (Data.Monoid.<>)
                                      (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                                      ((Prelude..)
                                         (\ bs
                                            -> (Data.Monoid.<>)
                                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                    (Prelude.fromIntegral
                                                       (Data.ByteString.length bs)))
                                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                         Data.ProtoLens.encodeMessage _v))
                            ((Data.Monoid.<>)
                               (let
                                  _v
                                    = Lens.Family2.view
                                        (Data.ProtoLens.Field.field @"networkPolicyDigest") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 66)
                                        ((\ bs
                                            -> (Data.Monoid.<>)
                                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                    (Prelude.fromIntegral
                                                       (Data.ByteString.length bs)))
                                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                           _v))
                               ((Data.Monoid.<>)
                                  (case
                                       Lens.Family2.view
                                         (Data.ProtoLens.Field.field @"maybe'budgets") _x
                                   of
                                     Prelude.Nothing -> Data.Monoid.mempty
                                     (Prelude.Just _v)
                                       -> (Data.Monoid.<>)
                                            (Data.ProtoLens.Encoding.Bytes.putVarInt 74)
                                            ((Prelude..)
                                               (\ bs
                                                  -> (Data.Monoid.<>)
                                                       (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                          (Prelude.fromIntegral
                                                             (Data.ByteString.length bs)))
                                                       (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                               Data.ProtoLens.encodeMessage _v))
                                  (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                     (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))))
instance Control.DeepSeq.NFData MachineContract where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachineContract'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MachineContract'image x__)
                (Control.DeepSeq.deepseq
                   (_MachineContract'capabilities x__)
                   (Control.DeepSeq.deepseq
                      (_MachineContract'compatibility x__)
                      (Control.DeepSeq.deepseq
                         (_MachineContract'compatibilityRevision x__)
                         (Control.DeepSeq.deepseq
                            (_MachineContract'suspension x__)
                            (Control.DeepSeq.deepseq
                               (_MachineContract'expiration x__)
                               (Control.DeepSeq.deepseq
                                  (_MachineContract'networkPolicyDigest x__)
                                  (Control.DeepSeq.deepseq (_MachineContract'budgets x__) ()))))))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' MachineEvent MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' MachineEvent (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.sequence' @:: Lens' MachineEvent Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.observedAtUnixMs' @:: Lens' MachineEvent Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.kind' @:: Lens' MachineEvent EventKind@
         * 'Proto.Machines.V1.Machines_Fields.state' @:: Lens' MachineEvent MachineStatus@
         * 'Proto.Machines.V1.Machines_Fields.pressure' @:: Lens' MachineEvent PressureKind@ -}
data MachineEvent
  = MachineEvent'_constructor {_MachineEvent'machine :: !(Prelude.Maybe MachineId),
                               _MachineEvent'sequence :: !Data.Word.Word64,
                               _MachineEvent'observedAtUnixMs :: !Data.Word.Word64,
                               _MachineEvent'kind :: !EventKind,
                               _MachineEvent'state :: !MachineStatus,
                               _MachineEvent'pressure :: !PressureKind,
                               _MachineEvent'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachineEvent where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachineEvent "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'machine
           (\ x__ y__ -> x__ {_MachineEvent'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineEvent "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'machine
           (\ x__ y__ -> x__ {_MachineEvent'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineEvent "sequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'sequence
           (\ x__ y__ -> x__ {_MachineEvent'sequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineEvent "observedAtUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'observedAtUnixMs
           (\ x__ y__ -> x__ {_MachineEvent'observedAtUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineEvent "kind" EventKind where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'kind (\ x__ y__ -> x__ {_MachineEvent'kind = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineEvent "state" MachineStatus where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'state (\ x__ y__ -> x__ {_MachineEvent'state = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineEvent "pressure" PressureKind where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineEvent'pressure
           (\ x__ y__ -> x__ {_MachineEvent'pressure = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachineEvent where
  messageName _ = Data.Text.pack "acyclic.machines.v1.MachineEvent"
  packedMessageDescriptor _
    = "\n\
      \\fMachineEvent\DC28\n\
      \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\SUB\n\
      \\bsequence\CAN\STX \SOH(\EOTR\bsequence\DC2-\n\
      \\DC3observed_at_unix_ms\CAN\ETX \SOH(\EOTR\DLEobservedAtUnixMs\DC22\n\
      \\EOTkind\CAN\EOT \SOH(\SO2\RS.acyclic.machines.v1.EventKindR\EOTkind\DC28\n\
      \\ENQstate\CAN\ENQ \SOH(\SO2\".acyclic.machines.v1.MachineStatusR\ENQstate\DC2=\n\
      \\bpressure\CAN\ACK \SOH(\SO2!.acyclic.machines.v1.PressureKindR\bpressure"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor MachineEvent
        sequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sequence")) ::
              Data.ProtoLens.FieldDescriptor MachineEvent
        observedAtUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "observed_at_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"observedAtUnixMs")) ::
              Data.ProtoLens.FieldDescriptor MachineEvent
        kind__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "kind"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor EventKind)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"kind")) ::
              Data.ProtoLens.FieldDescriptor MachineEvent
        state__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor MachineStatus)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"state")) ::
              Data.ProtoLens.FieldDescriptor MachineEvent
        pressure__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "pressure"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor PressureKind)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"pressure")) ::
              Data.ProtoLens.FieldDescriptor MachineEvent
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machine__field_descriptor),
           (Data.ProtoLens.Tag 2, sequence__field_descriptor),
           (Data.ProtoLens.Tag 3, observedAtUnixMs__field_descriptor),
           (Data.ProtoLens.Tag 4, kind__field_descriptor),
           (Data.ProtoLens.Tag 5, state__field_descriptor),
           (Data.ProtoLens.Tag 6, pressure__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachineEvent'_unknownFields
        (\ x__ y__ -> x__ {_MachineEvent'_unknownFields = y__})
  defMessage
    = MachineEvent'_constructor
        {_MachineEvent'machine = Prelude.Nothing,
         _MachineEvent'sequence = Data.ProtoLens.fieldDefault,
         _MachineEvent'observedAtUnixMs = Data.ProtoLens.fieldDefault,
         _MachineEvent'kind = Data.ProtoLens.fieldDefault,
         _MachineEvent'state = Data.ProtoLens.fieldDefault,
         _MachineEvent'pressure = Data.ProtoLens.fieldDefault,
         _MachineEvent'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MachineEvent -> Data.ProtoLens.Encoding.Bytes.Parser MachineEvent
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
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "sequence"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"sequence") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "observed_at_unix_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"observedAtUnixMs") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "kind"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"kind") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "state"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"state") y x)
                        48
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "pressure"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"pressure") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MachineEvent"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"sequence") _x
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
                            (Data.ProtoLens.Field.field @"observedAtUnixMs") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"kind") _x
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
                            _v = Lens.Family2.view (Data.ProtoLens.Field.field @"state") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  ((Prelude..)
                                     ((Prelude..)
                                        Data.ProtoLens.Encoding.Bytes.putVarInt
                                        Prelude.fromIntegral)
                                     Prelude.fromEnum _v))
                         ((Data.Monoid.<>)
                            (let
                               _v = Lens.Family2.view (Data.ProtoLens.Field.field @"pressure") _x
                             in
                               if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                   Data.Monoid.mempty
                               else
                                   (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt 48)
                                     ((Prelude..)
                                        ((Prelude..)
                                           Data.ProtoLens.Encoding.Bytes.putVarInt
                                           Prelude.fromIntegral)
                                        Prelude.fromEnum _v))
                            (Data.ProtoLens.Encoding.Wire.buildFieldSet
                               (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))
instance Control.DeepSeq.NFData MachineEvent where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachineEvent'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MachineEvent'machine x__)
                (Control.DeepSeq.deepseq
                   (_MachineEvent'sequence x__)
                   (Control.DeepSeq.deepseq
                      (_MachineEvent'observedAtUnixMs x__)
                      (Control.DeepSeq.deepseq
                         (_MachineEvent'kind x__)
                         (Control.DeepSeq.deepseq
                            (_MachineEvent'state x__)
                            (Control.DeepSeq.deepseq (_MachineEvent'pressure x__) ()))))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.value' @:: Lens' MachineId Data.ByteString.ByteString@ -}
data MachineId
  = MachineId'_constructor {_MachineId'value :: !Data.ByteString.ByteString,
                            _MachineId'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachineId where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachineId "value" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineId'value (\ x__ y__ -> x__ {_MachineId'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachineId where
  messageName _ = Data.Text.pack "acyclic.machines.v1.MachineId"
  packedMessageDescriptor _
    = "\n\
      \\tMachineId\DC2\DC4\n\
      \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor MachineId
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachineId'_unknownFields
        (\ x__ y__ -> x__ {_MachineId'_unknownFields = y__})
  defMessage
    = MachineId'_constructor
        {_MachineId'value = Data.ProtoLens.fieldDefault,
         _MachineId'_unknownFields = []}
  parseMessage
    = let
        loop :: MachineId -> Data.ProtoLens.Encoding.Bytes.Parser MachineId
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
          (do loop Data.ProtoLens.defMessage) "MachineId"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"value") _x
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
instance Control.DeepSeq.NFData MachineId where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachineId'_unknownFields x__)
             (Control.DeepSeq.deepseq (_MachineId'value x__) ())
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' MachineMutationRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' MachineMutationRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' MachineMutationRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' MachineMutationRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' MachineMutationRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' MachineMutationRequest (Prelude.Maybe MachineId)@ -}
data MachineMutationRequest
  = MachineMutationRequest'_constructor {_MachineMutationRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                         _MachineMutationRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                         _MachineMutationRequest'machine :: !(Prelude.Maybe MachineId),
                                         _MachineMutationRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachineMutationRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachineMutationRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineMutationRequest'protocol
           (\ x__ y__ -> x__ {_MachineMutationRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineMutationRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineMutationRequest'protocol
           (\ x__ y__ -> x__ {_MachineMutationRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineMutationRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineMutationRequest'idempotencyKey
           (\ x__ y__ -> x__ {_MachineMutationRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineMutationRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineMutationRequest'idempotencyKey
           (\ x__ y__ -> x__ {_MachineMutationRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineMutationRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineMutationRequest'machine
           (\ x__ y__ -> x__ {_MachineMutationRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineMutationRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineMutationRequest'machine
           (\ x__ y__ -> x__ {_MachineMutationRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachineMutationRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.MachineMutationRequest"
  packedMessageDescriptor _
    = "\n\
      \\SYNMachineMutationRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
      \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor MachineMutationRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor MachineMutationRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor MachineMutationRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, machine__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachineMutationRequest'_unknownFields
        (\ x__ y__ -> x__ {_MachineMutationRequest'_unknownFields = y__})
  defMessage
    = MachineMutationRequest'_constructor
        {_MachineMutationRequest'protocol = Prelude.Nothing,
         _MachineMutationRequest'idempotencyKey = Prelude.Nothing,
         _MachineMutationRequest'machine = Prelude.Nothing,
         _MachineMutationRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MachineMutationRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser MachineMutationRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MachineMutationRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
instance Control.DeepSeq.NFData MachineMutationRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachineMutationRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MachineMutationRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_MachineMutationRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_MachineMutationRequest'machine x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machines' @:: Lens' MachinePage [MachineState]@
         * 'Proto.Machines.V1.Machines_Fields.vec'machines' @:: Lens' MachinePage (Data.Vector.Vector MachineState)@
         * 'Proto.Machines.V1.Machines_Fields.next' @:: Lens' MachinePage MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'next' @:: Lens' MachinePage (Prelude.Maybe MachineId)@ -}
data MachinePage
  = MachinePage'_constructor {_MachinePage'machines :: !(Data.Vector.Vector MachineState),
                              _MachinePage'next :: !(Prelude.Maybe MachineId),
                              _MachinePage'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachinePage where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachinePage "machines" [MachineState] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachinePage'machines
           (\ x__ y__ -> x__ {_MachinePage'machines = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField MachinePage "vec'machines" (Data.Vector.Vector MachineState) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachinePage'machines
           (\ x__ y__ -> x__ {_MachinePage'machines = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachinePage "next" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachinePage'next (\ x__ y__ -> x__ {_MachinePage'next = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachinePage "maybe'next" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachinePage'next (\ x__ y__ -> x__ {_MachinePage'next = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachinePage where
  messageName _ = Data.Text.pack "acyclic.machines.v1.MachinePage"
  packedMessageDescriptor _
    = "\n\
      \\vMachinePage\DC2=\n\
      \\bmachines\CAN\SOH \ETX(\v2!.acyclic.machines.v1.MachineStateR\bmachines\DC22\n\
      \\EOTnext\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\EOTnext"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machines__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machines"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineState)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"machines")) ::
              Data.ProtoLens.FieldDescriptor MachinePage
        next__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "next"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'next")) ::
              Data.ProtoLens.FieldDescriptor MachinePage
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machines__field_descriptor),
           (Data.ProtoLens.Tag 2, next__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachinePage'_unknownFields
        (\ x__ y__ -> x__ {_MachinePage'_unknownFields = y__})
  defMessage
    = MachinePage'_constructor
        {_MachinePage'machines = Data.Vector.Generic.empty,
         _MachinePage'next = Prelude.Nothing,
         _MachinePage'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MachinePage
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld MachineState
             -> Data.ProtoLens.Encoding.Bytes.Parser MachinePage
        loop x mutable'machines
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'machines <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'machines)
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
                              (Data.ProtoLens.Field.field @"vec'machines") frozen'machines x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "machines"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'machines y)
                                loop x v
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "next"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"next") y x)
                                  mutable'machines
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'machines
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'machines <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'machines)
          "MachinePage"
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
                   (Data.ProtoLens.Field.field @"vec'machines") _x))
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'next") _x
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
instance Control.DeepSeq.NFData MachinePage where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachinePage'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MachinePage'machines x__)
                (Control.DeepSeq.deepseq (_MachinePage'next x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' MachineState MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' MachineState (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.status' @:: Lens' MachineState MachineStatus@
         * 'Proto.Machines.V1.Machines_Fields.contract' @:: Lens' MachineState MachineContract@
         * 'Proto.Machines.V1.Machines_Fields.maybe'contract' @:: Lens' MachineState (Prelude.Maybe MachineContract)@
         * 'Proto.Machines.V1.Machines_Fields.endpoints' @:: Lens' MachineState [Endpoint]@
         * 'Proto.Machines.V1.Machines_Fields.vec'endpoints' @:: Lens' MachineState (Data.Vector.Vector Endpoint)@
         * 'Proto.Machines.V1.Machines_Fields.lastCheckpoint' @:: Lens' MachineState CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'lastCheckpoint' @:: Lens' MachineState (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.createdAtUnixMs' @:: Lens' MachineState Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.changedAtUnixMs' @:: Lens' MachineState Data.Word.Word64@ -}
data MachineState
  = MachineState'_constructor {_MachineState'machine :: !(Prelude.Maybe MachineId),
                               _MachineState'status :: !MachineStatus,
                               _MachineState'contract :: !(Prelude.Maybe MachineContract),
                               _MachineState'endpoints :: !(Data.Vector.Vector Endpoint),
                               _MachineState'lastCheckpoint :: !(Prelude.Maybe CheckpointId),
                               _MachineState'createdAtUnixMs :: !Data.Word.Word64,
                               _MachineState'changedAtUnixMs :: !Data.Word.Word64,
                               _MachineState'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MachineState where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MachineState "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'machine
           (\ x__ y__ -> x__ {_MachineState'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineState "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'machine
           (\ x__ y__ -> x__ {_MachineState'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineState "status" MachineStatus where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'status
           (\ x__ y__ -> x__ {_MachineState'status = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineState "contract" MachineContract where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'contract
           (\ x__ y__ -> x__ {_MachineState'contract = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineState "maybe'contract" (Prelude.Maybe MachineContract) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'contract
           (\ x__ y__ -> x__ {_MachineState'contract = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineState "endpoints" [Endpoint] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'endpoints
           (\ x__ y__ -> x__ {_MachineState'endpoints = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField MachineState "vec'endpoints" (Data.Vector.Vector Endpoint) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'endpoints
           (\ x__ y__ -> x__ {_MachineState'endpoints = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineState "lastCheckpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'lastCheckpoint
           (\ x__ y__ -> x__ {_MachineState'lastCheckpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MachineState "maybe'lastCheckpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'lastCheckpoint
           (\ x__ y__ -> x__ {_MachineState'lastCheckpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineState "createdAtUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'createdAtUnixMs
           (\ x__ y__ -> x__ {_MachineState'createdAtUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MachineState "changedAtUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MachineState'changedAtUnixMs
           (\ x__ y__ -> x__ {_MachineState'changedAtUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message MachineState where
  messageName _ = Data.Text.pack "acyclic.machines.v1.MachineState"
  packedMessageDescriptor _
    = "\n\
      \\fMachineState\DC28\n\
      \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2:\n\
      \\ACKstatus\CAN\STX \SOH(\SO2\".acyclic.machines.v1.MachineStatusR\ACKstatus\DC2@\n\
      \\bcontract\CAN\ETX \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\DC2;\n\
      \\tendpoints\CAN\EOT \ETX(\v2\GS.acyclic.machines.v1.EndpointR\tendpoints\DC2J\n\
      \\SIlast_checkpoint\CAN\ENQ \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\SOlastCheckpoint\DC2+\n\
      \\DC2created_at_unix_ms\CAN\ACK \SOH(\EOTR\SIcreatedAtUnixMs\DC2+\n\
      \\DC2changed_at_unix_ms\CAN\a \SOH(\EOTR\SIchangedAtUnixMs"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor MachineState
        status__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "status"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor MachineStatus)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"status")) ::
              Data.ProtoLens.FieldDescriptor MachineState
        contract__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "contract"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineContract)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'contract")) ::
              Data.ProtoLens.FieldDescriptor MachineState
        endpoints__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "endpoints"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Endpoint)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"endpoints")) ::
              Data.ProtoLens.FieldDescriptor MachineState
        lastCheckpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "last_checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'lastCheckpoint")) ::
              Data.ProtoLens.FieldDescriptor MachineState
        createdAtUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created_at_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"createdAtUnixMs")) ::
              Data.ProtoLens.FieldDescriptor MachineState
        changedAtUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "changed_at_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"changedAtUnixMs")) ::
              Data.ProtoLens.FieldDescriptor MachineState
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machine__field_descriptor),
           (Data.ProtoLens.Tag 2, status__field_descriptor),
           (Data.ProtoLens.Tag 3, contract__field_descriptor),
           (Data.ProtoLens.Tag 4, endpoints__field_descriptor),
           (Data.ProtoLens.Tag 5, lastCheckpoint__field_descriptor),
           (Data.ProtoLens.Tag 6, createdAtUnixMs__field_descriptor),
           (Data.ProtoLens.Tag 7, changedAtUnixMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MachineState'_unknownFields
        (\ x__ y__ -> x__ {_MachineState'_unknownFields = y__})
  defMessage
    = MachineState'_constructor
        {_MachineState'machine = Prelude.Nothing,
         _MachineState'status = Data.ProtoLens.fieldDefault,
         _MachineState'contract = Prelude.Nothing,
         _MachineState'endpoints = Data.Vector.Generic.empty,
         _MachineState'lastCheckpoint = Prelude.Nothing,
         _MachineState'createdAtUnixMs = Data.ProtoLens.fieldDefault,
         _MachineState'changedAtUnixMs = Data.ProtoLens.fieldDefault,
         _MachineState'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MachineState
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Endpoint
             -> Data.ProtoLens.Encoding.Bytes.Parser MachineState
        loop x mutable'endpoints
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'endpoints <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                            (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                               mutable'endpoints)
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
                              (Data.ProtoLens.Field.field @"vec'endpoints") frozen'endpoints x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                                  mutable'endpoints
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "status"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"status") y x)
                                  mutable'endpoints
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "contract"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"contract") y x)
                                  mutable'endpoints
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "endpoints"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'endpoints y)
                                loop x v
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "last_checkpoint"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"lastCheckpoint") y x)
                                  mutable'endpoints
                        48
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "created_at_unix_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"createdAtUnixMs") y x)
                                  mutable'endpoints
                        56
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "changed_at_unix_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"changedAtUnixMs") y x)
                                  mutable'endpoints
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'endpoints
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'endpoints <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                     Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'endpoints)
          "MachineState"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"status") _x
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
                   (case
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'contract") _x
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
                         (Lens.Family2.view
                            (Data.ProtoLens.Field.field @"vec'endpoints") _x))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view
                                (Data.ProtoLens.Field.field @"maybe'lastCheckpoint") _x
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
                                 = Lens.Family2.view
                                     (Data.ProtoLens.Field.field @"createdAtUnixMs") _x
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
                                        (Data.ProtoLens.Field.field @"changedAtUnixMs") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 56)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                               (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                  (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))))
instance Control.DeepSeq.NFData MachineState where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MachineState'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MachineState'machine x__)
                (Control.DeepSeq.deepseq
                   (_MachineState'status x__)
                   (Control.DeepSeq.deepseq
                      (_MachineState'contract x__)
                      (Control.DeepSeq.deepseq
                         (_MachineState'endpoints x__)
                         (Control.DeepSeq.deepseq
                            (_MachineState'lastCheckpoint x__)
                            (Control.DeepSeq.deepseq
                               (_MachineState'createdAtUnixMs x__)
                               (Control.DeepSeq.deepseq
                                  (_MachineState'changedAtUnixMs x__) ())))))))
newtype MachineStatus'UnrecognizedValue
  = MachineStatus'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data MachineStatus
  = MACHINE_STATUS_UNSPECIFIED |
    MACHINE_STATUS_STARTING |
    MACHINE_STATUS_RUNNING |
    MACHINE_STATUS_SUSPENDING |
    MACHINE_STATUS_SUSPENDED |
    MACHINE_STATUS_WAKING |
    MACHINE_STATUS_DESTROYING |
    MACHINE_STATUS_DESTROYED |
    MACHINE_STATUS_FAILED |
    MACHINE_STATUS_INDETERMINATE |
    MachineStatus'Unrecognized !MachineStatus'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum MachineStatus where
  maybeToEnum 0 = Prelude.Just MACHINE_STATUS_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just MACHINE_STATUS_STARTING
  maybeToEnum 2 = Prelude.Just MACHINE_STATUS_RUNNING
  maybeToEnum 3 = Prelude.Just MACHINE_STATUS_SUSPENDING
  maybeToEnum 4 = Prelude.Just MACHINE_STATUS_SUSPENDED
  maybeToEnum 5 = Prelude.Just MACHINE_STATUS_WAKING
  maybeToEnum 6 = Prelude.Just MACHINE_STATUS_DESTROYING
  maybeToEnum 7 = Prelude.Just MACHINE_STATUS_DESTROYED
  maybeToEnum 8 = Prelude.Just MACHINE_STATUS_FAILED
  maybeToEnum 9 = Prelude.Just MACHINE_STATUS_INDETERMINATE
  maybeToEnum k
    = Prelude.Just
        (MachineStatus'Unrecognized
           (MachineStatus'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum MACHINE_STATUS_UNSPECIFIED = "MACHINE_STATUS_UNSPECIFIED"
  showEnum MACHINE_STATUS_STARTING = "MACHINE_STATUS_STARTING"
  showEnum MACHINE_STATUS_RUNNING = "MACHINE_STATUS_RUNNING"
  showEnum MACHINE_STATUS_SUSPENDING = "MACHINE_STATUS_SUSPENDING"
  showEnum MACHINE_STATUS_SUSPENDED = "MACHINE_STATUS_SUSPENDED"
  showEnum MACHINE_STATUS_WAKING = "MACHINE_STATUS_WAKING"
  showEnum MACHINE_STATUS_DESTROYING = "MACHINE_STATUS_DESTROYING"
  showEnum MACHINE_STATUS_DESTROYED = "MACHINE_STATUS_DESTROYED"
  showEnum MACHINE_STATUS_FAILED = "MACHINE_STATUS_FAILED"
  showEnum MACHINE_STATUS_INDETERMINATE
    = "MACHINE_STATUS_INDETERMINATE"
  showEnum
    (MachineStatus'Unrecognized (MachineStatus'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "MACHINE_STATUS_UNSPECIFIED"
    = Prelude.Just MACHINE_STATUS_UNSPECIFIED
    | (Prelude.==) k "MACHINE_STATUS_STARTING"
    = Prelude.Just MACHINE_STATUS_STARTING
    | (Prelude.==) k "MACHINE_STATUS_RUNNING"
    = Prelude.Just MACHINE_STATUS_RUNNING
    | (Prelude.==) k "MACHINE_STATUS_SUSPENDING"
    = Prelude.Just MACHINE_STATUS_SUSPENDING
    | (Prelude.==) k "MACHINE_STATUS_SUSPENDED"
    = Prelude.Just MACHINE_STATUS_SUSPENDED
    | (Prelude.==) k "MACHINE_STATUS_WAKING"
    = Prelude.Just MACHINE_STATUS_WAKING
    | (Prelude.==) k "MACHINE_STATUS_DESTROYING"
    = Prelude.Just MACHINE_STATUS_DESTROYING
    | (Prelude.==) k "MACHINE_STATUS_DESTROYED"
    = Prelude.Just MACHINE_STATUS_DESTROYED
    | (Prelude.==) k "MACHINE_STATUS_FAILED"
    = Prelude.Just MACHINE_STATUS_FAILED
    | (Prelude.==) k "MACHINE_STATUS_INDETERMINATE"
    = Prelude.Just MACHINE_STATUS_INDETERMINATE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded MachineStatus where
  minBound = MACHINE_STATUS_UNSPECIFIED
  maxBound = MACHINE_STATUS_INDETERMINATE
instance Prelude.Enum MachineStatus where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum MachineStatus: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum MACHINE_STATUS_UNSPECIFIED = 0
  fromEnum MACHINE_STATUS_STARTING = 1
  fromEnum MACHINE_STATUS_RUNNING = 2
  fromEnum MACHINE_STATUS_SUSPENDING = 3
  fromEnum MACHINE_STATUS_SUSPENDED = 4
  fromEnum MACHINE_STATUS_WAKING = 5
  fromEnum MACHINE_STATUS_DESTROYING = 6
  fromEnum MACHINE_STATUS_DESTROYED = 7
  fromEnum MACHINE_STATUS_FAILED = 8
  fromEnum MACHINE_STATUS_INDETERMINATE = 9
  fromEnum
    (MachineStatus'Unrecognized (MachineStatus'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ MACHINE_STATUS_INDETERMINATE
    = Prelude.error
        "MachineStatus.succ: bad argument MACHINE_STATUS_INDETERMINATE. This value would be out of bounds."
  succ MACHINE_STATUS_UNSPECIFIED = MACHINE_STATUS_STARTING
  succ MACHINE_STATUS_STARTING = MACHINE_STATUS_RUNNING
  succ MACHINE_STATUS_RUNNING = MACHINE_STATUS_SUSPENDING
  succ MACHINE_STATUS_SUSPENDING = MACHINE_STATUS_SUSPENDED
  succ MACHINE_STATUS_SUSPENDED = MACHINE_STATUS_WAKING
  succ MACHINE_STATUS_WAKING = MACHINE_STATUS_DESTROYING
  succ MACHINE_STATUS_DESTROYING = MACHINE_STATUS_DESTROYED
  succ MACHINE_STATUS_DESTROYED = MACHINE_STATUS_FAILED
  succ MACHINE_STATUS_FAILED = MACHINE_STATUS_INDETERMINATE
  succ (MachineStatus'Unrecognized _)
    = Prelude.error
        "MachineStatus.succ: bad argument: unrecognized value"
  pred MACHINE_STATUS_UNSPECIFIED
    = Prelude.error
        "MachineStatus.pred: bad argument MACHINE_STATUS_UNSPECIFIED. This value would be out of bounds."
  pred MACHINE_STATUS_STARTING = MACHINE_STATUS_UNSPECIFIED
  pred MACHINE_STATUS_RUNNING = MACHINE_STATUS_STARTING
  pred MACHINE_STATUS_SUSPENDING = MACHINE_STATUS_RUNNING
  pred MACHINE_STATUS_SUSPENDED = MACHINE_STATUS_SUSPENDING
  pred MACHINE_STATUS_WAKING = MACHINE_STATUS_SUSPENDED
  pred MACHINE_STATUS_DESTROYING = MACHINE_STATUS_WAKING
  pred MACHINE_STATUS_DESTROYED = MACHINE_STATUS_DESTROYING
  pred MACHINE_STATUS_FAILED = MACHINE_STATUS_DESTROYED
  pred MACHINE_STATUS_INDETERMINATE = MACHINE_STATUS_FAILED
  pred (MachineStatus'Unrecognized _)
    = Prelude.error
        "MachineStatus.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault MachineStatus where
  fieldDefault = MACHINE_STATUS_UNSPECIFIED
instance Control.DeepSeq.NFData MachineStatus where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' MutationAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' MutationAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' MutationAdmission MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' MutationAdmission (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' MutationAdmission CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' MutationAdmission (Prelude.Maybe CheckpointId)@ -}
data MutationAdmission
  = MutationAdmission'_constructor {_MutationAdmission'operation :: !(Prelude.Maybe OperationId),
                                    _MutationAdmission'machine :: !(Prelude.Maybe MachineId),
                                    _MutationAdmission'checkpoint :: !(Prelude.Maybe CheckpointId),
                                    _MutationAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MutationAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MutationAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationAdmission'operation
           (\ x__ y__ -> x__ {_MutationAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MutationAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationAdmission'operation
           (\ x__ y__ -> x__ {_MutationAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutationAdmission "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationAdmission'machine
           (\ x__ y__ -> x__ {_MutationAdmission'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MutationAdmission "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationAdmission'machine
           (\ x__ y__ -> x__ {_MutationAdmission'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutationAdmission "checkpoint" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationAdmission'checkpoint
           (\ x__ y__ -> x__ {_MutationAdmission'checkpoint = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MutationAdmission "maybe'checkpoint" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationAdmission'checkpoint
           (\ x__ y__ -> x__ {_MutationAdmission'checkpoint = y__}))
        Prelude.id
instance Data.ProtoLens.Message MutationAdmission where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.MutationAdmission"
  packedMessageDescriptor _
    = "\n\
      \\DC1MutationAdmission\DC2>\n\
      \\toperation\CAN\SOH \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC28\n\
      \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2A\n\
      \\n\
      \checkpoint\CAN\ETX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
      \checkpoint"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor MutationAdmission
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor MutationAdmission
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor MutationAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, operation__field_descriptor),
           (Data.ProtoLens.Tag 2, machine__field_descriptor),
           (Data.ProtoLens.Tag 3, checkpoint__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MutationAdmission'_unknownFields
        (\ x__ y__ -> x__ {_MutationAdmission'_unknownFields = y__})
  defMessage
    = MutationAdmission'_constructor
        {_MutationAdmission'operation = Prelude.Nothing,
         _MutationAdmission'machine = Prelude.Nothing,
         _MutationAdmission'checkpoint = Prelude.Nothing,
         _MutationAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MutationAdmission
          -> Data.ProtoLens.Encoding.Bytes.Parser MutationAdmission
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
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MutationAdmission"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'checkpoint") _x
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
instance Control.DeepSeq.NFData MutationAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MutationAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MutationAdmission'operation x__)
                (Control.DeepSeq.deepseq
                   (_MutationAdmission'machine x__)
                   (Control.DeepSeq.deepseq (_MutationAdmission'checkpoint x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.maybe'result' @:: Lens' MutationOutcome (Prelude.Maybe MutationOutcome'Result)@
         * 'Proto.Machines.V1.Machines_Fields.maybe'created' @:: Lens' MutationOutcome (Prelude.Maybe MachineState)@
         * 'Proto.Machines.V1.Machines_Fields.created' @:: Lens' MutationOutcome MachineState@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpointed' @:: Lens' MutationOutcome (Prelude.Maybe CheckpointState)@
         * 'Proto.Machines.V1.Machines_Fields.checkpointed' @:: Lens' MutationOutcome CheckpointState@
         * 'Proto.Machines.V1.Machines_Fields.maybe'forked' @:: Lens' MutationOutcome (Prelude.Maybe ForkedMachines)@
         * 'Proto.Machines.V1.Machines_Fields.forked' @:: Lens' MutationOutcome ForkedMachines@
         * 'Proto.Machines.V1.Machines_Fields.maybe'suspended' @:: Lens' MutationOutcome (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.suspended' @:: Lens' MutationOutcome MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'woken' @:: Lens' MutationOutcome (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.woken' @:: Lens' MutationOutcome MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'suspensionPolicySet' @:: Lens' MutationOutcome (Prelude.Maybe PolicySet)@
         * 'Proto.Machines.V1.Machines_Fields.suspensionPolicySet' @:: Lens' MutationOutcome PolicySet@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machineDestroyed' @:: Lens' MutationOutcome (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.machineDestroyed' @:: Lens' MutationOutcome MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpointDestroyed' @:: Lens' MutationOutcome (Prelude.Maybe CheckpointId)@
         * 'Proto.Machines.V1.Machines_Fields.checkpointDestroyed' @:: Lens' MutationOutcome CheckpointId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machineForked' @:: Lens' MutationOutcome (Prelude.Maybe ForkedLiveMachines)@
         * 'Proto.Machines.V1.Machines_Fields.machineForked' @:: Lens' MutationOutcome ForkedLiveMachines@ -}
data MutationOutcome
  = MutationOutcome'_constructor {_MutationOutcome'result :: !(Prelude.Maybe MutationOutcome'Result),
                                  _MutationOutcome'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MutationOutcome where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data MutationOutcome'Result
  = MutationOutcome'Created !MachineState |
    MutationOutcome'Checkpointed !CheckpointState |
    MutationOutcome'Forked !ForkedMachines |
    MutationOutcome'Suspended !MachineId |
    MutationOutcome'Woken !MachineId |
    MutationOutcome'SuspensionPolicySet !PolicySet |
    MutationOutcome'MachineDestroyed !MachineId |
    MutationOutcome'CheckpointDestroyed !CheckpointId |
    MutationOutcome'MachineForked !ForkedLiveMachines
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'result" (Prelude.Maybe MutationOutcome'Result) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'created" (Prelude.Maybe MachineState) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'Created x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'Created y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "created" MachineState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'Created x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'Created y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'checkpointed" (Prelude.Maybe CheckpointState) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'Checkpointed x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'Checkpointed y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "checkpointed" CheckpointState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'Checkpointed x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'Checkpointed y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'forked" (Prelude.Maybe ForkedMachines) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'Forked x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'Forked y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "forked" ForkedMachines where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'Forked x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'Forked y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'suspended" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'Suspended x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'Suspended y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "suspended" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'Suspended x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'Suspended y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'woken" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'Woken x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'Woken y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "woken" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'Woken x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'Woken y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'suspensionPolicySet" (Prelude.Maybe PolicySet) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'SuspensionPolicySet x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'SuspensionPolicySet y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "suspensionPolicySet" PolicySet where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'SuspensionPolicySet x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'SuspensionPolicySet y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'machineDestroyed" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'MachineDestroyed x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'MachineDestroyed y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "machineDestroyed" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'MachineDestroyed x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'MachineDestroyed y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'checkpointDestroyed" (Prelude.Maybe CheckpointId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'CheckpointDestroyed x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'CheckpointDestroyed y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "checkpointDestroyed" CheckpointId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'CheckpointDestroyed x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'CheckpointDestroyed y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutationOutcome "maybe'machineForked" (Prelude.Maybe ForkedLiveMachines) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutationOutcome'MachineForked x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutationOutcome'MachineForked y__))
instance Data.ProtoLens.Field.HasField MutationOutcome "machineForked" ForkedLiveMachines where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationOutcome'result
           (\ x__ y__ -> x__ {_MutationOutcome'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutationOutcome'MachineForked x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutationOutcome'MachineForked y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message MutationOutcome where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.MutationOutcome"
  packedMessageDescriptor _
    = "\n\
      \\SIMutationOutcome\DC2=\n\
      \\acreated\CAN\SOH \SOH(\v2!.acyclic.machines.v1.MachineStateH\NULR\acreated\DC2J\n\
      \\fcheckpointed\CAN\STX \SOH(\v2$.acyclic.machines.v1.CheckpointStateH\NULR\fcheckpointed\DC2=\n\
      \\ACKforked\CAN\ETX \SOH(\v2#.acyclic.machines.v1.ForkedMachinesH\NULR\ACKforked\DC2>\n\
      \\tsuspended\CAN\EOT \SOH(\v2\RS.acyclic.machines.v1.MachineIdH\NULR\tsuspended\DC26\n\
      \\ENQwoken\CAN\ENQ \SOH(\v2\RS.acyclic.machines.v1.MachineIdH\NULR\ENQwoken\DC2T\n\
      \\NAKsuspension_policy_set\CAN\ACK \SOH(\v2\RS.acyclic.machines.v1.PolicySetH\NULR\DC3suspensionPolicySet\DC2M\n\
      \\DC1machine_destroyed\CAN\a \SOH(\v2\RS.acyclic.machines.v1.MachineIdH\NULR\DLEmachineDestroyed\DC2V\n\
      \\DC4checkpoint_destroyed\CAN\b \SOH(\v2!.acyclic.machines.v1.CheckpointIdH\NULR\DC3checkpointDestroyed\DC2P\n\
      \\SOmachine_forked\CAN\t \SOH(\v2'.acyclic.machines.v1.ForkedLiveMachinesH\NULR\rmachineForkedB\b\n\
      \\ACKresult"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        created__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineState)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'created")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        checkpointed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpointed"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointState)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpointed")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        forked__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "forked"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ForkedMachines)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'forked")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        suspended__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suspended"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suspended")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        woken__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "woken"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'woken")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        suspensionPolicySet__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suspension_policy_set"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor PolicySet)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suspensionPolicySet")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        machineDestroyed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine_destroyed"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machineDestroyed")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        checkpointDestroyed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint_destroyed"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpointDestroyed")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
        machineForked__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine_forked"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ForkedLiveMachines)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machineForked")) ::
              Data.ProtoLens.FieldDescriptor MutationOutcome
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, created__field_descriptor),
           (Data.ProtoLens.Tag 2, checkpointed__field_descriptor),
           (Data.ProtoLens.Tag 3, forked__field_descriptor),
           (Data.ProtoLens.Tag 4, suspended__field_descriptor),
           (Data.ProtoLens.Tag 5, woken__field_descriptor),
           (Data.ProtoLens.Tag 6, suspensionPolicySet__field_descriptor),
           (Data.ProtoLens.Tag 7, machineDestroyed__field_descriptor),
           (Data.ProtoLens.Tag 8, checkpointDestroyed__field_descriptor),
           (Data.ProtoLens.Tag 9, machineForked__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MutationOutcome'_unknownFields
        (\ x__ y__ -> x__ {_MutationOutcome'_unknownFields = y__})
  defMessage
    = MutationOutcome'_constructor
        {_MutationOutcome'result = Prelude.Nothing,
         _MutationOutcome'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MutationOutcome
          -> Data.ProtoLens.Encoding.Bytes.Parser MutationOutcome
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
                                       "created"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"created") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpointed"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"checkpointed") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "forked"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"forked") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "suspended"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"suspended") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "woken"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"woken") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "suspension_policy_set"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"suspensionPolicySet") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine_destroyed"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"machineDestroyed") y x)
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint_destroyed"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"checkpointDestroyed") y x)
                        74
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine_forked"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"machineForked") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MutationOutcome"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'result") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (MutationOutcome'Created v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'Checkpointed v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'Forked v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'Suspended v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'Woken v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'SuspensionPolicySet v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'MachineDestroyed v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'CheckpointDestroyed v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 66)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (MutationOutcome'MachineForked v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 74)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData MutationOutcome where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MutationOutcome'_unknownFields x__)
             (Control.DeepSeq.deepseq (_MutationOutcome'result x__) ())
instance Control.DeepSeq.NFData MutationOutcome'Result where
  rnf (MutationOutcome'Created x__) = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'Checkpointed x__) = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'Forked x__) = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'Suspended x__) = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'Woken x__) = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'SuspensionPolicySet x__)
    = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'MachineDestroyed x__)
    = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'CheckpointDestroyed x__)
    = Control.DeepSeq.rnf x__
  rnf (MutationOutcome'MachineForked x__) = Control.DeepSeq.rnf x__
_MutationOutcome'Created ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result MachineState
_MutationOutcome'Created
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'Created
      (\ p__
         -> case p__ of
              (MutationOutcome'Created p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'Checkpointed ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result CheckpointState
_MutationOutcome'Checkpointed
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'Checkpointed
      (\ p__
         -> case p__ of
              (MutationOutcome'Checkpointed p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'Forked ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result ForkedMachines
_MutationOutcome'Forked
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'Forked
      (\ p__
         -> case p__ of
              (MutationOutcome'Forked p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'Suspended ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result MachineId
_MutationOutcome'Suspended
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'Suspended
      (\ p__
         -> case p__ of
              (MutationOutcome'Suspended p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'Woken ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result MachineId
_MutationOutcome'Woken
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'Woken
      (\ p__
         -> case p__ of
              (MutationOutcome'Woken p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'SuspensionPolicySet ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result PolicySet
_MutationOutcome'SuspensionPolicySet
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'SuspensionPolicySet
      (\ p__
         -> case p__ of
              (MutationOutcome'SuspensionPolicySet p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'MachineDestroyed ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result MachineId
_MutationOutcome'MachineDestroyed
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'MachineDestroyed
      (\ p__
         -> case p__ of
              (MutationOutcome'MachineDestroyed p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'CheckpointDestroyed ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result CheckpointId
_MutationOutcome'CheckpointDestroyed
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'CheckpointDestroyed
      (\ p__
         -> case p__ of
              (MutationOutcome'CheckpointDestroyed p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutationOutcome'MachineForked ::
  Data.ProtoLens.Prism.Prism' MutationOutcome'Result ForkedLiveMachines
_MutationOutcome'MachineForked
  = Data.ProtoLens.Prism.prism'
      MutationOutcome'MachineForked
      (\ p__
         -> case p__ of
              (MutationOutcome'MachineForked p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.value' @:: Lens' OperationId Data.ByteString.ByteString@ -}
data OperationId
  = OperationId'_constructor {_OperationId'value :: !Data.ByteString.ByteString,
                              _OperationId'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show OperationId where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField OperationId "value" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationId'value (\ x__ y__ -> x__ {_OperationId'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message OperationId where
  messageName _ = Data.Text.pack "acyclic.machines.v1.OperationId"
  packedMessageDescriptor _
    = "\n\
      \\vOperationId\DC2\DC4\n\
      \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"value")) ::
              Data.ProtoLens.FieldDescriptor OperationId
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _OperationId'_unknownFields
        (\ x__ y__ -> x__ {_OperationId'_unknownFields = y__})
  defMessage
    = OperationId'_constructor
        {_OperationId'value = Data.ProtoLens.fieldDefault,
         _OperationId'_unknownFields = []}
  parseMessage
    = let
        loop ::
          OperationId -> Data.ProtoLens.Encoding.Bytes.Parser OperationId
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
          (do loop Data.ProtoLens.defMessage) "OperationId"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"value") _x
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
instance Control.DeepSeq.NFData OperationId where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_OperationId'_unknownFields x__)
             (Control.DeepSeq.deepseq (_OperationId'value x__) ())
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.operations' @:: Lens' OperationPage [OperationState]@
         * 'Proto.Machines.V1.Machines_Fields.vec'operations' @:: Lens' OperationPage (Data.Vector.Vector OperationState)@ -}
data OperationPage
  = OperationPage'_constructor {_OperationPage'operations :: !(Data.Vector.Vector OperationState),
                                _OperationPage'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show OperationPage where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField OperationPage "operations" [OperationState] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationPage'operations
           (\ x__ y__ -> x__ {_OperationPage'operations = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField OperationPage "vec'operations" (Data.Vector.Vector OperationState) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationPage'operations
           (\ x__ y__ -> x__ {_OperationPage'operations = y__}))
        Prelude.id
instance Data.ProtoLens.Message OperationPage where
  messageName _ = Data.Text.pack "acyclic.machines.v1.OperationPage"
  packedMessageDescriptor _
    = "\n\
      \\rOperationPage\DC2C\n\
      \\n\
      \operations\CAN\SOH \ETX(\v2#.acyclic.machines.v1.OperationStateR\n\
      \operations"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        operations__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operations"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationState)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"operations")) ::
              Data.ProtoLens.FieldDescriptor OperationPage
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, operations__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _OperationPage'_unknownFields
        (\ x__ y__ -> x__ {_OperationPage'_unknownFields = y__})
  defMessage
    = OperationPage'_constructor
        {_OperationPage'operations = Data.Vector.Generic.empty,
         _OperationPage'_unknownFields = []}
  parseMessage
    = let
        loop ::
          OperationPage
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld OperationState
             -> Data.ProtoLens.Encoding.Bytes.Parser OperationPage
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
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
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
          "OperationPage"
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
                   (Data.ProtoLens.Field.field @"vec'operations") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData OperationPage where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_OperationPage'_unknownFields x__)
             (Control.DeepSeq.deepseq (_OperationPage'operations x__) ())
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' OperationRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' OperationRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' OperationRequest OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' OperationRequest (Prelude.Maybe OperationId)@ -}
data OperationRequest
  = OperationRequest'_constructor {_OperationRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                   _OperationRequest'operation :: !(Prelude.Maybe OperationId),
                                   _OperationRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show OperationRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField OperationRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationRequest'protocol
           (\ x__ y__ -> x__ {_OperationRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField OperationRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationRequest'protocol
           (\ x__ y__ -> x__ {_OperationRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField OperationRequest "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationRequest'operation
           (\ x__ y__ -> x__ {_OperationRequest'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField OperationRequest "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationRequest'operation
           (\ x__ y__ -> x__ {_OperationRequest'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Message OperationRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.OperationRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEOperationRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2>\n\
      \\toperation\CAN\STX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor OperationRequest
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor OperationRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, operation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _OperationRequest'_unknownFields
        (\ x__ y__ -> x__ {_OperationRequest'_unknownFields = y__})
  defMessage
    = OperationRequest'_constructor
        {_OperationRequest'protocol = Prelude.Nothing,
         _OperationRequest'operation = Prelude.Nothing,
         _OperationRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          OperationRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser OperationRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "OperationRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'operation") _x
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
instance Control.DeepSeq.NFData OperationRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_OperationRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_OperationRequest'protocol x__)
                (Control.DeepSeq.deepseq (_OperationRequest'operation x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' OperationState OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' OperationState (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.status' @:: Lens' OperationState OperationStatus@ -}
data OperationState
  = OperationState'_constructor {_OperationState'operation :: !(Prelude.Maybe OperationId),
                                 _OperationState'status :: !OperationStatus,
                                 _OperationState'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show OperationState where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField OperationState "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationState'operation
           (\ x__ y__ -> x__ {_OperationState'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField OperationState "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationState'operation
           (\ x__ y__ -> x__ {_OperationState'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField OperationState "status" OperationStatus where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _OperationState'status
           (\ x__ y__ -> x__ {_OperationState'status = y__}))
        Prelude.id
instance Data.ProtoLens.Message OperationState where
  messageName _ = Data.Text.pack "acyclic.machines.v1.OperationState"
  packedMessageDescriptor _
    = "\n\
      \\SOOperationState\DC2>\n\
      \\toperation\CAN\SOH \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2<\n\
      \\ACKstatus\CAN\STX \SOH(\SO2$.acyclic.machines.v1.OperationStatusR\ACKstatus"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor OperationState
        status__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "status"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor OperationStatus)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"status")) ::
              Data.ProtoLens.FieldDescriptor OperationState
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, operation__field_descriptor),
           (Data.ProtoLens.Tag 2, status__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _OperationState'_unknownFields
        (\ x__ y__ -> x__ {_OperationState'_unknownFields = y__})
  defMessage
    = OperationState'_constructor
        {_OperationState'operation = Prelude.Nothing,
         _OperationState'status = Data.ProtoLens.fieldDefault,
         _OperationState'_unknownFields = []}
  parseMessage
    = let
        loop ::
          OperationState
          -> Data.ProtoLens.Encoding.Bytes.Parser OperationState
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
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "status"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"status") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "OperationState"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"status") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData OperationState where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_OperationState'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_OperationState'operation x__)
                (Control.DeepSeq.deepseq (_OperationState'status x__) ()))
newtype OperationStatus'UnrecognizedValue
  = OperationStatus'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data OperationStatus
  = OPERATION_STATUS_UNSPECIFIED |
    OPERATION_STATUS_PENDING |
    OPERATION_STATUS_SUCCEEDED |
    OPERATION_STATUS_CANCELLED |
    OPERATION_STATUS_INDETERMINATE |
    OPERATION_STATUS_FAILED |
    OperationStatus'Unrecognized !OperationStatus'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum OperationStatus where
  maybeToEnum 0 = Prelude.Just OPERATION_STATUS_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just OPERATION_STATUS_PENDING
  maybeToEnum 2 = Prelude.Just OPERATION_STATUS_SUCCEEDED
  maybeToEnum 3 = Prelude.Just OPERATION_STATUS_CANCELLED
  maybeToEnum 4 = Prelude.Just OPERATION_STATUS_INDETERMINATE
  maybeToEnum 5 = Prelude.Just OPERATION_STATUS_FAILED
  maybeToEnum k
    = Prelude.Just
        (OperationStatus'Unrecognized
           (OperationStatus'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum OPERATION_STATUS_UNSPECIFIED
    = "OPERATION_STATUS_UNSPECIFIED"
  showEnum OPERATION_STATUS_PENDING = "OPERATION_STATUS_PENDING"
  showEnum OPERATION_STATUS_SUCCEEDED = "OPERATION_STATUS_SUCCEEDED"
  showEnum OPERATION_STATUS_CANCELLED = "OPERATION_STATUS_CANCELLED"
  showEnum OPERATION_STATUS_INDETERMINATE
    = "OPERATION_STATUS_INDETERMINATE"
  showEnum OPERATION_STATUS_FAILED = "OPERATION_STATUS_FAILED"
  showEnum
    (OperationStatus'Unrecognized (OperationStatus'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "OPERATION_STATUS_UNSPECIFIED"
    = Prelude.Just OPERATION_STATUS_UNSPECIFIED
    | (Prelude.==) k "OPERATION_STATUS_PENDING"
    = Prelude.Just OPERATION_STATUS_PENDING
    | (Prelude.==) k "OPERATION_STATUS_SUCCEEDED"
    = Prelude.Just OPERATION_STATUS_SUCCEEDED
    | (Prelude.==) k "OPERATION_STATUS_CANCELLED"
    = Prelude.Just OPERATION_STATUS_CANCELLED
    | (Prelude.==) k "OPERATION_STATUS_INDETERMINATE"
    = Prelude.Just OPERATION_STATUS_INDETERMINATE
    | (Prelude.==) k "OPERATION_STATUS_FAILED"
    = Prelude.Just OPERATION_STATUS_FAILED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded OperationStatus where
  minBound = OPERATION_STATUS_UNSPECIFIED
  maxBound = OPERATION_STATUS_FAILED
instance Prelude.Enum OperationStatus where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum OperationStatus: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum OPERATION_STATUS_UNSPECIFIED = 0
  fromEnum OPERATION_STATUS_PENDING = 1
  fromEnum OPERATION_STATUS_SUCCEEDED = 2
  fromEnum OPERATION_STATUS_CANCELLED = 3
  fromEnum OPERATION_STATUS_INDETERMINATE = 4
  fromEnum OPERATION_STATUS_FAILED = 5
  fromEnum
    (OperationStatus'Unrecognized (OperationStatus'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ OPERATION_STATUS_FAILED
    = Prelude.error
        "OperationStatus.succ: bad argument OPERATION_STATUS_FAILED. This value would be out of bounds."
  succ OPERATION_STATUS_UNSPECIFIED = OPERATION_STATUS_PENDING
  succ OPERATION_STATUS_PENDING = OPERATION_STATUS_SUCCEEDED
  succ OPERATION_STATUS_SUCCEEDED = OPERATION_STATUS_CANCELLED
  succ OPERATION_STATUS_CANCELLED = OPERATION_STATUS_INDETERMINATE
  succ OPERATION_STATUS_INDETERMINATE = OPERATION_STATUS_FAILED
  succ (OperationStatus'Unrecognized _)
    = Prelude.error
        "OperationStatus.succ: bad argument: unrecognized value"
  pred OPERATION_STATUS_UNSPECIFIED
    = Prelude.error
        "OperationStatus.pred: bad argument OPERATION_STATUS_UNSPECIFIED. This value would be out of bounds."
  pred OPERATION_STATUS_PENDING = OPERATION_STATUS_UNSPECIFIED
  pred OPERATION_STATUS_SUCCEEDED = OPERATION_STATUS_PENDING
  pred OPERATION_STATUS_CANCELLED = OPERATION_STATUS_SUCCEEDED
  pred OPERATION_STATUS_INDETERMINATE = OPERATION_STATUS_CANCELLED
  pred OPERATION_STATUS_FAILED = OPERATION_STATUS_INDETERMINATE
  pred (OperationStatus'Unrecognized _)
    = Prelude.error
        "OperationStatus.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault OperationStatus where
  fieldDefault = OPERATION_STATUS_UNSPECIFIED
instance Control.DeepSeq.NFData OperationStatus where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' PolicyAdmission MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' PolicyAdmission (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' PolicyAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' PolicyAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.policy' @:: Lens' PolicyAdmission SuspensionPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'policy' @:: Lens' PolicyAdmission (Prelude.Maybe SuspensionPolicy)@ -}
data PolicyAdmission
  = PolicyAdmission'_constructor {_PolicyAdmission'machine :: !(Prelude.Maybe MachineId),
                                  _PolicyAdmission'operation :: !(Prelude.Maybe OperationId),
                                  _PolicyAdmission'policy :: !(Prelude.Maybe SuspensionPolicy),
                                  _PolicyAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show PolicyAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField PolicyAdmission "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicyAdmission'machine
           (\ x__ y__ -> x__ {_PolicyAdmission'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PolicyAdmission "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicyAdmission'machine
           (\ x__ y__ -> x__ {_PolicyAdmission'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PolicyAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicyAdmission'operation
           (\ x__ y__ -> x__ {_PolicyAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PolicyAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicyAdmission'operation
           (\ x__ y__ -> x__ {_PolicyAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PolicyAdmission "policy" SuspensionPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicyAdmission'policy
           (\ x__ y__ -> x__ {_PolicyAdmission'policy = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PolicyAdmission "maybe'policy" (Prelude.Maybe SuspensionPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicyAdmission'policy
           (\ x__ y__ -> x__ {_PolicyAdmission'policy = y__}))
        Prelude.id
instance Data.ProtoLens.Message PolicyAdmission where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.PolicyAdmission"
  packedMessageDescriptor _
    = "\n\
      \\SIPolicyAdmission\DC28\n\
      \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2>\n\
      \\toperation\CAN\STX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2=\n\
      \\ACKpolicy\CAN\ETX \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\ACKpolicy"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor PolicyAdmission
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor PolicyAdmission
        policy__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "policy"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SuspensionPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'policy")) ::
              Data.ProtoLens.FieldDescriptor PolicyAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machine__field_descriptor),
           (Data.ProtoLens.Tag 2, operation__field_descriptor),
           (Data.ProtoLens.Tag 3, policy__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _PolicyAdmission'_unknownFields
        (\ x__ y__ -> x__ {_PolicyAdmission'_unknownFields = y__})
  defMessage
    = PolicyAdmission'_constructor
        {_PolicyAdmission'machine = Prelude.Nothing,
         _PolicyAdmission'operation = Prelude.Nothing,
         _PolicyAdmission'policy = Prelude.Nothing,
         _PolicyAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          PolicyAdmission
          -> Data.ProtoLens.Encoding.Bytes.Parser PolicyAdmission
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
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "policy"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"policy") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "PolicyAdmission"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                       (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'policy") _x
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
instance Control.DeepSeq.NFData PolicyAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_PolicyAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_PolicyAdmission'machine x__)
                (Control.DeepSeq.deepseq
                   (_PolicyAdmission'operation x__)
                   (Control.DeepSeq.deepseq (_PolicyAdmission'policy x__) ())))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' PolicySet MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' PolicySet (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.policy' @:: Lens' PolicySet SuspensionPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'policy' @:: Lens' PolicySet (Prelude.Maybe SuspensionPolicy)@ -}
data PolicySet
  = PolicySet'_constructor {_PolicySet'machine :: !(Prelude.Maybe MachineId),
                            _PolicySet'policy :: !(Prelude.Maybe SuspensionPolicy),
                            _PolicySet'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show PolicySet where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField PolicySet "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicySet'machine (\ x__ y__ -> x__ {_PolicySet'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PolicySet "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicySet'machine (\ x__ y__ -> x__ {_PolicySet'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField PolicySet "policy" SuspensionPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicySet'policy (\ x__ y__ -> x__ {_PolicySet'policy = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField PolicySet "maybe'policy" (Prelude.Maybe SuspensionPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _PolicySet'policy (\ x__ y__ -> x__ {_PolicySet'policy = y__}))
        Prelude.id
instance Data.ProtoLens.Message PolicySet where
  messageName _ = Data.Text.pack "acyclic.machines.v1.PolicySet"
  packedMessageDescriptor _
    = "\n\
      \\tPolicySet\DC28\n\
      \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2=\n\
      \\ACKpolicy\CAN\STX \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\ACKpolicy"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor PolicySet
        policy__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "policy"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SuspensionPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'policy")) ::
              Data.ProtoLens.FieldDescriptor PolicySet
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machine__field_descriptor),
           (Data.ProtoLens.Tag 2, policy__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _PolicySet'_unknownFields
        (\ x__ y__ -> x__ {_PolicySet'_unknownFields = y__})
  defMessage
    = PolicySet'_constructor
        {_PolicySet'machine = Prelude.Nothing,
         _PolicySet'policy = Prelude.Nothing,
         _PolicySet'_unknownFields = []}
  parseMessage
    = let
        loop :: PolicySet -> Data.ProtoLens.Encoding.Bytes.Parser PolicySet
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
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "policy"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"policy") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "PolicySet"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'policy") _x
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
instance Control.DeepSeq.NFData PolicySet where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_PolicySet'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_PolicySet'machine x__)
                (Control.DeepSeq.deepseq (_PolicySet'policy x__) ()))
newtype PressureKind'UnrecognizedValue
  = PressureKind'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data PressureKind
  = PRESSURE_KIND_UNSPECIFIED |
    PRESSURE_KIND_CUSTOMER_BUDGET |
    PRESSURE_KIND_MACHINE_LIMIT |
    PRESSURE_KIND_SERVICE_SATURATION |
    PressureKind'Unrecognized !PressureKind'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum PressureKind where
  maybeToEnum 0 = Prelude.Just PRESSURE_KIND_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just PRESSURE_KIND_CUSTOMER_BUDGET
  maybeToEnum 2 = Prelude.Just PRESSURE_KIND_MACHINE_LIMIT
  maybeToEnum 3 = Prelude.Just PRESSURE_KIND_SERVICE_SATURATION
  maybeToEnum k
    = Prelude.Just
        (PressureKind'Unrecognized
           (PressureKind'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum PRESSURE_KIND_UNSPECIFIED = "PRESSURE_KIND_UNSPECIFIED"
  showEnum PRESSURE_KIND_CUSTOMER_BUDGET
    = "PRESSURE_KIND_CUSTOMER_BUDGET"
  showEnum PRESSURE_KIND_MACHINE_LIMIT
    = "PRESSURE_KIND_MACHINE_LIMIT"
  showEnum PRESSURE_KIND_SERVICE_SATURATION
    = "PRESSURE_KIND_SERVICE_SATURATION"
  showEnum
    (PressureKind'Unrecognized (PressureKind'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "PRESSURE_KIND_UNSPECIFIED"
    = Prelude.Just PRESSURE_KIND_UNSPECIFIED
    | (Prelude.==) k "PRESSURE_KIND_CUSTOMER_BUDGET"
    = Prelude.Just PRESSURE_KIND_CUSTOMER_BUDGET
    | (Prelude.==) k "PRESSURE_KIND_MACHINE_LIMIT"
    = Prelude.Just PRESSURE_KIND_MACHINE_LIMIT
    | (Prelude.==) k "PRESSURE_KIND_SERVICE_SATURATION"
    = Prelude.Just PRESSURE_KIND_SERVICE_SATURATION
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded PressureKind where
  minBound = PRESSURE_KIND_UNSPECIFIED
  maxBound = PRESSURE_KIND_SERVICE_SATURATION
instance Prelude.Enum PressureKind where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum PressureKind: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum PRESSURE_KIND_UNSPECIFIED = 0
  fromEnum PRESSURE_KIND_CUSTOMER_BUDGET = 1
  fromEnum PRESSURE_KIND_MACHINE_LIMIT = 2
  fromEnum PRESSURE_KIND_SERVICE_SATURATION = 3
  fromEnum
    (PressureKind'Unrecognized (PressureKind'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ PRESSURE_KIND_SERVICE_SATURATION
    = Prelude.error
        "PressureKind.succ: bad argument PRESSURE_KIND_SERVICE_SATURATION. This value would be out of bounds."
  succ PRESSURE_KIND_UNSPECIFIED = PRESSURE_KIND_CUSTOMER_BUDGET
  succ PRESSURE_KIND_CUSTOMER_BUDGET = PRESSURE_KIND_MACHINE_LIMIT
  succ PRESSURE_KIND_MACHINE_LIMIT = PRESSURE_KIND_SERVICE_SATURATION
  succ (PressureKind'Unrecognized _)
    = Prelude.error
        "PressureKind.succ: bad argument: unrecognized value"
  pred PRESSURE_KIND_UNSPECIFIED
    = Prelude.error
        "PressureKind.pred: bad argument PRESSURE_KIND_UNSPECIFIED. This value would be out of bounds."
  pred PRESSURE_KIND_CUSTOMER_BUDGET = PRESSURE_KIND_UNSPECIFIED
  pred PRESSURE_KIND_MACHINE_LIMIT = PRESSURE_KIND_CUSTOMER_BUDGET
  pred PRESSURE_KIND_SERVICE_SATURATION = PRESSURE_KIND_MACHINE_LIMIT
  pred (PressureKind'Unrecognized _)
    = Prelude.error
        "PressureKind.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault PressureKind where
  fieldDefault = PRESSURE_KIND_UNSPECIFIED
instance Control.DeepSeq.NFData PressureKind where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.major' @:: Lens' ProtocolVersion Data.Word.Word32@
         * 'Proto.Machines.V1.Machines_Fields.minor' @:: Lens' ProtocolVersion Data.Word.Word32@ -}
data ProtocolVersion
  = ProtocolVersion'_constructor {_ProtocolVersion'major :: !Data.Word.Word32,
                                  _ProtocolVersion'minor :: !Data.Word.Word32,
                                  _ProtocolVersion'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ProtocolVersion where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ProtocolVersion "major" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ProtocolVersion'major
           (\ x__ y__ -> x__ {_ProtocolVersion'major = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ProtocolVersion "minor" Data.Word.Word32 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ProtocolVersion'minor
           (\ x__ y__ -> x__ {_ProtocolVersion'minor = y__}))
        Prelude.id
instance Data.ProtoLens.Message ProtocolVersion where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.ProtocolVersion"
  packedMessageDescriptor _
    = "\n\
      \\SIProtocolVersion\DC2\DC4\n\
      \\ENQmajor\CAN\SOH \SOH(\rR\ENQmajor\DC2\DC4\n\
      \\ENQminor\CAN\STX \SOH(\rR\ENQminor"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        major__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "major"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"major")) ::
              Data.ProtoLens.FieldDescriptor ProtocolVersion
        minor__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "minor"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt32Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word32)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"minor")) ::
              Data.ProtoLens.FieldDescriptor ProtocolVersion
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, major__field_descriptor),
           (Data.ProtoLens.Tag 2, minor__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ProtocolVersion'_unknownFields
        (\ x__ y__ -> x__ {_ProtocolVersion'_unknownFields = y__})
  defMessage
    = ProtocolVersion'_constructor
        {_ProtocolVersion'major = Data.ProtoLens.fieldDefault,
         _ProtocolVersion'minor = Data.ProtoLens.fieldDefault,
         _ProtocolVersion'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ProtocolVersion
          -> Data.ProtoLens.Encoding.Bytes.Parser ProtocolVersion
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
                                       "major"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"major") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.fromIntegral
                                          Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "minor"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"minor") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ProtocolVersion"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"major") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"minor") _x
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
instance Control.DeepSeq.NFData ProtocolVersion where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ProtocolVersion'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ProtocolVersion'major x__)
                (Control.DeepSeq.deepseq (_ProtocolVersion'minor x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' QualifyImageRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' QualifyImageRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.image' @:: Lens' QualifyImageRequest Image@
         * 'Proto.Machines.V1.Machines_Fields.maybe'image' @:: Lens' QualifyImageRequest (Prelude.Maybe Image)@ -}
data QualifyImageRequest
  = QualifyImageRequest'_constructor {_QualifyImageRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                      _QualifyImageRequest'image :: !(Prelude.Maybe Image),
                                      _QualifyImageRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show QualifyImageRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField QualifyImageRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _QualifyImageRequest'protocol
           (\ x__ y__ -> x__ {_QualifyImageRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField QualifyImageRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _QualifyImageRequest'protocol
           (\ x__ y__ -> x__ {_QualifyImageRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField QualifyImageRequest "image" Image where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _QualifyImageRequest'image
           (\ x__ y__ -> x__ {_QualifyImageRequest'image = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField QualifyImageRequest "maybe'image" (Prelude.Maybe Image) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _QualifyImageRequest'image
           (\ x__ y__ -> x__ {_QualifyImageRequest'image = y__}))
        Prelude.id
instance Data.ProtoLens.Message QualifyImageRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.QualifyImageRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC3QualifyImageRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC20\n\
      \\ENQimage\CAN\STX \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor QualifyImageRequest
        image__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "image"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Image)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'image")) ::
              Data.ProtoLens.FieldDescriptor QualifyImageRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, image__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _QualifyImageRequest'_unknownFields
        (\ x__ y__ -> x__ {_QualifyImageRequest'_unknownFields = y__})
  defMessage
    = QualifyImageRequest'_constructor
        {_QualifyImageRequest'protocol = Prelude.Nothing,
         _QualifyImageRequest'image = Prelude.Nothing,
         _QualifyImageRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          QualifyImageRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser QualifyImageRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "image"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"image") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "QualifyImageRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'image") _x
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
instance Control.DeepSeq.NFData QualifyImageRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_QualifyImageRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_QualifyImageRequest'protocol x__)
                (Control.DeepSeq.deepseq (_QualifyImageRequest'image x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' RecoverRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' RecoverRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' RecoverRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' RecoverRequest (Prelude.Maybe IdempotencyKey)@ -}
data RecoverRequest
  = RecoverRequest'_constructor {_RecoverRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                 _RecoverRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                 _RecoverRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RecoverRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RecoverRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoverRequest'protocol
           (\ x__ y__ -> x__ {_RecoverRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RecoverRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoverRequest'protocol
           (\ x__ y__ -> x__ {_RecoverRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RecoverRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoverRequest'idempotencyKey
           (\ x__ y__ -> x__ {_RecoverRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RecoverRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoverRequest'idempotencyKey
           (\ x__ y__ -> x__ {_RecoverRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Message RecoverRequest where
  messageName _ = Data.Text.pack "acyclic.machines.v1.RecoverRequest"
  packedMessageDescriptor _
    = "\n\
      \\SORecoverRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor RecoverRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor RecoverRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RecoverRequest'_unknownFields
        (\ x__ y__ -> x__ {_RecoverRequest'_unknownFields = y__})
  defMessage
    = RecoverRequest'_constructor
        {_RecoverRequest'protocol = Prelude.Nothing,
         _RecoverRequest'idempotencyKey = Prelude.Nothing,
         _RecoverRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RecoverRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser RecoverRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
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
          (do loop Data.ProtoLens.defMessage) "RecoverRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
instance Control.DeepSeq.NFData RecoverRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RecoverRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RecoverRequest'protocol x__)
                (Control.DeepSeq.deepseq (_RecoverRequest'idempotencyKey x__) ()))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.operation' @:: Lens' RecoveredAdmission OperationId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'operation' @:: Lens' RecoveredAdmission (Prelude.Maybe OperationId)@
         * 'Proto.Machines.V1.Machines_Fields.maybe'result' @:: Lens' RecoveredAdmission (Prelude.Maybe RecoveredAdmission'Result)@
         * 'Proto.Machines.V1.Machines_Fields.maybe'create' @:: Lens' RecoveredAdmission (Prelude.Maybe MachineAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.create' @:: Lens' RecoveredAdmission MachineAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'checkpoint' @:: Lens' RecoveredAdmission (Prelude.Maybe CheckpointAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.checkpoint' @:: Lens' RecoveredAdmission CheckpointAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'fork' @:: Lens' RecoveredAdmission (Prelude.Maybe ForkAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.fork' @:: Lens' RecoveredAdmission ForkAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'suspend' @:: Lens' RecoveredAdmission (Prelude.Maybe MutationAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.suspend' @:: Lens' RecoveredAdmission MutationAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'wake' @:: Lens' RecoveredAdmission (Prelude.Maybe MutationAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.wake' @:: Lens' RecoveredAdmission MutationAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'destroyMachine' @:: Lens' RecoveredAdmission (Prelude.Maybe MutationAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.destroyMachine' @:: Lens' RecoveredAdmission MutationAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'setSuspensionPolicy' @:: Lens' RecoveredAdmission (Prelude.Maybe PolicyAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.setSuspensionPolicy' @:: Lens' RecoveredAdmission PolicyAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'destroyCheckpoint' @:: Lens' RecoveredAdmission (Prelude.Maybe MutationAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.destroyCheckpoint' @:: Lens' RecoveredAdmission MutationAdmission@
         * 'Proto.Machines.V1.Machines_Fields.maybe'forkMachine' @:: Lens' RecoveredAdmission (Prelude.Maybe ForkMachineAdmission)@
         * 'Proto.Machines.V1.Machines_Fields.forkMachine' @:: Lens' RecoveredAdmission ForkMachineAdmission@ -}
data RecoveredAdmission
  = RecoveredAdmission'_constructor {_RecoveredAdmission'operation :: !(Prelude.Maybe OperationId),
                                     _RecoveredAdmission'result :: !(Prelude.Maybe RecoveredAdmission'Result),
                                     _RecoveredAdmission'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RecoveredAdmission where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data RecoveredAdmission'Result
  = RecoveredAdmission'Create !MachineAdmission |
    RecoveredAdmission'Checkpoint !CheckpointAdmission |
    RecoveredAdmission'Fork !ForkAdmission |
    RecoveredAdmission'Suspend !MutationAdmission |
    RecoveredAdmission'Wake !MutationAdmission |
    RecoveredAdmission'DestroyMachine !MutationAdmission |
    RecoveredAdmission'SetSuspensionPolicy !PolicyAdmission |
    RecoveredAdmission'DestroyCheckpoint !MutationAdmission |
    RecoveredAdmission'ForkMachine !ForkMachineAdmission
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField RecoveredAdmission "operation" OperationId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'operation
           (\ x__ y__ -> x__ {_RecoveredAdmission'operation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'operation" (Prelude.Maybe OperationId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'operation
           (\ x__ y__ -> x__ {_RecoveredAdmission'operation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'result" (Prelude.Maybe RecoveredAdmission'Result) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'create" (Prelude.Maybe MachineAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'Create x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'Create y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "create" MachineAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'Create x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'Create y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'checkpoint" (Prelude.Maybe CheckpointAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'Checkpoint x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'Checkpoint y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "checkpoint" CheckpointAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'Checkpoint x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'Checkpoint y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'fork" (Prelude.Maybe ForkAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'Fork x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'Fork y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "fork" ForkAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'Fork x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'Fork y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'suspend" (Prelude.Maybe MutationAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'Suspend x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'Suspend y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "suspend" MutationAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'Suspend x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'Suspend y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'wake" (Prelude.Maybe MutationAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'Wake x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'Wake y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "wake" MutationAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'Wake x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'Wake y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'destroyMachine" (Prelude.Maybe MutationAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'DestroyMachine x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'DestroyMachine y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "destroyMachine" MutationAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'DestroyMachine x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'DestroyMachine y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'setSuspensionPolicy" (Prelude.Maybe PolicyAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'SetSuspensionPolicy x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__
              -> Prelude.fmap RecoveredAdmission'SetSuspensionPolicy y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "setSuspensionPolicy" PolicyAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'SetSuspensionPolicy x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__
                 -> Prelude.fmap RecoveredAdmission'SetSuspensionPolicy y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'destroyCheckpoint" (Prelude.Maybe MutationAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'DestroyCheckpoint x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'DestroyCheckpoint y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "destroyCheckpoint" MutationAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'DestroyCheckpoint x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'DestroyCheckpoint y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "maybe'forkMachine" (Prelude.Maybe ForkMachineAdmission) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RecoveredAdmission'ForkMachine x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RecoveredAdmission'ForkMachine y__))
instance Data.ProtoLens.Field.HasField RecoveredAdmission "forkMachine" ForkMachineAdmission where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RecoveredAdmission'result
           (\ x__ y__ -> x__ {_RecoveredAdmission'result = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RecoveredAdmission'ForkMachine x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RecoveredAdmission'ForkMachine y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message RecoveredAdmission where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.RecoveredAdmission"
  packedMessageDescriptor _
    = "\n\
      \\DC2RecoveredAdmission\DC2>\n\
      \\toperation\CAN\SOH \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2?\n\
      \\ACKcreate\CAN\STX \SOH(\v2%.acyclic.machines.v1.MachineAdmissionH\NULR\ACKcreate\DC2J\n\
      \\n\
      \checkpoint\CAN\ETX \SOH(\v2(.acyclic.machines.v1.CheckpointAdmissionH\NULR\n\
      \checkpoint\DC28\n\
      \\EOTfork\CAN\EOT \SOH(\v2\".acyclic.machines.v1.ForkAdmissionH\NULR\EOTfork\DC2B\n\
      \\asuspend\CAN\ENQ \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\asuspend\DC2<\n\
      \\EOTwake\CAN\ACK \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\EOTwake\DC2Q\n\
      \\SIdestroy_machine\CAN\a \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\SOdestroyMachine\DC2Z\n\
      \\NAKset_suspension_policy\CAN\b \SOH(\v2$.acyclic.machines.v1.PolicyAdmissionH\NULR\DC3setSuspensionPolicy\DC2W\n\
      \\DC2destroy_checkpoint\CAN\t \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\DC1destroyCheckpoint\DC2N\n\
      \\ffork_machine\CAN\n\
      \ \SOH(\v2).acyclic.machines.v1.ForkMachineAdmissionH\NULR\vforkMachineB\b\n\
      \\ACKresult"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        operation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "operation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor OperationId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'operation")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        create__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "create"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'create")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        checkpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CheckpointAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'checkpoint")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        fork__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fork"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ForkAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'fork")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        suspend__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suspend"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suspend")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        wake__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "wake"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'wake")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        destroyMachine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destroy_machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'destroyMachine")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        setSuspensionPolicy__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "set_suspension_policy"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor PolicyAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'setSuspensionPolicy")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        destroyCheckpoint__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "destroy_checkpoint"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MutationAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'destroyCheckpoint")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
        forkMachine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fork_machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ForkMachineAdmission)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'forkMachine")) ::
              Data.ProtoLens.FieldDescriptor RecoveredAdmission
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, operation__field_descriptor),
           (Data.ProtoLens.Tag 2, create__field_descriptor),
           (Data.ProtoLens.Tag 3, checkpoint__field_descriptor),
           (Data.ProtoLens.Tag 4, fork__field_descriptor),
           (Data.ProtoLens.Tag 5, suspend__field_descriptor),
           (Data.ProtoLens.Tag 6, wake__field_descriptor),
           (Data.ProtoLens.Tag 7, destroyMachine__field_descriptor),
           (Data.ProtoLens.Tag 8, setSuspensionPolicy__field_descriptor),
           (Data.ProtoLens.Tag 9, destroyCheckpoint__field_descriptor),
           (Data.ProtoLens.Tag 10, forkMachine__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RecoveredAdmission'_unknownFields
        (\ x__ y__ -> x__ {_RecoveredAdmission'_unknownFields = y__})
  defMessage
    = RecoveredAdmission'_constructor
        {_RecoveredAdmission'operation = Prelude.Nothing,
         _RecoveredAdmission'result = Prelude.Nothing,
         _RecoveredAdmission'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RecoveredAdmission
          -> Data.ProtoLens.Encoding.Bytes.Parser RecoveredAdmission
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
                                       "operation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"operation") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "create"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"create") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "checkpoint"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"checkpoint") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "fork"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"fork") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "suspend"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"suspend") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "wake"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"wake") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "destroy_machine"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"destroyMachine") y x)
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "set_suspension_policy"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"setSuspensionPolicy") y x)
                        74
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "destroy_checkpoint"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"destroyCheckpoint") y x)
                        82
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "fork_machine"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"forkMachine") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RecoveredAdmission"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view
                    (Data.ProtoLens.Field.field @"maybe'operation") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'result") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just (RecoveredAdmission'Create v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'Checkpoint v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'Fork v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'Suspend v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'Wake v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'DestroyMachine v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'SetSuspensionPolicy v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 66)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'DestroyCheckpoint v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 74)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RecoveredAdmission'ForkMachine v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 82)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData RecoveredAdmission where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RecoveredAdmission'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RecoveredAdmission'operation x__)
                (Control.DeepSeq.deepseq (_RecoveredAdmission'result x__) ()))
instance Control.DeepSeq.NFData RecoveredAdmission'Result where
  rnf (RecoveredAdmission'Create x__) = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'Checkpoint x__) = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'Fork x__) = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'Suspend x__) = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'Wake x__) = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'DestroyMachine x__)
    = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'SetSuspensionPolicy x__)
    = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'DestroyCheckpoint x__)
    = Control.DeepSeq.rnf x__
  rnf (RecoveredAdmission'ForkMachine x__) = Control.DeepSeq.rnf x__
_RecoveredAdmission'Create ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result MachineAdmission
_RecoveredAdmission'Create
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'Create
      (\ p__
         -> case p__ of
              (RecoveredAdmission'Create p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'Checkpoint ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result CheckpointAdmission
_RecoveredAdmission'Checkpoint
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'Checkpoint
      (\ p__
         -> case p__ of
              (RecoveredAdmission'Checkpoint p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'Fork ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result ForkAdmission
_RecoveredAdmission'Fork
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'Fork
      (\ p__
         -> case p__ of
              (RecoveredAdmission'Fork p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'Suspend ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result MutationAdmission
_RecoveredAdmission'Suspend
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'Suspend
      (\ p__
         -> case p__ of
              (RecoveredAdmission'Suspend p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'Wake ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result MutationAdmission
_RecoveredAdmission'Wake
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'Wake
      (\ p__
         -> case p__ of
              (RecoveredAdmission'Wake p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'DestroyMachine ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result MutationAdmission
_RecoveredAdmission'DestroyMachine
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'DestroyMachine
      (\ p__
         -> case p__ of
              (RecoveredAdmission'DestroyMachine p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'SetSuspensionPolicy ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result PolicyAdmission
_RecoveredAdmission'SetSuspensionPolicy
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'SetSuspensionPolicy
      (\ p__
         -> case p__ of
              (RecoveredAdmission'SetSuspensionPolicy p__val)
                -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'DestroyCheckpoint ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result MutationAdmission
_RecoveredAdmission'DestroyCheckpoint
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'DestroyCheckpoint
      (\ p__
         -> case p__ of
              (RecoveredAdmission'DestroyCheckpoint p__val)
                -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RecoveredAdmission'ForkMachine ::
  Data.ProtoLens.Prism.Prism' RecoveredAdmission'Result ForkMachineAdmission
_RecoveredAdmission'ForkMachine
  = Data.ProtoLens.Prism.prism'
      RecoveredAdmission'ForkMachine
      (\ p__
         -> case p__ of
              (RecoveredAdmission'ForkMachine p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' SetSuspensionPolicyRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' SetSuspensionPolicyRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.idempotencyKey' @:: Lens' SetSuspensionPolicyRequest IdempotencyKey@
         * 'Proto.Machines.V1.Machines_Fields.maybe'idempotencyKey' @:: Lens' SetSuspensionPolicyRequest (Prelude.Maybe IdempotencyKey)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' SetSuspensionPolicyRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' SetSuspensionPolicyRequest (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.policy' @:: Lens' SetSuspensionPolicyRequest SuspensionPolicy@
         * 'Proto.Machines.V1.Machines_Fields.maybe'policy' @:: Lens' SetSuspensionPolicyRequest (Prelude.Maybe SuspensionPolicy)@ -}
data SetSuspensionPolicyRequest
  = SetSuspensionPolicyRequest'_constructor {_SetSuspensionPolicyRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                                             _SetSuspensionPolicyRequest'idempotencyKey :: !(Prelude.Maybe IdempotencyKey),
                                             _SetSuspensionPolicyRequest'machine :: !(Prelude.Maybe MachineId),
                                             _SetSuspensionPolicyRequest'policy :: !(Prelude.Maybe SuspensionPolicy),
                                             _SetSuspensionPolicyRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SetSuspensionPolicyRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'protocol
           (\ x__ y__ -> x__ {_SetSuspensionPolicyRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'protocol
           (\ x__ y__ -> x__ {_SetSuspensionPolicyRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "idempotencyKey" IdempotencyKey where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_SetSuspensionPolicyRequest'idempotencyKey = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "maybe'idempotencyKey" (Prelude.Maybe IdempotencyKey) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'idempotencyKey
           (\ x__ y__
              -> x__ {_SetSuspensionPolicyRequest'idempotencyKey = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'machine
           (\ x__ y__ -> x__ {_SetSuspensionPolicyRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'machine
           (\ x__ y__ -> x__ {_SetSuspensionPolicyRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "policy" SuspensionPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'policy
           (\ x__ y__ -> x__ {_SetSuspensionPolicyRequest'policy = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField SetSuspensionPolicyRequest "maybe'policy" (Prelude.Maybe SuspensionPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SetSuspensionPolicyRequest'policy
           (\ x__ y__ -> x__ {_SetSuspensionPolicyRequest'policy = y__}))
        Prelude.id
instance Data.ProtoLens.Message SetSuspensionPolicyRequest where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.SetSuspensionPolicyRequest"
  packedMessageDescriptor _
    = "\n\
      \\SUBSetSuspensionPolicyRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
      \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
      \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2=\n\
      \\ACKpolicy\CAN\EOT \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\ACKpolicy"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor SetSuspensionPolicyRequest
        idempotencyKey__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idempotency_key"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdempotencyKey)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idempotencyKey")) ::
              Data.ProtoLens.FieldDescriptor SetSuspensionPolicyRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor SetSuspensionPolicyRequest
        policy__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "policy"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor SuspensionPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'policy")) ::
              Data.ProtoLens.FieldDescriptor SetSuspensionPolicyRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, idempotencyKey__field_descriptor),
           (Data.ProtoLens.Tag 3, machine__field_descriptor),
           (Data.ProtoLens.Tag 4, policy__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SetSuspensionPolicyRequest'_unknownFields
        (\ x__ y__
           -> x__ {_SetSuspensionPolicyRequest'_unknownFields = y__})
  defMessage
    = SetSuspensionPolicyRequest'_constructor
        {_SetSuspensionPolicyRequest'protocol = Prelude.Nothing,
         _SetSuspensionPolicyRequest'idempotencyKey = Prelude.Nothing,
         _SetSuspensionPolicyRequest'machine = Prelude.Nothing,
         _SetSuspensionPolicyRequest'policy = Prelude.Nothing,
         _SetSuspensionPolicyRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SetSuspensionPolicyRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser SetSuspensionPolicyRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idempotency_key"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idempotencyKey") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "policy"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"policy") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SetSuspensionPolicyRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                       (Data.ProtoLens.Field.field @"maybe'idempotencyKey") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'policy") _x
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
instance Control.DeepSeq.NFData SetSuspensionPolicyRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SetSuspensionPolicyRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_SetSuspensionPolicyRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_SetSuspensionPolicyRequest'idempotencyKey x__)
                   (Control.DeepSeq.deepseq
                      (_SetSuspensionPolicyRequest'machine x__)
                      (Control.DeepSeq.deepseq
                         (_SetSuspensionPolicyRequest'policy x__) ()))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.maybe'policy' @:: Lens' SuspensionPolicy (Prelude.Maybe SuspensionPolicy'Policy)@
         * 'Proto.Machines.V1.Machines_Fields.maybe'manual' @:: Lens' SuspensionPolicy (Prelude.Maybe Prelude.Bool)@
         * 'Proto.Machines.V1.Machines_Fields.manual' @:: Lens' SuspensionPolicy Prelude.Bool@
         * 'Proto.Machines.V1.Machines_Fields.maybe'afterIdleMs' @:: Lens' SuspensionPolicy (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Machines.V1.Machines_Fields.afterIdleMs' @:: Lens' SuspensionPolicy Data.Word.Word64@ -}
data SuspensionPolicy
  = SuspensionPolicy'_constructor {_SuspensionPolicy'policy :: !(Prelude.Maybe SuspensionPolicy'Policy),
                                   _SuspensionPolicy'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show SuspensionPolicy where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data SuspensionPolicy'Policy
  = SuspensionPolicy'Manual !Prelude.Bool |
    SuspensionPolicy'AfterIdleMs !Data.Word.Word64
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField SuspensionPolicy "maybe'policy" (Prelude.Maybe SuspensionPolicy'Policy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SuspensionPolicy'policy
           (\ x__ y__ -> x__ {_SuspensionPolicy'policy = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField SuspensionPolicy "maybe'manual" (Prelude.Maybe Prelude.Bool) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SuspensionPolicy'policy
           (\ x__ y__ -> x__ {_SuspensionPolicy'policy = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (SuspensionPolicy'Manual x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap SuspensionPolicy'Manual y__))
instance Data.ProtoLens.Field.HasField SuspensionPolicy "manual" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SuspensionPolicy'policy
           (\ x__ y__ -> x__ {_SuspensionPolicy'policy = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (SuspensionPolicy'Manual x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap SuspensionPolicy'Manual y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField SuspensionPolicy "maybe'afterIdleMs" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SuspensionPolicy'policy
           (\ x__ y__ -> x__ {_SuspensionPolicy'policy = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (SuspensionPolicy'AfterIdleMs x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap SuspensionPolicy'AfterIdleMs y__))
instance Data.ProtoLens.Field.HasField SuspensionPolicy "afterIdleMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _SuspensionPolicy'policy
           (\ x__ y__ -> x__ {_SuspensionPolicy'policy = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (SuspensionPolicy'AfterIdleMs x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap SuspensionPolicy'AfterIdleMs y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message SuspensionPolicy where
  messageName _
    = Data.Text.pack "acyclic.machines.v1.SuspensionPolicy"
  packedMessageDescriptor _
    = "\n\
      \\DLESuspensionPolicy\DC2\CAN\n\
      \\ACKmanual\CAN\SOH \SOH(\bH\NULR\ACKmanual\DC2$\n\
      \\rafter_idle_ms\CAN\STX \SOH(\EOTH\NULR\vafterIdleMsB\b\n\
      \\ACKpolicy"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        manual__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "manual"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'manual")) ::
              Data.ProtoLens.FieldDescriptor SuspensionPolicy
        afterIdleMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "after_idle_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'afterIdleMs")) ::
              Data.ProtoLens.FieldDescriptor SuspensionPolicy
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, manual__field_descriptor),
           (Data.ProtoLens.Tag 2, afterIdleMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _SuspensionPolicy'_unknownFields
        (\ x__ y__ -> x__ {_SuspensionPolicy'_unknownFields = y__})
  defMessage
    = SuspensionPolicy'_constructor
        {_SuspensionPolicy'policy = Prelude.Nothing,
         _SuspensionPolicy'_unknownFields = []}
  parseMessage
    = let
        loop ::
          SuspensionPolicy
          -> Data.ProtoLens.Encoding.Bytes.Parser SuspensionPolicy
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
                                       "manual"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"manual") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "after_idle_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"afterIdleMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "SuspensionPolicy"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'policy") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (SuspensionPolicy'Manual v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                       ((Prelude..)
                          Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                          v)
                (Prelude.Just (SuspensionPolicy'AfterIdleMs v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData SuspensionPolicy where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_SuspensionPolicy'_unknownFields x__)
             (Control.DeepSeq.deepseq (_SuspensionPolicy'policy x__) ())
instance Control.DeepSeq.NFData SuspensionPolicy'Policy where
  rnf (SuspensionPolicy'Manual x__) = Control.DeepSeq.rnf x__
  rnf (SuspensionPolicy'AfterIdleMs x__) = Control.DeepSeq.rnf x__
_SuspensionPolicy'Manual ::
  Data.ProtoLens.Prism.Prism' SuspensionPolicy'Policy Prelude.Bool
_SuspensionPolicy'Manual
  = Data.ProtoLens.Prism.prism'
      SuspensionPolicy'Manual
      (\ p__
         -> case p__ of
              (SuspensionPolicy'Manual p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_SuspensionPolicy'AfterIdleMs ::
  Data.ProtoLens.Prism.Prism' SuspensionPolicy'Policy Data.Word.Word64
_SuspensionPolicy'AfterIdleMs
  = Data.ProtoLens.Prism.prism'
      SuspensionPolicy'AfterIdleMs
      (\ p__
         -> case p__ of
              (SuspensionPolicy'AfterIdleMs p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' UsageReceipt MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' UsageReceipt (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.startUnixMs' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.endUnixMs' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.elasticCpuNs' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.dedicatedCpuNs' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.privateResidentByteSeconds' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.durablePrivateBytes' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.lineageReceiptSha256' @:: Lens' UsageReceipt Data.ByteString.ByteString@
         * 'Proto.Machines.V1.Machines_Fields.egressBytes' @:: Lens' UsageReceipt Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.receipt' @:: Lens' UsageReceipt Data.ByteString.ByteString@ -}
data UsageReceipt
  = UsageReceipt'_constructor {_UsageReceipt'machine :: !(Prelude.Maybe MachineId),
                               _UsageReceipt'startUnixMs :: !Data.Word.Word64,
                               _UsageReceipt'endUnixMs :: !Data.Word.Word64,
                               _UsageReceipt'elasticCpuNs :: !Data.Word.Word64,
                               _UsageReceipt'dedicatedCpuNs :: !Data.Word.Word64,
                               _UsageReceipt'privateResidentByteSeconds :: !Data.Word.Word64,
                               _UsageReceipt'durablePrivateBytes :: !Data.Word.Word64,
                               _UsageReceipt'lineageReceiptSha256 :: !Data.ByteString.ByteString,
                               _UsageReceipt'egressBytes :: !Data.Word.Word64,
                               _UsageReceipt'receipt :: !Data.ByteString.ByteString,
                               _UsageReceipt'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UsageReceipt where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UsageReceipt "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'machine
           (\ x__ y__ -> x__ {_UsageReceipt'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UsageReceipt "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'machine
           (\ x__ y__ -> x__ {_UsageReceipt'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "startUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'startUnixMs
           (\ x__ y__ -> x__ {_UsageReceipt'startUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "endUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'endUnixMs
           (\ x__ y__ -> x__ {_UsageReceipt'endUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "elasticCpuNs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'elasticCpuNs
           (\ x__ y__ -> x__ {_UsageReceipt'elasticCpuNs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "dedicatedCpuNs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'dedicatedCpuNs
           (\ x__ y__ -> x__ {_UsageReceipt'dedicatedCpuNs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "privateResidentByteSeconds" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'privateResidentByteSeconds
           (\ x__ y__
              -> x__ {_UsageReceipt'privateResidentByteSeconds = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "durablePrivateBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'durablePrivateBytes
           (\ x__ y__ -> x__ {_UsageReceipt'durablePrivateBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "lineageReceiptSha256" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'lineageReceiptSha256
           (\ x__ y__ -> x__ {_UsageReceipt'lineageReceiptSha256 = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "egressBytes" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'egressBytes
           (\ x__ y__ -> x__ {_UsageReceipt'egressBytes = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "receipt" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'receipt
           (\ x__ y__ -> x__ {_UsageReceipt'receipt = y__}))
        Prelude.id
instance Data.ProtoLens.Message UsageReceipt where
  messageName _ = Data.Text.pack "acyclic.machines.v1.UsageReceipt"
  packedMessageDescriptor _
    = "\n\
      \\fUsageReceipt\DC28\n\
      \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\"\n\
      \\rstart_unix_ms\CAN\STX \SOH(\EOTR\vstartUnixMs\DC2\RS\n\
      \\vend_unix_ms\CAN\ETX \SOH(\EOTR\tendUnixMs\DC2$\n\
      \\SOelastic_cpu_ns\CAN\EOT \SOH(\EOTR\felasticCpuNs\DC2(\n\
      \\DLEdedicated_cpu_ns\CAN\ENQ \SOH(\EOTR\SOdedicatedCpuNs\DC2A\n\
      \\GSprivate_resident_byte_seconds\CAN\ACK \SOH(\EOTR\SUBprivateResidentByteSeconds\DC22\n\
      \\NAKdurable_private_bytes\CAN\a \SOH(\EOTR\DC3durablePrivateBytes\DC24\n\
      \\SYNlineage_receipt_sha256\CAN\v \SOH(\fR\DC4lineageReceiptSha256\DC2!\n\
      \\fegress_bytes\CAN\t \SOH(\EOTR\vegressBytes\DC2\CAN\n\
      \\areceipt\CAN\n\
      \ \SOH(\fR\areceiptJ\EOT\b\b\DLE\tR\DC4lineage_shared_bytes"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        startUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "start_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"startUnixMs")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        endUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "end_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"endUnixMs")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        elasticCpuNs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "elastic_cpu_ns"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"elasticCpuNs")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        dedicatedCpuNs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "dedicated_cpu_ns"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"dedicatedCpuNs")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        privateResidentByteSeconds__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "private_resident_byte_seconds"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"privateResidentByteSeconds")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        durablePrivateBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "durable_private_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"durablePrivateBytes")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        lineageReceiptSha256__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "lineage_receipt_sha256"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"lineageReceiptSha256")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        egressBytes__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "egress_bytes"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"egressBytes")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        receipt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "receipt"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"receipt")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, machine__field_descriptor),
           (Data.ProtoLens.Tag 2, startUnixMs__field_descriptor),
           (Data.ProtoLens.Tag 3, endUnixMs__field_descriptor),
           (Data.ProtoLens.Tag 4, elasticCpuNs__field_descriptor),
           (Data.ProtoLens.Tag 5, dedicatedCpuNs__field_descriptor),
           (Data.ProtoLens.Tag 6, 
            privateResidentByteSeconds__field_descriptor),
           (Data.ProtoLens.Tag 7, durablePrivateBytes__field_descriptor),
           (Data.ProtoLens.Tag 11, lineageReceiptSha256__field_descriptor),
           (Data.ProtoLens.Tag 9, egressBytes__field_descriptor),
           (Data.ProtoLens.Tag 10, receipt__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UsageReceipt'_unknownFields
        (\ x__ y__ -> x__ {_UsageReceipt'_unknownFields = y__})
  defMessage
    = UsageReceipt'_constructor
        {_UsageReceipt'machine = Prelude.Nothing,
         _UsageReceipt'startUnixMs = Data.ProtoLens.fieldDefault,
         _UsageReceipt'endUnixMs = Data.ProtoLens.fieldDefault,
         _UsageReceipt'elasticCpuNs = Data.ProtoLens.fieldDefault,
         _UsageReceipt'dedicatedCpuNs = Data.ProtoLens.fieldDefault,
         _UsageReceipt'privateResidentByteSeconds = Data.ProtoLens.fieldDefault,
         _UsageReceipt'durablePrivateBytes = Data.ProtoLens.fieldDefault,
         _UsageReceipt'lineageReceiptSha256 = Data.ProtoLens.fieldDefault,
         _UsageReceipt'egressBytes = Data.ProtoLens.fieldDefault,
         _UsageReceipt'receipt = Data.ProtoLens.fieldDefault,
         _UsageReceipt'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UsageReceipt -> Data.ProtoLens.Encoding.Bytes.Parser UsageReceipt
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
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "start_unix_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"startUnixMs") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "end_unix_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"endUnixMs") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "elastic_cpu_ns"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"elasticCpuNs") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "dedicated_cpu_ns"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"dedicatedCpuNs") y x)
                        48
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "private_resident_byte_seconds"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"privateResidentByteSeconds") y x)
                        56
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "durable_private_bytes"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"durablePrivateBytes") y x)
                        90
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "lineage_receipt_sha256"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"lineageReceiptSha256") y x)
                        72
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "egress_bytes"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"egressBytes") y x)
                        82
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "receipt"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"receipt") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UsageReceipt"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"startUnixMs") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"endUnixMs") _x
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"elasticCpuNs") _x
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
                                  (Data.ProtoLens.Field.field @"dedicatedCpuNs") _x
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
                                     (Data.ProtoLens.Field.field @"privateResidentByteSeconds") _x
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
                                        (Data.ProtoLens.Field.field @"durablePrivateBytes") _x
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
                                           (Data.ProtoLens.Field.field @"lineageReceiptSha256") _x
                                   in
                                     if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                         Data.Monoid.mempty
                                     else
                                         (Data.Monoid.<>)
                                           (Data.ProtoLens.Encoding.Bytes.putVarInt 90)
                                           ((\ bs
                                               -> (Data.Monoid.<>)
                                                    (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                       (Prelude.fromIntegral
                                                          (Data.ByteString.length bs)))
                                                    (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                              _v))
                                  ((Data.Monoid.<>)
                                     (let
                                        _v
                                          = Lens.Family2.view
                                              (Data.ProtoLens.Field.field @"egressBytes") _x
                                      in
                                        if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                            Data.Monoid.mempty
                                        else
                                            (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt 72)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                                     ((Data.Monoid.<>)
                                        (let
                                           _v
                                             = Lens.Family2.view
                                                 (Data.ProtoLens.Field.field @"receipt") _x
                                         in
                                           if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                               Data.Monoid.mempty
                                           else
                                               (Data.Monoid.<>)
                                                 (Data.ProtoLens.Encoding.Bytes.putVarInt 82)
                                                 ((\ bs
                                                     -> (Data.Monoid.<>)
                                                          (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                             (Prelude.fromIntegral
                                                                (Data.ByteString.length bs)))
                                                          (Data.ProtoLens.Encoding.Bytes.putBytes
                                                             bs))
                                                    _v))
                                        (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                           (Lens.Family2.view
                                              Data.ProtoLens.unknownFields _x)))))))))))
instance Control.DeepSeq.NFData UsageReceipt where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UsageReceipt'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_UsageReceipt'machine x__)
                (Control.DeepSeq.deepseq
                   (_UsageReceipt'startUnixMs x__)
                   (Control.DeepSeq.deepseq
                      (_UsageReceipt'endUnixMs x__)
                      (Control.DeepSeq.deepseq
                         (_UsageReceipt'elasticCpuNs x__)
                         (Control.DeepSeq.deepseq
                            (_UsageReceipt'dedicatedCpuNs x__)
                            (Control.DeepSeq.deepseq
                               (_UsageReceipt'privateResidentByteSeconds x__)
                               (Control.DeepSeq.deepseq
                                  (_UsageReceipt'durablePrivateBytes x__)
                                  (Control.DeepSeq.deepseq
                                     (_UsageReceipt'lineageReceiptSha256 x__)
                                     (Control.DeepSeq.deepseq
                                        (_UsageReceipt'egressBytes x__)
                                        (Control.DeepSeq.deepseq
                                           (_UsageReceipt'receipt x__) ()))))))))))
{- | Fields :
     
         * 'Proto.Machines.V1.Machines_Fields.protocol' @:: Lens' UsageRequest ProtocolVersion@
         * 'Proto.Machines.V1.Machines_Fields.maybe'protocol' @:: Lens' UsageRequest (Prelude.Maybe ProtocolVersion)@
         * 'Proto.Machines.V1.Machines_Fields.machine' @:: Lens' UsageRequest MachineId@
         * 'Proto.Machines.V1.Machines_Fields.maybe'machine' @:: Lens' UsageRequest (Prelude.Maybe MachineId)@
         * 'Proto.Machines.V1.Machines_Fields.startUnixMs' @:: Lens' UsageRequest Data.Word.Word64@
         * 'Proto.Machines.V1.Machines_Fields.endUnixMs' @:: Lens' UsageRequest Data.Word.Word64@ -}
data UsageRequest
  = UsageRequest'_constructor {_UsageRequest'protocol :: !(Prelude.Maybe ProtocolVersion),
                               _UsageRequest'machine :: !(Prelude.Maybe MachineId),
                               _UsageRequest'startUnixMs :: !Data.Word.Word64,
                               _UsageRequest'endUnixMs :: !Data.Word.Word64,
                               _UsageRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UsageRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UsageRequest "protocol" ProtocolVersion where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageRequest'protocol
           (\ x__ y__ -> x__ {_UsageRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UsageRequest "maybe'protocol" (Prelude.Maybe ProtocolVersion) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageRequest'protocol
           (\ x__ y__ -> x__ {_UsageRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageRequest "machine" MachineId where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageRequest'machine
           (\ x__ y__ -> x__ {_UsageRequest'machine = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UsageRequest "maybe'machine" (Prelude.Maybe MachineId) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageRequest'machine
           (\ x__ y__ -> x__ {_UsageRequest'machine = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageRequest "startUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageRequest'startUnixMs
           (\ x__ y__ -> x__ {_UsageRequest'startUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageRequest "endUnixMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageRequest'endUnixMs
           (\ x__ y__ -> x__ {_UsageRequest'endUnixMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message UsageRequest where
  messageName _ = Data.Text.pack "acyclic.machines.v1.UsageRequest"
  packedMessageDescriptor _
    = "\n\
      \\fUsageRequest\DC2@\n\
      \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC28\n\
      \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\"\n\
      \\rstart_unix_ms\CAN\ETX \SOH(\EOTR\vstartUnixMs\DC2\RS\n\
      \\vend_unix_ms\CAN\EOT \SOH(\EOTR\tendUnixMs"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolVersion)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor UsageRequest
        machine__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "machine"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor MachineId)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'machine")) ::
              Data.ProtoLens.FieldDescriptor UsageRequest
        startUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "start_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"startUnixMs")) ::
              Data.ProtoLens.FieldDescriptor UsageRequest
        endUnixMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "end_unix_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"endUnixMs")) ::
              Data.ProtoLens.FieldDescriptor UsageRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, machine__field_descriptor),
           (Data.ProtoLens.Tag 3, startUnixMs__field_descriptor),
           (Data.ProtoLens.Tag 4, endUnixMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UsageRequest'_unknownFields
        (\ x__ y__ -> x__ {_UsageRequest'_unknownFields = y__})
  defMessage
    = UsageRequest'_constructor
        {_UsageRequest'protocol = Prelude.Nothing,
         _UsageRequest'machine = Prelude.Nothing,
         _UsageRequest'startUnixMs = Data.ProtoLens.fieldDefault,
         _UsageRequest'endUnixMs = Data.ProtoLens.fieldDefault,
         _UsageRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          UsageRequest -> Data.ProtoLens.Encoding.Bytes.Parser UsageRequest
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
                                       "protocol"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"protocol") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "machine"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"machine") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "start_unix_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"startUnixMs") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "end_unix_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"endUnixMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "UsageRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'protocol") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'machine") _x
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
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"startUnixMs") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"endUnixMs") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData UsageRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UsageRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_UsageRequest'protocol x__)
                (Control.DeepSeq.deepseq
                   (_UsageRequest'machine x__)
                   (Control.DeepSeq.deepseq
                      (_UsageRequest'startUnixMs x__)
                      (Control.DeepSeq.deepseq (_UsageRequest'endUnixMs x__) ()))))
data MachinesService = MachinesService {}
instance Data.ProtoLens.Service.Types.Service MachinesService where
  type ServiceName MachinesService = "MachinesService"
  type ServicePackage MachinesService = "acyclic.machines.v1"
  type ServiceMethods MachinesService = '["cancel",
                                          "checkpoint",
                                          "create",
                                          "destroyCheckpoint",
                                          "destroyMachine",
                                          "events",
                                          "fork",
                                          "forkMachine",
                                          "inspectCheckpoint",
                                          "inspectMachine",
                                          "inspectOperation",
                                          "listMachines",
                                          "qualifyImage",
                                          "recover",
                                          "setSuspensionPolicy",
                                          "suspend",
                                          "usage",
                                          "wake",
                                          "watchOperation"]
  packedServiceDescriptor _
    = "\n\
      \\SIMachinesService\DC2a\n\
      \\fQualifyImage\DC2(.acyclic.machines.v1.QualifyImageRequest\SUB'.acyclic.machines.v1.ImageQualification\DC2Z\n\
      \\ACKCreate\DC2).acyclic.machines.v1.CreateMachineRequest\SUB%.acyclic.machines.v1.MachineAdmission\DC2e\n\
      \\n\
      \Checkpoint\DC2-.acyclic.machines.v1.CheckpointMachineRequest\SUB(.acyclic.machines.v1.CheckpointAdmission\DC2V\n\
      \\EOTFork\DC2*.acyclic.machines.v1.ForkCheckpointRequest\SUB\".acyclic.machines.v1.ForkAdmission\DC2a\n\
      \\vForkMachine\DC2'.acyclic.machines.v1.ForkMachineRequest\SUB).acyclic.machines.v1.ForkMachineAdmission\DC2^\n\
      \\aSuspend\DC2+.acyclic.machines.v1.MachineMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2[\n\
      \\EOTWake\DC2+.acyclic.machines.v1.MachineMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2l\n\
      \\DC3SetSuspensionPolicy\DC2/.acyclic.machines.v1.SetSuspensionPolicyRequest\SUB$.acyclic.machines.v1.PolicyAdmission\DC2e\n\
      \\SODestroyMachine\DC2+.acyclic.machines.v1.MachineMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2k\n\
      \\DC1DestroyCheckpoint\DC2..acyclic.machines.v1.CheckpointMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2W\n\
      \\aRecover\DC2#.acyclic.machines.v1.RecoverRequest\SUB'.acyclic.machines.v1.RecoveredAdmission\DC2_\n\
      \\SOInspectMachine\DC2*.acyclic.machines.v1.InspectMachineRequest\SUB!.acyclic.machines.v1.MachineState\DC2h\n\
      \\DC1InspectCheckpoint\DC2-.acyclic.machines.v1.InspectCheckpointRequest\SUB$.acyclic.machines.v1.CheckpointState\DC2Z\n\
      \\fListMachines\DC2(.acyclic.machines.v1.ListMachinesRequest\SUB .acyclic.machines.v1.MachinePage\DC2L\n\
      \\ACKEvents\DC2\".acyclic.machines.v1.EventsRequest\SUB\RS.acyclic.machines.v1.EventPage\DC2M\n\
      \\ENQUsage\DC2!.acyclic.machines.v1.UsageRequest\SUB!.acyclic.machines.v1.UsageReceipt\DC2T\n\
      \\ACKCancel\DC2%.acyclic.machines.v1.OperationRequest\SUB#.acyclic.machines.v1.OperationState\DC2^\n\
      \\DLEInspectOperation\DC2%.acyclic.machines.v1.OperationRequest\SUB#.acyclic.machines.v1.OperationState\DC2^\n\
      \\SOWatchOperation\DC2%.acyclic.machines.v1.OperationRequest\SUB#.acyclic.machines.v1.OperationState0\SOH"
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "qualifyImage" where
  type MethodName MachinesService "qualifyImage" = "QualifyImage"
  type MethodInput MachinesService "qualifyImage" = QualifyImageRequest
  type MethodOutput MachinesService "qualifyImage" = ImageQualification
  type MethodStreamingType MachinesService "qualifyImage" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "create" where
  type MethodName MachinesService "create" = "Create"
  type MethodInput MachinesService "create" = CreateMachineRequest
  type MethodOutput MachinesService "create" = MachineAdmission
  type MethodStreamingType MachinesService "create" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "checkpoint" where
  type MethodName MachinesService "checkpoint" = "Checkpoint"
  type MethodInput MachinesService "checkpoint" = CheckpointMachineRequest
  type MethodOutput MachinesService "checkpoint" = CheckpointAdmission
  type MethodStreamingType MachinesService "checkpoint" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "fork" where
  type MethodName MachinesService "fork" = "Fork"
  type MethodInput MachinesService "fork" = ForkCheckpointRequest
  type MethodOutput MachinesService "fork" = ForkAdmission
  type MethodStreamingType MachinesService "fork" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "forkMachine" where
  type MethodName MachinesService "forkMachine" = "ForkMachine"
  type MethodInput MachinesService "forkMachine" = ForkMachineRequest
  type MethodOutput MachinesService "forkMachine" = ForkMachineAdmission
  type MethodStreamingType MachinesService "forkMachine" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "suspend" where
  type MethodName MachinesService "suspend" = "Suspend"
  type MethodInput MachinesService "suspend" = MachineMutationRequest
  type MethodOutput MachinesService "suspend" = MutationAdmission
  type MethodStreamingType MachinesService "suspend" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "wake" where
  type MethodName MachinesService "wake" = "Wake"
  type MethodInput MachinesService "wake" = MachineMutationRequest
  type MethodOutput MachinesService "wake" = MutationAdmission
  type MethodStreamingType MachinesService "wake" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "setSuspensionPolicy" where
  type MethodName MachinesService "setSuspensionPolicy" = "SetSuspensionPolicy"
  type MethodInput MachinesService "setSuspensionPolicy" = SetSuspensionPolicyRequest
  type MethodOutput MachinesService "setSuspensionPolicy" = PolicyAdmission
  type MethodStreamingType MachinesService "setSuspensionPolicy" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "destroyMachine" where
  type MethodName MachinesService "destroyMachine" = "DestroyMachine"
  type MethodInput MachinesService "destroyMachine" = MachineMutationRequest
  type MethodOutput MachinesService "destroyMachine" = MutationAdmission
  type MethodStreamingType MachinesService "destroyMachine" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "destroyCheckpoint" where
  type MethodName MachinesService "destroyCheckpoint" = "DestroyCheckpoint"
  type MethodInput MachinesService "destroyCheckpoint" = CheckpointMutationRequest
  type MethodOutput MachinesService "destroyCheckpoint" = MutationAdmission
  type MethodStreamingType MachinesService "destroyCheckpoint" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "recover" where
  type MethodName MachinesService "recover" = "Recover"
  type MethodInput MachinesService "recover" = RecoverRequest
  type MethodOutput MachinesService "recover" = RecoveredAdmission
  type MethodStreamingType MachinesService "recover" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "inspectMachine" where
  type MethodName MachinesService "inspectMachine" = "InspectMachine"
  type MethodInput MachinesService "inspectMachine" = InspectMachineRequest
  type MethodOutput MachinesService "inspectMachine" = MachineState
  type MethodStreamingType MachinesService "inspectMachine" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "inspectCheckpoint" where
  type MethodName MachinesService "inspectCheckpoint" = "InspectCheckpoint"
  type MethodInput MachinesService "inspectCheckpoint" = InspectCheckpointRequest
  type MethodOutput MachinesService "inspectCheckpoint" = CheckpointState
  type MethodStreamingType MachinesService "inspectCheckpoint" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "listMachines" where
  type MethodName MachinesService "listMachines" = "ListMachines"
  type MethodInput MachinesService "listMachines" = ListMachinesRequest
  type MethodOutput MachinesService "listMachines" = MachinePage
  type MethodStreamingType MachinesService "listMachines" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "events" where
  type MethodName MachinesService "events" = "Events"
  type MethodInput MachinesService "events" = EventsRequest
  type MethodOutput MachinesService "events" = EventPage
  type MethodStreamingType MachinesService "events" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "usage" where
  type MethodName MachinesService "usage" = "Usage"
  type MethodInput MachinesService "usage" = UsageRequest
  type MethodOutput MachinesService "usage" = UsageReceipt
  type MethodStreamingType MachinesService "usage" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "cancel" where
  type MethodName MachinesService "cancel" = "Cancel"
  type MethodInput MachinesService "cancel" = OperationRequest
  type MethodOutput MachinesService "cancel" = OperationState
  type MethodStreamingType MachinesService "cancel" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "inspectOperation" where
  type MethodName MachinesService "inspectOperation" = "InspectOperation"
  type MethodInput MachinesService "inspectOperation" = OperationRequest
  type MethodOutput MachinesService "inspectOperation" = OperationState
  type MethodStreamingType MachinesService "inspectOperation" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl MachinesService "watchOperation" where
  type MethodName MachinesService "watchOperation" = "WatchOperation"
  type MethodInput MachinesService "watchOperation" = OperationRequest
  type MethodOutput MachinesService "watchOperation" = OperationState
  type MethodStreamingType MachinesService "watchOperation" = 'Data.ProtoLens.Service.Types.ServerStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\SUBmachines/v1/machines.proto\DC2\DC3acyclic.machines.v1\"=\n\
    \\SIProtocolVersion\DC2\DC4\n\
    \\ENQmajor\CAN\SOH \SOH(\rR\ENQmajor\DC2\DC4\n\
    \\ENQminor\CAN\STX \SOH(\rR\ENQminor\"#\n\
    \\vOperationId\DC2\DC4\n\
    \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue\"&\n\
    \\SOIdempotencyKey\DC2\DC4\n\
    \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue\"!\n\
    \\tMachineId\DC2\DC4\n\
    \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue\"$\n\
    \\fCheckpointId\DC2\DC4\n\
    \\ENQvalue\CAN\SOH \SOH(\fR\ENQvalue\"\231\SOH\n\
    \\ENQImage\DC22\n\
    \\EOTkind\CAN\SOH \SOH(\SO2\RS.acyclic.machines.v1.ImageKindR\EOTkind\DC2'\n\
    \\SOmanaged_digest\CAN\STX \SOH(\fH\NULR\rmanagedDigest\DC2%\n\
    \\rcustom_digest\CAN\ETX \SOH(\fH\NULR\fcustomDigest\DC2C\n\
    \\n\
    \checkpoint\CAN\EOT \SOH(\v2!.acyclic.machines.v1.CheckpointIdH\NULR\n\
    \checkpointB\NAK\n\
    \\DC3immutable_reference\"\142\SOH\n\
    \\DC3CompatibilityPolicy\DC2:\n\
    \\EOTmode\CAN\SOH \SOH(\SO2&.acyclic.machines.v1.CompatibilityModeR\EOTmode\DC2;\n\
    \\brequired\CAN\STX \ETX(\SO2\US.acyclic.machines.v1.CapabilityR\brequired\"\194\SOH\n\
    \\DC2ImageQualification\DC20\n\
    \\ENQimage\CAN\SOH \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\DC2C\n\
    \\fcapabilities\CAN\STX \ETX(\SO2\US.acyclic.machines.v1.CapabilityR\fcapabilities\DC25\n\
    \\SYNcompatibility_revision\CAN\ETX \SOH(\fR\NAKcompatibilityRevision\"\\\n\
    \\DLESuspensionPolicy\DC2\CAN\n\
    \\ACKmanual\CAN\SOH \SOH(\bH\NULR\ACKmanual\DC2$\n\
    \\rafter_idle_ms\CAN\STX \SOH(\EOTH\NULR\vafterIdleMsB\b\n\
    \\ACKpolicy\"f\n\
    \\DLEExpirationPolicy\DC27\n\
    \\EOTkind\CAN\SOH \SOH(\SO2#.acyclic.machines.v1.ExpirationKindR\EOTkind\DC2\EM\n\
    \\bvalue_ms\CAN\STX \SOH(\EOTR\avalueMs\"N\n\
    \\aBudgets\DC2!\n\
    \\fspend_micros\CAN\SOH \SOH(\EOTR\vspendMicros\DC2 \n\
    \\vconcurrency\CAN\STX \SOH(\rR\vconcurrency\"\156\EOT\n\
    \\SIMachineContract\DC20\n\
    \\ENQimage\CAN\SOH \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\DC2C\n\
    \\fcapabilities\CAN\STX \ETX(\SO2\US.acyclic.machines.v1.CapabilityR\fcapabilities\DC2N\n\
    \\rcompatibility\CAN\ETX \SOH(\v2(.acyclic.machines.v1.CompatibilityPolicyR\rcompatibility\DC25\n\
    \\SYNcompatibility_revision\CAN\EOT \SOH(\fR\NAKcompatibilityRevision\DC2E\n\
    \\n\
    \suspension\CAN\ACK \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\n\
    \suspension\DC2E\n\
    \\n\
    \expiration\CAN\a \SOH(\v2%.acyclic.machines.v1.ExpirationPolicyR\n\
    \expiration\DC22\n\
    \\NAKnetwork_policy_digest\CAN\b \SOH(\fR\DC3networkPolicyDigest\DC26\n\
    \\abudgets\CAN\t \SOH(\v2\FS.acyclic.machines.v1.BudgetsR\abudgetsJ\EOT\b\ENQ\DLE\ACKR\vperformance\"\137\SOH\n\
    \\DC3QualifyImageRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC20\n\
    \\ENQimage\CAN\STX \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\"\181\EOT\n\
    \\DC4CreateMachineRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC20\n\
    \\ENQimage\CAN\ETX \SOH(\v2\SUB.acyclic.machines.v1.ImageR\ENQimage\DC2N\n\
    \\rcompatibility\CAN\EOT \SOH(\v2(.acyclic.machines.v1.CompatibilityPolicyR\rcompatibility\DC2E\n\
    \\n\
    \suspension\CAN\ACK \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\n\
    \suspension\DC2E\n\
    \\n\
    \expiration\CAN\a \SOH(\v2%.acyclic.machines.v1.ExpirationPolicyR\n\
    \expiration\DC22\n\
    \\NAKnetwork_policy_digest\CAN\b \SOH(\fR\DC3networkPolicyDigest\DC26\n\
    \\abudgets\CAN\t \SOH(\v2\FS.acyclic.machines.v1.BudgetsR\abudgetsJ\EOT\b\ENQ\DLE\ACKR\vperformance\"\226\SOH\n\
    \\SYNMachineMutationRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
    \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\"\228\SOH\n\
    \\CANCheckpointMachineRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
    \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\"\147\STX\n\
    \\NAKForkCheckpointRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC2A\n\
    \\n\
    \checkpoint\CAN\ETX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\DC2\DC4\n\
    \\ENQcount\CAN\EOT \SOH(\rR\ENQcountJ\EOT\b\ENQ\DLE\ACKR\vperformance\"\244\SOH\n\
    \\DC2ForkMachineRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
    \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\DC4\n\
    \\ENQcount\CAN\EOT \SOH(\rR\ENQcount\"\165\STX\n\
    \\SUBSetSuspensionPolicyRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC28\n\
    \\amachine\CAN\ETX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2=\n\
    \\ACKpolicy\CAN\EOT \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\ACKpolicy\"\238\SOH\n\
    \\EMCheckpointMutationRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\DC2A\n\
    \\n\
    \checkpoint\CAN\ETX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\"\160\SOH\n\
    \\SORecoverRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2L\n\
    \\SIidempotency_key\CAN\STX \SOH(\v2#.acyclic.machines.v1.IdempotencyKeyR\SOidempotencyKey\"\147\SOH\n\
    \\NAKInspectMachineRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC28\n\
    \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\"\159\SOH\n\
    \\CANInspectCheckpointRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2A\n\
    \\n\
    \checkpoint\CAN\STX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\"\163\SOH\n\
    \\DC3ListMachinesRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC24\n\
    \\ENQafter\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ENQafter\DC2\DC4\n\
    \\ENQlimit\CAN\ETX \SOH(\rR\ENQlimit\"\148\SOH\n\
    \\DLEOperationRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC2>\n\
    \\toperation\CAN\STX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\"\142\SOH\n\
    \\SOOperationState\DC2>\n\
    \\toperation\CAN\SOH \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2<\n\
    \\ACKstatus\CAN\STX \SOH(\SO2$.acyclic.machines.v1.OperationStatusR\ACKstatus\"0\n\
    \\bEndpoint\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\DLE\n\
    \\ETXuri\CAN\STX \SOH(\tR\ETXuri\"\169\ETX\n\
    \\fMachineState\DC28\n\
    \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2:\n\
    \\ACKstatus\CAN\STX \SOH(\SO2\".acyclic.machines.v1.MachineStatusR\ACKstatus\DC2@\n\
    \\bcontract\CAN\ETX \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\DC2;\n\
    \\tendpoints\CAN\EOT \ETX(\v2\GS.acyclic.machines.v1.EndpointR\tendpoints\DC2J\n\
    \\SIlast_checkpoint\CAN\ENQ \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\SOlastCheckpoint\DC2+\n\
    \\DC2created_at_unix_ms\CAN\ACK \SOH(\EOTR\SIcreatedAtUnixMs\DC2+\n\
    \\DC2changed_at_unix_ms\CAN\a \SOH(\EOTR\SIchangedAtUnixMs\"\128\SOH\n\
    \\vMachinePage\DC2=\n\
    \\bmachines\CAN\SOH \ETX(\v2!.acyclic.machines.v1.MachineStateR\bmachines\DC22\n\
    \\EOTnext\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\EOTnext\"\151\STX\n\
    \\SICheckpointState\DC2A\n\
    \\n\
    \checkpoint\CAN\SOH \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\DC26\n\
    \\ACKsource\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2@\n\
    \\bcontract\CAN\ETX \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\DC2\SUB\n\
    \\bforkable\CAN\EOT \SOH(\bR\bforkable\DC2+\n\
    \\DC2created_at_unix_ms\CAN\ENQ \SOH(\EOTR\SIcreatedAtUnixMs\"\206\SOH\n\
    \\DLEMachineAdmission\DC28\n\
    \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2>\n\
    \\toperation\CAN\STX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
    \\bcontract\CAN\ETX \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\"\146\STX\n\
    \\DC3CheckpointAdmission\DC2A\n\
    \\n\
    \checkpoint\CAN\SOH \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\DC26\n\
    \\ACKsource\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2>\n\
    \\toperation\CAN\ETX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
    \\bcontract\CAN\EOT \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\"\144\STX\n\
    \\rForkAdmission\DC2A\n\
    \\n\
    \checkpoint\CAN\SOH \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\DC2:\n\
    \\bchildren\CAN\STX \ETX(\v2\RS.acyclic.machines.v1.MachineIdR\bchildren\DC2>\n\
    \\toperation\CAN\ETX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
    \\bcontract\CAN\EOT \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\"\203\STX\n\
    \\DC4ForkMachineAdmission\DC26\n\
    \\ACKsource\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2:\n\
    \\bchildren\CAN\STX \ETX(\v2\RS.acyclic.machines.v1.MachineIdR\bchildren\DC2>\n\
    \\toperation\CAN\ETX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2@\n\
    \\bcontract\CAN\EOT \SOH(\v2$.acyclic.machines.v1.MachineContractR\bcontract\DC2=\n\
    \\bfidelity\CAN\ENQ \SOH(\SO2!.acyclic.machines.v1.ForkFidelityR\bfidelity\"\202\SOH\n\
    \\SIPolicyAdmission\DC28\n\
    \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2>\n\
    \\toperation\CAN\STX \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2=\n\
    \\ACKpolicy\CAN\ETX \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\ACKpolicy\"\208\SOH\n\
    \\DC1MutationAdmission\DC2>\n\
    \\toperation\CAN\SOH \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC28\n\
    \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2A\n\
    \\n\
    \checkpoint\CAN\ETX \SOH(\v2!.acyclic.machines.v1.CheckpointIdR\n\
    \checkpoint\"\255\ENQ\n\
    \\DC2RecoveredAdmission\DC2>\n\
    \\toperation\CAN\SOH \SOH(\v2 .acyclic.machines.v1.OperationIdR\toperation\DC2?\n\
    \\ACKcreate\CAN\STX \SOH(\v2%.acyclic.machines.v1.MachineAdmissionH\NULR\ACKcreate\DC2J\n\
    \\n\
    \checkpoint\CAN\ETX \SOH(\v2(.acyclic.machines.v1.CheckpointAdmissionH\NULR\n\
    \checkpoint\DC28\n\
    \\EOTfork\CAN\EOT \SOH(\v2\".acyclic.machines.v1.ForkAdmissionH\NULR\EOTfork\DC2B\n\
    \\asuspend\CAN\ENQ \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\asuspend\DC2<\n\
    \\EOTwake\CAN\ACK \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\EOTwake\DC2Q\n\
    \\SIdestroy_machine\CAN\a \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\SOdestroyMachine\DC2Z\n\
    \\NAKset_suspension_policy\CAN\b \SOH(\v2$.acyclic.machines.v1.PolicyAdmissionH\NULR\DC3setSuspensionPolicy\DC2W\n\
    \\DC2destroy_checkpoint\CAN\t \SOH(\v2&.acyclic.machines.v1.MutationAdmissionH\NULR\DC1destroyCheckpoint\DC2N\n\
    \\ffork_machine\CAN\n\
    \ \SOH(\v2).acyclic.machines.v1.ForkMachineAdmissionH\NULR\vforkMachineB\b\n\
    \\ACKresult\"O\n\
    \\SOForkedMachines\DC2=\n\
    \\bmachines\CAN\SOH \ETX(\v2!.acyclic.machines.v1.MachineStateR\bmachines\"\202\SOH\n\
    \\DC2ForkedLiveMachines\DC26\n\
    \\ACKsource\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\ACKsource\DC2=\n\
    \\bfidelity\CAN\STX \SOH(\SO2!.acyclic.machines.v1.ForkFidelityR\bfidelity\DC2=\n\
    \\bchildren\CAN\ETX \ETX(\v2!.acyclic.machines.v1.MachineStateR\bchildren\"\132\SOH\n\
    \\tPolicySet\DC28\n\
    \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2=\n\
    \\ACKpolicy\CAN\STX \SOH(\v2%.acyclic.machines.v1.SuspensionPolicyR\ACKpolicy\"\172\ENQ\n\
    \\SIMutationOutcome\DC2=\n\
    \\acreated\CAN\SOH \SOH(\v2!.acyclic.machines.v1.MachineStateH\NULR\acreated\DC2J\n\
    \\fcheckpointed\CAN\STX \SOH(\v2$.acyclic.machines.v1.CheckpointStateH\NULR\fcheckpointed\DC2=\n\
    \\ACKforked\CAN\ETX \SOH(\v2#.acyclic.machines.v1.ForkedMachinesH\NULR\ACKforked\DC2>\n\
    \\tsuspended\CAN\EOT \SOH(\v2\RS.acyclic.machines.v1.MachineIdH\NULR\tsuspended\DC26\n\
    \\ENQwoken\CAN\ENQ \SOH(\v2\RS.acyclic.machines.v1.MachineIdH\NULR\ENQwoken\DC2T\n\
    \\NAKsuspension_policy_set\CAN\ACK \SOH(\v2\RS.acyclic.machines.v1.PolicySetH\NULR\DC3suspensionPolicySet\DC2M\n\
    \\DC1machine_destroyed\CAN\a \SOH(\v2\RS.acyclic.machines.v1.MachineIdH\NULR\DLEmachineDestroyed\DC2V\n\
    \\DC4checkpoint_destroyed\CAN\b \SOH(\v2!.acyclic.machines.v1.CheckpointIdH\NULR\DC3checkpointDestroyed\DC2P\n\
    \\SOmachine_forked\CAN\t \SOH(\v2'.acyclic.machines.v1.ForkedLiveMachinesH\NULR\rmachineForkedB\b\n\
    \\ACKresult\"T\n\
    \\rOperationPage\DC2C\n\
    \\n\
    \operations\CAN\SOH \ETX(\v2#.acyclic.machines.v1.OperationStateR\n\
    \operations\"\192\STX\n\
    \\fMachineEvent\DC28\n\
    \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\SUB\n\
    \\bsequence\CAN\STX \SOH(\EOTR\bsequence\DC2-\n\
    \\DC3observed_at_unix_ms\CAN\ETX \SOH(\EOTR\DLEobservedAtUnixMs\DC22\n\
    \\EOTkind\CAN\EOT \SOH(\SO2\RS.acyclic.machines.v1.EventKindR\EOTkind\DC28\n\
    \\ENQstate\CAN\ENQ \SOH(\SO2\".acyclic.machines.v1.MachineStatusR\ENQstate\DC2=\n\
    \\bpressure\CAN\ACK \SOH(\SO2!.acyclic.machines.v1.PressureKindR\bpressure\"\200\SOH\n\
    \\rEventsRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC28\n\
    \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2%\n\
    \\SOafter_sequence\CAN\ETX \SOH(\EOTR\rafterSequence\DC2\DC4\n\
    \\ENQlimit\CAN\EOT \SOH(\rR\ENQlimit\"k\n\
    \\tEventPage\DC29\n\
    \\ACKevents\CAN\SOH \ETX(\v2!.acyclic.machines.v1.MachineEventR\ACKevents\DC2#\n\
    \\rnext_sequence\CAN\STX \SOH(\EOTR\fnextSequence\"\206\SOH\n\
    \\fUsageRequest\DC2@\n\
    \\bprotocol\CAN\SOH \SOH(\v2$.acyclic.machines.v1.ProtocolVersionR\bprotocol\DC28\n\
    \\amachine\CAN\STX \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\"\n\
    \\rstart_unix_ms\CAN\ETX \SOH(\EOTR\vstartUnixMs\DC2\RS\n\
    \\vend_unix_ms\CAN\EOT \SOH(\EOTR\tendUnixMs\"\226\ETX\n\
    \\fUsageReceipt\DC28\n\
    \\amachine\CAN\SOH \SOH(\v2\RS.acyclic.machines.v1.MachineIdR\amachine\DC2\"\n\
    \\rstart_unix_ms\CAN\STX \SOH(\EOTR\vstartUnixMs\DC2\RS\n\
    \\vend_unix_ms\CAN\ETX \SOH(\EOTR\tendUnixMs\DC2$\n\
    \\SOelastic_cpu_ns\CAN\EOT \SOH(\EOTR\felasticCpuNs\DC2(\n\
    \\DLEdedicated_cpu_ns\CAN\ENQ \SOH(\EOTR\SOdedicatedCpuNs\DC2A\n\
    \\GSprivate_resident_byte_seconds\CAN\ACK \SOH(\EOTR\SUBprivateResidentByteSeconds\DC22\n\
    \\NAKdurable_private_bytes\CAN\a \SOH(\EOTR\DC3durablePrivateBytes\DC24\n\
    \\SYNlineage_receipt_sha256\CAN\v \SOH(\fR\DC4lineageReceiptSha256\DC2!\n\
    \\fegress_bytes\CAN\t \SOH(\EOTR\vegressBytes\DC2\CAN\n\
    \\areceipt\CAN\n\
    \ \SOH(\fR\areceiptJ\EOT\b\b\DLE\tR\DC4lineage_shared_bytes*u\n\
    \\tImageKind\DC2\SUB\n\
    \\SYNIMAGE_KIND_UNSPECIFIED\DLE\NUL\DC2\SUB\n\
    \\SYNIMAGE_KIND_MANAGED_OCI\DLE\SOH\DC2\NAK\n\
    \\DC1IMAGE_KIND_CUSTOM\DLE\STX\DC2\EM\n\
    \\NAKIMAGE_KIND_CHECKPOINT\DLE\ETX*\244\SOH\n\
    \\n\
    \Capability\DC2\SUB\n\
    \\SYNCAPABILITY_UNSPECIFIED\DLE\NUL\DC2\SUB\n\
    \\SYNCAPABILITY_ELASTIC_CPU\DLE\SOH\DC2\GS\n\
    \\EMCAPABILITY_ELASTIC_MEMORY\DLE\STX\DC2\RS\n\
    \\SUBCAPABILITY_LIVE_CHECKPOINT\DLE\ETX\DC2\CAN\n\
    \\DC4CAPABILITY_LIVE_FORK\DLE\EOT\DC2\GS\n\
    \\EMCAPABILITY_SUSPEND_RESUME\DLE\ENQ\DC2\FS\n\
    \\CANCAPABILITY_LIVE_MOVEMENT\DLE\ACK\DC2\CAN\n\
    \\DC4CAPABILITY_DISK_FORK\DLE\a*{\n\
    \\DC1CompatibilityMode\DC2\"\n\
    \\RSCOMPATIBILITY_MODE_UNSPECIFIED\DLE\NUL\DC2\"\n\
    \\RSCOMPATIBILITY_MODE_BEST_EFFORT\DLE\SOH\DC2\RS\n\
    \\SUBCOMPATIBILITY_MODE_REQUIRE\DLE\STX*\155\SOH\n\
    \\SOExpirationKind\DC2\US\n\
    \\ESCEXPIRATION_KIND_UNSPECIFIED\DLE\NUL\DC2\EM\n\
    \\NAKEXPIRATION_KIND_NEVER\DLE\SOH\DC2\ESC\n\
    \\ETBEXPIRATION_KIND_MAX_AGE\DLE\STX\DC2\SYN\n\
    \\DC2EXPIRATION_KIND_AT\DLE\ETX\DC2\CAN\n\
    \\DC4EXPIRATION_KIND_IDLE\DLE\EOT*\210\SOH\n\
    \\SIOperationStatus\DC2 \n\
    \\FSOPERATION_STATUS_UNSPECIFIED\DLE\NUL\DC2\FS\n\
    \\CANOPERATION_STATUS_PENDING\DLE\SOH\DC2\RS\n\
    \\SUBOPERATION_STATUS_SUCCEEDED\DLE\STX\DC2\RS\n\
    \\SUBOPERATION_STATUS_CANCELLED\DLE\ETX\DC2\"\n\
    \\RSOPERATION_STATUS_INDETERMINATE\DLE\EOT\DC2\ESC\n\
    \\ETBOPERATION_STATUS_FAILED\DLE\ENQ*\186\STX\n\
    \\rMachineStatus\DC2\RS\n\
    \\SUBMACHINE_STATUS_UNSPECIFIED\DLE\NUL\DC2\ESC\n\
    \\ETBMACHINE_STATUS_STARTING\DLE\SOH\DC2\SUB\n\
    \\SYNMACHINE_STATUS_RUNNING\DLE\STX\DC2\GS\n\
    \\EMMACHINE_STATUS_SUSPENDING\DLE\ETX\DC2\FS\n\
    \\CANMACHINE_STATUS_SUSPENDED\DLE\EOT\DC2\EM\n\
    \\NAKMACHINE_STATUS_WAKING\DLE\ENQ\DC2\GS\n\
    \\EMMACHINE_STATUS_DESTROYING\DLE\ACK\DC2\FS\n\
    \\CANMACHINE_STATUS_DESTROYED\DLE\a\DC2\EM\n\
    \\NAKMACHINE_STATUS_FAILED\DLE\b\DC2 \n\
    \\FSMACHINE_STATUS_INDETERMINATE\DLE\t*m\n\
    \\fForkFidelity\DC2\GS\n\
    \\EMFORK_FIDELITY_UNSPECIFIED\DLE\NUL\DC2!\n\
    \\GSFORK_FIDELITY_MEMORY_AND_DISK\DLE\SOH\DC2\ESC\n\
    \\ETBFORK_FIDELITY_DISK_ONLY\DLE\STX*\151\SOH\n\
    \\fPressureKind\DC2\GS\n\
    \\EMPRESSURE_KIND_UNSPECIFIED\DLE\NUL\DC2!\n\
    \\GSPRESSURE_KIND_CUSTOMER_BUDGET\DLE\SOH\DC2\US\n\
    \\ESCPRESSURE_KIND_MACHINE_LIMIT\DLE\STX\DC2$\n\
    \ PRESSURE_KIND_SERVICE_SATURATION\DLE\ETX*o\n\
    \\tEventKind\DC2\SUB\n\
    \\SYNEVENT_KIND_UNSPECIFIED\DLE\NUL\DC2\DC4\n\
    \\DLEEVENT_KIND_STATE\DLE\SOH\DC2\ETB\n\
    \\DC3EVENT_KIND_PRESSURE\DLE\STX\DC2\ETB\n\
    \\DC3EVENT_KIND_CAPACITY\DLE\ETX2\164\SO\n\
    \\SIMachinesService\DC2a\n\
    \\fQualifyImage\DC2(.acyclic.machines.v1.QualifyImageRequest\SUB'.acyclic.machines.v1.ImageQualification\DC2Z\n\
    \\ACKCreate\DC2).acyclic.machines.v1.CreateMachineRequest\SUB%.acyclic.machines.v1.MachineAdmission\DC2e\n\
    \\n\
    \Checkpoint\DC2-.acyclic.machines.v1.CheckpointMachineRequest\SUB(.acyclic.machines.v1.CheckpointAdmission\DC2V\n\
    \\EOTFork\DC2*.acyclic.machines.v1.ForkCheckpointRequest\SUB\".acyclic.machines.v1.ForkAdmission\DC2a\n\
    \\vForkMachine\DC2'.acyclic.machines.v1.ForkMachineRequest\SUB).acyclic.machines.v1.ForkMachineAdmission\DC2^\n\
    \\aSuspend\DC2+.acyclic.machines.v1.MachineMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2[\n\
    \\EOTWake\DC2+.acyclic.machines.v1.MachineMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2l\n\
    \\DC3SetSuspensionPolicy\DC2/.acyclic.machines.v1.SetSuspensionPolicyRequest\SUB$.acyclic.machines.v1.PolicyAdmission\DC2e\n\
    \\SODestroyMachine\DC2+.acyclic.machines.v1.MachineMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2k\n\
    \\DC1DestroyCheckpoint\DC2..acyclic.machines.v1.CheckpointMutationRequest\SUB&.acyclic.machines.v1.MutationAdmission\DC2W\n\
    \\aRecover\DC2#.acyclic.machines.v1.RecoverRequest\SUB'.acyclic.machines.v1.RecoveredAdmission\DC2_\n\
    \\SOInspectMachine\DC2*.acyclic.machines.v1.InspectMachineRequest\SUB!.acyclic.machines.v1.MachineState\DC2h\n\
    \\DC1InspectCheckpoint\DC2-.acyclic.machines.v1.InspectCheckpointRequest\SUB$.acyclic.machines.v1.CheckpointState\DC2Z\n\
    \\fListMachines\DC2(.acyclic.machines.v1.ListMachinesRequest\SUB .acyclic.machines.v1.MachinePage\DC2L\n\
    \\ACKEvents\DC2\".acyclic.machines.v1.EventsRequest\SUB\RS.acyclic.machines.v1.EventPage\DC2M\n\
    \\ENQUsage\DC2!.acyclic.machines.v1.UsageRequest\SUB!.acyclic.machines.v1.UsageReceipt\DC2T\n\
    \\ACKCancel\DC2%.acyclic.machines.v1.OperationRequest\SUB#.acyclic.machines.v1.OperationState\DC2^\n\
    \\DLEInspectOperation\DC2%.acyclic.machines.v1.OperationRequest\SUB#.acyclic.machines.v1.OperationState\DC2^\n\
    \\SOWatchOperation\DC2%.acyclic.machines.v1.OperationRequest\SUB#.acyclic.machines.v1.OperationState0\SOHB;Z9github.com/acyclic-labs/sdk/go/gen/machines/v1;machinesv1J\195}\n\
    \\a\DC2\ENQ\NUL\NUL\131\ETX\SOH\n\
    \\b\n\
    \\SOH\f\DC2\ETX\NUL\NUL\DC2\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\SOH\NUL\FS\n\
    \\b\n\
    \\SOH\b\DC2\ETX\ETX\NULP\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\ETX\NULP\n\
    \\n\
    \\n\
    \\STX\EOT\NUL\DC2\EOT\ENQ\NUL\b\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\ENQ\b\ETB\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\ACK\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\ACK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\ACK\t\SO\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\ACK\DC1\DC2\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\SOH\DC2\ETX\a\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ENQ\DC2\ETX\a\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\SOH\DC2\ETX\a\t\SO\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ETX\DC2\ETX\a\DC1\DC2\n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\t\NUL\v\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\t\b\DC3\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\n\
    \\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ENQ\DC2\ETX\n\
    \\STX\a\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\n\
    \\b\r\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\n\
    \\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\f\NUL\SO\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\f\b\SYN\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\r\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ENQ\DC2\ETX\r\STX\a\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\r\b\r\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\r\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT\SI\NUL\DC1\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX\SI\b\DC1\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX\DLE\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ENQ\DC2\ETX\DLE\STX\a\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX\DLE\b\r\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX\DLE\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT\DC2\NUL\DC4\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX\DC2\b\DC4\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX\DC3\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ENQ\DC2\ETX\DC3\STX\a\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX\DC3\b\r\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX\DC3\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\ENQ\NUL\DC2\EOT\SYN\NUL\ESC\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETX\SYN\ENQ\SO\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETX\ETB\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETX\ETB\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETX\ETB\ESC\FS\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETX\CAN\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETX\CAN\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETX\CAN\ESC\FS\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\STX\DC2\ETX\EM\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\SOH\DC2\ETX\EM\STX\DC3\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\STX\DC2\ETX\EM\SYN\ETB\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ETX\DC2\ETX\SUB\STX\FS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\SOH\DC2\ETX\SUB\STX\ETB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\STX\DC2\ETX\SUB\SUB\ESC\n\
    \\n\
    \\n\
    \\STX\EOT\ENQ\DC2\EOT\FS\NUL#\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETX\FS\b\r\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETX\GS\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ACK\DC2\ETX\GS\STX\v\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETX\GS\f\DLE\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETX\GS\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT\ENQ\b\NUL\DC2\EOT\RS\STX\"\ETX\n\
    \\f\n\
    \\ENQ\EOT\ENQ\b\NUL\SOH\DC2\ETX\RS\b\ESC\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\SOH\DC2\ETX\US\EOT\GS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ENQ\DC2\ETX\US\EOT\t\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\SOH\DC2\ETX\US\n\
    \\CAN\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ETX\DC2\ETX\US\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\STX\DC2\ETX \EOT\FS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\ENQ\DC2\ETX \EOT\t\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\SOH\DC2\ETX \n\
    \\ETB\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\STX\ETX\DC2\ETX \SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\ETX\DC2\ETX!\EOT \n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\ACK\DC2\ETX!\EOT\DLE\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\SOH\DC2\ETX!\DC1\ESC\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\ETX\ETX\DC2\ETX!\RS\US\n\
    \\n\
    \\n\
    \\STX\ENQ\SOH\DC2\EOT%\NUL2\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\SOH\SOH\DC2\ETX%\ENQ\SI\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\NUL\DC2\ETX&\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\NUL\SOH\DC2\ETX&\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\NUL\STX\DC2\ETX&\ESC\FS\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\SOH\DC2\ETX'\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\SOH\SOH\DC2\ETX'\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\SOH\STX\DC2\ETX'\ESC\FS\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\STX\DC2\ETX(\STX \n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\STX\SOH\DC2\ETX(\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\STX\STX\DC2\ETX(\RS\US\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\ETX\DC2\ETX)\STX!\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ETX\SOH\DC2\ETX)\STX\FS\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ETX\STX\DC2\ETX)\US \n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\EOT\DC2\ETX*\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\EOT\SOH\DC2\ETX*\STX\SYN\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\EOT\STX\DC2\ETX*\EM\SUB\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\ENQ\DC2\ETX+\STX \n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ENQ\SOH\DC2\ETX+\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ENQ\STX\DC2\ETX+\RS\US\n\
    \\v\n\
    \\EOT\ENQ\SOH\STX\ACK\DC2\ETX,\STX\US\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ACK\SOH\DC2\ETX,\STX\SUB\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\ACK\STX\DC2\ETX,\GS\RS\n\
    \\240\STX\n\
    \\EOT\ENQ\SOH\STX\a\DC2\ETX1\STX\ESC\SUB\226\STX ForkMachine copies a running machine's persistent disk, but not its memory or processes,\n\
    \ into fresh children. Which paths are persistent is provider-defined: a provider whose\n\
    \ machines boot from an immutable image may copy only its declared data directory. CAPABILITY_LIVE_FORK is the memory-and-disk form and takes precedence\n\
    \ when both are declared.\n\
    \\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\a\SOH\DC2\ETX1\STX\SYN\n\
    \\f\n\
    \\ENQ\ENQ\SOH\STX\a\STX\DC2\ETX1\EM\SUB\n\
    \\n\
    \\n\
    \\STX\ENQ\STX\DC2\EOT3\NUL7\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\STX\SOH\DC2\ETX3\ENQ\SYN\n\
    \\v\n\
    \\EOT\ENQ\STX\STX\NUL\DC2\ETX4\STX%\n\
    \\f\n\
    \\ENQ\ENQ\STX\STX\NUL\SOH\DC2\ETX4\STX \n\
    \\f\n\
    \\ENQ\ENQ\STX\STX\NUL\STX\DC2\ETX4#$\n\
    \\v\n\
    \\EOT\ENQ\STX\STX\SOH\DC2\ETX5\STX%\n\
    \\f\n\
    \\ENQ\ENQ\STX\STX\SOH\SOH\DC2\ETX5\STX \n\
    \\f\n\
    \\ENQ\ENQ\STX\STX\SOH\STX\DC2\ETX5#$\n\
    \\v\n\
    \\EOT\ENQ\STX\STX\STX\DC2\ETX6\STX!\n\
    \\f\n\
    \\ENQ\ENQ\STX\STX\STX\SOH\DC2\ETX6\STX\FS\n\
    \\f\n\
    \\ENQ\ENQ\STX\STX\STX\STX\DC2\ETX6\US \n\
    \\n\
    \\n\
    \\STX\EOT\ACK\DC2\EOT8\NUL;\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETX8\b\ESC\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX9\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ACK\DC2\ETX9\STX\DC3\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX9\DC4\CAN\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX9\ESC\FS\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\SOH\DC2\ETX:\STX#\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\EOT\DC2\ETX:\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ACK\DC2\ETX:\v\NAK\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\SOH\DC2\ETX:\SYN\RS\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ETX\DC2\ETX:!\"\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOT<\NUL@\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETX<\b\SUB\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETX=\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ACK\DC2\ETX=\STX\a\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETX=\b\r\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETX=\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\a\STX\SOH\DC2\ETX>\STX'\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\EOT\DC2\ETX>\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ACK\DC2\ETX>\v\NAK\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\SOH\DC2\ETX>\SYN\"\n\
    \\f\n\
    \\ENQ\EOT\a\STX\SOH\ETX\DC2\ETX>%&\n\
    \\v\n\
    \\EOT\EOT\a\STX\STX\DC2\ETX?\STX#\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\ENQ\DC2\ETX?\STX\a\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\SOH\DC2\ETX?\b\RS\n\
    \\f\n\
    \\ENQ\EOT\a\STX\STX\ETX\DC2\ETX?!\"\n\
    \\n\
    \\n\
    \\STX\EOT\b\DC2\EOTB\NULG\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETXB\b\CAN\n\
    \\f\n\
    \\EOT\EOT\b\b\NUL\DC2\EOTC\STXF\ETX\n\
    \\f\n\
    \\ENQ\EOT\b\b\NUL\SOH\DC2\ETXC\b\SO\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETXD\EOT\DC4\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ENQ\DC2\ETXD\EOT\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETXD\t\SI\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETXD\DC2\DC3\n\
    \\v\n\
    \\EOT\EOT\b\STX\SOH\DC2\ETXE\EOT\GS\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ENQ\DC2\ETXE\EOT\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\SOH\DC2\ETXE\v\CAN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ETX\DC2\ETXE\ESC\FS\n\
    \\n\
    \\n\
    \\STX\ENQ\ETX\DC2\EOTH\NULN\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\ETX\SOH\DC2\ETXH\ENQ\DC3\n\
    \\v\n\
    \\EOT\ENQ\ETX\STX\NUL\DC2\ETXI\STX\"\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\NUL\SOH\DC2\ETXI\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\NUL\STX\DC2\ETXI !\n\
    \\v\n\
    \\EOT\ENQ\ETX\STX\SOH\DC2\ETXJ\STX\FS\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\SOH\SOH\DC2\ETXJ\STX\ETB\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\SOH\STX\DC2\ETXJ\SUB\ESC\n\
    \\v\n\
    \\EOT\ENQ\ETX\STX\STX\DC2\ETXK\STX\RS\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\STX\SOH\DC2\ETXK\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\STX\STX\DC2\ETXK\FS\GS\n\
    \\v\n\
    \\EOT\ENQ\ETX\STX\ETX\DC2\ETXL\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\ETX\SOH\DC2\ETXL\STX\DC4\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\ETX\STX\DC2\ETXL\ETB\CAN\n\
    \\v\n\
    \\EOT\ENQ\ETX\STX\EOT\DC2\ETXM\STX\ESC\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\EOT\SOH\DC2\ETXM\STX\SYN\n\
    \\f\n\
    \\ENQ\ENQ\ETX\STX\EOT\STX\DC2\ETXM\EM\SUB\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOTO\NULR\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETXO\b\CAN\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETXP\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ACK\DC2\ETXP\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETXP\DC1\NAK\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETXP\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\t\STX\SOH\DC2\ETXQ\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ENQ\DC2\ETXQ\STX\b\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\SOH\DC2\ETXQ\t\DC1\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ETX\DC2\ETXQ\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\n\
    \\DC2\EOTS\NULV\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\n\
    \\SOH\DC2\ETXS\b\SI\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\ETXT\STX\SUB\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ENQ\DC2\ETXT\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\ETXT\t\NAK\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\ETXT\CAN\EM\n\
    \\v\n\
    \\EOT\EOT\n\
    \\STX\SOH\DC2\ETXU\STX\EM\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ENQ\DC2\ETXU\STX\b\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\SOH\DC2\ETXU\t\DC4\n\
    \\f\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ETX\DC2\ETXU\ETB\CAN\n\
    \\n\
    \\n\
    \\STX\EOT\v\DC2\EOTX\NULc\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\v\SOH\DC2\ETXX\b\ETB\n\
    \\v\n\
    \\EOT\EOT\v\STX\NUL\DC2\ETXY\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ACK\DC2\ETXY\STX\a\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\ETXY\b\r\n\
    \\f\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\ETXY\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\v\STX\SOH\DC2\ETXZ\STX'\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\EOT\DC2\ETXZ\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ACK\DC2\ETXZ\v\NAK\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\SOH\DC2\ETXZ\SYN\"\n\
    \\f\n\
    \\ENQ\EOT\v\STX\SOH\ETX\DC2\ETXZ%&\n\
    \\v\n\
    \\EOT\EOT\v\STX\STX\DC2\ETX[\STX(\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\ACK\DC2\ETX[\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\SOH\DC2\ETX[\SYN#\n\
    \\f\n\
    \\ENQ\EOT\v\STX\STX\ETX\DC2\ETX[&'\n\
    \\v\n\
    \\EOT\EOT\v\STX\ETX\DC2\ETX\\\STX#\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ETX\ENQ\DC2\ETX\\\STX\a\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ETX\SOH\DC2\ETX\\\b\RS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ETX\ETX\DC2\ETX\\!\"\n\
    \\n\
    \\n\
    \\ETX\EOT\v\t\DC2\ETX]\STX\r\n\
    \\v\n\
    \\EOT\EOT\v\t\NUL\DC2\ETX]\v\f\n\
    \\f\n\
    \\ENQ\EOT\v\t\NUL\SOH\DC2\ETX]\v\f\n\
    \\f\n\
    \\ENQ\EOT\v\t\NUL\STX\DC2\ETX]\v\f\n\
    \\n\
    \\n\
    \\ETX\EOT\v\n\
    \\DC2\ETX^\STX\EM\n\
    \\v\n\
    \\EOT\EOT\v\n\
    \\NUL\DC2\ETX^\v\CAN\n\
    \\v\n\
    \\EOT\EOT\v\STX\EOT\DC2\ETX_\STX\"\n\
    \\f\n\
    \\ENQ\EOT\v\STX\EOT\ACK\DC2\ETX_\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\v\STX\EOT\SOH\DC2\ETX_\DC3\GS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\EOT\ETX\DC2\ETX_ !\n\
    \\v\n\
    \\EOT\EOT\v\STX\ENQ\DC2\ETX`\STX\"\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ENQ\ACK\DC2\ETX`\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ENQ\SOH\DC2\ETX`\DC3\GS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ENQ\ETX\DC2\ETX` !\n\
    \\v\n\
    \\EOT\EOT\v\STX\ACK\DC2\ETXa\STX\"\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ACK\ENQ\DC2\ETXa\STX\a\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ACK\SOH\DC2\ETXa\b\GS\n\
    \\f\n\
    \\ENQ\EOT\v\STX\ACK\ETX\DC2\ETXa !\n\
    \\v\n\
    \\EOT\EOT\v\STX\a\DC2\ETXb\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\v\STX\a\ACK\DC2\ETXb\STX\t\n\
    \\f\n\
    \\ENQ\EOT\v\STX\a\SOH\DC2\ETXb\n\
    \\DC1\n\
    \\f\n\
    \\ENQ\EOT\v\STX\a\ETX\DC2\ETXb\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\f\DC2\EOTd\NULg\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\f\SOH\DC2\ETXd\b\ESC\n\
    \\v\n\
    \\EOT\EOT\f\STX\NUL\DC2\ETXe\STX\US\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ACK\DC2\ETXe\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\ETXe\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\ETXe\GS\RS\n\
    \\v\n\
    \\EOT\EOT\f\STX\SOH\DC2\ETXf\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ACK\DC2\ETXf\STX\a\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\ETXf\b\r\n\
    \\f\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\ETXf\DLE\DC1\n\
    \\n\
    \\n\
    \\STX\EOT\r\DC2\EOTh\NULs\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\r\SOH\DC2\ETXh\b\FS\n\
    \\v\n\
    \\EOT\EOT\r\STX\NUL\DC2\ETXi\STX\US\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ACK\DC2\ETXi\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\ETXi\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\ETXi\GS\RS\n\
    \\v\n\
    \\EOT\EOT\r\STX\SOH\DC2\ETXj\STX%\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\ACK\DC2\ETXj\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\SOH\DC2\ETXj\DC1 \n\
    \\f\n\
    \\ENQ\EOT\r\STX\SOH\ETX\DC2\ETXj#$\n\
    \\v\n\
    \\EOT\EOT\r\STX\STX\DC2\ETXk\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\STX\ACK\DC2\ETXk\STX\a\n\
    \\f\n\
    \\ENQ\EOT\r\STX\STX\SOH\DC2\ETXk\b\r\n\
    \\f\n\
    \\ENQ\EOT\r\STX\STX\ETX\DC2\ETXk\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\r\STX\ETX\DC2\ETXl\STX(\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ETX\ACK\DC2\ETXl\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ETX\SOH\DC2\ETXl\SYN#\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ETX\ETX\DC2\ETXl&'\n\
    \\n\
    \\n\
    \\ETX\EOT\r\t\DC2\ETXm\STX\r\n\
    \\v\n\
    \\EOT\EOT\r\t\NUL\DC2\ETXm\v\f\n\
    \\f\n\
    \\ENQ\EOT\r\t\NUL\SOH\DC2\ETXm\v\f\n\
    \\f\n\
    \\ENQ\EOT\r\t\NUL\STX\DC2\ETXm\v\f\n\
    \\n\
    \\n\
    \\ETX\EOT\r\n\
    \\DC2\ETXn\STX\EM\n\
    \\v\n\
    \\EOT\EOT\r\n\
    \\NUL\DC2\ETXn\v\CAN\n\
    \\v\n\
    \\EOT\EOT\r\STX\EOT\DC2\ETXo\STX\"\n\
    \\f\n\
    \\ENQ\EOT\r\STX\EOT\ACK\DC2\ETXo\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\EOT\SOH\DC2\ETXo\DC3\GS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\EOT\ETX\DC2\ETXo !\n\
    \\v\n\
    \\EOT\EOT\r\STX\ENQ\DC2\ETXp\STX\"\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ENQ\ACK\DC2\ETXp\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ENQ\SOH\DC2\ETXp\DC3\GS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ENQ\ETX\DC2\ETXp !\n\
    \\v\n\
    \\EOT\EOT\r\STX\ACK\DC2\ETXq\STX\"\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ACK\ENQ\DC2\ETXq\STX\a\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ACK\SOH\DC2\ETXq\b\GS\n\
    \\f\n\
    \\ENQ\EOT\r\STX\ACK\ETX\DC2\ETXq !\n\
    \\v\n\
    \\EOT\EOT\r\STX\a\DC2\ETXr\STX\SYN\n\
    \\f\n\
    \\ENQ\EOT\r\STX\a\ACK\DC2\ETXr\STX\t\n\
    \\f\n\
    \\ENQ\EOT\r\STX\a\SOH\DC2\ETXr\n\
    \\DC1\n\
    \\f\n\
    \\ENQ\EOT\r\STX\a\ETX\DC2\ETXr\DC4\NAK\n\
    \\n\
    \\n\
    \\STX\EOT\SO\DC2\EOTt\NULx\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SO\SOH\DC2\ETXt\b\RS\n\
    \\v\n\
    \\EOT\EOT\SO\STX\NUL\DC2\ETXu\STX\US\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ACK\DC2\ETXu\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\ETXu\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\ETXu\GS\RS\n\
    \\v\n\
    \\EOT\EOT\SO\STX\SOH\DC2\ETXv\STX%\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ACK\DC2\ETXv\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\SOH\DC2\ETXv\DC1 \n\
    \\f\n\
    \\ENQ\EOT\SO\STX\SOH\ETX\DC2\ETXv#$\n\
    \\v\n\
    \\EOT\EOT\SO\STX\STX\DC2\ETXw\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ACK\DC2\ETXw\STX\v\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\SOH\DC2\ETXw\f\DC3\n\
    \\f\n\
    \\ENQ\EOT\SO\STX\STX\ETX\DC2\ETXw\SYN\ETB\n\
    \\n\
    \\n\
    \\STX\EOT\SI\DC2\EOTy\NUL}\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SI\SOH\DC2\ETXy\b \n\
    \\v\n\
    \\EOT\EOT\SI\STX\NUL\DC2\ETXz\STX\US\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ACK\DC2\ETXz\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\ETXz\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\ETXz\GS\RS\n\
    \\v\n\
    \\EOT\EOT\SI\STX\SOH\DC2\ETX{\STX%\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ACK\DC2\ETX{\STX\DLE\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\SOH\DC2\ETX{\DC1 \n\
    \\f\n\
    \\ENQ\EOT\SI\STX\SOH\ETX\DC2\ETX{#$\n\
    \\v\n\
    \\EOT\EOT\SI\STX\STX\DC2\ETX|\STX\CAN\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\ACK\DC2\ETX|\STX\v\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\SOH\DC2\ETX|\f\DC3\n\
    \\f\n\
    \\ENQ\EOT\SI\STX\STX\ETX\DC2\ETX|\SYN\ETB\n\
    \\v\n\
    \\STX\EOT\DLE\DC2\ENQ~\NUL\133\SOH\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\DLE\SOH\DC2\ETX~\b\GS\n\
    \\v\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\ETX\DEL\STX\US\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ACK\DC2\ETX\DEL\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\ETX\DEL\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\ETX\DEL\GS\RS\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\SOH\DC2\EOT\128\SOH\STX%\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\ACK\DC2\EOT\128\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\SOH\DC2\EOT\128\SOH\DC1 \n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\ETX\DC2\EOT\128\SOH#$\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\STX\DC2\EOT\129\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\ACK\DC2\EOT\129\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\SOH\DC2\EOT\129\SOH\SI\EM\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\ETX\DC2\EOT\129\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\ETX\DC2\EOT\130\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\ENQ\DC2\EOT\130\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\SOH\DC2\EOT\130\SOH\t\SO\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\ETX\DC2\EOT\130\SOH\DC1\DC2\n\
    \\v\n\
    \\ETX\EOT\DLE\t\DC2\EOT\131\SOH\STX\r\n\
    \\f\n\
    \\EOT\EOT\DLE\t\NUL\DC2\EOT\131\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\DLE\t\NUL\SOH\DC2\EOT\131\SOH\v\f\n\
    \\r\n\
    \\ENQ\EOT\DLE\t\NUL\STX\DC2\EOT\131\SOH\v\f\n\
    \\v\n\
    \\ETX\EOT\DLE\n\
    \\DC2\EOT\132\SOH\STX\EM\n\
    \\f\n\
    \\EOT\EOT\DLE\n\
    \\NUL\DC2\EOT\132\SOH\v\CAN\n\
    \\226\EOT\n\
    \\STX\EOT\DC1\DC2\ACK\141\SOH\NUL\146\SOH\SOH\SUB\211\EOT Forks a running machine, without an intermediate checkpoint, into `count` fresh\n\
    \ children. Admission requires CAPABILITY_LIVE_FORK or CAPABILITY_DISK_FORK in the source\n\
    \ machine's contract; a provider or machine without either rejects with UNIMPLEMENTED so\n\
    \ callers fall back to Checkpoint + Fork or a restart. The admitted ForkFidelity states\n\
    \ what the children inherited. Children inherit the source's exact MachineContract and\n\
    \ receive fresh MachineIds and endpoints; open network connections are never carried over.\n\
    \ See the acyclic-machines crate documentation for the complete semantics.\n\
    \\n\
    \\v\n\
    \\ETX\EOT\DC1\SOH\DC2\EOT\141\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\EOT\142\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\ACK\DC2\EOT\142\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\EOT\142\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\EOT\142\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\SOH\DC2\EOT\143\SOH\STX%\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\ACK\DC2\EOT\143\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\SOH\DC2\EOT\143\SOH\DC1 \n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\ETX\DC2\EOT\143\SOH#$\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\STX\DC2\EOT\144\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\STX\ACK\DC2\EOT\144\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\STX\SOH\DC2\EOT\144\SOH\f\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\STX\ETX\DC2\EOT\144\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\ETX\DC2\EOT\145\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\ETX\ENQ\DC2\EOT\145\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\ETX\SOH\DC2\EOT\145\SOH\t\SO\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\ETX\ETX\DC2\EOT\145\SOH\DC1\DC2\n\
    \\f\n\
    \\STX\EOT\DC2\DC2\ACK\147\SOH\NUL\152\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC2\SOH\DC2\EOT\147\SOH\b\"\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\EOT\148\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\ACK\DC2\EOT\148\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\EOT\148\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\EOT\148\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\SOH\DC2\EOT\149\SOH\STX%\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\SOH\ACK\DC2\EOT\149\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\SOH\SOH\DC2\EOT\149\SOH\DC1 \n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\SOH\ETX\DC2\EOT\149\SOH#$\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\STX\DC2\EOT\150\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\STX\ACK\DC2\EOT\150\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\STX\SOH\DC2\EOT\150\SOH\f\DC3\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\STX\ETX\DC2\EOT\150\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\DC2\STX\ETX\DC2\EOT\151\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\ETX\ACK\DC2\EOT\151\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\ETX\SOH\DC2\EOT\151\SOH\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\ETX\ETX\DC2\EOT\151\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\DC3\DC2\ACK\153\SOH\NUL\157\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC3\SOH\DC2\EOT\153\SOH\b!\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\EOT\154\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ACK\DC2\EOT\154\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\EOT\154\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\EOT\154\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\SOH\DC2\EOT\155\SOH\STX%\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\ACK\DC2\EOT\155\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\SOH\DC2\EOT\155\SOH\DC1 \n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\ETX\DC2\EOT\155\SOH#$\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\STX\DC2\EOT\156\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\STX\ACK\DC2\EOT\156\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\STX\SOH\DC2\EOT\156\SOH\SI\EM\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\STX\ETX\DC2\EOT\156\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\DC4\DC2\ACK\158\SOH\NUL\161\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC4\SOH\DC2\EOT\158\SOH\b\SYN\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\EOT\159\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ACK\DC2\EOT\159\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\EOT\159\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\EOT\159\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\EOT\160\SOH\STX%\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ACK\DC2\EOT\160\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\EOT\160\SOH\DC1 \n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\EOT\160\SOH#$\n\
    \\f\n\
    \\STX\EOT\NAK\DC2\ACK\162\SOH\NUL\165\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\162\SOH\b\GS\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\163\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ACK\DC2\EOT\163\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\163\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\163\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\SOH\DC2\EOT\164\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ACK\DC2\EOT\164\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\SOH\DC2\EOT\164\SOH\f\DC3\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ETX\DC2\EOT\164\SOH\SYN\ETB\n\
    \\f\n\
    \\STX\EOT\SYN\DC2\ACK\166\SOH\NUL\169\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\166\SOH\b \n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\167\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ACK\DC2\EOT\167\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\167\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\167\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\168\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ACK\DC2\EOT\168\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\168\SOH\SI\EM\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\168\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\170\SOH\NUL\174\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\170\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\171\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ACK\DC2\EOT\171\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\171\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\171\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\172\SOH\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ACK\DC2\EOT\172\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\172\SOH\f\DC1\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\172\SOH\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\STX\DC2\EOT\173\SOH\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ENQ\DC2\EOT\173\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\SOH\DC2\EOT\173\SOH\t\SO\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ETX\DC2\EOT\173\SOH\DC1\DC2\n\
    \\f\n\
    \\STX\EOT\CAN\DC2\ACK\175\SOH\NUL\178\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\CAN\SOH\DC2\EOT\175\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\NUL\DC2\EOT\176\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ACK\DC2\EOT\176\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\SOH\DC2\EOT\176\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ETX\DC2\EOT\176\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\SOH\DC2\EOT\177\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ACK\DC2\EOT\177\SOH\STX\r\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\SOH\DC2\EOT\177\SOH\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ETX\DC2\EOT\177\SOH\SUB\ESC\n\
    \\f\n\
    \\STX\ENQ\EOT\DC2\ACK\180\SOH\NUL\187\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\EOT\SOH\DC2\EOT\180\SOH\ENQ\DC4\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\NUL\DC2\EOT\181\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\NUL\SOH\DC2\EOT\181\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\NUL\STX\DC2\EOT\181\SOH!\"\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\SOH\DC2\EOT\182\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\SOH\SOH\DC2\EOT\182\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\SOH\STX\DC2\EOT\182\SOH\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\STX\DC2\EOT\183\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\STX\SOH\DC2\EOT\183\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\STX\STX\DC2\EOT\183\SOH\US \n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\ETX\DC2\EOT\184\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ETX\SOH\DC2\EOT\184\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ETX\STX\DC2\EOT\184\SOH\US \n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\EOT\DC2\EOT\185\SOH\STX%\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\EOT\SOH\DC2\EOT\185\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\EOT\STX\DC2\EOT\185\SOH#$\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\ENQ\DC2\EOT\186\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ENQ\SOH\DC2\EOT\186\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ENQ\STX\DC2\EOT\186\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT\EM\DC2\ACK\188\SOH\NUL\191\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\EM\SOH\DC2\EOT\188\SOH\b\SYN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\NUL\DC2\EOT\189\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ACK\DC2\EOT\189\SOH\STX\r\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\SOH\DC2\EOT\189\SOH\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ETX\DC2\EOT\189\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\EM\STX\SOH\DC2\EOT\190\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ACK\DC2\EOT\190\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\SOH\DC2\EOT\190\SOH\DC2\CAN\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ETX\DC2\EOT\190\SOH\ESC\FS\n\
    \\f\n\
    \\STX\ENQ\ENQ\DC2\ACK\192\SOH\NUL\203\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\ENQ\SOH\DC2\EOT\192\SOH\ENQ\DC2\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\NUL\DC2\EOT\193\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\NUL\SOH\DC2\EOT\193\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\NUL\STX\DC2\EOT\193\SOH\US \n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\SOH\DC2\EOT\194\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\SOH\SOH\DC2\EOT\194\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\SOH\STX\DC2\EOT\194\SOH\FS\GS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\STX\DC2\EOT\195\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\STX\SOH\DC2\EOT\195\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\STX\STX\DC2\EOT\195\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\ETX\DC2\EOT\196\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ETX\SOH\DC2\EOT\196\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ETX\STX\DC2\EOT\196\SOH\RS\US\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\EOT\DC2\EOT\197\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\EOT\SOH\DC2\EOT\197\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\EOT\STX\DC2\EOT\197\SOH\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\ENQ\DC2\EOT\198\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ENQ\SOH\DC2\EOT\198\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ENQ\STX\DC2\EOT\198\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\ACK\DC2\EOT\199\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ACK\SOH\DC2\EOT\199\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ACK\STX\DC2\EOT\199\SOH\RS\US\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\a\DC2\EOT\200\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\a\SOH\DC2\EOT\200\SOH\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\a\STX\DC2\EOT\200\SOH\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\b\DC2\EOT\201\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\b\SOH\DC2\EOT\201\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\b\STX\DC2\EOT\201\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\t\DC2\EOT\202\SOH\STX#\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\t\SOH\DC2\EOT\202\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\t\STX\DC2\EOT\202\SOH!\"\n\
    \\f\n\
    \\STX\EOT\SUB\DC2\ACK\204\SOH\NUL\207\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SUB\SOH\DC2\EOT\204\SOH\b\DLE\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\NUL\DC2\EOT\205\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ENQ\DC2\EOT\205\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\SOH\DC2\EOT\205\SOH\t\r\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ETX\DC2\EOT\205\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\SOH\DC2\EOT\206\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ENQ\DC2\EOT\206\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\SOH\DC2\EOT\206\SOH\t\f\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ETX\DC2\EOT\206\SOH\SI\DLE\n\
    \\f\n\
    \\STX\EOT\ESC\DC2\ACK\208\SOH\NUL\216\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\ESC\SOH\DC2\EOT\208\SOH\b\DC4\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\NUL\DC2\EOT\209\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ACK\DC2\EOT\209\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\SOH\DC2\EOT\209\SOH\f\DC3\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ETX\DC2\EOT\209\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\SOH\DC2\EOT\210\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ACK\DC2\EOT\210\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\SOH\DC2\EOT\210\SOH\DLE\SYN\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ETX\DC2\EOT\210\SOH\EM\SUB\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\STX\DC2\EOT\211\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ACK\DC2\EOT\211\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\SOH\DC2\EOT\211\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ETX\DC2\EOT\211\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\ETX\DC2\EOT\212\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\EOT\DC2\EOT\212\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\ACK\DC2\EOT\212\SOH\v\DC3\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\SOH\DC2\EOT\212\SOH\DC4\GS\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\ETX\DC2\EOT\212\SOH !\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\EOT\DC2\EOT\213\SOH\STX#\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\ACK\DC2\EOT\213\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\SOH\DC2\EOT\213\SOH\SI\RS\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\ETX\DC2\EOT\213\SOH!\"\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\ENQ\DC2\EOT\214\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ENQ\ENQ\DC2\EOT\214\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ENQ\SOH\DC2\EOT\214\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ENQ\ETX\DC2\EOT\214\SOH\RS\US\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\ACK\DC2\EOT\215\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ACK\ENQ\DC2\EOT\215\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ACK\SOH\DC2\EOT\215\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ACK\ETX\DC2\EOT\215\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\FS\DC2\ACK\217\SOH\NUL\220\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\FS\SOH\DC2\EOT\217\SOH\b\DC3\n\
    \\f\n\
    \\EOT\EOT\FS\STX\NUL\DC2\EOT\218\SOH\STX%\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\EOT\DC2\EOT\218\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ACK\DC2\EOT\218\SOH\v\ETB\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\SOH\DC2\EOT\218\SOH\CAN \n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ETX\DC2\EOT\218\SOH#$\n\
    \\f\n\
    \\EOT\EOT\FS\STX\SOH\DC2\EOT\219\SOH\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ACK\DC2\EOT\219\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\SOH\DC2\EOT\219\SOH\f\DLE\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ETX\DC2\EOT\219\SOH\DC3\DC4\n\
    \\f\n\
    \\STX\EOT\GS\DC2\ACK\221\SOH\NUL\227\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\GS\SOH\DC2\EOT\221\SOH\b\ETB\n\
    \\f\n\
    \\EOT\EOT\GS\STX\NUL\DC2\EOT\222\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ACK\DC2\EOT\222\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\SOH\DC2\EOT\222\SOH\SI\EM\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ETX\DC2\EOT\222\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\GS\STX\SOH\DC2\EOT\223\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\SOH\ACK\DC2\EOT\223\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\SOH\SOH\DC2\EOT\223\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\SOH\ETX\DC2\EOT\223\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\GS\STX\STX\DC2\EOT\224\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\STX\ACK\DC2\EOT\224\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\STX\SOH\DC2\EOT\224\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\STX\ETX\DC2\EOT\224\SOH\GS\RS\n\
    \\f\n\
    \\EOT\EOT\GS\STX\ETX\DC2\EOT\225\SOH\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\ETX\ENQ\DC2\EOT\225\SOH\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\ETX\SOH\DC2\EOT\225\SOH\a\SI\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\ETX\ETX\DC2\EOT\225\SOH\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\GS\STX\EOT\DC2\EOT\226\SOH\STX \n\
    \\r\n\
    \\ENQ\EOT\GS\STX\EOT\ENQ\DC2\EOT\226\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\EOT\SOH\DC2\EOT\226\SOH\t\ESC\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\EOT\ETX\DC2\EOT\226\SOH\RS\US\n\
    \\f\n\
    \\STX\EOT\RS\DC2\ACK\228\SOH\NUL\232\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\RS\SOH\DC2\EOT\228\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\RS\STX\NUL\DC2\EOT\229\SOH\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ACK\DC2\EOT\229\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\SOH\DC2\EOT\229\SOH\f\DC3\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\NUL\ETX\DC2\EOT\229\SOH\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\RS\STX\SOH\DC2\EOT\230\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ACK\DC2\EOT\230\SOH\STX\r\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\SOH\DC2\EOT\230\SOH\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\SOH\ETX\DC2\EOT\230\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\RS\STX\STX\DC2\EOT\231\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\ACK\DC2\EOT\231\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\SOH\DC2\EOT\231\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\RS\STX\STX\ETX\DC2\EOT\231\SOH\GS\RS\n\
    \\f\n\
    \\STX\EOT\US\DC2\ACK\233\SOH\NUL\238\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\US\SOH\DC2\EOT\233\SOH\b\ESC\n\
    \\f\n\
    \\EOT\EOT\US\STX\NUL\DC2\EOT\234\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ACK\DC2\EOT\234\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\SOH\DC2\EOT\234\SOH\SI\EM\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ETX\DC2\EOT\234\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT\US\STX\SOH\DC2\EOT\235\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ACK\DC2\EOT\235\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\SOH\DC2\EOT\235\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ETX\DC2\EOT\235\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\US\STX\STX\DC2\EOT\236\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\ACK\DC2\EOT\236\SOH\STX\r\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\SOH\DC2\EOT\236\SOH\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT\US\STX\STX\ETX\DC2\EOT\236\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\US\STX\ETX\DC2\EOT\237\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT\US\STX\ETX\ACK\DC2\EOT\237\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\US\STX\ETX\SOH\DC2\EOT\237\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\US\STX\ETX\ETX\DC2\EOT\237\SOH\GS\RS\n\
    \\f\n\
    \\STX\EOT \DC2\ACK\239\SOH\NUL\244\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT \SOH\DC2\EOT\239\SOH\b\NAK\n\
    \\f\n\
    \\EOT\EOT \STX\NUL\DC2\EOT\240\SOH\STX\RS\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ACK\DC2\EOT\240\SOH\STX\SO\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\SOH\DC2\EOT\240\SOH\SI\EM\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ETX\DC2\EOT\240\SOH\FS\GS\n\
    \\f\n\
    \\EOT\EOT \STX\SOH\DC2\EOT\241\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\EOT\DC2\EOT\241\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ACK\DC2\EOT\241\SOH\v\DC4\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\SOH\DC2\EOT\241\SOH\NAK\GS\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ETX\DC2\EOT\241\SOH !\n\
    \\f\n\
    \\EOT\EOT \STX\STX\DC2\EOT\242\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT \STX\STX\ACK\DC2\EOT\242\SOH\STX\r\n\
    \\r\n\
    \\ENQ\EOT \STX\STX\SOH\DC2\EOT\242\SOH\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT \STX\STX\ETX\DC2\EOT\242\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT \STX\ETX\DC2\EOT\243\SOH\STX\US\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\ACK\DC2\EOT\243\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\SOH\DC2\EOT\243\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT \STX\ETX\ETX\DC2\EOT\243\SOH\GS\RS\n\
    \\f\n\
    \\STX\ENQ\ACK\DC2\ACK\245\SOH\NUL\252\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\ACK\SOH\DC2\EOT\245\SOH\ENQ\DC1\n\
    \\f\n\
    \\EOT\ENQ\ACK\STX\NUL\DC2\EOT\246\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\ACK\STX\NUL\SOH\DC2\EOT\246\SOH\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\ACK\STX\NUL\STX\DC2\EOT\246\SOH\RS\US\n\
    \b\n\
    \\EOT\ENQ\ACK\STX\SOH\DC2\EOT\248\SOH\STX$\SUBT Children resume from the source's memory, processes, and disk at the fork instant.\n\
    \\n\
    \\r\n\
    \\ENQ\ENQ\ACK\STX\SOH\SOH\DC2\EOT\248\SOH\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\ACK\STX\SOH\STX\DC2\EOT\248\SOH\"#\n\
    \\190\SOH\n\
    \\EOT\ENQ\ACK\STX\STX\DC2\EOT\251\SOH\STX\RS\SUB\175\SOH Children boot fresh over a copy of the source's persistent disk (provider-defined; see\n\
    \ CAPABILITY_DISK_FORK) taken at one consistent instant; no process state is inherited.\n\
    \\n\
    \\r\n\
    \\ENQ\ENQ\ACK\STX\STX\SOH\DC2\EOT\251\SOH\STX\EM\n\
    \\r\n\
    \\ENQ\ENQ\ACK\STX\STX\STX\DC2\EOT\251\SOH\FS\GS\n\
    \\f\n\
    \\STX\EOT!\DC2\ACK\253\SOH\NUL\131\STX\SOH\n\
    \\v\n\
    \\ETX\EOT!\SOH\DC2\EOT\253\SOH\b\FS\n\
    \\f\n\
    \\EOT\EOT!\STX\NUL\DC2\EOT\254\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ACK\DC2\EOT\254\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\SOH\DC2\EOT\254\SOH\f\DC2\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ETX\DC2\EOT\254\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT!\STX\SOH\DC2\EOT\255\SOH\STX\"\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\EOT\DC2\EOT\255\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ACK\DC2\EOT\255\SOH\v\DC4\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\SOH\DC2\EOT\255\SOH\NAK\GS\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ETX\DC2\EOT\255\SOH !\n\
    \\f\n\
    \\EOT\EOT!\STX\STX\DC2\EOT\128\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\ACK\DC2\EOT\128\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\SOH\DC2\EOT\128\STX\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\ETX\DC2\EOT\128\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT!\STX\ETX\DC2\EOT\129\STX\STX\US\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\ACK\DC2\EOT\129\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\SOH\DC2\EOT\129\STX\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\ETX\DC2\EOT\129\STX\GS\RS\n\
    \\f\n\
    \\EOT\EOT!\STX\EOT\DC2\EOT\130\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT!\STX\EOT\ACK\DC2\EOT\130\STX\STX\SO\n\
    \\r\n\
    \\ENQ\EOT!\STX\EOT\SOH\DC2\EOT\130\STX\SI\ETB\n\
    \\r\n\
    \\ENQ\EOT!\STX\EOT\ETX\DC2\EOT\130\STX\SUB\ESC\n\
    \\f\n\
    \\STX\EOT\"\DC2\ACK\132\STX\NUL\136\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\"\SOH\DC2\EOT\132\STX\b\ETB\n\
    \\f\n\
    \\EOT\EOT\"\STX\NUL\DC2\EOT\133\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ACK\DC2\EOT\133\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\SOH\DC2\EOT\133\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ETX\DC2\EOT\133\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT\"\STX\SOH\DC2\EOT\134\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\"\STX\SOH\ACK\DC2\EOT\134\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT\"\STX\SOH\SOH\DC2\EOT\134\STX\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT\"\STX\SOH\ETX\DC2\EOT\134\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\"\STX\STX\DC2\EOT\135\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT\"\STX\STX\ACK\DC2\EOT\135\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\"\STX\STX\SOH\DC2\EOT\135\STX\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT\"\STX\STX\ETX\DC2\EOT\135\STX\FS\GS\n\
    \\f\n\
    \\STX\EOT#\DC2\ACK\137\STX\NUL\141\STX\SOH\n\
    \\v\n\
    \\ETX\EOT#\SOH\DC2\EOT\137\STX\b\EM\n\
    \\f\n\
    \\EOT\EOT#\STX\NUL\DC2\EOT\138\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ACK\DC2\EOT\138\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\SOH\DC2\EOT\138\STX\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ETX\DC2\EOT\138\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT#\STX\SOH\DC2\EOT\139\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ACK\DC2\EOT\139\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\SOH\DC2\EOT\139\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT#\STX\SOH\ETX\DC2\EOT\139\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT#\STX\STX\DC2\EOT\140\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT#\STX\STX\ACK\DC2\EOT\140\STX\STX\SO\n\
    \\r\n\
    \\ENQ\EOT#\STX\STX\SOH\DC2\EOT\140\STX\SI\EM\n\
    \\r\n\
    \\ENQ\EOT#\STX\STX\ETX\DC2\EOT\140\STX\FS\GS\n\
    \\f\n\
    \\STX\EOT$\DC2\ACK\142\STX\NUL\155\STX\SOH\n\
    \\v\n\
    \\ETX\EOT$\SOH\DC2\EOT\142\STX\b\SUB\n\
    \\f\n\
    \\EOT\EOT$\STX\NUL\DC2\EOT\143\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ACK\DC2\EOT\143\STX\STX\r\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\SOH\DC2\EOT\143\STX\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ETX\DC2\EOT\143\STX\SUB\ESC\n\
    \\SO\n\
    \\EOT\EOT$\b\NUL\DC2\ACK\144\STX\STX\154\STX\ETX\n\
    \\r\n\
    \\ENQ\EOT$\b\NUL\SOH\DC2\EOT\144\STX\b\SO\n\
    \\f\n\
    \\EOT\EOT$\STX\SOH\DC2\EOT\145\STX\EOT \n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\ACK\DC2\EOT\145\STX\EOT\DC4\n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\SOH\DC2\EOT\145\STX\NAK\ESC\n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\ETX\DC2\EOT\145\STX\RS\US\n\
    \\f\n\
    \\EOT\EOT$\STX\STX\DC2\EOT\146\STX\EOT'\n\
    \\r\n\
    \\ENQ\EOT$\STX\STX\ACK\DC2\EOT\146\STX\EOT\ETB\n\
    \\r\n\
    \\ENQ\EOT$\STX\STX\SOH\DC2\EOT\146\STX\CAN\"\n\
    \\r\n\
    \\ENQ\EOT$\STX\STX\ETX\DC2\EOT\146\STX%&\n\
    \\f\n\
    \\EOT\EOT$\STX\ETX\DC2\EOT\147\STX\EOT\ESC\n\
    \\r\n\
    \\ENQ\EOT$\STX\ETX\ACK\DC2\EOT\147\STX\EOT\DC1\n\
    \\r\n\
    \\ENQ\EOT$\STX\ETX\SOH\DC2\EOT\147\STX\DC2\SYN\n\
    \\r\n\
    \\ENQ\EOT$\STX\ETX\ETX\DC2\EOT\147\STX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT$\STX\EOT\DC2\EOT\148\STX\EOT\"\n\
    \\r\n\
    \\ENQ\EOT$\STX\EOT\ACK\DC2\EOT\148\STX\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT$\STX\EOT\SOH\DC2\EOT\148\STX\SYN\GS\n\
    \\r\n\
    \\ENQ\EOT$\STX\EOT\ETX\DC2\EOT\148\STX !\n\
    \\f\n\
    \\EOT\EOT$\STX\ENQ\DC2\EOT\149\STX\EOT\US\n\
    \\r\n\
    \\ENQ\EOT$\STX\ENQ\ACK\DC2\EOT\149\STX\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT$\STX\ENQ\SOH\DC2\EOT\149\STX\SYN\SUB\n\
    \\r\n\
    \\ENQ\EOT$\STX\ENQ\ETX\DC2\EOT\149\STX\GS\RS\n\
    \\f\n\
    \\EOT\EOT$\STX\ACK\DC2\EOT\150\STX\EOT*\n\
    \\r\n\
    \\ENQ\EOT$\STX\ACK\ACK\DC2\EOT\150\STX\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT$\STX\ACK\SOH\DC2\EOT\150\STX\SYN%\n\
    \\r\n\
    \\ENQ\EOT$\STX\ACK\ETX\DC2\EOT\150\STX()\n\
    \\f\n\
    \\EOT\EOT$\STX\a\DC2\EOT\151\STX\EOT.\n\
    \\r\n\
    \\ENQ\EOT$\STX\a\ACK\DC2\EOT\151\STX\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT$\STX\a\SOH\DC2\EOT\151\STX\DC4)\n\
    \\r\n\
    \\ENQ\EOT$\STX\a\ETX\DC2\EOT\151\STX,-\n\
    \\f\n\
    \\EOT\EOT$\STX\b\DC2\EOT\152\STX\EOT-\n\
    \\r\n\
    \\ENQ\EOT$\STX\b\ACK\DC2\EOT\152\STX\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT$\STX\b\SOH\DC2\EOT\152\STX\SYN(\n\
    \\r\n\
    \\ENQ\EOT$\STX\b\ETX\DC2\EOT\152\STX+,\n\
    \\f\n\
    \\EOT\EOT$\STX\t\DC2\EOT\153\STX\EOT+\n\
    \\r\n\
    \\ENQ\EOT$\STX\t\ACK\DC2\EOT\153\STX\EOT\CAN\n\
    \\r\n\
    \\ENQ\EOT$\STX\t\SOH\DC2\EOT\153\STX\EM%\n\
    \\r\n\
    \\ENQ\EOT$\STX\t\ETX\DC2\EOT\153\STX(*\n\
    \\148\SOH\n\
    \\STX\EOT%\DC2\ACK\159\STX\NUL\161\STX\SOH\SUB\133\SOH Terminal simulator result. Unlike an admission, this contains the checked\n\
    \ observations produced after the operation has completed.\n\
    \\n\
    \\v\n\
    \\ETX\EOT%\SOH\DC2\EOT\159\STX\b\SYN\n\
    \\f\n\
    \\EOT\EOT%\STX\NUL\DC2\EOT\160\STX\STX%\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\EOT\DC2\EOT\160\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ACK\DC2\EOT\160\STX\v\ETB\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\SOH\DC2\EOT\160\STX\CAN \n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ETX\DC2\EOT\160\STX#$\n\
    \\f\n\
    \\STX\EOT&\DC2\ACK\162\STX\NUL\166\STX\SOH\n\
    \\v\n\
    \\ETX\EOT&\SOH\DC2\EOT\162\STX\b\SUB\n\
    \\f\n\
    \\EOT\EOT&\STX\NUL\DC2\EOT\163\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ACK\DC2\EOT\163\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\SOH\DC2\EOT\163\STX\f\DC2\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ETX\DC2\EOT\163\STX\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT&\STX\SOH\DC2\EOT\164\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ACK\DC2\EOT\164\STX\STX\SO\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\SOH\DC2\EOT\164\STX\SI\ETB\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ETX\DC2\EOT\164\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT&\STX\STX\DC2\EOT\165\STX\STX%\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\EOT\DC2\EOT\165\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\ACK\DC2\EOT\165\STX\v\ETB\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\SOH\DC2\EOT\165\STX\CAN \n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\ETX\DC2\EOT\165\STX#$\n\
    \\f\n\
    \\STX\EOT'\DC2\ACK\167\STX\NUL\170\STX\SOH\n\
    \\v\n\
    \\ETX\EOT'\SOH\DC2\EOT\167\STX\b\DC1\n\
    \\f\n\
    \\EOT\EOT'\STX\NUL\DC2\EOT\168\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\ACK\DC2\EOT\168\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\SOH\DC2\EOT\168\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\ETX\DC2\EOT\168\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT'\STX\SOH\DC2\EOT\169\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\ACK\DC2\EOT\169\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\SOH\DC2\EOT\169\STX\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\ETX\DC2\EOT\169\STX\FS\GS\n\
    \\f\n\
    \\STX\EOT(\DC2\ACK\171\STX\NUL\183\STX\SOH\n\
    \\v\n\
    \\ETX\EOT(\SOH\DC2\EOT\171\STX\b\ETB\n\
    \\SO\n\
    \\EOT\EOT(\b\NUL\DC2\ACK\172\STX\STX\182\STX\ETX\n\
    \\r\n\
    \\ENQ\EOT(\b\NUL\SOH\DC2\EOT\172\STX\b\SO\n\
    \\f\n\
    \\EOT\EOT(\STX\NUL\DC2\EOT\173\STX\EOT\GS\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\ACK\DC2\EOT\173\STX\EOT\DLE\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\SOH\DC2\EOT\173\STX\DC1\CAN\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\ETX\DC2\EOT\173\STX\ESC\FS\n\
    \\f\n\
    \\EOT\EOT(\STX\SOH\DC2\EOT\174\STX\EOT%\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\ACK\DC2\EOT\174\STX\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\SOH\DC2\EOT\174\STX\DC4 \n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\ETX\DC2\EOT\174\STX#$\n\
    \\f\n\
    \\EOT\EOT(\STX\STX\DC2\EOT\175\STX\EOT\RS\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\ACK\DC2\EOT\175\STX\EOT\DC2\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\SOH\DC2\EOT\175\STX\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\ETX\DC2\EOT\175\STX\FS\GS\n\
    \\f\n\
    \\EOT\EOT(\STX\ETX\DC2\EOT\176\STX\EOT\FS\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\ACK\DC2\EOT\176\STX\EOT\r\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\SOH\DC2\EOT\176\STX\SO\ETB\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\ETX\DC2\EOT\176\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT(\STX\EOT\DC2\EOT\177\STX\EOT\CAN\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\ACK\DC2\EOT\177\STX\EOT\r\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\SOH\DC2\EOT\177\STX\SO\DC3\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\ETX\DC2\EOT\177\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT(\STX\ENQ\DC2\EOT\178\STX\EOT(\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\ACK\DC2\EOT\178\STX\EOT\r\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\SOH\DC2\EOT\178\STX\SO#\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\ETX\DC2\EOT\178\STX&'\n\
    \\f\n\
    \\EOT\EOT(\STX\ACK\DC2\EOT\179\STX\EOT$\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\ACK\DC2\EOT\179\STX\EOT\r\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\SOH\DC2\EOT\179\STX\SO\US\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\ETX\DC2\EOT\179\STX\"#\n\
    \\f\n\
    \\EOT\EOT(\STX\a\DC2\EOT\180\STX\EOT*\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\ACK\DC2\EOT\180\STX\EOT\DLE\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\SOH\DC2\EOT\180\STX\DC1%\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\ETX\DC2\EOT\180\STX()\n\
    \\f\n\
    \\EOT\EOT(\STX\b\DC2\EOT\181\STX\EOT*\n\
    \\r\n\
    \\ENQ\EOT(\STX\b\ACK\DC2\EOT\181\STX\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT(\STX\b\SOH\DC2\EOT\181\STX\ETB%\n\
    \\r\n\
    \\ENQ\EOT(\STX\b\ETX\DC2\EOT\181\STX()\n\
    \\f\n\
    \\STX\EOT)\DC2\ACK\184\STX\NUL\186\STX\SOH\n\
    \\v\n\
    \\ETX\EOT)\SOH\DC2\EOT\184\STX\b\NAK\n\
    \\f\n\
    \\EOT\EOT)\STX\NUL\DC2\EOT\185\STX\STX)\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\EOT\DC2\EOT\185\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\ACK\DC2\EOT\185\STX\v\EM\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\SOH\DC2\EOT\185\STX\SUB$\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\ETX\DC2\EOT\185\STX'(\n\
    \\f\n\
    \\STX\ENQ\a\DC2\ACK\188\STX\NUL\193\STX\SOH\n\
    \\v\n\
    \\ETX\ENQ\a\SOH\DC2\EOT\188\STX\ENQ\DC1\n\
    \\f\n\
    \\EOT\ENQ\a\STX\NUL\DC2\EOT\189\STX\STX \n\
    \\r\n\
    \\ENQ\ENQ\a\STX\NUL\SOH\DC2\EOT\189\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\NUL\STX\DC2\EOT\189\STX\RS\US\n\
    \\f\n\
    \\EOT\ENQ\a\STX\SOH\DC2\EOT\190\STX\STX$\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\SOH\SOH\DC2\EOT\190\STX\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\SOH\STX\DC2\EOT\190\STX\"#\n\
    \\f\n\
    \\EOT\ENQ\a\STX\STX\DC2\EOT\191\STX\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\STX\SOH\DC2\EOT\191\STX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\STX\STX\DC2\EOT\191\STX !\n\
    \\f\n\
    \\EOT\ENQ\a\STX\ETX\DC2\EOT\192\STX\STX'\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\ETX\SOH\DC2\EOT\192\STX\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\a\STX\ETX\STX\DC2\EOT\192\STX%&\n\
    \\f\n\
    \\STX\ENQ\b\DC2\ACK\194\STX\NUL\199\STX\SOH\n\
    \\v\n\
    \\ETX\ENQ\b\SOH\DC2\EOT\194\STX\ENQ\SO\n\
    \\f\n\
    \\EOT\ENQ\b\STX\NUL\DC2\EOT\195\STX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\NUL\SOH\DC2\EOT\195\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\NUL\STX\DC2\EOT\195\STX\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\b\STX\SOH\DC2\EOT\196\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\SOH\SOH\DC2\EOT\196\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\SOH\STX\DC2\EOT\196\STX\NAK\SYN\n\
    \\f\n\
    \\EOT\ENQ\b\STX\STX\DC2\EOT\197\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\STX\SOH\DC2\EOT\197\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\STX\STX\DC2\EOT\197\STX\CAN\EM\n\
    \\f\n\
    \\EOT\ENQ\b\STX\ETX\DC2\EOT\198\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\ETX\SOH\DC2\EOT\198\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\b\STX\ETX\STX\DC2\EOT\198\STX\CAN\EM\n\
    \\f\n\
    \\STX\EOT*\DC2\ACK\200\STX\NUL\207\STX\SOH\n\
    \\v\n\
    \\ETX\EOT*\SOH\DC2\EOT\200\STX\b\DC4\n\
    \\f\n\
    \\EOT\EOT*\STX\NUL\DC2\EOT\201\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\ACK\DC2\EOT\201\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\SOH\DC2\EOT\201\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\ETX\DC2\EOT\201\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT*\STX\SOH\DC2\EOT\202\STX\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT*\STX\SOH\ENQ\DC2\EOT\202\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT*\STX\SOH\SOH\DC2\EOT\202\STX\t\DC1\n\
    \\r\n\
    \\ENQ\EOT*\STX\SOH\ETX\DC2\EOT\202\STX\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT*\STX\STX\DC2\EOT\203\STX\STX!\n\
    \\r\n\
    \\ENQ\EOT*\STX\STX\ENQ\DC2\EOT\203\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT*\STX\STX\SOH\DC2\EOT\203\STX\t\FS\n\
    \\r\n\
    \\ENQ\EOT*\STX\STX\ETX\DC2\EOT\203\STX\US \n\
    \\f\n\
    \\EOT\EOT*\STX\ETX\DC2\EOT\204\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT*\STX\ETX\ACK\DC2\EOT\204\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT*\STX\ETX\SOH\DC2\EOT\204\STX\f\DLE\n\
    \\r\n\
    \\ENQ\EOT*\STX\ETX\ETX\DC2\EOT\204\STX\DC3\DC4\n\
    \\f\n\
    \\EOT\EOT*\STX\EOT\DC2\EOT\205\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\EOT*\STX\EOT\ACK\DC2\EOT\205\STX\STX\SI\n\
    \\r\n\
    \\ENQ\EOT*\STX\EOT\SOH\DC2\EOT\205\STX\DLE\NAK\n\
    \\r\n\
    \\ENQ\EOT*\STX\EOT\ETX\DC2\EOT\205\STX\CAN\EM\n\
    \\f\n\
    \\EOT\EOT*\STX\ENQ\DC2\EOT\206\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT*\STX\ENQ\ACK\DC2\EOT\206\STX\STX\SO\n\
    \\r\n\
    \\ENQ\EOT*\STX\ENQ\SOH\DC2\EOT\206\STX\SI\ETB\n\
    \\r\n\
    \\ENQ\EOT*\STX\ENQ\ETX\DC2\EOT\206\STX\SUB\ESC\n\
    \\f\n\
    \\STX\EOT+\DC2\ACK\208\STX\NUL\213\STX\SOH\n\
    \\v\n\
    \\ETX\EOT+\SOH\DC2\EOT\208\STX\b\NAK\n\
    \\f\n\
    \\EOT\EOT+\STX\NUL\DC2\EOT\209\STX\STX\US\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\ACK\DC2\EOT\209\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\SOH\DC2\EOT\209\STX\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\ETX\DC2\EOT\209\STX\GS\RS\n\
    \\f\n\
    \\EOT\EOT+\STX\SOH\DC2\EOT\210\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT+\STX\SOH\ACK\DC2\EOT\210\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT+\STX\SOH\SOH\DC2\EOT\210\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT+\STX\SOH\ETX\DC2\EOT\210\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT+\STX\STX\DC2\EOT\211\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT+\STX\STX\ENQ\DC2\EOT\211\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT+\STX\STX\SOH\DC2\EOT\211\STX\t\ETB\n\
    \\r\n\
    \\ENQ\EOT+\STX\STX\ETX\DC2\EOT\211\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT+\STX\ETX\DC2\EOT\212\STX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT+\STX\ETX\ENQ\DC2\EOT\212\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT+\STX\ETX\SOH\DC2\EOT\212\STX\t\SO\n\
    \\r\n\
    \\ENQ\EOT+\STX\ETX\ETX\DC2\EOT\212\STX\DC1\DC2\n\
    \\f\n\
    \\STX\EOT,\DC2\ACK\214\STX\NUL\217\STX\SOH\n\
    \\v\n\
    \\ETX\EOT,\SOH\DC2\EOT\214\STX\b\DC1\n\
    \\f\n\
    \\EOT\EOT,\STX\NUL\DC2\EOT\215\STX\STX#\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\EOT\DC2\EOT\215\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\ACK\DC2\EOT\215\STX\v\ETB\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\SOH\DC2\EOT\215\STX\CAN\RS\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\ETX\DC2\EOT\215\STX!\"\n\
    \\f\n\
    \\EOT\EOT,\STX\SOH\DC2\EOT\216\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\ENQ\DC2\EOT\216\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\SOH\DC2\EOT\216\STX\t\SYN\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\ETX\DC2\EOT\216\STX\EM\SUB\n\
    \\f\n\
    \\STX\EOT-\DC2\ACK\218\STX\NUL\223\STX\SOH\n\
    \\v\n\
    \\ETX\EOT-\SOH\DC2\EOT\218\STX\b\DC4\n\
    \\f\n\
    \\EOT\EOT-\STX\NUL\DC2\EOT\219\STX\STX\US\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\ACK\DC2\EOT\219\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\SOH\DC2\EOT\219\STX\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\ETX\DC2\EOT\219\STX\GS\RS\n\
    \\f\n\
    \\EOT\EOT-\STX\SOH\DC2\EOT\220\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\ACK\DC2\EOT\220\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\SOH\DC2\EOT\220\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\ETX\DC2\EOT\220\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT-\STX\STX\DC2\EOT\221\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\ENQ\DC2\EOT\221\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\SOH\DC2\EOT\221\STX\t\SYN\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\ETX\DC2\EOT\221\STX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT-\STX\ETX\DC2\EOT\222\STX\STX\EM\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\ENQ\DC2\EOT\222\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\SOH\DC2\EOT\222\STX\t\DC4\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\ETX\DC2\EOT\222\STX\ETB\CAN\n\
    \\f\n\
    \\STX\EOT.\DC2\ACK\224\STX\NUL\237\STX\SOH\n\
    \\v\n\
    \\ETX\EOT.\SOH\DC2\EOT\224\STX\b\DC4\n\
    \\f\n\
    \\EOT\EOT.\STX\NUL\DC2\EOT\225\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\ACK\DC2\EOT\225\STX\STX\v\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\SOH\DC2\EOT\225\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\ETX\DC2\EOT\225\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT.\STX\SOH\DC2\EOT\226\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\ENQ\DC2\EOT\226\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\SOH\DC2\EOT\226\STX\t\SYN\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\ETX\DC2\EOT\226\STX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT.\STX\STX\DC2\EOT\227\STX\STX\EM\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\ENQ\DC2\EOT\227\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\SOH\DC2\EOT\227\STX\t\DC4\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\ETX\DC2\EOT\227\STX\ETB\CAN\n\
    \\f\n\
    \\EOT\EOT.\STX\ETX\DC2\EOT\228\STX\STX\FS\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\ENQ\DC2\EOT\228\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\SOH\DC2\EOT\228\STX\t\ETB\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\ETX\DC2\EOT\228\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT.\STX\EOT\DC2\EOT\229\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\ENQ\DC2\EOT\229\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\SOH\DC2\EOT\229\STX\t\EM\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\ETX\DC2\EOT\229\STX\FS\GS\n\
    \\f\n\
    \\EOT\EOT.\STX\ENQ\DC2\EOT\230\STX\STX+\n\
    \\r\n\
    \\ENQ\EOT.\STX\ENQ\ENQ\DC2\EOT\230\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\ENQ\SOH\DC2\EOT\230\STX\t&\n\
    \\r\n\
    \\ENQ\EOT.\STX\ENQ\ETX\DC2\EOT\230\STX)*\n\
    \\f\n\
    \\EOT\EOT.\STX\ACK\DC2\EOT\231\STX\STX#\n\
    \\r\n\
    \\ENQ\EOT.\STX\ACK\ENQ\DC2\EOT\231\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\ACK\SOH\DC2\EOT\231\STX\t\RS\n\
    \\r\n\
    \\ENQ\EOT.\STX\ACK\ETX\DC2\EOT\231\STX!\"\n\
    \\v\n\
    \\ETX\EOT.\t\DC2\EOT\232\STX\STX\r\n\
    \\f\n\
    \\EOT\EOT.\t\NUL\DC2\EOT\232\STX\v\f\n\
    \\r\n\
    \\ENQ\EOT.\t\NUL\SOH\DC2\EOT\232\STX\v\f\n\
    \\r\n\
    \\ENQ\EOT.\t\NUL\STX\DC2\EOT\232\STX\v\f\n\
    \\v\n\
    \\ETX\EOT.\n\
    \\DC2\EOT\233\STX\STX\"\n\
    \\f\n\
    \\EOT\EOT.\n\
    \\NUL\DC2\EOT\233\STX\v!\n\
    \\f\n\
    \\EOT\EOT.\STX\a\DC2\EOT\234\STX\STX$\n\
    \\r\n\
    \\ENQ\EOT.\STX\a\ENQ\DC2\EOT\234\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT.\STX\a\SOH\DC2\EOT\234\STX\b\RS\n\
    \\r\n\
    \\ENQ\EOT.\STX\a\ETX\DC2\EOT\234\STX!#\n\
    \\f\n\
    \\EOT\EOT.\STX\b\DC2\EOT\235\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\EOT.\STX\b\ENQ\DC2\EOT\235\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\b\SOH\DC2\EOT\235\STX\t\NAK\n\
    \\r\n\
    \\ENQ\EOT.\STX\b\ETX\DC2\EOT\235\STX\CAN\EM\n\
    \\f\n\
    \\EOT\EOT.\STX\t\DC2\EOT\236\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\EOT.\STX\t\ENQ\DC2\EOT\236\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT.\STX\t\SOH\DC2\EOT\236\STX\b\SI\n\
    \\r\n\
    \\ENQ\EOT.\STX\t\ETX\DC2\EOT\236\STX\DC2\DC4\n\
    \\f\n\
    \\STX\ACK\NUL\DC2\ACK\239\STX\NUL\131\ETX\SOH\n\
    \\v\n\
    \\ETX\ACK\NUL\SOH\DC2\EOT\239\STX\b\ETB\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\EOT\240\STX\STXE\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\EOT\240\STX\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\EOT\240\STX\DC3&\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\EOT\240\STX1C\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\SOH\DC2\EOT\241\STX\STX>\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\SOH\DC2\EOT\241\STX\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\STX\DC2\EOT\241\STX\r!\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SOH\ETX\DC2\EOT\241\STX,<\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\STX\DC2\EOT\242\STX\STXI\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\SOH\DC2\EOT\242\STX\ACK\DLE\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\STX\DC2\EOT\242\STX\DC1)\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\STX\ETX\DC2\EOT\242\STX4G\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ETX\DC2\EOT\243\STX\STX:\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\SOH\DC2\EOT\243\STX\ACK\n\
    \\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\STX\DC2\EOT\243\STX\v \n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ETX\ETX\DC2\EOT\243\STX+8\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\EOT\DC2\EOT\244\STX\STXE\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\SOH\DC2\EOT\244\STX\ACK\DC1\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\STX\DC2\EOT\244\STX\DC2$\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\EOT\ETX\DC2\EOT\244\STX/C\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ENQ\DC2\EOT\245\STX\STXB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\SOH\DC2\EOT\245\STX\ACK\r\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\STX\DC2\EOT\245\STX\SO$\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ENQ\ETX\DC2\EOT\245\STX/@\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\ACK\DC2\EOT\246\STX\STX?\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\SOH\DC2\EOT\246\STX\ACK\n\
    \\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\STX\DC2\EOT\246\STX\v!\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\ACK\ETX\DC2\EOT\246\STX,=\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\a\DC2\EOT\247\STX\STXP\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\SOH\DC2\EOT\247\STX\ACK\EM\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\STX\DC2\EOT\247\STX\SUB4\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\a\ETX\DC2\EOT\247\STX?N\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\b\DC2\EOT\248\STX\STXI\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\b\SOH\DC2\EOT\248\STX\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\b\STX\DC2\EOT\248\STX\NAK+\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\b\ETX\DC2\EOT\248\STX6G\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\t\DC2\EOT\249\STX\STXO\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\t\SOH\DC2\EOT\249\STX\ACK\ETB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\t\STX\DC2\EOT\249\STX\CAN1\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\t\ETX\DC2\EOT\249\STX<M\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\n\
    \\DC2\EOT\250\STX\STX;\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\n\
    \\SOH\DC2\EOT\250\STX\ACK\r\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\n\
    \\STX\DC2\EOT\250\STX\SO\FS\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\n\
    \\ETX\DC2\EOT\250\STX'9\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\v\DC2\EOT\251\STX\STXC\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\v\SOH\DC2\EOT\251\STX\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\v\STX\DC2\EOT\251\STX\NAK*\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\v\ETX\DC2\EOT\251\STX5A\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\f\DC2\EOT\252\STX\STXL\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\f\SOH\DC2\EOT\252\STX\ACK\ETB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\f\STX\DC2\EOT\252\STX\CAN0\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\f\ETX\DC2\EOT\252\STX;J\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\r\DC2\EOT\253\STX\STX>\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\r\SOH\DC2\EOT\253\STX\ACK\DC2\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\r\STX\DC2\EOT\253\STX\DC3&\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\r\ETX\DC2\EOT\253\STX1<\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\SO\DC2\EOT\254\STX\STX0\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SO\SOH\DC2\EOT\254\STX\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SO\STX\DC2\EOT\254\STX\r\SUB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SO\ETX\DC2\EOT\254\STX%.\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\SI\DC2\EOT\255\STX\STX1\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SI\SOH\DC2\EOT\255\STX\ACK\v\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SI\STX\DC2\EOT\255\STX\f\CAN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\SI\ETX\DC2\EOT\255\STX#/\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\DLE\DC2\EOT\128\ETX\STX8\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DLE\SOH\DC2\EOT\128\ETX\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DLE\STX\DC2\EOT\128\ETX\r\GS\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DLE\ETX\DC2\EOT\128\ETX(6\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\DC1\DC2\EOT\129\ETX\STXB\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC1\SOH\DC2\EOT\129\ETX\ACK\SYN\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC1\STX\DC2\EOT\129\ETX\ETB'\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC1\ETX\DC2\EOT\129\ETX2@\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\DC2\DC2\EOT\130\ETX\STXG\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC2\SOH\DC2\EOT\130\ETX\ACK\DC4\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC2\STX\DC2\EOT\130\ETX\NAK%\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC2\ACK\DC2\EOT\130\ETX06\n\
    \\r\n\
    \\ENQ\ACK\NUL\STX\DC2\ETX\DC2\EOT\130\ETX7Eb\ACKproto3"