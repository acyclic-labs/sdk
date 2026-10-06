{- This file was auto-generated from inference/v1/inference.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Inference.V1.Inference (
        ModelsService(..), ContextsService(..), WarmContextsService(..),
        RunsService(..), EvaluationsService(..), Compact(),
        ContextProvenance(), ContextProvenance'Origin(..),
        _ContextProvenance'Created, _ContextProvenance'Derived,
        _ContextProvenance'Forked, _ContextProvenance'Transferred,
        _ContextProvenance'Generated, _ContextProvenance'RunInput,
        ContextView(), CreateContextRequest(), CreateEvaluationRequest(),
        Edit(), Edit'Action(..), _Edit'Append, _Edit'InsertBefore,
        _Edit'InsertAfter, _Edit'Replace, _Edit'Delete, Edits(), Empty(),
        EvaluationAggregate(), EvaluationAggregation(..),
        EvaluationAggregation(), EvaluationAggregation'UnrecognizedValue,
        EvaluationArtifact(), EvaluationCase(), EvaluationCaseOutcome(..),
        EvaluationCaseOutcome(), EvaluationCaseOutcome'UnrecognizedValue,
        EvaluationCaseResult(), EvaluationGrader(),
        EvaluationGraderObservation(), EvaluationMetric(),
        EvaluationMetricValue(), EvaluationResult(), EvaluationSpec(),
        EvaluationState(..), EvaluationState(),
        EvaluationState'UnrecognizedValue, EvaluationSuite(),
        EvaluationView(), ExactRational(), GenerateRunRequest(),
        GenerateRunResponse(), GenerationProvenance(), IdleKvPolicy(),
        IdleKvRetention(), Insert(), InspectContextRequest(),
        InspectEvaluationRequest(), InspectRunRequest(),
        InspectWarmRequest(), Item(), ItemKind(..), ItemKind(),
        ItemKind'UnrecognizedValue, ListModelsRequest(),
        ListModelsResponse(), LogicalUsage(), ModelCapability(),
        MutateContextRequest(), MutateContextRequest'Action(..),
        _MutateContextRequest'Edit, _MutateContextRequest'Fork,
        _MutateContextRequest'Truncate, _MutateContextRequest'Compact,
        _MutateContextRequest'Release, _MutateContextRequest'Transfer,
        MutationReceipt(), ProvenanceSource(), ReleaseWarmRequest(),
        RenewWarmRequest(), Replace(), RequestIdentity(),
        RetainWarmRequest(), RetentionProfile(), RunEvent(),
        RunEvent'Event(..), _RunEvent'Output, _RunEvent'Usage,
        _RunEvent'Terminal, _RunEvent'Progress, RunInputProvenance(),
        RunProgress(), RunResult(), RunTerminal(..), RunTerminal(),
        RunTerminal'UnrecognizedValue, RunView(), Transfer(),
        TransferProvenance(), Truncate(), UsageReceipt(), WarmState(..),
        WarmState(), WarmState'UnrecognizedValue, WarmView(),
        WatchRunRequest()
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
import qualified Proto.Validation.V1.Options
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.selected' @:: Lens' Compact [Data.ByteString.ByteString]@
         * 'Proto.Inference.V1.Inference_Fields.vec'selected' @:: Lens' Compact (Data.Vector.Vector Data.ByteString.ByteString)@
         * 'Proto.Inference.V1.Inference_Fields.replacement' @:: Lens' Compact [Item]@
         * 'Proto.Inference.V1.Inference_Fields.vec'replacement' @:: Lens' Compact (Data.Vector.Vector Item)@ -}
data Compact
  = Compact'_constructor {_Compact'selected :: !(Data.Vector.Vector Data.ByteString.ByteString),
                          _Compact'replacement :: !(Data.Vector.Vector Item),
                          _Compact'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Compact where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Compact "selected" [Data.ByteString.ByteString] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Compact'selected (\ x__ y__ -> x__ {_Compact'selected = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField Compact "vec'selected" (Data.Vector.Vector Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Compact'selected (\ x__ y__ -> x__ {_Compact'selected = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Compact "replacement" [Item] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Compact'replacement
           (\ x__ y__ -> x__ {_Compact'replacement = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField Compact "vec'replacement" (Data.Vector.Vector Item) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Compact'replacement
           (\ x__ y__ -> x__ {_Compact'replacement = y__}))
        Prelude.id
instance Data.ProtoLens.Message Compact where
  messageName _ = Data.Text.pack "inference.customer.v1.Compact"
  packedMessageDescriptor _
    = "\n\
      \\aCompact\DC2\SUB\n\
      \\bselected\CAN\SOH \ETX(\fR\bselected\DC2=\n\
      \\vreplacement\CAN\STX \ETX(\v2\ESC.inference.customer.v1.ItemR\vreplacement"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        selected__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "selected"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"selected")) ::
              Data.ProtoLens.FieldDescriptor Compact
        replacement__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "replacement"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Item)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"replacement")) ::
              Data.ProtoLens.FieldDescriptor Compact
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, selected__field_descriptor),
           (Data.ProtoLens.Tag 2, replacement__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Compact'_unknownFields
        (\ x__ y__ -> x__ {_Compact'_unknownFields = y__})
  defMessage
    = Compact'_constructor
        {_Compact'selected = Data.Vector.Generic.empty,
         _Compact'replacement = Data.Vector.Generic.empty,
         _Compact'_unknownFields = []}
  parseMessage
    = let
        loop ::
          Compact
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Item
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.ByteString.ByteString
                -> Data.ProtoLens.Encoding.Bytes.Parser Compact
        loop x mutable'replacement mutable'selected
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'replacement <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                              (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                 mutable'replacement)
                      frozen'selected <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'selected)
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
                              (Data.ProtoLens.Field.field @"vec'replacement") frozen'replacement
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'selected") frozen'selected x)))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getBytes
                                              (Prelude.fromIntegral len))
                                        "selected"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'selected y)
                                loop x mutable'replacement v
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "replacement"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'replacement y)
                                loop x v mutable'selected
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'replacement mutable'selected
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'replacement <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       Data.ProtoLens.Encoding.Growing.new
              mutable'selected <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              loop
                Data.ProtoLens.defMessage mutable'replacement mutable'selected)
          "Compact"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                (\ _v
                   -> (Data.Monoid.<>)
                        (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                        ((\ bs
                            -> (Data.Monoid.<>)
                                 (Data.ProtoLens.Encoding.Bytes.putVarInt
                                    (Prelude.fromIntegral (Data.ByteString.length bs)))
                                 (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                           _v))
                (Lens.Family2.view
                   (Data.ProtoLens.Field.field @"vec'selected") _x))
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
                      (Data.ProtoLens.Field.field @"vec'replacement") _x))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData Compact where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Compact'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Compact'selected x__)
                (Control.DeepSeq.deepseq (_Compact'replacement x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.maybe'origin' @:: Lens' ContextProvenance (Prelude.Maybe ContextProvenance'Origin)@
         * 'Proto.Inference.V1.Inference_Fields.maybe'created' @:: Lens' ContextProvenance (Prelude.Maybe Empty)@
         * 'Proto.Inference.V1.Inference_Fields.created' @:: Lens' ContextProvenance Empty@
         * 'Proto.Inference.V1.Inference_Fields.maybe'derived' @:: Lens' ContextProvenance (Prelude.Maybe ProvenanceSource)@
         * 'Proto.Inference.V1.Inference_Fields.derived' @:: Lens' ContextProvenance ProvenanceSource@
         * 'Proto.Inference.V1.Inference_Fields.maybe'forked' @:: Lens' ContextProvenance (Prelude.Maybe ProvenanceSource)@
         * 'Proto.Inference.V1.Inference_Fields.forked' @:: Lens' ContextProvenance ProvenanceSource@
         * 'Proto.Inference.V1.Inference_Fields.maybe'transferred' @:: Lens' ContextProvenance (Prelude.Maybe TransferProvenance)@
         * 'Proto.Inference.V1.Inference_Fields.transferred' @:: Lens' ContextProvenance TransferProvenance@
         * 'Proto.Inference.V1.Inference_Fields.maybe'generated' @:: Lens' ContextProvenance (Prelude.Maybe GenerationProvenance)@
         * 'Proto.Inference.V1.Inference_Fields.generated' @:: Lens' ContextProvenance GenerationProvenance@
         * 'Proto.Inference.V1.Inference_Fields.maybe'runInput' @:: Lens' ContextProvenance (Prelude.Maybe RunInputProvenance)@
         * 'Proto.Inference.V1.Inference_Fields.runInput' @:: Lens' ContextProvenance RunInputProvenance@ -}
data ContextProvenance
  = ContextProvenance'_constructor {_ContextProvenance'origin :: !(Prelude.Maybe ContextProvenance'Origin),
                                    _ContextProvenance'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ContextProvenance where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data ContextProvenance'Origin
  = ContextProvenance'Created !Empty |
    ContextProvenance'Derived !ProvenanceSource |
    ContextProvenance'Forked !ProvenanceSource |
    ContextProvenance'Transferred !TransferProvenance |
    ContextProvenance'Generated !GenerationProvenance |
    ContextProvenance'RunInput !RunInputProvenance
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'origin" (Prelude.Maybe ContextProvenance'Origin) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'created" (Prelude.Maybe Empty) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ContextProvenance'Created x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ContextProvenance'Created y__))
instance Data.ProtoLens.Field.HasField ContextProvenance "created" Empty where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ContextProvenance'Created x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ContextProvenance'Created y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'derived" (Prelude.Maybe ProvenanceSource) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ContextProvenance'Derived x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ContextProvenance'Derived y__))
instance Data.ProtoLens.Field.HasField ContextProvenance "derived" ProvenanceSource where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ContextProvenance'Derived x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ContextProvenance'Derived y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'forked" (Prelude.Maybe ProvenanceSource) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ContextProvenance'Forked x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ContextProvenance'Forked y__))
instance Data.ProtoLens.Field.HasField ContextProvenance "forked" ProvenanceSource where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ContextProvenance'Forked x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ContextProvenance'Forked y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'transferred" (Prelude.Maybe TransferProvenance) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ContextProvenance'Transferred x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ContextProvenance'Transferred y__))
instance Data.ProtoLens.Field.HasField ContextProvenance "transferred" TransferProvenance where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ContextProvenance'Transferred x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ContextProvenance'Transferred y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'generated" (Prelude.Maybe GenerationProvenance) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ContextProvenance'Generated x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ContextProvenance'Generated y__))
instance Data.ProtoLens.Field.HasField ContextProvenance "generated" GenerationProvenance where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ContextProvenance'Generated x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ContextProvenance'Generated y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField ContextProvenance "maybe'runInput" (Prelude.Maybe RunInputProvenance) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (ContextProvenance'RunInput x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap ContextProvenance'RunInput y__))
instance Data.ProtoLens.Field.HasField ContextProvenance "runInput" RunInputProvenance where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextProvenance'origin
           (\ x__ y__ -> x__ {_ContextProvenance'origin = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (ContextProvenance'RunInput x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap ContextProvenance'RunInput y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message ContextProvenance where
  messageName _
    = Data.Text.pack "inference.customer.v1.ContextProvenance"
  packedMessageDescriptor _
    = "\n\
      \\DC1ContextProvenance\DC28\n\
      \\acreated\CAN\SOH \SOH(\v2\FS.inference.customer.v1.EmptyH\NULR\acreated\DC2C\n\
      \\aderived\CAN\STX \SOH(\v2'.inference.customer.v1.ProvenanceSourceH\NULR\aderived\DC2A\n\
      \\ACKforked\CAN\ETX \SOH(\v2'.inference.customer.v1.ProvenanceSourceH\NULR\ACKforked\DC2M\n\
      \\vtransferred\CAN\EOT \SOH(\v2).inference.customer.v1.TransferProvenanceH\NULR\vtransferred\DC2K\n\
      \\tgenerated\CAN\ENQ \SOH(\v2+.inference.customer.v1.GenerationProvenanceH\NULR\tgenerated\DC2H\n\
      \\trun_input\CAN\ACK \SOH(\v2).inference.customer.v1.RunInputProvenanceH\NULR\brunInputB\SO\n\
      \\ACKorigin\DC2\EOT\224\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        created__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "created"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Empty)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'created")) ::
              Data.ProtoLens.FieldDescriptor ContextProvenance
        derived__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "derived"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProvenanceSource)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'derived")) ::
              Data.ProtoLens.FieldDescriptor ContextProvenance
        forked__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "forked"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProvenanceSource)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'forked")) ::
              Data.ProtoLens.FieldDescriptor ContextProvenance
        transferred__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "transferred"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor TransferProvenance)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'transferred")) ::
              Data.ProtoLens.FieldDescriptor ContextProvenance
        generated__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "generated"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor GenerationProvenance)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'generated")) ::
              Data.ProtoLens.FieldDescriptor ContextProvenance
        runInput__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run_input"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RunInputProvenance)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'runInput")) ::
              Data.ProtoLens.FieldDescriptor ContextProvenance
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, created__field_descriptor),
           (Data.ProtoLens.Tag 2, derived__field_descriptor),
           (Data.ProtoLens.Tag 3, forked__field_descriptor),
           (Data.ProtoLens.Tag 4, transferred__field_descriptor),
           (Data.ProtoLens.Tag 5, generated__field_descriptor),
           (Data.ProtoLens.Tag 6, runInput__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ContextProvenance'_unknownFields
        (\ x__ y__ -> x__ {_ContextProvenance'_unknownFields = y__})
  defMessage
    = ContextProvenance'_constructor
        {_ContextProvenance'origin = Prelude.Nothing,
         _ContextProvenance'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ContextProvenance
          -> Data.ProtoLens.Encoding.Bytes.Parser ContextProvenance
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
                                       "derived"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"derived") y x)
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
                                       "transferred"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"transferred") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "generated"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"generated") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "run_input"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"runInput") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ContextProvenance"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'origin") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (ContextProvenance'Created v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ContextProvenance'Derived v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ContextProvenance'Forked v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ContextProvenance'Transferred v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ContextProvenance'Generated v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (ContextProvenance'RunInput v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ContextProvenance where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ContextProvenance'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ContextProvenance'origin x__) ())
instance Control.DeepSeq.NFData ContextProvenance'Origin where
  rnf (ContextProvenance'Created x__) = Control.DeepSeq.rnf x__
  rnf (ContextProvenance'Derived x__) = Control.DeepSeq.rnf x__
  rnf (ContextProvenance'Forked x__) = Control.DeepSeq.rnf x__
  rnf (ContextProvenance'Transferred x__) = Control.DeepSeq.rnf x__
  rnf (ContextProvenance'Generated x__) = Control.DeepSeq.rnf x__
  rnf (ContextProvenance'RunInput x__) = Control.DeepSeq.rnf x__
_ContextProvenance'Created ::
  Data.ProtoLens.Prism.Prism' ContextProvenance'Origin Empty
_ContextProvenance'Created
  = Data.ProtoLens.Prism.prism'
      ContextProvenance'Created
      (\ p__
         -> case p__ of
              (ContextProvenance'Created p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ContextProvenance'Derived ::
  Data.ProtoLens.Prism.Prism' ContextProvenance'Origin ProvenanceSource
_ContextProvenance'Derived
  = Data.ProtoLens.Prism.prism'
      ContextProvenance'Derived
      (\ p__
         -> case p__ of
              (ContextProvenance'Derived p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ContextProvenance'Forked ::
  Data.ProtoLens.Prism.Prism' ContextProvenance'Origin ProvenanceSource
_ContextProvenance'Forked
  = Data.ProtoLens.Prism.prism'
      ContextProvenance'Forked
      (\ p__
         -> case p__ of
              (ContextProvenance'Forked p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ContextProvenance'Transferred ::
  Data.ProtoLens.Prism.Prism' ContextProvenance'Origin TransferProvenance
_ContextProvenance'Transferred
  = Data.ProtoLens.Prism.prism'
      ContextProvenance'Transferred
      (\ p__
         -> case p__ of
              (ContextProvenance'Transferred p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ContextProvenance'Generated ::
  Data.ProtoLens.Prism.Prism' ContextProvenance'Origin GenerationProvenance
_ContextProvenance'Generated
  = Data.ProtoLens.Prism.prism'
      ContextProvenance'Generated
      (\ p__
         -> case p__ of
              (ContextProvenance'Generated p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_ContextProvenance'RunInput ::
  Data.ProtoLens.Prism.Prism' ContextProvenance'Origin RunInputProvenance
_ContextProvenance'RunInput
  = Data.ProtoLens.Prism.prism'
      ContextProvenance'RunInput
      (\ p__
         -> case p__ of
              (ContextProvenance'RunInput p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.revision' @:: Lens' ContextView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.parent' @:: Lens' ContextView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maybe'parent' @:: Lens' ContextView (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Inference.V1.Inference_Fields.lineage' @:: Lens' ContextView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.executionProfile' @:: Lens' ContextView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.contentDigest' @:: Lens' ContextView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.items' @:: Lens' ContextView [Item]@
         * 'Proto.Inference.V1.Inference_Fields.vec'items' @:: Lens' ContextView (Data.Vector.Vector Item)@
         * 'Proto.Inference.V1.Inference_Fields.model' @:: Lens' ContextView Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.provenance' @:: Lens' ContextView ContextProvenance@
         * 'Proto.Inference.V1.Inference_Fields.maybe'provenance' @:: Lens' ContextView (Prelude.Maybe ContextProvenance)@ -}
data ContextView
  = ContextView'_constructor {_ContextView'revision :: !Data.ByteString.ByteString,
                              _ContextView'parent :: !(Prelude.Maybe Data.ByteString.ByteString),
                              _ContextView'lineage :: !Data.ByteString.ByteString,
                              _ContextView'executionProfile :: !Data.ByteString.ByteString,
                              _ContextView'contentDigest :: !Data.ByteString.ByteString,
                              _ContextView'items :: !(Data.Vector.Vector Item),
                              _ContextView'model :: !Data.Text.Text,
                              _ContextView'provenance :: !(Prelude.Maybe ContextProvenance),
                              _ContextView'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ContextView where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ContextView "revision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'revision
           (\ x__ y__ -> x__ {_ContextView'revision = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "parent" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'parent (\ x__ y__ -> x__ {_ContextView'parent = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField ContextView "maybe'parent" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'parent (\ x__ y__ -> x__ {_ContextView'parent = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "lineage" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'lineage
           (\ x__ y__ -> x__ {_ContextView'lineage = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "executionProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'executionProfile
           (\ x__ y__ -> x__ {_ContextView'executionProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "contentDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'contentDigest
           (\ x__ y__ -> x__ {_ContextView'contentDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "items" [Item] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'items (\ x__ y__ -> x__ {_ContextView'items = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ContextView "vec'items" (Data.Vector.Vector Item) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'items (\ x__ y__ -> x__ {_ContextView'items = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "model" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'model (\ x__ y__ -> x__ {_ContextView'model = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ContextView "provenance" ContextProvenance where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'provenance
           (\ x__ y__ -> x__ {_ContextView'provenance = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ContextView "maybe'provenance" (Prelude.Maybe ContextProvenance) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ContextView'provenance
           (\ x__ y__ -> x__ {_ContextView'provenance = y__}))
        Prelude.id
instance Data.ProtoLens.Message ContextView where
  messageName _ = Data.Text.pack "inference.customer.v1.ContextView"
  packedMessageDescriptor _
    = "\n\
      \\vContextView\DC2 \n\
      \\brevision\CAN\SOH \SOH(\fR\brevisionB\EOT\200\243\CAN \DC2!\n\
      \\ACKparent\CAN\STX \SOH(\fH\NULR\ACKparentB\EOT\200\243\CAN \136\SOH\SOH\DC2\RS\n\
      \\alineage\CAN\ETX \SOH(\fR\alineageB\EOT\200\243\CAN \DC21\n\
      \\DC1execution_profile\CAN\EOT \SOH(\fR\DLEexecutionProfileB\EOT\200\243\CAN \DC2+\n\
      \\SOcontent_digest\CAN\ENQ \SOH(\fR\rcontentDigestB\EOT\200\243\CAN \DC21\n\
      \\ENQitems\CAN\ACK \ETX(\v2\ESC.inference.customer.v1.ItemR\ENQitems\DC2\ESC\n\
      \\ENQmodel\CAN\a \SOH(\tR\ENQmodelB\ENQ\248\243\CAN\128\STX\DC2N\n\
      \\n\
      \provenance\CAN\b \SOH(\v2(.inference.customer.v1.ContextProvenanceR\n\
      \provenanceB\EOT\208\243\CAN\SOHB\t\n\
      \\a_parent"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        revision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"revision")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        parent__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "parent"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'parent")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        lineage__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "lineage"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"lineage")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        executionProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "execution_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"executionProfile")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        contentDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "content_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"contentDigest")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        items__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "items"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Item)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"items")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        model__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"model")) ::
              Data.ProtoLens.FieldDescriptor ContextView
        provenance__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "provenance"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ContextProvenance)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'provenance")) ::
              Data.ProtoLens.FieldDescriptor ContextView
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, revision__field_descriptor),
           (Data.ProtoLens.Tag 2, parent__field_descriptor),
           (Data.ProtoLens.Tag 3, lineage__field_descriptor),
           (Data.ProtoLens.Tag 4, executionProfile__field_descriptor),
           (Data.ProtoLens.Tag 5, contentDigest__field_descriptor),
           (Data.ProtoLens.Tag 6, items__field_descriptor),
           (Data.ProtoLens.Tag 7, model__field_descriptor),
           (Data.ProtoLens.Tag 8, provenance__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ContextView'_unknownFields
        (\ x__ y__ -> x__ {_ContextView'_unknownFields = y__})
  defMessage
    = ContextView'_constructor
        {_ContextView'revision = Data.ProtoLens.fieldDefault,
         _ContextView'parent = Prelude.Nothing,
         _ContextView'lineage = Data.ProtoLens.fieldDefault,
         _ContextView'executionProfile = Data.ProtoLens.fieldDefault,
         _ContextView'contentDigest = Data.ProtoLens.fieldDefault,
         _ContextView'items = Data.Vector.Generic.empty,
         _ContextView'model = Data.ProtoLens.fieldDefault,
         _ContextView'provenance = Prelude.Nothing,
         _ContextView'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ContextView
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Item
             -> Data.ProtoLens.Encoding.Bytes.Parser ContextView
        loop x mutable'items
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'items <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'items)
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
                              (Data.ProtoLens.Field.field @"vec'items") frozen'items x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "revision"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"revision") y x)
                                  mutable'items
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "parent"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"parent") y x)
                                  mutable'items
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "lineage"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"lineage") y x)
                                  mutable'items
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "execution_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"executionProfile") y x)
                                  mutable'items
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "content_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"contentDigest") y x)
                                  mutable'items
                        50
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "items"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'items y)
                                loop x v
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "model"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"model") y x)
                                  mutable'items
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "provenance"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"provenance") y x)
                                  mutable'items
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'items
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'items <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'items)
          "ContextView"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"revision") _x
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'parent") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just _v)
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                          ((\ bs
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                      (Prelude.fromIntegral (Data.ByteString.length bs)))
                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             _v))
                ((Data.Monoid.<>)
                   (let
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"lineage") _x
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
                               (Data.ProtoLens.Field.field @"executionProfile") _x
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
                         (let
                            _v
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"contentDigest") _x
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
                         ((Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                               (\ _v
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
                               (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'items") _x))
                            ((Data.Monoid.<>)
                               (let
                                  _v = Lens.Family2.view (Data.ProtoLens.Field.field @"model") _x
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
                                  (case
                                       Lens.Family2.view
                                         (Data.ProtoLens.Field.field @"maybe'provenance") _x
                                   of
                                     Prelude.Nothing -> Data.Monoid.mempty
                                     (Prelude.Just _v)
                                       -> (Data.Monoid.<>)
                                            (Data.ProtoLens.Encoding.Bytes.putVarInt 66)
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
instance Control.DeepSeq.NFData ContextView where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ContextView'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ContextView'revision x__)
                (Control.DeepSeq.deepseq
                   (_ContextView'parent x__)
                   (Control.DeepSeq.deepseq
                      (_ContextView'lineage x__)
                      (Control.DeepSeq.deepseq
                         (_ContextView'executionProfile x__)
                         (Control.DeepSeq.deepseq
                            (_ContextView'contentDigest x__)
                            (Control.DeepSeq.deepseq
                               (_ContextView'items x__)
                               (Control.DeepSeq.deepseq
                                  (_ContextView'model x__)
                                  (Control.DeepSeq.deepseq (_ContextView'provenance x__) ()))))))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' CreateContextRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' CreateContextRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.model' @:: Lens' CreateContextRequest Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.items' @:: Lens' CreateContextRequest [Item]@
         * 'Proto.Inference.V1.Inference_Fields.vec'items' @:: Lens' CreateContextRequest (Data.Vector.Vector Item)@ -}
data CreateContextRequest
  = CreateContextRequest'_constructor {_CreateContextRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                       _CreateContextRequest'model :: !Data.Text.Text,
                                       _CreateContextRequest'items :: !(Data.Vector.Vector Item),
                                       _CreateContextRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateContextRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateContextRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateContextRequest'identity
           (\ x__ y__ -> x__ {_CreateContextRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateContextRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateContextRequest'identity
           (\ x__ y__ -> x__ {_CreateContextRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateContextRequest "model" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateContextRequest'model
           (\ x__ y__ -> x__ {_CreateContextRequest'model = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateContextRequest "items" [Item] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateContextRequest'items
           (\ x__ y__ -> x__ {_CreateContextRequest'items = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CreateContextRequest "vec'items" (Data.Vector.Vector Item) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateContextRequest'items
           (\ x__ y__ -> x__ {_CreateContextRequest'items = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateContextRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.CreateContextRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC4CreateContextRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\DC4\n\
      \\ENQmodel\CAN\STX \SOH(\tR\ENQmodel\DC21\n\
      \\ENQitems\CAN\ETX \ETX(\v2\ESC.inference.customer.v1.ItemR\ENQitems"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor CreateContextRequest
        model__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"model")) ::
              Data.ProtoLens.FieldDescriptor CreateContextRequest
        items__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "items"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Item)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"items")) ::
              Data.ProtoLens.FieldDescriptor CreateContextRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, model__field_descriptor),
           (Data.ProtoLens.Tag 3, items__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateContextRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateContextRequest'_unknownFields = y__})
  defMessage
    = CreateContextRequest'_constructor
        {_CreateContextRequest'identity = Prelude.Nothing,
         _CreateContextRequest'model = Data.ProtoLens.fieldDefault,
         _CreateContextRequest'items = Data.Vector.Generic.empty,
         _CreateContextRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateContextRequest
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Item
             -> Data.ProtoLens.Encoding.Bytes.Parser CreateContextRequest
        loop x mutable'items
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'items <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'items)
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
                              (Data.ProtoLens.Field.field @"vec'items") frozen'items x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                                  mutable'items
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "model"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"model") y x)
                                  mutable'items
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "items"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'items y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'items
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'items <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'items)
          "CreateContextRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"model") _x
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
                      (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'items") _x))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData CreateContextRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateContextRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateContextRequest'identity x__)
                (Control.DeepSeq.deepseq
                   (_CreateContextRequest'model x__)
                   (Control.DeepSeq.deepseq (_CreateContextRequest'items x__) ())))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' CreateEvaluationRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' CreateEvaluationRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.spec' @:: Lens' CreateEvaluationRequest EvaluationSpec@
         * 'Proto.Inference.V1.Inference_Fields.maybe'spec' @:: Lens' CreateEvaluationRequest (Prelude.Maybe EvaluationSpec)@ -}
data CreateEvaluationRequest
  = CreateEvaluationRequest'_constructor {_CreateEvaluationRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                          _CreateEvaluationRequest'spec :: !(Prelude.Maybe EvaluationSpec),
                                          _CreateEvaluationRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CreateEvaluationRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CreateEvaluationRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateEvaluationRequest'identity
           (\ x__ y__ -> x__ {_CreateEvaluationRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateEvaluationRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateEvaluationRequest'identity
           (\ x__ y__ -> x__ {_CreateEvaluationRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField CreateEvaluationRequest "spec" EvaluationSpec where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateEvaluationRequest'spec
           (\ x__ y__ -> x__ {_CreateEvaluationRequest'spec = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField CreateEvaluationRequest "maybe'spec" (Prelude.Maybe EvaluationSpec) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CreateEvaluationRequest'spec
           (\ x__ y__ -> x__ {_CreateEvaluationRequest'spec = y__}))
        Prelude.id
instance Data.ProtoLens.Message CreateEvaluationRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.CreateEvaluationRequest"
  packedMessageDescriptor _
    = "\n\
      \\ETBCreateEvaluationRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2?\n\
      \\EOTspec\CAN\STX \SOH(\v2%.inference.customer.v1.EvaluationSpecR\EOTspecB\EOT\208\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor CreateEvaluationRequest
        spec__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "spec"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationSpec)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'spec")) ::
              Data.ProtoLens.FieldDescriptor CreateEvaluationRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, spec__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CreateEvaluationRequest'_unknownFields
        (\ x__ y__ -> x__ {_CreateEvaluationRequest'_unknownFields = y__})
  defMessage
    = CreateEvaluationRequest'_constructor
        {_CreateEvaluationRequest'identity = Prelude.Nothing,
         _CreateEvaluationRequest'spec = Prelude.Nothing,
         _CreateEvaluationRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CreateEvaluationRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser CreateEvaluationRequest
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "spec"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"spec") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "CreateEvaluationRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'spec") _x
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
instance Control.DeepSeq.NFData CreateEvaluationRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CreateEvaluationRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_CreateEvaluationRequest'identity x__)
                (Control.DeepSeq.deepseq (_CreateEvaluationRequest'spec x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.maybe'action' @:: Lens' Edit (Prelude.Maybe Edit'Action)@
         * 'Proto.Inference.V1.Inference_Fields.maybe'append' @:: Lens' Edit (Prelude.Maybe Item)@
         * 'Proto.Inference.V1.Inference_Fields.append' @:: Lens' Edit Item@
         * 'Proto.Inference.V1.Inference_Fields.maybe'insertBefore' @:: Lens' Edit (Prelude.Maybe Insert)@
         * 'Proto.Inference.V1.Inference_Fields.insertBefore' @:: Lens' Edit Insert@
         * 'Proto.Inference.V1.Inference_Fields.maybe'insertAfter' @:: Lens' Edit (Prelude.Maybe Insert)@
         * 'Proto.Inference.V1.Inference_Fields.insertAfter' @:: Lens' Edit Insert@
         * 'Proto.Inference.V1.Inference_Fields.maybe'replace' @:: Lens' Edit (Prelude.Maybe Replace)@
         * 'Proto.Inference.V1.Inference_Fields.replace' @:: Lens' Edit Replace@
         * 'Proto.Inference.V1.Inference_Fields.maybe'delete' @:: Lens' Edit (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Inference.V1.Inference_Fields.delete' @:: Lens' Edit Data.ByteString.ByteString@ -}
data Edit
  = Edit'_constructor {_Edit'action :: !(Prelude.Maybe Edit'Action),
                       _Edit'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Edit where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data Edit'Action
  = Edit'Append !Item |
    Edit'InsertBefore !Insert |
    Edit'InsertAfter !Insert |
    Edit'Replace !Replace |
    Edit'Delete !Data.ByteString.ByteString
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField Edit "maybe'action" (Prelude.Maybe Edit'Action) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Edit "maybe'append" (Prelude.Maybe Item) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Edit'Append x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Edit'Append y__))
instance Data.ProtoLens.Field.HasField Edit "append" Item where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Edit'Append x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Edit'Append y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField Edit "maybe'insertBefore" (Prelude.Maybe Insert) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Edit'InsertBefore x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Edit'InsertBefore y__))
instance Data.ProtoLens.Field.HasField Edit "insertBefore" Insert where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Edit'InsertBefore x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Edit'InsertBefore y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField Edit "maybe'insertAfter" (Prelude.Maybe Insert) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Edit'InsertAfter x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Edit'InsertAfter y__))
instance Data.ProtoLens.Field.HasField Edit "insertAfter" Insert where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Edit'InsertAfter x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Edit'InsertAfter y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField Edit "maybe'replace" (Prelude.Maybe Replace) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Edit'Replace x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Edit'Replace y__))
instance Data.ProtoLens.Field.HasField Edit "replace" Replace where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Edit'Replace x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Edit'Replace y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField Edit "maybe'delete" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (Edit'Delete x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap Edit'Delete y__))
instance Data.ProtoLens.Field.HasField Edit "delete" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edit'action (\ x__ y__ -> x__ {_Edit'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (Edit'Delete x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap Edit'Delete y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Message Edit where
  messageName _ = Data.Text.pack "inference.customer.v1.Edit"
  packedMessageDescriptor _
    = "\n\
      \\EOTEdit\DC25\n\
      \\ACKappend\CAN\SOH \SOH(\v2\ESC.inference.customer.v1.ItemH\NULR\ACKappend\DC2D\n\
      \\rinsert_before\CAN\STX \SOH(\v2\GS.inference.customer.v1.InsertH\NULR\finsertBefore\DC2B\n\
      \\finsert_after\CAN\ETX \SOH(\v2\GS.inference.customer.v1.InsertH\NULR\vinsertAfter\DC2:\n\
      \\areplace\CAN\EOT \SOH(\v2\RS.inference.customer.v1.ReplaceH\NULR\areplace\DC2\CAN\n\
      \\ACKdelete\CAN\ENQ \SOH(\fH\NULR\ACKdeleteB\b\n\
      \\ACKaction"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        append__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "append"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Item)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'append")) ::
              Data.ProtoLens.FieldDescriptor Edit
        insertBefore__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "insert_before"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Insert)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'insertBefore")) ::
              Data.ProtoLens.FieldDescriptor Edit
        insertAfter__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "insert_after"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Insert)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'insertAfter")) ::
              Data.ProtoLens.FieldDescriptor Edit
        replace__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "replace"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Replace)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'replace")) ::
              Data.ProtoLens.FieldDescriptor Edit
        delete__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "delete"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'delete")) ::
              Data.ProtoLens.FieldDescriptor Edit
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, append__field_descriptor),
           (Data.ProtoLens.Tag 2, insertBefore__field_descriptor),
           (Data.ProtoLens.Tag 3, insertAfter__field_descriptor),
           (Data.ProtoLens.Tag 4, replace__field_descriptor),
           (Data.ProtoLens.Tag 5, delete__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Edit'_unknownFields
        (\ x__ y__ -> x__ {_Edit'_unknownFields = y__})
  defMessage
    = Edit'_constructor
        {_Edit'action = Prelude.Nothing, _Edit'_unknownFields = []}
  parseMessage
    = let
        loop :: Edit -> Data.ProtoLens.Encoding.Bytes.Parser Edit
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
                                       "insert_before"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"insertBefore") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "insert_after"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"insertAfter") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "replace"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"replace") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "delete"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"delete") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Edit"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'action") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just (Edit'Append v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (Edit'InsertBefore v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (Edit'InsertAfter v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (Edit'Replace v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                       ((Prelude..)
                          (\ bs
                             -> (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (Prelude.fromIntegral (Data.ByteString.length bs)))
                                  (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          Data.ProtoLens.encodeMessage v)
                (Prelude.Just (Edit'Delete v))
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                       ((\ bs
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                   (Prelude.fromIntegral (Data.ByteString.length bs)))
                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData Edit where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Edit'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Edit'action x__) ())
instance Control.DeepSeq.NFData Edit'Action where
  rnf (Edit'Append x__) = Control.DeepSeq.rnf x__
  rnf (Edit'InsertBefore x__) = Control.DeepSeq.rnf x__
  rnf (Edit'InsertAfter x__) = Control.DeepSeq.rnf x__
  rnf (Edit'Replace x__) = Control.DeepSeq.rnf x__
  rnf (Edit'Delete x__) = Control.DeepSeq.rnf x__
_Edit'Append :: Data.ProtoLens.Prism.Prism' Edit'Action Item
_Edit'Append
  = Data.ProtoLens.Prism.prism'
      Edit'Append
      (\ p__
         -> case p__ of
              (Edit'Append p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Edit'InsertBefore ::
  Data.ProtoLens.Prism.Prism' Edit'Action Insert
_Edit'InsertBefore
  = Data.ProtoLens.Prism.prism'
      Edit'InsertBefore
      (\ p__
         -> case p__ of
              (Edit'InsertBefore p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Edit'InsertAfter :: Data.ProtoLens.Prism.Prism' Edit'Action Insert
_Edit'InsertAfter
  = Data.ProtoLens.Prism.prism'
      Edit'InsertAfter
      (\ p__
         -> case p__ of
              (Edit'InsertAfter p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Edit'Replace :: Data.ProtoLens.Prism.Prism' Edit'Action Replace
_Edit'Replace
  = Data.ProtoLens.Prism.prism'
      Edit'Replace
      (\ p__
         -> case p__ of
              (Edit'Replace p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_Edit'Delete ::
  Data.ProtoLens.Prism.Prism' Edit'Action Data.ByteString.ByteString
_Edit'Delete
  = Data.ProtoLens.Prism.prism'
      Edit'Delete
      (\ p__
         -> case p__ of
              (Edit'Delete p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.edits' @:: Lens' Edits [Edit]@
         * 'Proto.Inference.V1.Inference_Fields.vec'edits' @:: Lens' Edits (Data.Vector.Vector Edit)@ -}
data Edits
  = Edits'_constructor {_Edits'edits :: !(Data.Vector.Vector Edit),
                        _Edits'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Edits where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Edits "edits" [Edit] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edits'edits (\ x__ y__ -> x__ {_Edits'edits = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField Edits "vec'edits" (Data.Vector.Vector Edit) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Edits'edits (\ x__ y__ -> x__ {_Edits'edits = y__}))
        Prelude.id
instance Data.ProtoLens.Message Edits where
  messageName _ = Data.Text.pack "inference.customer.v1.Edits"
  packedMessageDescriptor _
    = "\n\
      \\ENQEdits\DC21\n\
      \\ENQedits\CAN\SOH \ETX(\v2\ESC.inference.customer.v1.EditR\ENQedits"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        edits__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "edits"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Edit)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"edits")) ::
              Data.ProtoLens.FieldDescriptor Edits
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, edits__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Edits'_unknownFields
        (\ x__ y__ -> x__ {_Edits'_unknownFields = y__})
  defMessage
    = Edits'_constructor
        {_Edits'edits = Data.Vector.Generic.empty,
         _Edits'_unknownFields = []}
  parseMessage
    = let
        loop ::
          Edits
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Edit
             -> Data.ProtoLens.Encoding.Bytes.Parser Edits
        loop x mutable'edits
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'edits <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'edits)
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
                              (Data.ProtoLens.Field.field @"vec'edits") frozen'edits x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "edits"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'edits y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'edits
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'edits <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'edits)
          "Edits"
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
                (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'edits") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData Edits where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Edits'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Edits'edits x__) ())
{- | Fields :
      -}
data Empty
  = Empty'_constructor {_Empty'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Empty where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Message Empty where
  messageName _ = Data.Text.pack "inference.customer.v1.Empty"
  packedMessageDescriptor _
    = "\n\
      \\ENQEmpty"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag = let in Data.Map.fromList []
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Empty'_unknownFields
        (\ x__ y__ -> x__ {_Empty'_unknownFields = y__})
  defMessage = Empty'_constructor {_Empty'_unknownFields = []}
  parseMessage
    = let
        loop :: Empty -> Data.ProtoLens.Encoding.Bytes.Parser Empty
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
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Empty"
  buildMessage
    = \ _x
        -> Data.ProtoLens.Encoding.Wire.buildFieldSet
             (Lens.Family2.view Data.ProtoLens.unknownFields _x)
instance Control.DeepSeq.NFData Empty where
  rnf
    = \ x__ -> Control.DeepSeq.deepseq (_Empty'_unknownFields x__) ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.candidateDigest' @:: Lens' EvaluationAggregate Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.metricIdentity' @:: Lens' EvaluationAggregate Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.aggregation' @:: Lens' EvaluationAggregate EvaluationAggregation@
         * 'Proto.Inference.V1.Inference_Fields.value' @:: Lens' EvaluationAggregate ExactRational@
         * 'Proto.Inference.V1.Inference_Fields.maybe'value' @:: Lens' EvaluationAggregate (Prelude.Maybe ExactRational)@ -}
data EvaluationAggregate
  = EvaluationAggregate'_constructor {_EvaluationAggregate'candidateDigest :: !Data.ByteString.ByteString,
                                      _EvaluationAggregate'metricIdentity :: !Data.Text.Text,
                                      _EvaluationAggregate'aggregation :: !EvaluationAggregation,
                                      _EvaluationAggregate'value :: !(Prelude.Maybe ExactRational),
                                      _EvaluationAggregate'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationAggregate where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationAggregate "candidateDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationAggregate'candidateDigest
           (\ x__ y__ -> x__ {_EvaluationAggregate'candidateDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationAggregate "metricIdentity" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationAggregate'metricIdentity
           (\ x__ y__ -> x__ {_EvaluationAggregate'metricIdentity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationAggregate "aggregation" EvaluationAggregation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationAggregate'aggregation
           (\ x__ y__ -> x__ {_EvaluationAggregate'aggregation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationAggregate "value" ExactRational where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationAggregate'value
           (\ x__ y__ -> x__ {_EvaluationAggregate'value = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationAggregate "maybe'value" (Prelude.Maybe ExactRational) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationAggregate'value
           (\ x__ y__ -> x__ {_EvaluationAggregate'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationAggregate where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationAggregate"
  packedMessageDescriptor _
    = "\n\
      \\DC3EvaluationAggregate\DC2/\n\
      \\DLEcandidate_digest\CAN\SOH \SOH(\fR\SIcandidateDigestB\EOT\200\243\CAN \DC2'\n\
      \\SImetric_identity\CAN\STX \SOH(\tR\SOmetricIdentity\DC2T\n\
      \\vaggregation\CAN\ETX \SOH(\SO2,.inference.customer.v1.EvaluationAggregationR\vaggregationB\EOT\136\244\CAN\SOH\DC2@\n\
      \\ENQvalue\CAN\EOT \SOH(\v2$.inference.customer.v1.ExactRationalR\ENQvalueB\EOT\208\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        candidateDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "candidate_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"candidateDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationAggregate
        metricIdentity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metric_identity"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"metricIdentity")) ::
              Data.ProtoLens.FieldDescriptor EvaluationAggregate
        aggregation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "aggregation"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationAggregation)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"aggregation")) ::
              Data.ProtoLens.FieldDescriptor EvaluationAggregate
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ExactRational)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'value")) ::
              Data.ProtoLens.FieldDescriptor EvaluationAggregate
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, candidateDigest__field_descriptor),
           (Data.ProtoLens.Tag 2, metricIdentity__field_descriptor),
           (Data.ProtoLens.Tag 3, aggregation__field_descriptor),
           (Data.ProtoLens.Tag 4, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationAggregate'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationAggregate'_unknownFields = y__})
  defMessage
    = EvaluationAggregate'_constructor
        {_EvaluationAggregate'candidateDigest = Data.ProtoLens.fieldDefault,
         _EvaluationAggregate'metricIdentity = Data.ProtoLens.fieldDefault,
         _EvaluationAggregate'aggregation = Data.ProtoLens.fieldDefault,
         _EvaluationAggregate'value = Prelude.Nothing,
         _EvaluationAggregate'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationAggregate
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationAggregate
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
                                       "candidate_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"candidateDigest") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "metric_identity"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"metricIdentity") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "aggregation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"aggregation") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
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
          (do loop Data.ProtoLens.defMessage) "EvaluationAggregate"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"candidateDigest") _x
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
                         (Data.ProtoLens.Field.field @"metricIdentity") _x
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
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"aggregation") _x
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
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'value") _x
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
instance Control.DeepSeq.NFData EvaluationAggregate where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationAggregate'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationAggregate'candidateDigest x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationAggregate'metricIdentity x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationAggregate'aggregation x__)
                      (Control.DeepSeq.deepseq (_EvaluationAggregate'value x__) ()))))
newtype EvaluationAggregation'UnrecognizedValue
  = EvaluationAggregation'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data EvaluationAggregation
  = EVALUATION_AGGREGATION_UNSPECIFIED |
    EVALUATION_AGGREGATION_MEAN |
    EVALUATION_AGGREGATION_SUM |
    EVALUATION_AGGREGATION_MINIMUM |
    EVALUATION_AGGREGATION_MAXIMUM |
    EvaluationAggregation'Unrecognized !EvaluationAggregation'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum EvaluationAggregation where
  maybeToEnum 0 = Prelude.Just EVALUATION_AGGREGATION_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just EVALUATION_AGGREGATION_MEAN
  maybeToEnum 2 = Prelude.Just EVALUATION_AGGREGATION_SUM
  maybeToEnum 3 = Prelude.Just EVALUATION_AGGREGATION_MINIMUM
  maybeToEnum 4 = Prelude.Just EVALUATION_AGGREGATION_MAXIMUM
  maybeToEnum k
    = Prelude.Just
        (EvaluationAggregation'Unrecognized
           (EvaluationAggregation'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum EVALUATION_AGGREGATION_UNSPECIFIED
    = "EVALUATION_AGGREGATION_UNSPECIFIED"
  showEnum EVALUATION_AGGREGATION_MEAN
    = "EVALUATION_AGGREGATION_MEAN"
  showEnum EVALUATION_AGGREGATION_SUM = "EVALUATION_AGGREGATION_SUM"
  showEnum EVALUATION_AGGREGATION_MINIMUM
    = "EVALUATION_AGGREGATION_MINIMUM"
  showEnum EVALUATION_AGGREGATION_MAXIMUM
    = "EVALUATION_AGGREGATION_MAXIMUM"
  showEnum
    (EvaluationAggregation'Unrecognized (EvaluationAggregation'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "EVALUATION_AGGREGATION_UNSPECIFIED"
    = Prelude.Just EVALUATION_AGGREGATION_UNSPECIFIED
    | (Prelude.==) k "EVALUATION_AGGREGATION_MEAN"
    = Prelude.Just EVALUATION_AGGREGATION_MEAN
    | (Prelude.==) k "EVALUATION_AGGREGATION_SUM"
    = Prelude.Just EVALUATION_AGGREGATION_SUM
    | (Prelude.==) k "EVALUATION_AGGREGATION_MINIMUM"
    = Prelude.Just EVALUATION_AGGREGATION_MINIMUM
    | (Prelude.==) k "EVALUATION_AGGREGATION_MAXIMUM"
    = Prelude.Just EVALUATION_AGGREGATION_MAXIMUM
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded EvaluationAggregation where
  minBound = EVALUATION_AGGREGATION_UNSPECIFIED
  maxBound = EVALUATION_AGGREGATION_MAXIMUM
instance Prelude.Enum EvaluationAggregation where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum EvaluationAggregation: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum EVALUATION_AGGREGATION_UNSPECIFIED = 0
  fromEnum EVALUATION_AGGREGATION_MEAN = 1
  fromEnum EVALUATION_AGGREGATION_SUM = 2
  fromEnum EVALUATION_AGGREGATION_MINIMUM = 3
  fromEnum EVALUATION_AGGREGATION_MAXIMUM = 4
  fromEnum
    (EvaluationAggregation'Unrecognized (EvaluationAggregation'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ EVALUATION_AGGREGATION_MAXIMUM
    = Prelude.error
        "EvaluationAggregation.succ: bad argument EVALUATION_AGGREGATION_MAXIMUM. This value would be out of bounds."
  succ EVALUATION_AGGREGATION_UNSPECIFIED
    = EVALUATION_AGGREGATION_MEAN
  succ EVALUATION_AGGREGATION_MEAN = EVALUATION_AGGREGATION_SUM
  succ EVALUATION_AGGREGATION_SUM = EVALUATION_AGGREGATION_MINIMUM
  succ EVALUATION_AGGREGATION_MINIMUM
    = EVALUATION_AGGREGATION_MAXIMUM
  succ (EvaluationAggregation'Unrecognized _)
    = Prelude.error
        "EvaluationAggregation.succ: bad argument: unrecognized value"
  pred EVALUATION_AGGREGATION_UNSPECIFIED
    = Prelude.error
        "EvaluationAggregation.pred: bad argument EVALUATION_AGGREGATION_UNSPECIFIED. This value would be out of bounds."
  pred EVALUATION_AGGREGATION_MEAN
    = EVALUATION_AGGREGATION_UNSPECIFIED
  pred EVALUATION_AGGREGATION_SUM = EVALUATION_AGGREGATION_MEAN
  pred EVALUATION_AGGREGATION_MINIMUM = EVALUATION_AGGREGATION_SUM
  pred EVALUATION_AGGREGATION_MAXIMUM
    = EVALUATION_AGGREGATION_MINIMUM
  pred (EvaluationAggregation'Unrecognized _)
    = Prelude.error
        "EvaluationAggregation.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault EvaluationAggregation where
  fieldDefault = EVALUATION_AGGREGATION_UNSPECIFIED
instance Control.DeepSeq.NFData EvaluationAggregation where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.digest' @:: Lens' EvaluationArtifact Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.mediaType' @:: Lens' EvaluationArtifact Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.logicalSize' @:: Lens' EvaluationArtifact Data.Word.Word64@ -}
data EvaluationArtifact
  = EvaluationArtifact'_constructor {_EvaluationArtifact'digest :: !Data.ByteString.ByteString,
                                     _EvaluationArtifact'mediaType :: !Data.Text.Text,
                                     _EvaluationArtifact'logicalSize :: !Data.Word.Word64,
                                     _EvaluationArtifact'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationArtifact where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationArtifact "digest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationArtifact'digest
           (\ x__ y__ -> x__ {_EvaluationArtifact'digest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationArtifact "mediaType" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationArtifact'mediaType
           (\ x__ y__ -> x__ {_EvaluationArtifact'mediaType = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationArtifact "logicalSize" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationArtifact'logicalSize
           (\ x__ y__ -> x__ {_EvaluationArtifact'logicalSize = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationArtifact where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationArtifact"
  packedMessageDescriptor _
    = "\n\
      \\DC2EvaluationArtifact\DC2\FS\n\
      \\ACKdigest\CAN\SOH \SOH(\fR\ACKdigestB\EOT\200\243\CAN \DC2$\n\
      \\n\
      \media_type\CAN\STX \SOH(\tR\tmediaTypeB\ENQ\248\243\CAN\128\STX\DC2'\n\
      \\flogical_size\CAN\ETX \SOH(\EOTR\vlogicalSizeB\EOT\216\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        digest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"digest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationArtifact
        mediaType__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "media_type"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"mediaType")) ::
              Data.ProtoLens.FieldDescriptor EvaluationArtifact
        logicalSize__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "logical_size"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"logicalSize")) ::
              Data.ProtoLens.FieldDescriptor EvaluationArtifact
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, digest__field_descriptor),
           (Data.ProtoLens.Tag 2, mediaType__field_descriptor),
           (Data.ProtoLens.Tag 3, logicalSize__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationArtifact'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationArtifact'_unknownFields = y__})
  defMessage
    = EvaluationArtifact'_constructor
        {_EvaluationArtifact'digest = Data.ProtoLens.fieldDefault,
         _EvaluationArtifact'mediaType = Data.ProtoLens.fieldDefault,
         _EvaluationArtifact'logicalSize = Data.ProtoLens.fieldDefault,
         _EvaluationArtifact'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationArtifact
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationArtifact
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
                                       "digest"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"digest") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "media_type"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"mediaType") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "logical_size"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"logicalSize") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "EvaluationArtifact"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"digest") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"mediaType") _x
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
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"logicalSize") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData EvaluationArtifact where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationArtifact'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationArtifact'digest x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationArtifact'mediaType x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationArtifact'logicalSize x__) ())))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.caseId' @:: Lens' EvaluationCase Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.input' @:: Lens' EvaluationCase Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.inputArtifactDigest' @:: Lens' EvaluationCase Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maybe'inputArtifactDigest' @:: Lens' EvaluationCase (Prelude.Maybe Data.ByteString.ByteString)@ -}
data EvaluationCase
  = EvaluationCase'_constructor {_EvaluationCase'caseId :: !Data.ByteString.ByteString,
                                 _EvaluationCase'input :: !Data.ByteString.ByteString,
                                 _EvaluationCase'inputArtifactDigest :: !(Prelude.Maybe Data.ByteString.ByteString),
                                 _EvaluationCase'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationCase where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationCase "caseId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCase'caseId
           (\ x__ y__ -> x__ {_EvaluationCase'caseId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationCase "input" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCase'input
           (\ x__ y__ -> x__ {_EvaluationCase'input = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationCase "inputArtifactDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCase'inputArtifactDigest
           (\ x__ y__ -> x__ {_EvaluationCase'inputArtifactDigest = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField EvaluationCase "maybe'inputArtifactDigest" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCase'inputArtifactDigest
           (\ x__ y__ -> x__ {_EvaluationCase'inputArtifactDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationCase where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationCase"
  packedMessageDescriptor _
    = "\n\
      \\SOEvaluationCase\DC2\GS\n\
      \\acase_id\CAN\SOH \SOH(\fR\ACKcaseIdB\EOT\200\243\CAN\DLE\DC2\DC4\n\
      \\ENQinput\CAN\STX \SOH(\fR\ENQinput\DC2=\n\
      \\NAKinput_artifact_digest\CAN\ETX \SOH(\fH\NULR\DC3inputArtifactDigestB\EOT\200\243\CAN \136\SOH\SOHB\CAN\n\
      \\SYN_input_artifact_digest"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        caseId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "case_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"caseId")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCase
        input__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "input"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"input")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCase
        inputArtifactDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "input_artifact_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'inputArtifactDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCase
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, caseId__field_descriptor),
           (Data.ProtoLens.Tag 2, input__field_descriptor),
           (Data.ProtoLens.Tag 3, inputArtifactDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationCase'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationCase'_unknownFields = y__})
  defMessage
    = EvaluationCase'_constructor
        {_EvaluationCase'caseId = Data.ProtoLens.fieldDefault,
         _EvaluationCase'input = Data.ProtoLens.fieldDefault,
         _EvaluationCase'inputArtifactDigest = Prelude.Nothing,
         _EvaluationCase'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationCase
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationCase
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
                                       "case_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"caseId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "input"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"input") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "input_artifact_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"inputArtifactDigest") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "EvaluationCase"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"caseId") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"input") _x
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
                          (Data.ProtoLens.Field.field @"maybe'inputArtifactDigest") _x
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
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData EvaluationCase where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationCase'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationCase'caseId x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationCase'input x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationCase'inputArtifactDigest x__) ())))
newtype EvaluationCaseOutcome'UnrecognizedValue
  = EvaluationCaseOutcome'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data EvaluationCaseOutcome
  = EVALUATION_CASE_OUTCOME_UNSPECIFIED |
    EVALUATION_CASE_OUTCOME_SCORED |
    EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED |
    EVALUATION_CASE_OUTCOME_GRADER_FAILED |
    EvaluationCaseOutcome'Unrecognized !EvaluationCaseOutcome'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum EvaluationCaseOutcome where
  maybeToEnum 0 = Prelude.Just EVALUATION_CASE_OUTCOME_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just EVALUATION_CASE_OUTCOME_SCORED
  maybeToEnum 2
    = Prelude.Just EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
  maybeToEnum 3 = Prelude.Just EVALUATION_CASE_OUTCOME_GRADER_FAILED
  maybeToEnum k
    = Prelude.Just
        (EvaluationCaseOutcome'Unrecognized
           (EvaluationCaseOutcome'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum EVALUATION_CASE_OUTCOME_UNSPECIFIED
    = "EVALUATION_CASE_OUTCOME_UNSPECIFIED"
  showEnum EVALUATION_CASE_OUTCOME_SCORED
    = "EVALUATION_CASE_OUTCOME_SCORED"
  showEnum EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
    = "EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED"
  showEnum EVALUATION_CASE_OUTCOME_GRADER_FAILED
    = "EVALUATION_CASE_OUTCOME_GRADER_FAILED"
  showEnum
    (EvaluationCaseOutcome'Unrecognized (EvaluationCaseOutcome'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "EVALUATION_CASE_OUTCOME_UNSPECIFIED"
    = Prelude.Just EVALUATION_CASE_OUTCOME_UNSPECIFIED
    | (Prelude.==) k "EVALUATION_CASE_OUTCOME_SCORED"
    = Prelude.Just EVALUATION_CASE_OUTCOME_SCORED
    | (Prelude.==) k "EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED"
    = Prelude.Just EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
    | (Prelude.==) k "EVALUATION_CASE_OUTCOME_GRADER_FAILED"
    = Prelude.Just EVALUATION_CASE_OUTCOME_GRADER_FAILED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded EvaluationCaseOutcome where
  minBound = EVALUATION_CASE_OUTCOME_UNSPECIFIED
  maxBound = EVALUATION_CASE_OUTCOME_GRADER_FAILED
instance Prelude.Enum EvaluationCaseOutcome where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum EvaluationCaseOutcome: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum EVALUATION_CASE_OUTCOME_UNSPECIFIED = 0
  fromEnum EVALUATION_CASE_OUTCOME_SCORED = 1
  fromEnum EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED = 2
  fromEnum EVALUATION_CASE_OUTCOME_GRADER_FAILED = 3
  fromEnum
    (EvaluationCaseOutcome'Unrecognized (EvaluationCaseOutcome'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ EVALUATION_CASE_OUTCOME_GRADER_FAILED
    = Prelude.error
        "EvaluationCaseOutcome.succ: bad argument EVALUATION_CASE_OUTCOME_GRADER_FAILED. This value would be out of bounds."
  succ EVALUATION_CASE_OUTCOME_UNSPECIFIED
    = EVALUATION_CASE_OUTCOME_SCORED
  succ EVALUATION_CASE_OUTCOME_SCORED
    = EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
  succ EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
    = EVALUATION_CASE_OUTCOME_GRADER_FAILED
  succ (EvaluationCaseOutcome'Unrecognized _)
    = Prelude.error
        "EvaluationCaseOutcome.succ: bad argument: unrecognized value"
  pred EVALUATION_CASE_OUTCOME_UNSPECIFIED
    = Prelude.error
        "EvaluationCaseOutcome.pred: bad argument EVALUATION_CASE_OUTCOME_UNSPECIFIED. This value would be out of bounds."
  pred EVALUATION_CASE_OUTCOME_SCORED
    = EVALUATION_CASE_OUTCOME_UNSPECIFIED
  pred EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
    = EVALUATION_CASE_OUTCOME_SCORED
  pred EVALUATION_CASE_OUTCOME_GRADER_FAILED
    = EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED
  pred (EvaluationCaseOutcome'Unrecognized _)
    = Prelude.error
        "EvaluationCaseOutcome.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault EvaluationCaseOutcome where
  fieldDefault = EVALUATION_CASE_OUTCOME_UNSPECIFIED
instance Control.DeepSeq.NFData EvaluationCaseOutcome where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.candidateDigest' @:: Lens' EvaluationCaseResult Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.caseId' @:: Lens' EvaluationCaseResult Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.observation' @:: Lens' EvaluationCaseResult EvaluationGraderObservation@
         * 'Proto.Inference.V1.Inference_Fields.maybe'observation' @:: Lens' EvaluationCaseResult (Prelude.Maybe EvaluationGraderObservation)@
         * 'Proto.Inference.V1.Inference_Fields.metrics' @:: Lens' EvaluationCaseResult [EvaluationMetricValue]@
         * 'Proto.Inference.V1.Inference_Fields.vec'metrics' @:: Lens' EvaluationCaseResult (Data.Vector.Vector EvaluationMetricValue)@
         * 'Proto.Inference.V1.Inference_Fields.outcome' @:: Lens' EvaluationCaseResult EvaluationCaseOutcome@ -}
data EvaluationCaseResult
  = EvaluationCaseResult'_constructor {_EvaluationCaseResult'candidateDigest :: !Data.ByteString.ByteString,
                                       _EvaluationCaseResult'caseId :: !Data.ByteString.ByteString,
                                       _EvaluationCaseResult'observation :: !(Prelude.Maybe EvaluationGraderObservation),
                                       _EvaluationCaseResult'metrics :: !(Data.Vector.Vector EvaluationMetricValue),
                                       _EvaluationCaseResult'outcome :: !EvaluationCaseOutcome,
                                       _EvaluationCaseResult'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationCaseResult where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "candidateDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'candidateDigest
           (\ x__ y__ -> x__ {_EvaluationCaseResult'candidateDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "caseId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'caseId
           (\ x__ y__ -> x__ {_EvaluationCaseResult'caseId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "observation" EvaluationGraderObservation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'observation
           (\ x__ y__ -> x__ {_EvaluationCaseResult'observation = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "maybe'observation" (Prelude.Maybe EvaluationGraderObservation) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'observation
           (\ x__ y__ -> x__ {_EvaluationCaseResult'observation = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "metrics" [EvaluationMetricValue] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'metrics
           (\ x__ y__ -> x__ {_EvaluationCaseResult'metrics = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "vec'metrics" (Data.Vector.Vector EvaluationMetricValue) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'metrics
           (\ x__ y__ -> x__ {_EvaluationCaseResult'metrics = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationCaseResult "outcome" EvaluationCaseOutcome where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationCaseResult'outcome
           (\ x__ y__ -> x__ {_EvaluationCaseResult'outcome = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationCaseResult where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationCaseResult"
  packedMessageDescriptor _
    = "\n\
      \\DC4EvaluationCaseResult\DC2/\n\
      \\DLEcandidate_digest\CAN\SOH \SOH(\fR\SIcandidateDigestB\EOT\200\243\CAN \DC2\GS\n\
      \\acase_id\CAN\STX \SOH(\fR\ACKcaseIdB\EOT\200\243\CAN\DLE\DC2Z\n\
      \\vobservation\CAN\ETX \SOH(\v22.inference.customer.v1.EvaluationGraderObservationR\vobservationB\EOT\208\243\CAN\SOH\DC2F\n\
      \\ametrics\CAN\EOT \ETX(\v2,.inference.customer.v1.EvaluationMetricValueR\ametrics\DC2L\n\
      \\aoutcome\CAN\ENQ \SOH(\SO2,.inference.customer.v1.EvaluationCaseOutcomeR\aoutcomeB\EOT\136\244\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        candidateDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "candidate_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"candidateDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCaseResult
        caseId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "case_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"caseId")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCaseResult
        observation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "observation"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationGraderObservation)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'observation")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCaseResult
        metrics__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metrics"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationMetricValue)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"metrics")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCaseResult
        outcome__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "outcome"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationCaseOutcome)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"outcome")) ::
              Data.ProtoLens.FieldDescriptor EvaluationCaseResult
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, candidateDigest__field_descriptor),
           (Data.ProtoLens.Tag 2, caseId__field_descriptor),
           (Data.ProtoLens.Tag 3, observation__field_descriptor),
           (Data.ProtoLens.Tag 4, metrics__field_descriptor),
           (Data.ProtoLens.Tag 5, outcome__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationCaseResult'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationCaseResult'_unknownFields = y__})
  defMessage
    = EvaluationCaseResult'_constructor
        {_EvaluationCaseResult'candidateDigest = Data.ProtoLens.fieldDefault,
         _EvaluationCaseResult'caseId = Data.ProtoLens.fieldDefault,
         _EvaluationCaseResult'observation = Prelude.Nothing,
         _EvaluationCaseResult'metrics = Data.Vector.Generic.empty,
         _EvaluationCaseResult'outcome = Data.ProtoLens.fieldDefault,
         _EvaluationCaseResult'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationCaseResult
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld EvaluationMetricValue
             -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationCaseResult
        loop x mutable'metrics
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'metrics <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'metrics)
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
                              (Data.ProtoLens.Field.field @"vec'metrics") frozen'metrics x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "candidate_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"candidateDigest") y x)
                                  mutable'metrics
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "case_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"caseId") y x)
                                  mutable'metrics
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "observation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"observation") y x)
                                  mutable'metrics
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "metrics"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'metrics y)
                                loop x v
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "outcome"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"outcome") y x)
                                  mutable'metrics
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'metrics
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'metrics <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'metrics)
          "EvaluationCaseResult"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"candidateDigest") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"caseId") _x
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
                          (Data.ProtoLens.Field.field @"maybe'observation") _x
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
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'metrics") _x))
                      ((Data.Monoid.<>)
                         (let
                            _v = Lens.Family2.view (Data.ProtoLens.Field.field @"outcome") _x
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
instance Control.DeepSeq.NFData EvaluationCaseResult where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationCaseResult'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationCaseResult'candidateDigest x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationCaseResult'caseId x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationCaseResult'observation x__)
                      (Control.DeepSeq.deepseq
                         (_EvaluationCaseResult'metrics x__)
                         (Control.DeepSeq.deepseq
                            (_EvaluationCaseResult'outcome x__) ())))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.handle' @:: Lens' EvaluationGrader Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.artifactDigest' @:: Lens' EvaluationGrader Data.ByteString.ByteString@ -}
data EvaluationGrader
  = EvaluationGrader'_constructor {_EvaluationGrader'handle :: !Data.ByteString.ByteString,
                                   _EvaluationGrader'artifactDigest :: !Data.ByteString.ByteString,
                                   _EvaluationGrader'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationGrader where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationGrader "handle" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationGrader'handle
           (\ x__ y__ -> x__ {_EvaluationGrader'handle = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationGrader "artifactDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationGrader'artifactDigest
           (\ x__ y__ -> x__ {_EvaluationGrader'artifactDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationGrader where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationGrader"
  packedMessageDescriptor _
    = "\n\
      \\DLEEvaluationGrader\DC2\GS\n\
      \\ACKhandle\CAN\SOH \SOH(\fR\ACKhandleB\ENQ\248\243\CAN\128 \DC2-\n\
      \\SIartifact_digest\CAN\STX \SOH(\fR\SOartifactDigestB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        handle__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "handle"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"handle")) ::
              Data.ProtoLens.FieldDescriptor EvaluationGrader
        artifactDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "artifact_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"artifactDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationGrader
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, handle__field_descriptor),
           (Data.ProtoLens.Tag 2, artifactDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationGrader'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationGrader'_unknownFields = y__})
  defMessage
    = EvaluationGrader'_constructor
        {_EvaluationGrader'handle = Data.ProtoLens.fieldDefault,
         _EvaluationGrader'artifactDigest = Data.ProtoLens.fieldDefault,
         _EvaluationGrader'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationGrader
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationGrader
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
                                       "handle"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"handle") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "artifact_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"artifactDigest") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "EvaluationGrader"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"handle") _x
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
                         (Data.ProtoLens.Field.field @"artifactDigest") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData EvaluationGrader where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationGrader'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationGrader'handle x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationGrader'artifactDigest x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.nativeOutputDigest' @:: Lens' EvaluationGraderObservation Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.observationDigest' @:: Lens' EvaluationGraderObservation Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.bindingDigest' @:: Lens' EvaluationGraderObservation Data.ByteString.ByteString@ -}
data EvaluationGraderObservation
  = EvaluationGraderObservation'_constructor {_EvaluationGraderObservation'nativeOutputDigest :: !Data.ByteString.ByteString,
                                              _EvaluationGraderObservation'observationDigest :: !Data.ByteString.ByteString,
                                              _EvaluationGraderObservation'bindingDigest :: !Data.ByteString.ByteString,
                                              _EvaluationGraderObservation'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationGraderObservation where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationGraderObservation "nativeOutputDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationGraderObservation'nativeOutputDigest
           (\ x__ y__
              -> x__ {_EvaluationGraderObservation'nativeOutputDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationGraderObservation "observationDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationGraderObservation'observationDigest
           (\ x__ y__
              -> x__ {_EvaluationGraderObservation'observationDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationGraderObservation "bindingDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationGraderObservation'bindingDigest
           (\ x__ y__
              -> x__ {_EvaluationGraderObservation'bindingDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationGraderObservation where
  messageName _
    = Data.Text.pack
        "inference.customer.v1.EvaluationGraderObservation"
  packedMessageDescriptor _
    = "\n\
      \\ESCEvaluationGraderObservation\DC26\n\
      \\DC4native_output_digest\CAN\SOH \SOH(\fR\DC2nativeOutputDigestB\EOT\200\243\CAN \DC23\n\
      \\DC2observation_digest\CAN\STX \SOH(\fR\DC1observationDigestB\EOT\200\243\CAN \DC2+\n\
      \\SObinding_digest\CAN\ETX \SOH(\fR\rbindingDigestB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        nativeOutputDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "native_output_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"nativeOutputDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationGraderObservation
        observationDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "observation_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"observationDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationGraderObservation
        bindingDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "binding_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"bindingDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationGraderObservation
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, nativeOutputDigest__field_descriptor),
           (Data.ProtoLens.Tag 2, observationDigest__field_descriptor),
           (Data.ProtoLens.Tag 3, bindingDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationGraderObservation'_unknownFields
        (\ x__ y__
           -> x__ {_EvaluationGraderObservation'_unknownFields = y__})
  defMessage
    = EvaluationGraderObservation'_constructor
        {_EvaluationGraderObservation'nativeOutputDigest = Data.ProtoLens.fieldDefault,
         _EvaluationGraderObservation'observationDigest = Data.ProtoLens.fieldDefault,
         _EvaluationGraderObservation'bindingDigest = Data.ProtoLens.fieldDefault,
         _EvaluationGraderObservation'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationGraderObservation
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationGraderObservation
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
                                       "native_output_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"nativeOutputDigest") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "observation_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"observationDigest") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "binding_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"bindingDigest") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "EvaluationGraderObservation"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"nativeOutputDigest") _x
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
                         (Data.ProtoLens.Field.field @"observationDigest") _x
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
                            (Data.ProtoLens.Field.field @"bindingDigest") _x
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
instance Control.DeepSeq.NFData EvaluationGraderObservation where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationGraderObservation'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationGraderObservation'nativeOutputDigest x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationGraderObservation'observationDigest x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationGraderObservation'bindingDigest x__) ())))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' EvaluationMetric Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.aggregation' @:: Lens' EvaluationMetric EvaluationAggregation@ -}
data EvaluationMetric
  = EvaluationMetric'_constructor {_EvaluationMetric'identity :: !Data.Text.Text,
                                   _EvaluationMetric'aggregation :: !EvaluationAggregation,
                                   _EvaluationMetric'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationMetric where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationMetric "identity" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationMetric'identity
           (\ x__ y__ -> x__ {_EvaluationMetric'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationMetric "aggregation" EvaluationAggregation where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationMetric'aggregation
           (\ x__ y__ -> x__ {_EvaluationMetric'aggregation = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationMetric where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationMetric"
  packedMessageDescriptor _
    = "\n\
      \\DLEEvaluationMetric\DC2!\n\
      \\bidentity\CAN\SOH \SOH(\tR\bidentityB\ENQ\248\243\CAN\128\STX\DC2T\n\
      \\vaggregation\CAN\STX \SOH(\SO2,.inference.customer.v1.EvaluationAggregationR\vaggregationB\EOT\136\244\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"identity")) ::
              Data.ProtoLens.FieldDescriptor EvaluationMetric
        aggregation__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "aggregation"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationAggregation)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"aggregation")) ::
              Data.ProtoLens.FieldDescriptor EvaluationMetric
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, aggregation__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationMetric'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationMetric'_unknownFields = y__})
  defMessage
    = EvaluationMetric'_constructor
        {_EvaluationMetric'identity = Data.ProtoLens.fieldDefault,
         _EvaluationMetric'aggregation = Data.ProtoLens.fieldDefault,
         _EvaluationMetric'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationMetric
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationMetric
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "aggregation"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"aggregation") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "EvaluationMetric"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"identity") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"aggregation") _x
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
instance Control.DeepSeq.NFData EvaluationMetric where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationMetric'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationMetric'identity x__)
                (Control.DeepSeq.deepseq (_EvaluationMetric'aggregation x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.metricIdentity' @:: Lens' EvaluationMetricValue Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.value' @:: Lens' EvaluationMetricValue ExactRational@
         * 'Proto.Inference.V1.Inference_Fields.maybe'value' @:: Lens' EvaluationMetricValue (Prelude.Maybe ExactRational)@ -}
data EvaluationMetricValue
  = EvaluationMetricValue'_constructor {_EvaluationMetricValue'metricIdentity :: !Data.Text.Text,
                                        _EvaluationMetricValue'value :: !(Prelude.Maybe ExactRational),
                                        _EvaluationMetricValue'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationMetricValue where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationMetricValue "metricIdentity" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationMetricValue'metricIdentity
           (\ x__ y__ -> x__ {_EvaluationMetricValue'metricIdentity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationMetricValue "value" ExactRational where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationMetricValue'value
           (\ x__ y__ -> x__ {_EvaluationMetricValue'value = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationMetricValue "maybe'value" (Prelude.Maybe ExactRational) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationMetricValue'value
           (\ x__ y__ -> x__ {_EvaluationMetricValue'value = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationMetricValue where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationMetricValue"
  packedMessageDescriptor _
    = "\n\
      \\NAKEvaluationMetricValue\DC2'\n\
      \\SImetric_identity\CAN\SOH \SOH(\tR\SOmetricIdentity\DC2@\n\
      \\ENQvalue\CAN\STX \SOH(\v2$.inference.customer.v1.ExactRationalR\ENQvalueB\EOT\208\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        metricIdentity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metric_identity"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"metricIdentity")) ::
              Data.ProtoLens.FieldDescriptor EvaluationMetricValue
        value__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "value"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ExactRational)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'value")) ::
              Data.ProtoLens.FieldDescriptor EvaluationMetricValue
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, metricIdentity__field_descriptor),
           (Data.ProtoLens.Tag 2, value__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationMetricValue'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationMetricValue'_unknownFields = y__})
  defMessage
    = EvaluationMetricValue'_constructor
        {_EvaluationMetricValue'metricIdentity = Data.ProtoLens.fieldDefault,
         _EvaluationMetricValue'value = Prelude.Nothing,
         _EvaluationMetricValue'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationMetricValue
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationMetricValue
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
                                       "metric_identity"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"metricIdentity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
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
          (do loop Data.ProtoLens.defMessage) "EvaluationMetricValue"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"metricIdentity") _x
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'value") _x
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
instance Control.DeepSeq.NFData EvaluationMetricValue where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationMetricValue'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationMetricValue'metricIdentity x__)
                (Control.DeepSeq.deepseq (_EvaluationMetricValue'value x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.specDigest' @:: Lens' EvaluationResult Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.caseResults' @:: Lens' EvaluationResult [EvaluationCaseResult]@
         * 'Proto.Inference.V1.Inference_Fields.vec'caseResults' @:: Lens' EvaluationResult (Data.Vector.Vector EvaluationCaseResult)@
         * 'Proto.Inference.V1.Inference_Fields.aggregates' @:: Lens' EvaluationResult [EvaluationAggregate]@
         * 'Proto.Inference.V1.Inference_Fields.vec'aggregates' @:: Lens' EvaluationResult (Data.Vector.Vector EvaluationAggregate)@
         * 'Proto.Inference.V1.Inference_Fields.resultDigest' @:: Lens' EvaluationResult Data.ByteString.ByteString@ -}
data EvaluationResult
  = EvaluationResult'_constructor {_EvaluationResult'specDigest :: !Data.ByteString.ByteString,
                                   _EvaluationResult'caseResults :: !(Data.Vector.Vector EvaluationCaseResult),
                                   _EvaluationResult'aggregates :: !(Data.Vector.Vector EvaluationAggregate),
                                   _EvaluationResult'resultDigest :: !Data.ByteString.ByteString,
                                   _EvaluationResult'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationResult where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationResult "specDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationResult'specDigest
           (\ x__ y__ -> x__ {_EvaluationResult'specDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationResult "caseResults" [EvaluationCaseResult] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationResult'caseResults
           (\ x__ y__ -> x__ {_EvaluationResult'caseResults = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EvaluationResult "vec'caseResults" (Data.Vector.Vector EvaluationCaseResult) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationResult'caseResults
           (\ x__ y__ -> x__ {_EvaluationResult'caseResults = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationResult "aggregates" [EvaluationAggregate] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationResult'aggregates
           (\ x__ y__ -> x__ {_EvaluationResult'aggregates = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EvaluationResult "vec'aggregates" (Data.Vector.Vector EvaluationAggregate) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationResult'aggregates
           (\ x__ y__ -> x__ {_EvaluationResult'aggregates = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationResult "resultDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationResult'resultDigest
           (\ x__ y__ -> x__ {_EvaluationResult'resultDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationResult where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationResult"
  packedMessageDescriptor _
    = "\n\
      \\DLEEvaluationResult\DC2%\n\
      \\vspec_digest\CAN\SOH \SOH(\fR\n\
      \specDigestB\EOT\200\243\CAN \DC2N\n\
      \\fcase_results\CAN\STX \ETX(\v2+.inference.customer.v1.EvaluationCaseResultR\vcaseResults\DC2J\n\
      \\n\
      \aggregates\CAN\ETX \ETX(\v2*.inference.customer.v1.EvaluationAggregateR\n\
      \aggregates\DC2)\n\
      \\rresult_digest\CAN\EOT \SOH(\fR\fresultDigestB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        specDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "spec_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"specDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationResult
        caseResults__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "case_results"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationCaseResult)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"caseResults")) ::
              Data.ProtoLens.FieldDescriptor EvaluationResult
        aggregates__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "aggregates"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationAggregate)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"aggregates")) ::
              Data.ProtoLens.FieldDescriptor EvaluationResult
        resultDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "result_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"resultDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationResult
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, specDigest__field_descriptor),
           (Data.ProtoLens.Tag 2, caseResults__field_descriptor),
           (Data.ProtoLens.Tag 3, aggregates__field_descriptor),
           (Data.ProtoLens.Tag 4, resultDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationResult'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationResult'_unknownFields = y__})
  defMessage
    = EvaluationResult'_constructor
        {_EvaluationResult'specDigest = Data.ProtoLens.fieldDefault,
         _EvaluationResult'caseResults = Data.Vector.Generic.empty,
         _EvaluationResult'aggregates = Data.Vector.Generic.empty,
         _EvaluationResult'resultDigest = Data.ProtoLens.fieldDefault,
         _EvaluationResult'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationResult
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld EvaluationAggregate
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld EvaluationCaseResult
                -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationResult
        loop x mutable'aggregates mutable'caseResults
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'aggregates <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                             (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                mutable'aggregates)
                      frozen'caseResults <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                              (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                 mutable'caseResults)
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
                              (Data.ProtoLens.Field.field @"vec'aggregates") frozen'aggregates
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'caseResults") frozen'caseResults
                                 x)))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "spec_digest"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"specDigest") y x)
                                  mutable'aggregates mutable'caseResults
                        18
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "case_results"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'caseResults y)
                                loop x mutable'aggregates v
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "aggregates"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'aggregates y)
                                loop x v mutable'caseResults
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "result_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"resultDigest") y x)
                                  mutable'aggregates mutable'caseResults
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'aggregates mutable'caseResults
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'aggregates <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                      Data.ProtoLens.Encoding.Growing.new
              mutable'caseResults <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       Data.ProtoLens.Encoding.Growing.new
              loop
                Data.ProtoLens.defMessage mutable'aggregates mutable'caseResults)
          "EvaluationResult"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"specDigest") _x
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
                      (Data.ProtoLens.Field.field @"vec'caseResults") _x))
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
                         (Data.ProtoLens.Field.field @"vec'aggregates") _x))
                   ((Data.Monoid.<>)
                      (let
                         _v
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"resultDigest") _x
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
instance Control.DeepSeq.NFData EvaluationResult where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationResult'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationResult'specDigest x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationResult'caseResults x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationResult'aggregates x__)
                      (Control.DeepSeq.deepseq
                         (_EvaluationResult'resultDigest x__) ()))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.candidates' @:: Lens' EvaluationSpec [EvaluationArtifact]@
         * 'Proto.Inference.V1.Inference_Fields.vec'candidates' @:: Lens' EvaluationSpec (Data.Vector.Vector EvaluationArtifact)@
         * 'Proto.Inference.V1.Inference_Fields.suite' @:: Lens' EvaluationSpec EvaluationSuite@
         * 'Proto.Inference.V1.Inference_Fields.maybe'suite' @:: Lens' EvaluationSpec (Prelude.Maybe EvaluationSuite)@
         * 'Proto.Inference.V1.Inference_Fields.grader' @:: Lens' EvaluationSpec EvaluationGrader@
         * 'Proto.Inference.V1.Inference_Fields.maybe'grader' @:: Lens' EvaluationSpec (Prelude.Maybe EvaluationGrader)@
         * 'Proto.Inference.V1.Inference_Fields.metrics' @:: Lens' EvaluationSpec [EvaluationMetric]@
         * 'Proto.Inference.V1.Inference_Fields.vec'metrics' @:: Lens' EvaluationSpec (Data.Vector.Vector EvaluationMetric)@
         * 'Proto.Inference.V1.Inference_Fields.maximumCaseResults' @:: Lens' EvaluationSpec Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.specDigest' @:: Lens' EvaluationSpec Data.ByteString.ByteString@ -}
data EvaluationSpec
  = EvaluationSpec'_constructor {_EvaluationSpec'candidates :: !(Data.Vector.Vector EvaluationArtifact),
                                 _EvaluationSpec'suite :: !(Prelude.Maybe EvaluationSuite),
                                 _EvaluationSpec'grader :: !(Prelude.Maybe EvaluationGrader),
                                 _EvaluationSpec'metrics :: !(Data.Vector.Vector EvaluationMetric),
                                 _EvaluationSpec'maximumCaseResults :: !Data.Word.Word64,
                                 _EvaluationSpec'specDigest :: !Data.ByteString.ByteString,
                                 _EvaluationSpec'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationSpec where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationSpec "candidates" [EvaluationArtifact] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'candidates
           (\ x__ y__ -> x__ {_EvaluationSpec'candidates = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EvaluationSpec "vec'candidates" (Data.Vector.Vector EvaluationArtifact) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'candidates
           (\ x__ y__ -> x__ {_EvaluationSpec'candidates = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSpec "suite" EvaluationSuite where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'suite
           (\ x__ y__ -> x__ {_EvaluationSpec'suite = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationSpec "maybe'suite" (Prelude.Maybe EvaluationSuite) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'suite
           (\ x__ y__ -> x__ {_EvaluationSpec'suite = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSpec "grader" EvaluationGrader where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'grader
           (\ x__ y__ -> x__ {_EvaluationSpec'grader = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationSpec "maybe'grader" (Prelude.Maybe EvaluationGrader) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'grader
           (\ x__ y__ -> x__ {_EvaluationSpec'grader = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSpec "metrics" [EvaluationMetric] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'metrics
           (\ x__ y__ -> x__ {_EvaluationSpec'metrics = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EvaluationSpec "vec'metrics" (Data.Vector.Vector EvaluationMetric) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'metrics
           (\ x__ y__ -> x__ {_EvaluationSpec'metrics = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSpec "maximumCaseResults" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'maximumCaseResults
           (\ x__ y__ -> x__ {_EvaluationSpec'maximumCaseResults = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSpec "specDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSpec'specDigest
           (\ x__ y__ -> x__ {_EvaluationSpec'specDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationSpec where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationSpec"
  packedMessageDescriptor _
    = "\n\
      \\SOEvaluationSpec\DC2T\n\
      \\n\
      \candidates\CAN\SOH \ETX(\v2).inference.customer.v1.EvaluationArtifactR\n\
      \candidatesB\t\232\243\CAN\SOH\240\243\CAN\128\STX\DC2B\n\
      \\ENQsuite\CAN\STX \SOH(\v2&.inference.customer.v1.EvaluationSuiteR\ENQsuiteB\EOT\208\243\CAN\SOH\DC2E\n\
      \\ACKgrader\CAN\ETX \SOH(\v2'.inference.customer.v1.EvaluationGraderR\ACKgraderB\EOT\208\243\CAN\SOH\DC2K\n\
      \\ametrics\CAN\EOT \ETX(\v2'.inference.customer.v1.EvaluationMetricR\ametricsB\b\232\243\CAN\SOH\240\243\CAN@\DC2<\n\
      \\DC4maximum_case_results\CAN\ENQ \SOH(\EOTR\DC2maximumCaseResultsB\n\
      \\216\243\CAN\SOH\128\244\CAN\128\128\EOT\DC2%\n\
      \\vspec_digest\CAN\ACK \SOH(\fR\n\
      \specDigestB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        candidates__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "candidates"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationArtifact)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"candidates")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSpec
        suite__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "suite"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationSuite)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'suite")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSpec
        grader__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "grader"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationGrader)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'grader")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSpec
        metrics__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "metrics"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationMetric)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"metrics")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSpec
        maximumCaseResults__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "maximum_case_results"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maximumCaseResults")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSpec
        specDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "spec_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"specDigest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSpec
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, candidates__field_descriptor),
           (Data.ProtoLens.Tag 2, suite__field_descriptor),
           (Data.ProtoLens.Tag 3, grader__field_descriptor),
           (Data.ProtoLens.Tag 4, metrics__field_descriptor),
           (Data.ProtoLens.Tag 5, maximumCaseResults__field_descriptor),
           (Data.ProtoLens.Tag 6, specDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationSpec'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationSpec'_unknownFields = y__})
  defMessage
    = EvaluationSpec'_constructor
        {_EvaluationSpec'candidates = Data.Vector.Generic.empty,
         _EvaluationSpec'suite = Prelude.Nothing,
         _EvaluationSpec'grader = Prelude.Nothing,
         _EvaluationSpec'metrics = Data.Vector.Generic.empty,
         _EvaluationSpec'maximumCaseResults = Data.ProtoLens.fieldDefault,
         _EvaluationSpec'specDigest = Data.ProtoLens.fieldDefault,
         _EvaluationSpec'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationSpec
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld EvaluationArtifact
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld EvaluationMetric
                -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationSpec
        loop x mutable'candidates mutable'metrics
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'candidates <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                             (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                mutable'candidates)
                      frozen'metrics <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                             mutable'metrics)
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
                              (Data.ProtoLens.Field.field @"vec'candidates") frozen'candidates
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'metrics") frozen'metrics x)))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "candidates"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'candidates y)
                                loop x v mutable'metrics
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "suite"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"suite") y x)
                                  mutable'candidates mutable'metrics
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "grader"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"grader") y x)
                                  mutable'candidates mutable'metrics
                        34
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "metrics"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'metrics y)
                                loop x mutable'candidates v
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "maximum_case_results"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"maximumCaseResults") y x)
                                  mutable'candidates mutable'metrics
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "spec_digest"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"specDigest") y x)
                                  mutable'candidates mutable'metrics
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'candidates mutable'metrics
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'candidates <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                      Data.ProtoLens.Encoding.Growing.new
              mutable'metrics <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                   Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'candidates mutable'metrics)
          "EvaluationSpec"
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
                   (Data.ProtoLens.Field.field @"vec'candidates") _x))
             ((Data.Monoid.<>)
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'suite") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'grader") _x
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
                         (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'metrics") _x))
                      ((Data.Monoid.<>)
                         (let
                            _v
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"maximumCaseResults") _x
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
                                 = Lens.Family2.view (Data.ProtoLens.Field.field @"specDigest") _x
                             in
                               if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                   Data.Monoid.mempty
                               else
                                   (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                                     ((\ bs
                                         -> (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                 (Prelude.fromIntegral (Data.ByteString.length bs)))
                                              (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                        _v))
                            (Data.ProtoLens.Encoding.Wire.buildFieldSet
                               (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))))
instance Control.DeepSeq.NFData EvaluationSpec where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationSpec'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationSpec'candidates x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationSpec'suite x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationSpec'grader x__)
                      (Control.DeepSeq.deepseq
                         (_EvaluationSpec'metrics x__)
                         (Control.DeepSeq.deepseq
                            (_EvaluationSpec'maximumCaseResults x__)
                            (Control.DeepSeq.deepseq (_EvaluationSpec'specDigest x__) ()))))))
newtype EvaluationState'UnrecognizedValue
  = EvaluationState'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data EvaluationState
  = EVALUATION_STATE_UNSPECIFIED |
    EVALUATION_STATE_ADMITTED |
    EVALUATION_STATE_RUNNING |
    EVALUATION_STATE_COMPLETED |
    EVALUATION_STATE_FAILED |
    EVALUATION_STATE_CANCELLED |
    EvaluationState'Unrecognized !EvaluationState'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum EvaluationState where
  maybeToEnum 0 = Prelude.Just EVALUATION_STATE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just EVALUATION_STATE_ADMITTED
  maybeToEnum 2 = Prelude.Just EVALUATION_STATE_RUNNING
  maybeToEnum 3 = Prelude.Just EVALUATION_STATE_COMPLETED
  maybeToEnum 4 = Prelude.Just EVALUATION_STATE_FAILED
  maybeToEnum 5 = Prelude.Just EVALUATION_STATE_CANCELLED
  maybeToEnum k
    = Prelude.Just
        (EvaluationState'Unrecognized
           (EvaluationState'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum EVALUATION_STATE_UNSPECIFIED
    = "EVALUATION_STATE_UNSPECIFIED"
  showEnum EVALUATION_STATE_ADMITTED = "EVALUATION_STATE_ADMITTED"
  showEnum EVALUATION_STATE_RUNNING = "EVALUATION_STATE_RUNNING"
  showEnum EVALUATION_STATE_COMPLETED = "EVALUATION_STATE_COMPLETED"
  showEnum EVALUATION_STATE_FAILED = "EVALUATION_STATE_FAILED"
  showEnum EVALUATION_STATE_CANCELLED = "EVALUATION_STATE_CANCELLED"
  showEnum
    (EvaluationState'Unrecognized (EvaluationState'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "EVALUATION_STATE_UNSPECIFIED"
    = Prelude.Just EVALUATION_STATE_UNSPECIFIED
    | (Prelude.==) k "EVALUATION_STATE_ADMITTED"
    = Prelude.Just EVALUATION_STATE_ADMITTED
    | (Prelude.==) k "EVALUATION_STATE_RUNNING"
    = Prelude.Just EVALUATION_STATE_RUNNING
    | (Prelude.==) k "EVALUATION_STATE_COMPLETED"
    = Prelude.Just EVALUATION_STATE_COMPLETED
    | (Prelude.==) k "EVALUATION_STATE_FAILED"
    = Prelude.Just EVALUATION_STATE_FAILED
    | (Prelude.==) k "EVALUATION_STATE_CANCELLED"
    = Prelude.Just EVALUATION_STATE_CANCELLED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded EvaluationState where
  minBound = EVALUATION_STATE_UNSPECIFIED
  maxBound = EVALUATION_STATE_CANCELLED
instance Prelude.Enum EvaluationState where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum EvaluationState: "
              (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum EVALUATION_STATE_UNSPECIFIED = 0
  fromEnum EVALUATION_STATE_ADMITTED = 1
  fromEnum EVALUATION_STATE_RUNNING = 2
  fromEnum EVALUATION_STATE_COMPLETED = 3
  fromEnum EVALUATION_STATE_FAILED = 4
  fromEnum EVALUATION_STATE_CANCELLED = 5
  fromEnum
    (EvaluationState'Unrecognized (EvaluationState'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ EVALUATION_STATE_CANCELLED
    = Prelude.error
        "EvaluationState.succ: bad argument EVALUATION_STATE_CANCELLED. This value would be out of bounds."
  succ EVALUATION_STATE_UNSPECIFIED = EVALUATION_STATE_ADMITTED
  succ EVALUATION_STATE_ADMITTED = EVALUATION_STATE_RUNNING
  succ EVALUATION_STATE_RUNNING = EVALUATION_STATE_COMPLETED
  succ EVALUATION_STATE_COMPLETED = EVALUATION_STATE_FAILED
  succ EVALUATION_STATE_FAILED = EVALUATION_STATE_CANCELLED
  succ (EvaluationState'Unrecognized _)
    = Prelude.error
        "EvaluationState.succ: bad argument: unrecognized value"
  pred EVALUATION_STATE_UNSPECIFIED
    = Prelude.error
        "EvaluationState.pred: bad argument EVALUATION_STATE_UNSPECIFIED. This value would be out of bounds."
  pred EVALUATION_STATE_ADMITTED = EVALUATION_STATE_UNSPECIFIED
  pred EVALUATION_STATE_RUNNING = EVALUATION_STATE_ADMITTED
  pred EVALUATION_STATE_COMPLETED = EVALUATION_STATE_RUNNING
  pred EVALUATION_STATE_FAILED = EVALUATION_STATE_COMPLETED
  pred EVALUATION_STATE_CANCELLED = EVALUATION_STATE_FAILED
  pred (EvaluationState'Unrecognized _)
    = Prelude.error
        "EvaluationState.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault EvaluationState where
  fieldDefault = EVALUATION_STATE_UNSPECIFIED
instance Control.DeepSeq.NFData EvaluationState where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' EvaluationSuite Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.digest' @:: Lens' EvaluationSuite Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.cases' @:: Lens' EvaluationSuite [EvaluationCase]@
         * 'Proto.Inference.V1.Inference_Fields.vec'cases' @:: Lens' EvaluationSuite (Data.Vector.Vector EvaluationCase)@ -}
data EvaluationSuite
  = EvaluationSuite'_constructor {_EvaluationSuite'identity :: !Data.Text.Text,
                                  _EvaluationSuite'digest :: !Data.ByteString.ByteString,
                                  _EvaluationSuite'cases :: !(Data.Vector.Vector EvaluationCase),
                                  _EvaluationSuite'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationSuite where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationSuite "identity" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSuite'identity
           (\ x__ y__ -> x__ {_EvaluationSuite'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSuite "digest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSuite'digest
           (\ x__ y__ -> x__ {_EvaluationSuite'digest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationSuite "cases" [EvaluationCase] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSuite'cases
           (\ x__ y__ -> x__ {_EvaluationSuite'cases = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField EvaluationSuite "vec'cases" (Data.Vector.Vector EvaluationCase) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationSuite'cases
           (\ x__ y__ -> x__ {_EvaluationSuite'cases = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationSuite where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationSuite"
  packedMessageDescriptor _
    = "\n\
      \\SIEvaluationSuite\DC2!\n\
      \\bidentity\CAN\SOH \SOH(\tR\bidentityB\ENQ\248\243\CAN\128\STX\DC2\FS\n\
      \\ACKdigest\CAN\STX \SOH(\fR\ACKdigestB\EOT\200\243\CAN \DC2F\n\
      \\ENQcases\CAN\ETX \ETX(\v2%.inference.customer.v1.EvaluationCaseR\ENQcasesB\t\232\243\CAN\SOH\240\243\CAN\128 "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"identity")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSuite
        digest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"digest")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSuite
        cases__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "cases"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationCase)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"cases")) ::
              Data.ProtoLens.FieldDescriptor EvaluationSuite
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, digest__field_descriptor),
           (Data.ProtoLens.Tag 3, cases__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationSuite'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationSuite'_unknownFields = y__})
  defMessage
    = EvaluationSuite'_constructor
        {_EvaluationSuite'identity = Data.ProtoLens.fieldDefault,
         _EvaluationSuite'digest = Data.ProtoLens.fieldDefault,
         _EvaluationSuite'cases = Data.Vector.Generic.empty,
         _EvaluationSuite'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationSuite
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld EvaluationCase
             -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationSuite
        loop x mutable'cases
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'cases <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                        (Data.ProtoLens.Encoding.Growing.unsafeFreeze mutable'cases)
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
                              (Data.ProtoLens.Field.field @"vec'cases") frozen'cases x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                                  mutable'cases
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "digest"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"digest") y x)
                                  mutable'cases
                        26
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "cases"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'cases y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'cases
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'cases <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                 Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'cases)
          "EvaluationSuite"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"identity") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"digest") _x
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
                      (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'cases") _x))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData EvaluationSuite where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationSuite'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationSuite'identity x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationSuite'digest x__)
                   (Control.DeepSeq.deepseq (_EvaluationSuite'cases x__) ())))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.evaluationId' @:: Lens' EvaluationView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.spec' @:: Lens' EvaluationView EvaluationSpec@
         * 'Proto.Inference.V1.Inference_Fields.maybe'spec' @:: Lens' EvaluationView (Prelude.Maybe EvaluationSpec)@
         * 'Proto.Inference.V1.Inference_Fields.state' @:: Lens' EvaluationView EvaluationState@
         * 'Proto.Inference.V1.Inference_Fields.result' @:: Lens' EvaluationView EvaluationResult@
         * 'Proto.Inference.V1.Inference_Fields.maybe'result' @:: Lens' EvaluationView (Prelude.Maybe EvaluationResult)@
         * 'Proto.Inference.V1.Inference_Fields.sequence' @:: Lens' EvaluationView Data.Word.Word64@ -}
data EvaluationView
  = EvaluationView'_constructor {_EvaluationView'evaluationId :: !Data.ByteString.ByteString,
                                 _EvaluationView'spec :: !(Prelude.Maybe EvaluationSpec),
                                 _EvaluationView'state :: !EvaluationState,
                                 _EvaluationView'result :: !(Prelude.Maybe EvaluationResult),
                                 _EvaluationView'sequence :: !Data.Word.Word64,
                                 _EvaluationView'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show EvaluationView where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField EvaluationView "evaluationId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'evaluationId
           (\ x__ y__ -> x__ {_EvaluationView'evaluationId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationView "spec" EvaluationSpec where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'spec
           (\ x__ y__ -> x__ {_EvaluationView'spec = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationView "maybe'spec" (Prelude.Maybe EvaluationSpec) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'spec
           (\ x__ y__ -> x__ {_EvaluationView'spec = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationView "state" EvaluationState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'state
           (\ x__ y__ -> x__ {_EvaluationView'state = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationView "result" EvaluationResult where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'result
           (\ x__ y__ -> x__ {_EvaluationView'result = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField EvaluationView "maybe'result" (Prelude.Maybe EvaluationResult) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'result
           (\ x__ y__ -> x__ {_EvaluationView'result = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField EvaluationView "sequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _EvaluationView'sequence
           (\ x__ y__ -> x__ {_EvaluationView'sequence = y__}))
        Prelude.id
instance Data.ProtoLens.Message EvaluationView where
  messageName _
    = Data.Text.pack "inference.customer.v1.EvaluationView"
  packedMessageDescriptor _
    = "\n\
      \\SOEvaluationView\DC2)\n\
      \\revaluation_id\CAN\SOH \SOH(\fR\fevaluationIdB\EOT\200\243\CAN\DLE\DC2?\n\
      \\EOTspec\CAN\STX \SOH(\v2%.inference.customer.v1.EvaluationSpecR\EOTspecB\EOT\208\243\CAN\SOH\DC2B\n\
      \\ENQstate\CAN\ETX \SOH(\SO2&.inference.customer.v1.EvaluationStateR\ENQstateB\EOT\136\244\CAN\SOH\DC2D\n\
      \\ACKresult\CAN\EOT \SOH(\v2'.inference.customer.v1.EvaluationResultH\NULR\ACKresult\136\SOH\SOH\DC2 \n\
      \\bsequence\CAN\ENQ \SOH(\EOTR\bsequenceB\EOT\216\243\CAN\SOHB\t\n\
      \\a_result"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        evaluationId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "evaluation_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"evaluationId")) ::
              Data.ProtoLens.FieldDescriptor EvaluationView
        spec__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "spec"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationSpec)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'spec")) ::
              Data.ProtoLens.FieldDescriptor EvaluationView
        state__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationState)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"state")) ::
              Data.ProtoLens.FieldDescriptor EvaluationView
        result__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "result"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor EvaluationResult)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'result")) ::
              Data.ProtoLens.FieldDescriptor EvaluationView
        sequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sequence")) ::
              Data.ProtoLens.FieldDescriptor EvaluationView
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, evaluationId__field_descriptor),
           (Data.ProtoLens.Tag 2, spec__field_descriptor),
           (Data.ProtoLens.Tag 3, state__field_descriptor),
           (Data.ProtoLens.Tag 4, result__field_descriptor),
           (Data.ProtoLens.Tag 5, sequence__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _EvaluationView'_unknownFields
        (\ x__ y__ -> x__ {_EvaluationView'_unknownFields = y__})
  defMessage
    = EvaluationView'_constructor
        {_EvaluationView'evaluationId = Data.ProtoLens.fieldDefault,
         _EvaluationView'spec = Prelude.Nothing,
         _EvaluationView'state = Data.ProtoLens.fieldDefault,
         _EvaluationView'result = Prelude.Nothing,
         _EvaluationView'sequence = Data.ProtoLens.fieldDefault,
         _EvaluationView'_unknownFields = []}
  parseMessage
    = let
        loop ::
          EvaluationView
          -> Data.ProtoLens.Encoding.Bytes.Parser EvaluationView
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
                                       "evaluation_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"evaluationId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "spec"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"spec") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "state"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"state") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "result"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"result") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "sequence"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"sequence") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "EvaluationView"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"evaluationId") _x
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'spec") _x
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
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'result") _x
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
                            _v = Lens.Family2.view (Data.ProtoLens.Field.field @"sequence") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData EvaluationView where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_EvaluationView'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_EvaluationView'evaluationId x__)
                (Control.DeepSeq.deepseq
                   (_EvaluationView'spec x__)
                   (Control.DeepSeq.deepseq
                      (_EvaluationView'state x__)
                      (Control.DeepSeq.deepseq
                         (_EvaluationView'result x__)
                         (Control.DeepSeq.deepseq (_EvaluationView'sequence x__) ())))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.numerator' @:: Lens' ExactRational Data.Int.Int64@
         * 'Proto.Inference.V1.Inference_Fields.denominator' @:: Lens' ExactRational Data.Word.Word64@ -}
data ExactRational
  = ExactRational'_constructor {_ExactRational'numerator :: !Data.Int.Int64,
                                _ExactRational'denominator :: !Data.Word.Word64,
                                _ExactRational'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ExactRational where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ExactRational "numerator" Data.Int.Int64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ExactRational'numerator
           (\ x__ y__ -> x__ {_ExactRational'numerator = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ExactRational "denominator" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ExactRational'denominator
           (\ x__ y__ -> x__ {_ExactRational'denominator = y__}))
        Prelude.id
instance Data.ProtoLens.Message ExactRational where
  messageName _
    = Data.Text.pack "inference.customer.v1.ExactRational"
  packedMessageDescriptor _
    = "\n\
      \\rExactRational\DC2\FS\n\
      \\tnumerator\CAN\SOH \SOH(\DC2R\tnumerator\DC2&\n\
      \\vdenominator\CAN\STX \SOH(\EOTR\vdenominatorB\EOT\216\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        numerator__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "numerator"
              (Data.ProtoLens.ScalarField Data.ProtoLens.SInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Int.Int64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"numerator")) ::
              Data.ProtoLens.FieldDescriptor ExactRational
        denominator__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "denominator"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"denominator")) ::
              Data.ProtoLens.FieldDescriptor ExactRational
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, numerator__field_descriptor),
           (Data.ProtoLens.Tag 2, denominator__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ExactRational'_unknownFields
        (\ x__ y__ -> x__ {_ExactRational'_unknownFields = y__})
  defMessage
    = ExactRational'_constructor
        {_ExactRational'numerator = Data.ProtoLens.fieldDefault,
         _ExactRational'denominator = Data.ProtoLens.fieldDefault,
         _ExactRational'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ExactRational -> Data.ProtoLens.Encoding.Bytes.Parser ExactRational
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
                                          Data.ProtoLens.Encoding.Bytes.wordToSignedInt64
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "numerator"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"numerator") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "denominator"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"denominator") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ExactRational"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"numerator") _x
              in
                if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                    Data.Monoid.mempty
                else
                    (Data.Monoid.<>)
                      (Data.ProtoLens.Encoding.Bytes.putVarInt 8)
                      ((Prelude..)
                         ((Prelude..)
                            Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral)
                         Data.ProtoLens.Encoding.Bytes.signedInt64ToWord _v))
             ((Data.Monoid.<>)
                (let
                   _v
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"denominator") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ExactRational where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ExactRational'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ExactRational'numerator x__)
                (Control.DeepSeq.deepseq (_ExactRational'denominator x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' GenerateRunRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' GenerateRunRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.context' @:: Lens' GenerateRunRequest Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.input' @:: Lens' GenerateRunRequest Item@
         * 'Proto.Inference.V1.Inference_Fields.maybe'input' @:: Lens' GenerateRunRequest (Prelude.Maybe Item)@
         * 'Proto.Inference.V1.Inference_Fields.maximumOutput' @:: Lens' GenerateRunRequest Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.seed' @:: Lens' GenerateRunRequest Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maybe'seed' @:: Lens' GenerateRunRequest (Prelude.Maybe Data.Word.Word64)@ -}
data GenerateRunRequest
  = GenerateRunRequest'_constructor {_GenerateRunRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                     _GenerateRunRequest'context :: !Data.ByteString.ByteString,
                                     _GenerateRunRequest'input :: !(Prelude.Maybe Item),
                                     _GenerateRunRequest'maximumOutput :: !Data.Word.Word64,
                                     _GenerateRunRequest'seed :: !(Prelude.Maybe Data.Word.Word64),
                                     _GenerateRunRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GenerateRunRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField GenerateRunRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'identity
           (\ x__ y__ -> x__ {_GenerateRunRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GenerateRunRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'identity
           (\ x__ y__ -> x__ {_GenerateRunRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GenerateRunRequest "context" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'context
           (\ x__ y__ -> x__ {_GenerateRunRequest'context = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GenerateRunRequest "input" Item where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'input
           (\ x__ y__ -> x__ {_GenerateRunRequest'input = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GenerateRunRequest "maybe'input" (Prelude.Maybe Item) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'input
           (\ x__ y__ -> x__ {_GenerateRunRequest'input = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GenerateRunRequest "maximumOutput" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'maximumOutput
           (\ x__ y__ -> x__ {_GenerateRunRequest'maximumOutput = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GenerateRunRequest "seed" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'seed
           (\ x__ y__ -> x__ {_GenerateRunRequest'seed = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField GenerateRunRequest "maybe'seed" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunRequest'seed
           (\ x__ y__ -> x__ {_GenerateRunRequest'seed = y__}))
        Prelude.id
instance Data.ProtoLens.Message GenerateRunRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.GenerateRunRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2GenerateRunRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\RS\n\
      \\acontext\CAN\STX \SOH(\fR\acontextB\EOT\200\243\CAN \DC27\n\
      \\ENQinput\CAN\ETX \SOH(\v2\ESC.inference.customer.v1.ItemR\ENQinputB\EOT\208\243\CAN\SOH\DC2+\n\
      \\SOmaximum_output\CAN\EOT \SOH(\EOTR\rmaximumOutputB\EOT\216\243\CAN\SOH\DC2\ETB\n\
      \\EOTseed\CAN\ENQ \SOH(\EOTH\NULR\EOTseed\136\SOH\SOHB\a\n\
      \\ENQ_seed"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor GenerateRunRequest
        context__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "context"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"context")) ::
              Data.ProtoLens.FieldDescriptor GenerateRunRequest
        input__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "input"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Item)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'input")) ::
              Data.ProtoLens.FieldDescriptor GenerateRunRequest
        maximumOutput__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "maximum_output"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maximumOutput")) ::
              Data.ProtoLens.FieldDescriptor GenerateRunRequest
        seed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "seed"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'seed")) ::
              Data.ProtoLens.FieldDescriptor GenerateRunRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, context__field_descriptor),
           (Data.ProtoLens.Tag 3, input__field_descriptor),
           (Data.ProtoLens.Tag 4, maximumOutput__field_descriptor),
           (Data.ProtoLens.Tag 5, seed__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GenerateRunRequest'_unknownFields
        (\ x__ y__ -> x__ {_GenerateRunRequest'_unknownFields = y__})
  defMessage
    = GenerateRunRequest'_constructor
        {_GenerateRunRequest'identity = Prelude.Nothing,
         _GenerateRunRequest'context = Data.ProtoLens.fieldDefault,
         _GenerateRunRequest'input = Prelude.Nothing,
         _GenerateRunRequest'maximumOutput = Data.ProtoLens.fieldDefault,
         _GenerateRunRequest'seed = Prelude.Nothing,
         _GenerateRunRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          GenerateRunRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser GenerateRunRequest
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "context"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"context") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "input"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"input") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "maximum_output"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"maximumOutput") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "seed"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"seed") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "GenerateRunRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"context") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'input") _x
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
                               (Data.ProtoLens.Field.field @"maximumOutput") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'seed") _x
                          of
                            Prelude.Nothing -> Data.Monoid.mempty
                            (Prelude.Just _v)
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         (Data.ProtoLens.Encoding.Wire.buildFieldSet
                            (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))
instance Control.DeepSeq.NFData GenerateRunRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GenerateRunRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_GenerateRunRequest'identity x__)
                (Control.DeepSeq.deepseq
                   (_GenerateRunRequest'context x__)
                   (Control.DeepSeq.deepseq
                      (_GenerateRunRequest'input x__)
                      (Control.DeepSeq.deepseq
                         (_GenerateRunRequest'maximumOutput x__)
                         (Control.DeepSeq.deepseq (_GenerateRunRequest'seed x__) ())))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.run' @:: Lens' GenerateRunResponse RunView@
         * 'Proto.Inference.V1.Inference_Fields.maybe'run' @:: Lens' GenerateRunResponse (Prelude.Maybe RunView)@ -}
data GenerateRunResponse
  = GenerateRunResponse'_constructor {_GenerateRunResponse'run :: !(Prelude.Maybe RunView),
                                      _GenerateRunResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GenerateRunResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField GenerateRunResponse "run" RunView where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunResponse'run
           (\ x__ y__ -> x__ {_GenerateRunResponse'run = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField GenerateRunResponse "maybe'run" (Prelude.Maybe RunView) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerateRunResponse'run
           (\ x__ y__ -> x__ {_GenerateRunResponse'run = y__}))
        Prelude.id
instance Data.ProtoLens.Message GenerateRunResponse where
  messageName _
    = Data.Text.pack "inference.customer.v1.GenerateRunResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC3GenerateRunResponse\DC26\n\
      \\ETXrun\CAN\SOH \SOH(\v2\RS.inference.customer.v1.RunViewR\ETXrunB\EOT\208\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        run__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RunView)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'run")) ::
              Data.ProtoLens.FieldDescriptor GenerateRunResponse
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, run__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GenerateRunResponse'_unknownFields
        (\ x__ y__ -> x__ {_GenerateRunResponse'_unknownFields = y__})
  defMessage
    = GenerateRunResponse'_constructor
        {_GenerateRunResponse'run = Prelude.Nothing,
         _GenerateRunResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          GenerateRunResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser GenerateRunResponse
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
                                       "run"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"run") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "GenerateRunResponse"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'run") _x
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
instance Control.DeepSeq.NFData GenerateRunResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GenerateRunResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_GenerateRunResponse'run x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.runId' @:: Lens' GenerationProvenance Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.terminalReceiptDigest' @:: Lens' GenerationProvenance Data.ByteString.ByteString@ -}
data GenerationProvenance
  = GenerationProvenance'_constructor {_GenerationProvenance'runId :: !Data.ByteString.ByteString,
                                       _GenerationProvenance'terminalReceiptDigest :: !Data.ByteString.ByteString,
                                       _GenerationProvenance'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show GenerationProvenance where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField GenerationProvenance "runId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerationProvenance'runId
           (\ x__ y__ -> x__ {_GenerationProvenance'runId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField GenerationProvenance "terminalReceiptDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _GenerationProvenance'terminalReceiptDigest
           (\ x__ y__
              -> x__ {_GenerationProvenance'terminalReceiptDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message GenerationProvenance where
  messageName _
    = Data.Text.pack "inference.customer.v1.GenerationProvenance"
  packedMessageDescriptor _
    = "\n\
      \\DC4GenerationProvenance\DC2\ESC\n\
      \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2<\n\
      \\ETBterminal_receipt_digest\CAN\STX \SOH(\fR\NAKterminalReceiptDigestB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        runId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"runId")) ::
              Data.ProtoLens.FieldDescriptor GenerationProvenance
        terminalReceiptDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "terminal_receipt_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"terminalReceiptDigest")) ::
              Data.ProtoLens.FieldDescriptor GenerationProvenance
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, runId__field_descriptor),
           (Data.ProtoLens.Tag 2, terminalReceiptDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _GenerationProvenance'_unknownFields
        (\ x__ y__ -> x__ {_GenerationProvenance'_unknownFields = y__})
  defMessage
    = GenerationProvenance'_constructor
        {_GenerationProvenance'runId = Data.ProtoLens.fieldDefault,
         _GenerationProvenance'terminalReceiptDigest = Data.ProtoLens.fieldDefault,
         _GenerationProvenance'_unknownFields = []}
  parseMessage
    = let
        loop ::
          GenerationProvenance
          -> Data.ProtoLens.Encoding.Bytes.Parser GenerationProvenance
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
                                       "run_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"runId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "terminal_receipt_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"terminalReceiptDigest") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "GenerationProvenance"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"runId") _x
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
                         (Data.ProtoLens.Field.field @"terminalReceiptDigest") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData GenerationProvenance where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_GenerationProvenance'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_GenerationProvenance'runId x__)
                (Control.DeepSeq.deepseq
                   (_GenerationProvenance'terminalReceiptDigest x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.profile' @:: Lens' IdleKvPolicy Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.idleTimeoutMs' @:: Lens' IdleKvPolicy Data.Word.Word64@ -}
data IdleKvPolicy
  = IdleKvPolicy'_constructor {_IdleKvPolicy'profile :: !Data.ByteString.ByteString,
                               _IdleKvPolicy'idleTimeoutMs :: !Data.Word.Word64,
                               _IdleKvPolicy'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show IdleKvPolicy where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField IdleKvPolicy "profile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvPolicy'profile
           (\ x__ y__ -> x__ {_IdleKvPolicy'profile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdleKvPolicy "idleTimeoutMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvPolicy'idleTimeoutMs
           (\ x__ y__ -> x__ {_IdleKvPolicy'idleTimeoutMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message IdleKvPolicy where
  messageName _ = Data.Text.pack "inference.customer.v1.IdleKvPolicy"
  packedMessageDescriptor _
    = "\n\
      \\fIdleKvPolicy\DC2\RS\n\
      \\aprofile\CAN\SOH \SOH(\fR\aprofileB\EOT\200\243\CAN \DC2,\n\
      \\SIidle_timeout_ms\CAN\STX \SOH(\EOTR\ridleTimeoutMsB\EOT\216\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        profile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"profile")) ::
              Data.ProtoLens.FieldDescriptor IdleKvPolicy
        idleTimeoutMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idle_timeout_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"idleTimeoutMs")) ::
              Data.ProtoLens.FieldDescriptor IdleKvPolicy
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, profile__field_descriptor),
           (Data.ProtoLens.Tag 2, idleTimeoutMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _IdleKvPolicy'_unknownFields
        (\ x__ y__ -> x__ {_IdleKvPolicy'_unknownFields = y__})
  defMessage
    = IdleKvPolicy'_constructor
        {_IdleKvPolicy'profile = Data.ProtoLens.fieldDefault,
         _IdleKvPolicy'idleTimeoutMs = Data.ProtoLens.fieldDefault,
         _IdleKvPolicy'_unknownFields = []}
  parseMessage
    = let
        loop ::
          IdleKvPolicy -> Data.ProtoLens.Encoding.Bytes.Parser IdleKvPolicy
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
                                       "profile"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"profile") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "idle_timeout_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idleTimeoutMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "IdleKvPolicy"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"profile") _x
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
                         (Data.ProtoLens.Field.field @"idleTimeoutMs") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData IdleKvPolicy where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_IdleKvPolicy'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_IdleKvPolicy'profile x__)
                (Control.DeepSeq.deepseq (_IdleKvPolicy'idleTimeoutMs x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.policy' @:: Lens' IdleKvRetention IdleKvPolicy@
         * 'Proto.Inference.V1.Inference_Fields.maybe'policy' @:: Lens' IdleKvRetention (Prelude.Maybe IdleKvPolicy)@
         * 'Proto.Inference.V1.Inference_Fields.retainedAtMs' @:: Lens' IdleKvRetention Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.lastUsedAtMs' @:: Lens' IdleKvRetention Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maybe'lastUsedAtMs' @:: Lens' IdleKvRetention (Prelude.Maybe Data.Word.Word64)@
         * 'Proto.Inference.V1.Inference_Fields.lastRunId' @:: Lens' IdleKvRetention Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maybe'lastRunId' @:: Lens' IdleKvRetention (Prelude.Maybe Data.ByteString.ByteString)@ -}
data IdleKvRetention
  = IdleKvRetention'_constructor {_IdleKvRetention'policy :: !(Prelude.Maybe IdleKvPolicy),
                                  _IdleKvRetention'retainedAtMs :: !Data.Word.Word64,
                                  _IdleKvRetention'lastUsedAtMs :: !(Prelude.Maybe Data.Word.Word64),
                                  _IdleKvRetention'lastRunId :: !(Prelude.Maybe Data.ByteString.ByteString),
                                  _IdleKvRetention'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show IdleKvRetention where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField IdleKvRetention "policy" IdleKvPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'policy
           (\ x__ y__ -> x__ {_IdleKvRetention'policy = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField IdleKvRetention "maybe'policy" (Prelude.Maybe IdleKvPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'policy
           (\ x__ y__ -> x__ {_IdleKvRetention'policy = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdleKvRetention "retainedAtMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'retainedAtMs
           (\ x__ y__ -> x__ {_IdleKvRetention'retainedAtMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdleKvRetention "lastUsedAtMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'lastUsedAtMs
           (\ x__ y__ -> x__ {_IdleKvRetention'lastUsedAtMs = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField IdleKvRetention "maybe'lastUsedAtMs" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'lastUsedAtMs
           (\ x__ y__ -> x__ {_IdleKvRetention'lastUsedAtMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField IdleKvRetention "lastRunId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'lastRunId
           (\ x__ y__ -> x__ {_IdleKvRetention'lastRunId = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField IdleKvRetention "maybe'lastRunId" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _IdleKvRetention'lastRunId
           (\ x__ y__ -> x__ {_IdleKvRetention'lastRunId = y__}))
        Prelude.id
instance Data.ProtoLens.Message IdleKvRetention where
  messageName _
    = Data.Text.pack "inference.customer.v1.IdleKvRetention"
  packedMessageDescriptor _
    = "\n\
      \\SIIdleKvRetention\DC2A\n\
      \\ACKpolicy\CAN\SOH \SOH(\v2#.inference.customer.v1.IdleKvPolicyR\ACKpolicyB\EOT\208\243\CAN\SOH\DC2*\n\
      \\SOretained_at_ms\CAN\STX \SOH(\EOTR\fretainedAtMsB\EOT\216\243\CAN\SOH\DC20\n\
      \\SIlast_used_at_ms\CAN\ETX \SOH(\EOTH\NULR\flastUsedAtMsB\EOT\216\243\CAN\SOH\136\SOH\SOH\DC2)\n\
      \\vlast_run_id\CAN\EOT \SOH(\fH\SOHR\tlastRunIdB\EOT\200\243\CAN\DLE\136\SOH\SOHB\DC2\n\
      \\DLE_last_used_at_msB\SO\n\
      \\f_last_run_id"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        policy__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "policy"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdleKvPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'policy")) ::
              Data.ProtoLens.FieldDescriptor IdleKvRetention
        retainedAtMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "retained_at_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"retainedAtMs")) ::
              Data.ProtoLens.FieldDescriptor IdleKvRetention
        lastUsedAtMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "last_used_at_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'lastUsedAtMs")) ::
              Data.ProtoLens.FieldDescriptor IdleKvRetention
        lastRunId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "last_run_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'lastRunId")) ::
              Data.ProtoLens.FieldDescriptor IdleKvRetention
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, policy__field_descriptor),
           (Data.ProtoLens.Tag 2, retainedAtMs__field_descriptor),
           (Data.ProtoLens.Tag 3, lastUsedAtMs__field_descriptor),
           (Data.ProtoLens.Tag 4, lastRunId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _IdleKvRetention'_unknownFields
        (\ x__ y__ -> x__ {_IdleKvRetention'_unknownFields = y__})
  defMessage
    = IdleKvRetention'_constructor
        {_IdleKvRetention'policy = Prelude.Nothing,
         _IdleKvRetention'retainedAtMs = Data.ProtoLens.fieldDefault,
         _IdleKvRetention'lastUsedAtMs = Prelude.Nothing,
         _IdleKvRetention'lastRunId = Prelude.Nothing,
         _IdleKvRetention'_unknownFields = []}
  parseMessage
    = let
        loop ::
          IdleKvRetention
          -> Data.ProtoLens.Encoding.Bytes.Parser IdleKvRetention
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
                                       "policy"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"policy") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "retained_at_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"retainedAtMs") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "last_used_at_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"lastUsedAtMs") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "last_run_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"lastRunId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "IdleKvRetention"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'policy") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"retainedAtMs") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                ((Data.Monoid.<>)
                   (case
                        Lens.Family2.view
                          (Data.ProtoLens.Field.field @"maybe'lastUsedAtMs") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just _v)
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'lastRunId") _x
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
instance Control.DeepSeq.NFData IdleKvRetention where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_IdleKvRetention'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_IdleKvRetention'policy x__)
                (Control.DeepSeq.deepseq
                   (_IdleKvRetention'retainedAtMs x__)
                   (Control.DeepSeq.deepseq
                      (_IdleKvRetention'lastUsedAtMs x__)
                      (Control.DeepSeq.deepseq (_IdleKvRetention'lastRunId x__) ()))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.target' @:: Lens' Insert Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.item' @:: Lens' Insert Item@
         * 'Proto.Inference.V1.Inference_Fields.maybe'item' @:: Lens' Insert (Prelude.Maybe Item)@ -}
data Insert
  = Insert'_constructor {_Insert'target :: !Data.ByteString.ByteString,
                         _Insert'item :: !(Prelude.Maybe Item),
                         _Insert'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Insert where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Insert "target" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Insert'target (\ x__ y__ -> x__ {_Insert'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Insert "item" Item where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Insert'item (\ x__ y__ -> x__ {_Insert'item = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField Insert "maybe'item" (Prelude.Maybe Item) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Insert'item (\ x__ y__ -> x__ {_Insert'item = y__}))
        Prelude.id
instance Data.ProtoLens.Message Insert where
  messageName _ = Data.Text.pack "inference.customer.v1.Insert"
  packedMessageDescriptor _
    = "\n\
      \\ACKInsert\DC2\SYN\n\
      \\ACKtarget\CAN\SOH \SOH(\fR\ACKtarget\DC2/\n\
      \\EOTitem\CAN\STX \SOH(\v2\ESC.inference.customer.v1.ItemR\EOTitem"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        target__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "target"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"target")) ::
              Data.ProtoLens.FieldDescriptor Insert
        item__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "item"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Item)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'item")) ::
              Data.ProtoLens.FieldDescriptor Insert
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, target__field_descriptor),
           (Data.ProtoLens.Tag 2, item__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Insert'_unknownFields
        (\ x__ y__ -> x__ {_Insert'_unknownFields = y__})
  defMessage
    = Insert'_constructor
        {_Insert'target = Data.ProtoLens.fieldDefault,
         _Insert'item = Prelude.Nothing, _Insert'_unknownFields = []}
  parseMessage
    = let
        loop :: Insert -> Data.ProtoLens.Encoding.Bytes.Parser Insert
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
                                       "target"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"target") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "item"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"item") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Insert"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"target") _x
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'item") _x
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
instance Control.DeepSeq.NFData Insert where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Insert'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Insert'target x__)
                (Control.DeepSeq.deepseq (_Insert'item x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.revision' @:: Lens' InspectContextRequest Data.ByteString.ByteString@ -}
data InspectContextRequest
  = InspectContextRequest'_constructor {_InspectContextRequest'revision :: !Data.ByteString.ByteString,
                                        _InspectContextRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectContextRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectContextRequest "revision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectContextRequest'revision
           (\ x__ y__ -> x__ {_InspectContextRequest'revision = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectContextRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.InspectContextRequest"
  packedMessageDescriptor _
    = "\n\
      \\NAKInspectContextRequest\DC2 \n\
      \\brevision\CAN\SOH \SOH(\fR\brevisionB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        revision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"revision")) ::
              Data.ProtoLens.FieldDescriptor InspectContextRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, revision__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectContextRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectContextRequest'_unknownFields = y__})
  defMessage
    = InspectContextRequest'_constructor
        {_InspectContextRequest'revision = Data.ProtoLens.fieldDefault,
         _InspectContextRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectContextRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectContextRequest
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
                                       "revision"
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
          (do loop Data.ProtoLens.defMessage) "InspectContextRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"revision") _x
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
instance Control.DeepSeq.NFData InspectContextRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectContextRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectContextRequest'revision x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.evaluationId' @:: Lens' InspectEvaluationRequest Data.ByteString.ByteString@ -}
data InspectEvaluationRequest
  = InspectEvaluationRequest'_constructor {_InspectEvaluationRequest'evaluationId :: !Data.ByteString.ByteString,
                                           _InspectEvaluationRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectEvaluationRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectEvaluationRequest "evaluationId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectEvaluationRequest'evaluationId
           (\ x__ y__ -> x__ {_InspectEvaluationRequest'evaluationId = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectEvaluationRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.InspectEvaluationRequest"
  packedMessageDescriptor _
    = "\n\
      \\CANInspectEvaluationRequest\DC2)\n\
      \\revaluation_id\CAN\SOH \SOH(\fR\fevaluationIdB\EOT\200\243\CAN\DLE"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        evaluationId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "evaluation_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"evaluationId")) ::
              Data.ProtoLens.FieldDescriptor InspectEvaluationRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, evaluationId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectEvaluationRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectEvaluationRequest'_unknownFields = y__})
  defMessage
    = InspectEvaluationRequest'_constructor
        {_InspectEvaluationRequest'evaluationId = Data.ProtoLens.fieldDefault,
         _InspectEvaluationRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectEvaluationRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectEvaluationRequest
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
                                       "evaluation_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"evaluationId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectEvaluationRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"evaluationId") _x
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
instance Control.DeepSeq.NFData InspectEvaluationRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectEvaluationRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_InspectEvaluationRequest'evaluationId x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.runId' @:: Lens' InspectRunRequest Data.ByteString.ByteString@ -}
data InspectRunRequest
  = InspectRunRequest'_constructor {_InspectRunRequest'runId :: !Data.ByteString.ByteString,
                                    _InspectRunRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectRunRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectRunRequest "runId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectRunRequest'runId
           (\ x__ y__ -> x__ {_InspectRunRequest'runId = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectRunRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.InspectRunRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1InspectRunRequest\DC2\ESC\n\
      \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        runId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"runId")) ::
              Data.ProtoLens.FieldDescriptor InspectRunRequest
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, runId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectRunRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectRunRequest'_unknownFields = y__})
  defMessage
    = InspectRunRequest'_constructor
        {_InspectRunRequest'runId = Data.ProtoLens.fieldDefault,
         _InspectRunRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectRunRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectRunRequest
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
                                       "run_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"runId") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectRunRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"runId") _x
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
instance Control.DeepSeq.NFData InspectRunRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectRunRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectRunRequest'runId x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.commitment' @:: Lens' InspectWarmRequest Data.ByteString.ByteString@ -}
data InspectWarmRequest
  = InspectWarmRequest'_constructor {_InspectWarmRequest'commitment :: !Data.ByteString.ByteString,
                                     _InspectWarmRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show InspectWarmRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField InspectWarmRequest "commitment" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _InspectWarmRequest'commitment
           (\ x__ y__ -> x__ {_InspectWarmRequest'commitment = y__}))
        Prelude.id
instance Data.ProtoLens.Message InspectWarmRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.InspectWarmRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2InspectWarmRequest\DC2$\n\
      \\n\
      \commitment\CAN\SOH \SOH(\fR\n\
      \commitmentB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        commitment__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commitment"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitment")) ::
              Data.ProtoLens.FieldDescriptor InspectWarmRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, commitment__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _InspectWarmRequest'_unknownFields
        (\ x__ y__ -> x__ {_InspectWarmRequest'_unknownFields = y__})
  defMessage
    = InspectWarmRequest'_constructor
        {_InspectWarmRequest'commitment = Data.ProtoLens.fieldDefault,
         _InspectWarmRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          InspectWarmRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser InspectWarmRequest
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
                                       "commitment"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitment") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "InspectWarmRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"commitment") _x
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
instance Control.DeepSeq.NFData InspectWarmRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_InspectWarmRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq (_InspectWarmRequest'commitment x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.id' @:: Lens' Item Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.kind' @:: Lens' Item ItemKind@
         * 'Proto.Inference.V1.Inference_Fields.payload' @:: Lens' Item Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.link' @:: Lens' Item Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.continuationProfile' @:: Lens' Item Data.ByteString.ByteString@ -}
data Item
  = Item'_constructor {_Item'id :: !Data.ByteString.ByteString,
                       _Item'kind :: !ItemKind,
                       _Item'payload :: !Data.ByteString.ByteString,
                       _Item'link :: !Data.ByteString.ByteString,
                       _Item'continuationProfile :: !Data.ByteString.ByteString,
                       _Item'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Item where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Item "id" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Item'id (\ x__ y__ -> x__ {_Item'id = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Item "kind" ItemKind where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Item'kind (\ x__ y__ -> x__ {_Item'kind = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Item "payload" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Item'payload (\ x__ y__ -> x__ {_Item'payload = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Item "link" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Item'link (\ x__ y__ -> x__ {_Item'link = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Item "continuationProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Item'continuationProfile
           (\ x__ y__ -> x__ {_Item'continuationProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Message Item where
  messageName _ = Data.Text.pack "inference.customer.v1.Item"
  packedMessageDescriptor _
    = "\n\
      \\EOTItem\DC2\SO\n\
      \\STXid\CAN\SOH \SOH(\fR\STXid\DC23\n\
      \\EOTkind\CAN\STX \SOH(\SO2\US.inference.customer.v1.ItemKindR\EOTkind\DC2\CAN\n\
      \\apayload\CAN\ETX \SOH(\fR\apayload\DC2\DC2\n\
      \\EOTlink\CAN\EOT \SOH(\fR\EOTlink\DC21\n\
      \\DC4continuation_profile\CAN\ENQ \SOH(\fR\DC3continuationProfile"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        id__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"id")) ::
              Data.ProtoLens.FieldDescriptor Item
        kind__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "kind"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor ItemKind)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"kind")) ::
              Data.ProtoLens.FieldDescriptor Item
        payload__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "payload"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"payload")) ::
              Data.ProtoLens.FieldDescriptor Item
        link__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "link"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"link")) ::
              Data.ProtoLens.FieldDescriptor Item
        continuationProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "continuation_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"continuationProfile")) ::
              Data.ProtoLens.FieldDescriptor Item
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, id__field_descriptor),
           (Data.ProtoLens.Tag 2, kind__field_descriptor),
           (Data.ProtoLens.Tag 3, payload__field_descriptor),
           (Data.ProtoLens.Tag 4, link__field_descriptor),
           (Data.ProtoLens.Tag 5, continuationProfile__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Item'_unknownFields
        (\ x__ y__ -> x__ {_Item'_unknownFields = y__})
  defMessage
    = Item'_constructor
        {_Item'id = Data.ProtoLens.fieldDefault,
         _Item'kind = Data.ProtoLens.fieldDefault,
         _Item'payload = Data.ProtoLens.fieldDefault,
         _Item'link = Data.ProtoLens.fieldDefault,
         _Item'continuationProfile = Data.ProtoLens.fieldDefault,
         _Item'_unknownFields = []}
  parseMessage
    = let
        loop :: Item -> Data.ProtoLens.Encoding.Bytes.Parser Item
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
                                       "id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"id") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "kind"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"kind") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "payload"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"payload") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "link"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"link") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "continuation_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"continuationProfile") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Item"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"id") _x
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
                (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"kind") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"payload") _x
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
                      (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"link") _x
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
                         (let
                            _v
                              = Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"continuationProfile") _x
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
instance Control.DeepSeq.NFData Item where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Item'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Item'id x__)
                (Control.DeepSeq.deepseq
                   (_Item'kind x__)
                   (Control.DeepSeq.deepseq
                      (_Item'payload x__)
                      (Control.DeepSeq.deepseq
                         (_Item'link x__)
                         (Control.DeepSeq.deepseq (_Item'continuationProfile x__) ())))))
newtype ItemKind'UnrecognizedValue
  = ItemKind'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data ItemKind
  = ITEM_KIND_UNSPECIFIED |
    ITEM_KIND_INSTRUCTION |
    ITEM_KIND_SYSTEM |
    ITEM_KIND_DEVELOPER |
    ITEM_KIND_USER |
    ITEM_KIND_ASSISTANT |
    ITEM_KIND_TOOL_DEFINITION |
    ITEM_KIND_TOOL_CALL |
    ITEM_KIND_TOOL_RESULT |
    ITEM_KIND_IMAGE |
    ITEM_KIND_AUDIO |
    ITEM_KIND_FILE |
    ITEM_KIND_CONTINUATION |
    ItemKind'Unrecognized !ItemKind'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum ItemKind where
  maybeToEnum 0 = Prelude.Just ITEM_KIND_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just ITEM_KIND_INSTRUCTION
  maybeToEnum 2 = Prelude.Just ITEM_KIND_SYSTEM
  maybeToEnum 3 = Prelude.Just ITEM_KIND_DEVELOPER
  maybeToEnum 4 = Prelude.Just ITEM_KIND_USER
  maybeToEnum 5 = Prelude.Just ITEM_KIND_ASSISTANT
  maybeToEnum 6 = Prelude.Just ITEM_KIND_TOOL_DEFINITION
  maybeToEnum 7 = Prelude.Just ITEM_KIND_TOOL_CALL
  maybeToEnum 8 = Prelude.Just ITEM_KIND_TOOL_RESULT
  maybeToEnum 9 = Prelude.Just ITEM_KIND_IMAGE
  maybeToEnum 10 = Prelude.Just ITEM_KIND_AUDIO
  maybeToEnum 11 = Prelude.Just ITEM_KIND_FILE
  maybeToEnum 12 = Prelude.Just ITEM_KIND_CONTINUATION
  maybeToEnum k
    = Prelude.Just
        (ItemKind'Unrecognized
           (ItemKind'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum ITEM_KIND_UNSPECIFIED = "ITEM_KIND_UNSPECIFIED"
  showEnum ITEM_KIND_INSTRUCTION = "ITEM_KIND_INSTRUCTION"
  showEnum ITEM_KIND_SYSTEM = "ITEM_KIND_SYSTEM"
  showEnum ITEM_KIND_DEVELOPER = "ITEM_KIND_DEVELOPER"
  showEnum ITEM_KIND_USER = "ITEM_KIND_USER"
  showEnum ITEM_KIND_ASSISTANT = "ITEM_KIND_ASSISTANT"
  showEnum ITEM_KIND_TOOL_DEFINITION = "ITEM_KIND_TOOL_DEFINITION"
  showEnum ITEM_KIND_TOOL_CALL = "ITEM_KIND_TOOL_CALL"
  showEnum ITEM_KIND_TOOL_RESULT = "ITEM_KIND_TOOL_RESULT"
  showEnum ITEM_KIND_IMAGE = "ITEM_KIND_IMAGE"
  showEnum ITEM_KIND_AUDIO = "ITEM_KIND_AUDIO"
  showEnum ITEM_KIND_FILE = "ITEM_KIND_FILE"
  showEnum ITEM_KIND_CONTINUATION = "ITEM_KIND_CONTINUATION"
  showEnum (ItemKind'Unrecognized (ItemKind'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "ITEM_KIND_UNSPECIFIED"
    = Prelude.Just ITEM_KIND_UNSPECIFIED
    | (Prelude.==) k "ITEM_KIND_INSTRUCTION"
    = Prelude.Just ITEM_KIND_INSTRUCTION
    | (Prelude.==) k "ITEM_KIND_SYSTEM" = Prelude.Just ITEM_KIND_SYSTEM
    | (Prelude.==) k "ITEM_KIND_DEVELOPER"
    = Prelude.Just ITEM_KIND_DEVELOPER
    | (Prelude.==) k "ITEM_KIND_USER" = Prelude.Just ITEM_KIND_USER
    | (Prelude.==) k "ITEM_KIND_ASSISTANT"
    = Prelude.Just ITEM_KIND_ASSISTANT
    | (Prelude.==) k "ITEM_KIND_TOOL_DEFINITION"
    = Prelude.Just ITEM_KIND_TOOL_DEFINITION
    | (Prelude.==) k "ITEM_KIND_TOOL_CALL"
    = Prelude.Just ITEM_KIND_TOOL_CALL
    | (Prelude.==) k "ITEM_KIND_TOOL_RESULT"
    = Prelude.Just ITEM_KIND_TOOL_RESULT
    | (Prelude.==) k "ITEM_KIND_IMAGE" = Prelude.Just ITEM_KIND_IMAGE
    | (Prelude.==) k "ITEM_KIND_AUDIO" = Prelude.Just ITEM_KIND_AUDIO
    | (Prelude.==) k "ITEM_KIND_FILE" = Prelude.Just ITEM_KIND_FILE
    | (Prelude.==) k "ITEM_KIND_CONTINUATION"
    = Prelude.Just ITEM_KIND_CONTINUATION
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded ItemKind where
  minBound = ITEM_KIND_UNSPECIFIED
  maxBound = ITEM_KIND_CONTINUATION
instance Prelude.Enum ItemKind where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum ItemKind: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum ITEM_KIND_UNSPECIFIED = 0
  fromEnum ITEM_KIND_INSTRUCTION = 1
  fromEnum ITEM_KIND_SYSTEM = 2
  fromEnum ITEM_KIND_DEVELOPER = 3
  fromEnum ITEM_KIND_USER = 4
  fromEnum ITEM_KIND_ASSISTANT = 5
  fromEnum ITEM_KIND_TOOL_DEFINITION = 6
  fromEnum ITEM_KIND_TOOL_CALL = 7
  fromEnum ITEM_KIND_TOOL_RESULT = 8
  fromEnum ITEM_KIND_IMAGE = 9
  fromEnum ITEM_KIND_AUDIO = 10
  fromEnum ITEM_KIND_FILE = 11
  fromEnum ITEM_KIND_CONTINUATION = 12
  fromEnum (ItemKind'Unrecognized (ItemKind'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ ITEM_KIND_CONTINUATION
    = Prelude.error
        "ItemKind.succ: bad argument ITEM_KIND_CONTINUATION. This value would be out of bounds."
  succ ITEM_KIND_UNSPECIFIED = ITEM_KIND_INSTRUCTION
  succ ITEM_KIND_INSTRUCTION = ITEM_KIND_SYSTEM
  succ ITEM_KIND_SYSTEM = ITEM_KIND_DEVELOPER
  succ ITEM_KIND_DEVELOPER = ITEM_KIND_USER
  succ ITEM_KIND_USER = ITEM_KIND_ASSISTANT
  succ ITEM_KIND_ASSISTANT = ITEM_KIND_TOOL_DEFINITION
  succ ITEM_KIND_TOOL_DEFINITION = ITEM_KIND_TOOL_CALL
  succ ITEM_KIND_TOOL_CALL = ITEM_KIND_TOOL_RESULT
  succ ITEM_KIND_TOOL_RESULT = ITEM_KIND_IMAGE
  succ ITEM_KIND_IMAGE = ITEM_KIND_AUDIO
  succ ITEM_KIND_AUDIO = ITEM_KIND_FILE
  succ ITEM_KIND_FILE = ITEM_KIND_CONTINUATION
  succ (ItemKind'Unrecognized _)
    = Prelude.error "ItemKind.succ: bad argument: unrecognized value"
  pred ITEM_KIND_UNSPECIFIED
    = Prelude.error
        "ItemKind.pred: bad argument ITEM_KIND_UNSPECIFIED. This value would be out of bounds."
  pred ITEM_KIND_INSTRUCTION = ITEM_KIND_UNSPECIFIED
  pred ITEM_KIND_SYSTEM = ITEM_KIND_INSTRUCTION
  pred ITEM_KIND_DEVELOPER = ITEM_KIND_SYSTEM
  pred ITEM_KIND_USER = ITEM_KIND_DEVELOPER
  pred ITEM_KIND_ASSISTANT = ITEM_KIND_USER
  pred ITEM_KIND_TOOL_DEFINITION = ITEM_KIND_ASSISTANT
  pred ITEM_KIND_TOOL_CALL = ITEM_KIND_TOOL_DEFINITION
  pred ITEM_KIND_TOOL_RESULT = ITEM_KIND_TOOL_CALL
  pred ITEM_KIND_IMAGE = ITEM_KIND_TOOL_RESULT
  pred ITEM_KIND_AUDIO = ITEM_KIND_IMAGE
  pred ITEM_KIND_FILE = ITEM_KIND_AUDIO
  pred ITEM_KIND_CONTINUATION = ITEM_KIND_FILE
  pred (ItemKind'Unrecognized _)
    = Prelude.error "ItemKind.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault ItemKind where
  fieldDefault = ITEM_KIND_UNSPECIFIED
instance Control.DeepSeq.NFData ItemKind where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
      -}
data ListModelsRequest
  = ListModelsRequest'_constructor {_ListModelsRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListModelsRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Message ListModelsRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.ListModelsRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1ListModelsRequest"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag = let in Data.Map.fromList []
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListModelsRequest'_unknownFields
        (\ x__ y__ -> x__ {_ListModelsRequest'_unknownFields = y__})
  defMessage
    = ListModelsRequest'_constructor
        {_ListModelsRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListModelsRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ListModelsRequest
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
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ListModelsRequest"
  buildMessage
    = \ _x
        -> Data.ProtoLens.Encoding.Wire.buildFieldSet
             (Lens.Family2.view Data.ProtoLens.unknownFields _x)
instance Control.DeepSeq.NFData ListModelsRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListModelsRequest'_unknownFields x__) ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.models' @:: Lens' ListModelsResponse [ModelCapability]@
         * 'Proto.Inference.V1.Inference_Fields.vec'models' @:: Lens' ListModelsResponse (Data.Vector.Vector ModelCapability)@ -}
data ListModelsResponse
  = ListModelsResponse'_constructor {_ListModelsResponse'models :: !(Data.Vector.Vector ModelCapability),
                                     _ListModelsResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ListModelsResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ListModelsResponse "models" [ModelCapability] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListModelsResponse'models
           (\ x__ y__ -> x__ {_ListModelsResponse'models = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ListModelsResponse "vec'models" (Data.Vector.Vector ModelCapability) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ListModelsResponse'models
           (\ x__ y__ -> x__ {_ListModelsResponse'models = y__}))
        Prelude.id
instance Data.ProtoLens.Message ListModelsResponse where
  messageName _
    = Data.Text.pack "inference.customer.v1.ListModelsResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC2ListModelsResponse\DC2I\n\
      \\ACKmodels\CAN\SOH \ETX(\v2&.inference.customer.v1.ModelCapabilityR\ACKmodelsB\t\232\243\CAN\SOH\240\243\CAN\128 "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        models__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "models"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ModelCapability)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked (Data.ProtoLens.Field.field @"models")) ::
              Data.ProtoLens.FieldDescriptor ListModelsResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, models__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ListModelsResponse'_unknownFields
        (\ x__ y__ -> x__ {_ListModelsResponse'_unknownFields = y__})
  defMessage
    = ListModelsResponse'_constructor
        {_ListModelsResponse'models = Data.Vector.Generic.empty,
         _ListModelsResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ListModelsResponse
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld ModelCapability
             -> Data.ProtoLens.Encoding.Bytes.Parser ListModelsResponse
        loop x mutable'models
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'models <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                         (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                            mutable'models)
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
                              (Data.ProtoLens.Field.field @"vec'models") frozen'models x))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "models"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'models y)
                                loop x v
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'models
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'models <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                  Data.ProtoLens.Encoding.Growing.new
              loop Data.ProtoLens.defMessage mutable'models)
          "ListModelsResponse"
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
                (Lens.Family2.view (Data.ProtoLens.Field.field @"vec'models") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ListModelsResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ListModelsResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ListModelsResponse'models x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.newPrefill' @:: Lens' LogicalUsage Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.generatedOutput' @:: Lens' LogicalUsage Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.effectiveContextReads' @:: Lens' LogicalUsage Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.retainedByteMillis' @:: Lens' LogicalUsage Data.Word.Word64@ -}
data LogicalUsage
  = LogicalUsage'_constructor {_LogicalUsage'newPrefill :: !Data.Word.Word64,
                               _LogicalUsage'generatedOutput :: !Data.Word.Word64,
                               _LogicalUsage'effectiveContextReads :: !Data.Word.Word64,
                               _LogicalUsage'retainedByteMillis :: !Data.Word.Word64,
                               _LogicalUsage'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show LogicalUsage where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField LogicalUsage "newPrefill" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _LogicalUsage'newPrefill
           (\ x__ y__ -> x__ {_LogicalUsage'newPrefill = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField LogicalUsage "generatedOutput" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _LogicalUsage'generatedOutput
           (\ x__ y__ -> x__ {_LogicalUsage'generatedOutput = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField LogicalUsage "effectiveContextReads" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _LogicalUsage'effectiveContextReads
           (\ x__ y__ -> x__ {_LogicalUsage'effectiveContextReads = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField LogicalUsage "retainedByteMillis" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _LogicalUsage'retainedByteMillis
           (\ x__ y__ -> x__ {_LogicalUsage'retainedByteMillis = y__}))
        Prelude.id
instance Data.ProtoLens.Message LogicalUsage where
  messageName _ = Data.Text.pack "inference.customer.v1.LogicalUsage"
  packedMessageDescriptor _
    = "\n\
      \\fLogicalUsage\DC2\US\n\
      \\vnew_prefill\CAN\SOH \SOH(\EOTR\n\
      \newPrefill\DC2)\n\
      \\DLEgenerated_output\CAN\STX \SOH(\EOTR\SIgeneratedOutput\DC26\n\
      \\ETBeffective_context_reads\CAN\ETX \SOH(\EOTR\NAKeffectiveContextReads\DC20\n\
      \\DC4retained_byte_millis\CAN\EOT \SOH(\EOTR\DC2retainedByteMillis"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        newPrefill__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "new_prefill"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"newPrefill")) ::
              Data.ProtoLens.FieldDescriptor LogicalUsage
        generatedOutput__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "generated_output"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"generatedOutput")) ::
              Data.ProtoLens.FieldDescriptor LogicalUsage
        effectiveContextReads__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "effective_context_reads"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"effectiveContextReads")) ::
              Data.ProtoLens.FieldDescriptor LogicalUsage
        retainedByteMillis__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "retained_byte_millis"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"retainedByteMillis")) ::
              Data.ProtoLens.FieldDescriptor LogicalUsage
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, newPrefill__field_descriptor),
           (Data.ProtoLens.Tag 2, generatedOutput__field_descriptor),
           (Data.ProtoLens.Tag 3, effectiveContextReads__field_descriptor),
           (Data.ProtoLens.Tag 4, retainedByteMillis__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _LogicalUsage'_unknownFields
        (\ x__ y__ -> x__ {_LogicalUsage'_unknownFields = y__})
  defMessage
    = LogicalUsage'_constructor
        {_LogicalUsage'newPrefill = Data.ProtoLens.fieldDefault,
         _LogicalUsage'generatedOutput = Data.ProtoLens.fieldDefault,
         _LogicalUsage'effectiveContextReads = Data.ProtoLens.fieldDefault,
         _LogicalUsage'retainedByteMillis = Data.ProtoLens.fieldDefault,
         _LogicalUsage'_unknownFields = []}
  parseMessage
    = let
        loop ::
          LogicalUsage -> Data.ProtoLens.Encoding.Bytes.Parser LogicalUsage
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
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "new_prefill"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"newPrefill") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "generated_output"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"generatedOutput") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "effective_context_reads"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"effectiveContextReads") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt
                                       "retained_byte_millis"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"retainedByteMillis") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "LogicalUsage"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"newPrefill") _x
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
                     = Lens.Family2.view
                         (Data.ProtoLens.Field.field @"generatedOutput") _x
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
                            (Data.ProtoLens.Field.field @"effectiveContextReads") _x
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
                           = Lens.Family2.view
                               (Data.ProtoLens.Field.field @"retainedByteMillis") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData LogicalUsage where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_LogicalUsage'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_LogicalUsage'newPrefill x__)
                (Control.DeepSeq.deepseq
                   (_LogicalUsage'generatedOutput x__)
                   (Control.DeepSeq.deepseq
                      (_LogicalUsage'effectiveContextReads x__)
                      (Control.DeepSeq.deepseq
                         (_LogicalUsage'retainedByteMillis x__) ()))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.model' @:: Lens' ModelCapability Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.executionProfile' @:: Lens' ModelCapability Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maximumContext' @:: Lens' ModelCapability Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maximumOutput' @:: Lens' ModelCapability Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.features' @:: Lens' ModelCapability [Data.Text.Text]@
         * 'Proto.Inference.V1.Inference_Fields.vec'features' @:: Lens' ModelCapability (Data.Vector.Vector Data.Text.Text)@
         * 'Proto.Inference.V1.Inference_Fields.retentionProfiles' @:: Lens' ModelCapability [RetentionProfile]@
         * 'Proto.Inference.V1.Inference_Fields.vec'retentionProfiles' @:: Lens' ModelCapability (Data.Vector.Vector RetentionProfile)@
         * 'Proto.Inference.V1.Inference_Fields.idleKvProfiles' @:: Lens' ModelCapability [RetentionProfile]@
         * 'Proto.Inference.V1.Inference_Fields.vec'idleKvProfiles' @:: Lens' ModelCapability (Data.Vector.Vector RetentionProfile)@ -}
data ModelCapability
  = ModelCapability'_constructor {_ModelCapability'model :: !Data.Text.Text,
                                  _ModelCapability'executionProfile :: !Data.ByteString.ByteString,
                                  _ModelCapability'maximumContext :: !Data.Word.Word64,
                                  _ModelCapability'maximumOutput :: !Data.Word.Word64,
                                  _ModelCapability'features :: !(Data.Vector.Vector Data.Text.Text),
                                  _ModelCapability'retentionProfiles :: !(Data.Vector.Vector RetentionProfile),
                                  _ModelCapability'idleKvProfiles :: !(Data.Vector.Vector RetentionProfile),
                                  _ModelCapability'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ModelCapability where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ModelCapability "model" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'model
           (\ x__ y__ -> x__ {_ModelCapability'model = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ModelCapability "executionProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'executionProfile
           (\ x__ y__ -> x__ {_ModelCapability'executionProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ModelCapability "maximumContext" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'maximumContext
           (\ x__ y__ -> x__ {_ModelCapability'maximumContext = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ModelCapability "maximumOutput" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'maximumOutput
           (\ x__ y__ -> x__ {_ModelCapability'maximumOutput = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ModelCapability "features" [Data.Text.Text] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'features
           (\ x__ y__ -> x__ {_ModelCapability'features = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ModelCapability "vec'features" (Data.Vector.Vector Data.Text.Text) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'features
           (\ x__ y__ -> x__ {_ModelCapability'features = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ModelCapability "retentionProfiles" [RetentionProfile] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'retentionProfiles
           (\ x__ y__ -> x__ {_ModelCapability'retentionProfiles = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ModelCapability "vec'retentionProfiles" (Data.Vector.Vector RetentionProfile) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'retentionProfiles
           (\ x__ y__ -> x__ {_ModelCapability'retentionProfiles = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ModelCapability "idleKvProfiles" [RetentionProfile] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'idleKvProfiles
           (\ x__ y__ -> x__ {_ModelCapability'idleKvProfiles = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField ModelCapability "vec'idleKvProfiles" (Data.Vector.Vector RetentionProfile) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ModelCapability'idleKvProfiles
           (\ x__ y__ -> x__ {_ModelCapability'idleKvProfiles = y__}))
        Prelude.id
instance Data.ProtoLens.Message ModelCapability where
  messageName _
    = Data.Text.pack "inference.customer.v1.ModelCapability"
  packedMessageDescriptor _
    = "\n\
      \\SIModelCapability\DC2\ESC\n\
      \\ENQmodel\CAN\SOH \SOH(\tR\ENQmodelB\ENQ\248\243\CAN\128\STX\DC21\n\
      \\DC1execution_profile\CAN\STX \SOH(\fR\DLEexecutionProfileB\EOT\200\243\CAN \DC2-\n\
      \\SImaximum_context\CAN\ETX \SOH(\EOTR\SOmaximumContextB\EOT\216\243\CAN\SOH\DC2+\n\
      \\SOmaximum_output\CAN\EOT \SOH(\EOTR\rmaximumOutputB\EOT\216\243\CAN\SOH\DC2(\n\
      \\bfeatures\CAN\ENQ \ETX(\tR\bfeaturesB\f\232\243\CAN\SOH\240\243\CAN@\144\244\CAN@\DC2\\\n\
      \\DC2retention_profiles\CAN\ACK \ETX(\v2'.inference.customer.v1.RetentionProfileR\DC1retentionProfilesB\EOT\240\243\CAN@\DC2W\n\
      \\DLEidle_kv_profiles\CAN\a \ETX(\v2'.inference.customer.v1.RetentionProfileR\SOidleKvProfilesB\EOT\240\243\CAN@"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        model__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"model")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
        executionProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "execution_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"executionProfile")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
        maximumContext__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "maximum_context"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maximumContext")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
        maximumOutput__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "maximum_output"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maximumOutput")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
        features__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "features"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"features")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
        retentionProfiles__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "retention_profiles"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RetentionProfile)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"retentionProfiles")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
        idleKvProfiles__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idle_kv_profiles"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RetentionProfile)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"idleKvProfiles")) ::
              Data.ProtoLens.FieldDescriptor ModelCapability
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, model__field_descriptor),
           (Data.ProtoLens.Tag 2, executionProfile__field_descriptor),
           (Data.ProtoLens.Tag 3, maximumContext__field_descriptor),
           (Data.ProtoLens.Tag 4, maximumOutput__field_descriptor),
           (Data.ProtoLens.Tag 5, features__field_descriptor),
           (Data.ProtoLens.Tag 6, retentionProfiles__field_descriptor),
           (Data.ProtoLens.Tag 7, idleKvProfiles__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ModelCapability'_unknownFields
        (\ x__ y__ -> x__ {_ModelCapability'_unknownFields = y__})
  defMessage
    = ModelCapability'_constructor
        {_ModelCapability'model = Data.ProtoLens.fieldDefault,
         _ModelCapability'executionProfile = Data.ProtoLens.fieldDefault,
         _ModelCapability'maximumContext = Data.ProtoLens.fieldDefault,
         _ModelCapability'maximumOutput = Data.ProtoLens.fieldDefault,
         _ModelCapability'features = Data.Vector.Generic.empty,
         _ModelCapability'retentionProfiles = Data.Vector.Generic.empty,
         _ModelCapability'idleKvProfiles = Data.Vector.Generic.empty,
         _ModelCapability'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ModelCapability
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Data.Text.Text
             -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld RetentionProfile
                -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld RetentionProfile
                   -> Data.ProtoLens.Encoding.Bytes.Parser ModelCapability
        loop
          x
          mutable'features
          mutable'idleKvProfiles
          mutable'retentionProfiles
          = do end <- Data.ProtoLens.Encoding.Bytes.atEnd
               if end then
                   do frozen'features <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                           (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                              mutable'features)
                      frozen'idleKvProfiles <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                 (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                    mutable'idleKvProfiles)
                      frozen'retentionProfiles <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                                    (Data.ProtoLens.Encoding.Growing.unsafeFreeze
                                                       mutable'retentionProfiles)
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
                              (Data.ProtoLens.Field.field @"vec'features") frozen'features
                              (Lens.Family2.set
                                 (Data.ProtoLens.Field.field @"vec'idleKvProfiles")
                                 frozen'idleKvProfiles
                                 (Lens.Family2.set
                                    (Data.ProtoLens.Field.field @"vec'retentionProfiles")
                                    frozen'retentionProfiles x))))
               else
                   do tag <- Data.ProtoLens.Encoding.Bytes.getVarInt
                      case tag of
                        10
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "model"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"model") y x)
                                  mutable'features mutable'idleKvProfiles mutable'retentionProfiles
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "execution_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"executionProfile") y x)
                                  mutable'features mutable'idleKvProfiles mutable'retentionProfiles
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "maximum_context"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"maximumContext") y x)
                                  mutable'features mutable'idleKvProfiles mutable'retentionProfiles
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "maximum_output"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"maximumOutput") y x)
                                  mutable'features mutable'idleKvProfiles mutable'retentionProfiles
                        42
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.getText
                                              (Prelude.fromIntegral len))
                                        "features"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append mutable'features y)
                                loop x v mutable'idleKvProfiles mutable'retentionProfiles
                        50
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "retention_profiles"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'retentionProfiles y)
                                loop x mutable'features mutable'idleKvProfiles v
                        58
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "idle_kv_profiles"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'idleKvProfiles y)
                                loop x mutable'features v mutable'retentionProfiles
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
                                  mutable'features mutable'idleKvProfiles mutable'retentionProfiles
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do mutable'features <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                    Data.ProtoLens.Encoding.Growing.new
              mutable'idleKvProfiles <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                          Data.ProtoLens.Encoding.Growing.new
              mutable'retentionProfiles <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                             Data.ProtoLens.Encoding.Growing.new
              loop
                Data.ProtoLens.defMessage mutable'features mutable'idleKvProfiles
                mutable'retentionProfiles)
          "ModelCapability"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"model") _x
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
                         (Data.ProtoLens.Field.field @"executionProfile") _x
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
                            (Data.ProtoLens.Field.field @"maximumContext") _x
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
                           = Lens.Family2.view
                               (Data.ProtoLens.Field.field @"maximumOutput") _x
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
                                       Data.Text.Encoding.encodeUtf8 _v))
                            (Lens.Family2.view
                               (Data.ProtoLens.Field.field @"vec'features") _x))
                         ((Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                               (\ _v
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
                               (Lens.Family2.view
                                  (Data.ProtoLens.Field.field @"vec'retentionProfiles") _x))
                            ((Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.foldMapBuilder
                                  (\ _v
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
                                  (Lens.Family2.view
                                     (Data.ProtoLens.Field.field @"vec'idleKvProfiles") _x))
                               (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                  (Lens.Family2.view Data.ProtoLens.unknownFields _x))))))))
instance Control.DeepSeq.NFData ModelCapability where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ModelCapability'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ModelCapability'model x__)
                (Control.DeepSeq.deepseq
                   (_ModelCapability'executionProfile x__)
                   (Control.DeepSeq.deepseq
                      (_ModelCapability'maximumContext x__)
                      (Control.DeepSeq.deepseq
                         (_ModelCapability'maximumOutput x__)
                         (Control.DeepSeq.deepseq
                            (_ModelCapability'features x__)
                            (Control.DeepSeq.deepseq
                               (_ModelCapability'retentionProfiles x__)
                               (Control.DeepSeq.deepseq
                                  (_ModelCapability'idleKvProfiles x__) ())))))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' MutateContextRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' MutateContextRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.source' @:: Lens' MutateContextRequest Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maybe'action' @:: Lens' MutateContextRequest (Prelude.Maybe MutateContextRequest'Action)@
         * 'Proto.Inference.V1.Inference_Fields.maybe'edit' @:: Lens' MutateContextRequest (Prelude.Maybe Edits)@
         * 'Proto.Inference.V1.Inference_Fields.edit' @:: Lens' MutateContextRequest Edits@
         * 'Proto.Inference.V1.Inference_Fields.maybe'fork' @:: Lens' MutateContextRequest (Prelude.Maybe Empty)@
         * 'Proto.Inference.V1.Inference_Fields.fork' @:: Lens' MutateContextRequest Empty@
         * 'Proto.Inference.V1.Inference_Fields.maybe'truncate' @:: Lens' MutateContextRequest (Prelude.Maybe Truncate)@
         * 'Proto.Inference.V1.Inference_Fields.truncate' @:: Lens' MutateContextRequest Truncate@
         * 'Proto.Inference.V1.Inference_Fields.maybe'compact' @:: Lens' MutateContextRequest (Prelude.Maybe Compact)@
         * 'Proto.Inference.V1.Inference_Fields.compact' @:: Lens' MutateContextRequest Compact@
         * 'Proto.Inference.V1.Inference_Fields.maybe'release' @:: Lens' MutateContextRequest (Prelude.Maybe Empty)@
         * 'Proto.Inference.V1.Inference_Fields.release' @:: Lens' MutateContextRequest Empty@
         * 'Proto.Inference.V1.Inference_Fields.maybe'transfer' @:: Lens' MutateContextRequest (Prelude.Maybe Transfer)@
         * 'Proto.Inference.V1.Inference_Fields.transfer' @:: Lens' MutateContextRequest Transfer@ -}
data MutateContextRequest
  = MutateContextRequest'_constructor {_MutateContextRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                       _MutateContextRequest'source :: !Data.ByteString.ByteString,
                                       _MutateContextRequest'action :: !(Prelude.Maybe MutateContextRequest'Action),
                                       _MutateContextRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MutateContextRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data MutateContextRequest'Action
  = MutateContextRequest'Edit !Edits |
    MutateContextRequest'Fork !Empty |
    MutateContextRequest'Truncate !Truncate |
    MutateContextRequest'Compact !Compact |
    MutateContextRequest'Release !Empty |
    MutateContextRequest'Transfer !Transfer
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField MutateContextRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'identity
           (\ x__ y__ -> x__ {_MutateContextRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'identity
           (\ x__ y__ -> x__ {_MutateContextRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutateContextRequest "source" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'source
           (\ x__ y__ -> x__ {_MutateContextRequest'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'action" (Prelude.Maybe MutateContextRequest'Action) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'edit" (Prelude.Maybe Edits) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutateContextRequest'Edit x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutateContextRequest'Edit y__))
instance Data.ProtoLens.Field.HasField MutateContextRequest "edit" Edits where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutateContextRequest'Edit x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutateContextRequest'Edit y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'fork" (Prelude.Maybe Empty) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutateContextRequest'Fork x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutateContextRequest'Fork y__))
instance Data.ProtoLens.Field.HasField MutateContextRequest "fork" Empty where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutateContextRequest'Fork x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutateContextRequest'Fork y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'truncate" (Prelude.Maybe Truncate) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutateContextRequest'Truncate x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutateContextRequest'Truncate y__))
instance Data.ProtoLens.Field.HasField MutateContextRequest "truncate" Truncate where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutateContextRequest'Truncate x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutateContextRequest'Truncate y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'compact" (Prelude.Maybe Compact) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutateContextRequest'Compact x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutateContextRequest'Compact y__))
instance Data.ProtoLens.Field.HasField MutateContextRequest "compact" Compact where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutateContextRequest'Compact x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutateContextRequest'Compact y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'release" (Prelude.Maybe Empty) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutateContextRequest'Release x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutateContextRequest'Release y__))
instance Data.ProtoLens.Field.HasField MutateContextRequest "release" Empty where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutateContextRequest'Release x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutateContextRequest'Release y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField MutateContextRequest "maybe'transfer" (Prelude.Maybe Transfer) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (MutateContextRequest'Transfer x__val))
                     -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap MutateContextRequest'Transfer y__))
instance Data.ProtoLens.Field.HasField MutateContextRequest "transfer" Transfer where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutateContextRequest'action
           (\ x__ y__ -> x__ {_MutateContextRequest'action = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (MutateContextRequest'Transfer x__val))
                        -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap MutateContextRequest'Transfer y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message MutateContextRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.MutateContextRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC4MutateContextRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\FS\n\
      \\ACKsource\CAN\STX \SOH(\fR\ACKsourceB\EOT\200\243\CAN \DC22\n\
      \\EOTedit\CAN\ETX \SOH(\v2\FS.inference.customer.v1.EditsH\NULR\EOTedit\DC22\n\
      \\EOTfork\CAN\EOT \SOH(\v2\FS.inference.customer.v1.EmptyH\NULR\EOTfork\DC2=\n\
      \\btruncate\CAN\ENQ \SOH(\v2\US.inference.customer.v1.TruncateH\NULR\btruncate\DC2:\n\
      \\acompact\CAN\ACK \SOH(\v2\RS.inference.customer.v1.CompactH\NULR\acompact\DC28\n\
      \\arelease\CAN\a \SOH(\v2\FS.inference.customer.v1.EmptyH\NULR\arelease\DC2=\n\
      \\btransfer\CAN\b \SOH(\v2\US.inference.customer.v1.TransferH\NULR\btransferB\SO\n\
      \\ACKaction\DC2\EOT\224\243\CAN\SOH"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        edit__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "edit"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Edits)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'edit")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        fork__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "fork"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Empty)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'fork")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        truncate__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "truncate"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Truncate)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'truncate")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        compact__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "compact"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Compact)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'compact")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        release__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "release"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Empty)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'release")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
        transfer__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "transfer"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Transfer)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'transfer")) ::
              Data.ProtoLens.FieldDescriptor MutateContextRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, source__field_descriptor),
           (Data.ProtoLens.Tag 3, edit__field_descriptor),
           (Data.ProtoLens.Tag 4, fork__field_descriptor),
           (Data.ProtoLens.Tag 5, truncate__field_descriptor),
           (Data.ProtoLens.Tag 6, compact__field_descriptor),
           (Data.ProtoLens.Tag 7, release__field_descriptor),
           (Data.ProtoLens.Tag 8, transfer__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MutateContextRequest'_unknownFields
        (\ x__ y__ -> x__ {_MutateContextRequest'_unknownFields = y__})
  defMessage
    = MutateContextRequest'_constructor
        {_MutateContextRequest'identity = Prelude.Nothing,
         _MutateContextRequest'source = Data.ProtoLens.fieldDefault,
         _MutateContextRequest'action = Prelude.Nothing,
         _MutateContextRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MutateContextRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser MutateContextRequest
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "edit"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"edit") y x)
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
                                       "truncate"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"truncate") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "compact"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"compact") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "release"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"release") y x)
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "transfer"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"transfer") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MutateContextRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"source") _x
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
                        Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'action") _x
                    of
                      Prelude.Nothing -> Data.Monoid.mempty
                      (Prelude.Just (MutateContextRequest'Edit v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (MutateContextRequest'Fork v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 34)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (MutateContextRequest'Truncate v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (MutateContextRequest'Compact v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 50)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (MutateContextRequest'Release v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v)
                      (Prelude.Just (MutateContextRequest'Transfer v))
                        -> (Data.Monoid.<>)
                             (Data.ProtoLens.Encoding.Bytes.putVarInt 66)
                             ((Prelude..)
                                (\ bs
                                   -> (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt
                                           (Prelude.fromIntegral (Data.ByteString.length bs)))
                                        (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                                Data.ProtoLens.encodeMessage v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData MutateContextRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MutateContextRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MutateContextRequest'identity x__)
                (Control.DeepSeq.deepseq
                   (_MutateContextRequest'source x__)
                   (Control.DeepSeq.deepseq (_MutateContextRequest'action x__) ())))
instance Control.DeepSeq.NFData MutateContextRequest'Action where
  rnf (MutateContextRequest'Edit x__) = Control.DeepSeq.rnf x__
  rnf (MutateContextRequest'Fork x__) = Control.DeepSeq.rnf x__
  rnf (MutateContextRequest'Truncate x__) = Control.DeepSeq.rnf x__
  rnf (MutateContextRequest'Compact x__) = Control.DeepSeq.rnf x__
  rnf (MutateContextRequest'Release x__) = Control.DeepSeq.rnf x__
  rnf (MutateContextRequest'Transfer x__) = Control.DeepSeq.rnf x__
_MutateContextRequest'Edit ::
  Data.ProtoLens.Prism.Prism' MutateContextRequest'Action Edits
_MutateContextRequest'Edit
  = Data.ProtoLens.Prism.prism'
      MutateContextRequest'Edit
      (\ p__
         -> case p__ of
              (MutateContextRequest'Edit p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutateContextRequest'Fork ::
  Data.ProtoLens.Prism.Prism' MutateContextRequest'Action Empty
_MutateContextRequest'Fork
  = Data.ProtoLens.Prism.prism'
      MutateContextRequest'Fork
      (\ p__
         -> case p__ of
              (MutateContextRequest'Fork p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutateContextRequest'Truncate ::
  Data.ProtoLens.Prism.Prism' MutateContextRequest'Action Truncate
_MutateContextRequest'Truncate
  = Data.ProtoLens.Prism.prism'
      MutateContextRequest'Truncate
      (\ p__
         -> case p__ of
              (MutateContextRequest'Truncate p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutateContextRequest'Compact ::
  Data.ProtoLens.Prism.Prism' MutateContextRequest'Action Compact
_MutateContextRequest'Compact
  = Data.ProtoLens.Prism.prism'
      MutateContextRequest'Compact
      (\ p__
         -> case p__ of
              (MutateContextRequest'Compact p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutateContextRequest'Release ::
  Data.ProtoLens.Prism.Prism' MutateContextRequest'Action Empty
_MutateContextRequest'Release
  = Data.ProtoLens.Prism.prism'
      MutateContextRequest'Release
      (\ p__
         -> case p__ of
              (MutateContextRequest'Release p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_MutateContextRequest'Transfer ::
  Data.ProtoLens.Prism.Prism' MutateContextRequest'Action Transfer
_MutateContextRequest'Transfer
  = Data.ProtoLens.Prism.prism'
      MutateContextRequest'Transfer
      (\ p__
         -> case p__ of
              (MutateContextRequest'Transfer p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.revision' @:: Lens' MutationReceipt Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.commandDigest' @:: Lens' MutationReceipt Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.sequence' @:: Lens' MutationReceipt Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.retained' @:: Lens' MutationReceipt Prelude.Bool@ -}
data MutationReceipt
  = MutationReceipt'_constructor {_MutationReceipt'revision :: !Data.ByteString.ByteString,
                                  _MutationReceipt'commandDigest :: !Data.ByteString.ByteString,
                                  _MutationReceipt'sequence :: !Data.Word.Word64,
                                  _MutationReceipt'retained :: !Prelude.Bool,
                                  _MutationReceipt'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show MutationReceipt where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField MutationReceipt "revision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationReceipt'revision
           (\ x__ y__ -> x__ {_MutationReceipt'revision = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutationReceipt "commandDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationReceipt'commandDigest
           (\ x__ y__ -> x__ {_MutationReceipt'commandDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutationReceipt "sequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationReceipt'sequence
           (\ x__ y__ -> x__ {_MutationReceipt'sequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField MutationReceipt "retained" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _MutationReceipt'retained
           (\ x__ y__ -> x__ {_MutationReceipt'retained = y__}))
        Prelude.id
instance Data.ProtoLens.Message MutationReceipt where
  messageName _
    = Data.Text.pack "inference.customer.v1.MutationReceipt"
  packedMessageDescriptor _
    = "\n\
      \\SIMutationReceipt\DC2 \n\
      \\brevision\CAN\SOH \SOH(\fR\brevisionB\EOT\200\243\CAN \DC2+\n\
      \\SOcommand_digest\CAN\STX \SOH(\fR\rcommandDigestB\EOT\200\243\CAN \DC2 \n\
      \\bsequence\CAN\ETX \SOH(\EOTR\bsequenceB\EOT\216\243\CAN\SOH\DC2\SUB\n\
      \\bretained\CAN\EOT \SOH(\bR\bretained"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        revision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"revision")) ::
              Data.ProtoLens.FieldDescriptor MutationReceipt
        commandDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "command_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commandDigest")) ::
              Data.ProtoLens.FieldDescriptor MutationReceipt
        sequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sequence")) ::
              Data.ProtoLens.FieldDescriptor MutationReceipt
        retained__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "retained"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"retained")) ::
              Data.ProtoLens.FieldDescriptor MutationReceipt
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, revision__field_descriptor),
           (Data.ProtoLens.Tag 2, commandDigest__field_descriptor),
           (Data.ProtoLens.Tag 3, sequence__field_descriptor),
           (Data.ProtoLens.Tag 4, retained__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _MutationReceipt'_unknownFields
        (\ x__ y__ -> x__ {_MutationReceipt'_unknownFields = y__})
  defMessage
    = MutationReceipt'_constructor
        {_MutationReceipt'revision = Data.ProtoLens.fieldDefault,
         _MutationReceipt'commandDigest = Data.ProtoLens.fieldDefault,
         _MutationReceipt'sequence = Data.ProtoLens.fieldDefault,
         _MutationReceipt'retained = Data.ProtoLens.fieldDefault,
         _MutationReceipt'_unknownFields = []}
  parseMessage
    = let
        loop ::
          MutationReceipt
          -> Data.ProtoLens.Encoding.Bytes.Parser MutationReceipt
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
                                       "revision"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"revision") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "command_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"commandDigest") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "sequence"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"sequence") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "retained"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"retained") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "MutationReceipt"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"revision") _x
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
                         (Data.ProtoLens.Field.field @"commandDigest") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"sequence") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (let
                         _v = Lens.Family2.view (Data.ProtoLens.Field.field @"retained") _x
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
instance Control.DeepSeq.NFData MutationReceipt where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_MutationReceipt'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_MutationReceipt'revision x__)
                (Control.DeepSeq.deepseq
                   (_MutationReceipt'commandDigest x__)
                   (Control.DeepSeq.deepseq
                      (_MutationReceipt'sequence x__)
                      (Control.DeepSeq.deepseq (_MutationReceipt'retained x__) ()))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.source' @:: Lens' ProvenanceSource Data.ByteString.ByteString@ -}
data ProvenanceSource
  = ProvenanceSource'_constructor {_ProvenanceSource'source :: !Data.ByteString.ByteString,
                                   _ProvenanceSource'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ProvenanceSource where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ProvenanceSource "source" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ProvenanceSource'source
           (\ x__ y__ -> x__ {_ProvenanceSource'source = y__}))
        Prelude.id
instance Data.ProtoLens.Message ProvenanceSource where
  messageName _
    = Data.Text.pack "inference.customer.v1.ProvenanceSource"
  packedMessageDescriptor _
    = "\n\
      \\DLEProvenanceSource\DC2\FS\n\
      \\ACKsource\CAN\SOH \SOH(\fR\ACKsourceB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor ProvenanceSource
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ProvenanceSource'_unknownFields
        (\ x__ y__ -> x__ {_ProvenanceSource'_unknownFields = y__})
  defMessage
    = ProvenanceSource'_constructor
        {_ProvenanceSource'source = Data.ProtoLens.fieldDefault,
         _ProvenanceSource'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ProvenanceSource
          -> Data.ProtoLens.Encoding.Bytes.Parser ProvenanceSource
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
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ProvenanceSource"
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
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData ProvenanceSource where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ProvenanceSource'_unknownFields x__)
             (Control.DeepSeq.deepseq (_ProvenanceSource'source x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' ReleaseWarmRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' ReleaseWarmRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.commitment' @:: Lens' ReleaseWarmRequest Data.ByteString.ByteString@ -}
data ReleaseWarmRequest
  = ReleaseWarmRequest'_constructor {_ReleaseWarmRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                     _ReleaseWarmRequest'commitment :: !Data.ByteString.ByteString,
                                     _ReleaseWarmRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ReleaseWarmRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ReleaseWarmRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReleaseWarmRequest'identity
           (\ x__ y__ -> x__ {_ReleaseWarmRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField ReleaseWarmRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReleaseWarmRequest'identity
           (\ x__ y__ -> x__ {_ReleaseWarmRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ReleaseWarmRequest "commitment" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ReleaseWarmRequest'commitment
           (\ x__ y__ -> x__ {_ReleaseWarmRequest'commitment = y__}))
        Prelude.id
instance Data.ProtoLens.Message ReleaseWarmRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.ReleaseWarmRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC2ReleaseWarmRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2$\n\
      \\n\
      \commitment\CAN\STX \SOH(\fR\n\
      \commitmentB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor ReleaseWarmRequest
        commitment__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commitment"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitment")) ::
              Data.ProtoLens.FieldDescriptor ReleaseWarmRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, commitment__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ReleaseWarmRequest'_unknownFields
        (\ x__ y__ -> x__ {_ReleaseWarmRequest'_unknownFields = y__})
  defMessage
    = ReleaseWarmRequest'_constructor
        {_ReleaseWarmRequest'identity = Prelude.Nothing,
         _ReleaseWarmRequest'commitment = Data.ProtoLens.fieldDefault,
         _ReleaseWarmRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ReleaseWarmRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser ReleaseWarmRequest
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commitment"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitment") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ReleaseWarmRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"commitment") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData ReleaseWarmRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ReleaseWarmRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ReleaseWarmRequest'identity x__)
                (Control.DeepSeq.deepseq (_ReleaseWarmRequest'commitment x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' RenewWarmRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' RenewWarmRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.commitment' @:: Lens' RenewWarmRequest Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.expiresAtMs' @:: Lens' RenewWarmRequest Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.idleTimeoutMs' @:: Lens' RenewWarmRequest Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maybe'idleTimeoutMs' @:: Lens' RenewWarmRequest (Prelude.Maybe Data.Word.Word64)@ -}
data RenewWarmRequest
  = RenewWarmRequest'_constructor {_RenewWarmRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                   _RenewWarmRequest'commitment :: !Data.ByteString.ByteString,
                                   _RenewWarmRequest'expiresAtMs :: !Data.Word.Word64,
                                   _RenewWarmRequest'idleTimeoutMs :: !(Prelude.Maybe Data.Word.Word64),
                                   _RenewWarmRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RenewWarmRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RenewWarmRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RenewWarmRequest'identity
           (\ x__ y__ -> x__ {_RenewWarmRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RenewWarmRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RenewWarmRequest'identity
           (\ x__ y__ -> x__ {_RenewWarmRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RenewWarmRequest "commitment" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RenewWarmRequest'commitment
           (\ x__ y__ -> x__ {_RenewWarmRequest'commitment = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RenewWarmRequest "expiresAtMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RenewWarmRequest'expiresAtMs
           (\ x__ y__ -> x__ {_RenewWarmRequest'expiresAtMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RenewWarmRequest "idleTimeoutMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RenewWarmRequest'idleTimeoutMs
           (\ x__ y__ -> x__ {_RenewWarmRequest'idleTimeoutMs = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField RenewWarmRequest "maybe'idleTimeoutMs" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RenewWarmRequest'idleTimeoutMs
           (\ x__ y__ -> x__ {_RenewWarmRequest'idleTimeoutMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message RenewWarmRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.RenewWarmRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLERenewWarmRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2$\n\
      \\n\
      \commitment\CAN\STX \SOH(\fR\n\
      \commitmentB\EOT\200\243\CAN \DC2\"\n\
      \\rexpires_at_ms\CAN\ETX \SOH(\EOTR\vexpiresAtMs\DC21\n\
      \\SIidle_timeout_ms\CAN\EOT \SOH(\EOTH\NULR\ridleTimeoutMsB\EOT\216\243\CAN\SOH\136\SOH\SOHB\DC2\n\
      \\DLE_idle_timeout_ms"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor RenewWarmRequest
        commitment__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commitment"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitment")) ::
              Data.ProtoLens.FieldDescriptor RenewWarmRequest
        expiresAtMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expires_at_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expiresAtMs")) ::
              Data.ProtoLens.FieldDescriptor RenewWarmRequest
        idleTimeoutMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idle_timeout_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idleTimeoutMs")) ::
              Data.ProtoLens.FieldDescriptor RenewWarmRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, commitment__field_descriptor),
           (Data.ProtoLens.Tag 3, expiresAtMs__field_descriptor),
           (Data.ProtoLens.Tag 4, idleTimeoutMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RenewWarmRequest'_unknownFields
        (\ x__ y__ -> x__ {_RenewWarmRequest'_unknownFields = y__})
  defMessage
    = RenewWarmRequest'_constructor
        {_RenewWarmRequest'identity = Prelude.Nothing,
         _RenewWarmRequest'commitment = Data.ProtoLens.fieldDefault,
         _RenewWarmRequest'expiresAtMs = Data.ProtoLens.fieldDefault,
         _RenewWarmRequest'idleTimeoutMs = Prelude.Nothing,
         _RenewWarmRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RenewWarmRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser RenewWarmRequest
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "commitment"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitment") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "expires_at_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expiresAtMs") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "idle_timeout_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"idleTimeoutMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RenewWarmRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"commitment") _x
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
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"expiresAtMs") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view
                             (Data.ProtoLens.Field.field @"maybe'idleTimeoutMs") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData RenewWarmRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RenewWarmRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RenewWarmRequest'identity x__)
                (Control.DeepSeq.deepseq
                   (_RenewWarmRequest'commitment x__)
                   (Control.DeepSeq.deepseq
                      (_RenewWarmRequest'expiresAtMs x__)
                      (Control.DeepSeq.deepseq
                         (_RenewWarmRequest'idleTimeoutMs x__) ()))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.target' @:: Lens' Replace Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.payload' @:: Lens' Replace Data.ByteString.ByteString@ -}
data Replace
  = Replace'_constructor {_Replace'target :: !Data.ByteString.ByteString,
                          _Replace'payload :: !Data.ByteString.ByteString,
                          _Replace'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Replace where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Replace "target" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Replace'target (\ x__ y__ -> x__ {_Replace'target = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Replace "payload" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Replace'payload (\ x__ y__ -> x__ {_Replace'payload = y__}))
        Prelude.id
instance Data.ProtoLens.Message Replace where
  messageName _ = Data.Text.pack "inference.customer.v1.Replace"
  packedMessageDescriptor _
    = "\n\
      \\aReplace\DC2\SYN\n\
      \\ACKtarget\CAN\SOH \SOH(\fR\ACKtarget\DC2\CAN\n\
      \\apayload\CAN\STX \SOH(\fR\apayload"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        target__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "target"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"target")) ::
              Data.ProtoLens.FieldDescriptor Replace
        payload__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "payload"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"payload")) ::
              Data.ProtoLens.FieldDescriptor Replace
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, target__field_descriptor),
           (Data.ProtoLens.Tag 2, payload__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Replace'_unknownFields
        (\ x__ y__ -> x__ {_Replace'_unknownFields = y__})
  defMessage
    = Replace'_constructor
        {_Replace'target = Data.ProtoLens.fieldDefault,
         _Replace'payload = Data.ProtoLens.fieldDefault,
         _Replace'_unknownFields = []}
  parseMessage
    = let
        loop :: Replace -> Data.ProtoLens.Encoding.Bytes.Parser Replace
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
                                       "target"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"target") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "payload"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"payload") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Replace"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"target") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"payload") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData Replace where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Replace'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Replace'target x__)
                (Control.DeepSeq.deepseq (_Replace'payload x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.clientInstance' @:: Lens' RequestIdentity Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.requestId' @:: Lens' RequestIdentity Data.ByteString.ByteString@ -}
data RequestIdentity
  = RequestIdentity'_constructor {_RequestIdentity'clientInstance :: !Data.ByteString.ByteString,
                                  _RequestIdentity'requestId :: !Data.ByteString.ByteString,
                                  _RequestIdentity'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RequestIdentity where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RequestIdentity "clientInstance" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RequestIdentity'clientInstance
           (\ x__ y__ -> x__ {_RequestIdentity'clientInstance = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RequestIdentity "requestId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RequestIdentity'requestId
           (\ x__ y__ -> x__ {_RequestIdentity'requestId = y__}))
        Prelude.id
instance Data.ProtoLens.Message RequestIdentity where
  messageName _
    = Data.Text.pack "inference.customer.v1.RequestIdentity"
  packedMessageDescriptor _
    = "\n\
      \\SIRequestIdentity\DC2-\n\
      \\SIclient_instance\CAN\SOH \SOH(\fR\SOclientInstanceB\EOT\200\243\CAN\DLE\DC2#\n\
      \\n\
      \request_id\CAN\STX \SOH(\fR\trequestIdB\EOT\200\243\CAN\DLE"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        clientInstance__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "client_instance"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"clientInstance")) ::
              Data.ProtoLens.FieldDescriptor RequestIdentity
        requestId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "request_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"requestId")) ::
              Data.ProtoLens.FieldDescriptor RequestIdentity
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, clientInstance__field_descriptor),
           (Data.ProtoLens.Tag 2, requestId__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RequestIdentity'_unknownFields
        (\ x__ y__ -> x__ {_RequestIdentity'_unknownFields = y__})
  defMessage
    = RequestIdentity'_constructor
        {_RequestIdentity'clientInstance = Data.ProtoLens.fieldDefault,
         _RequestIdentity'requestId = Data.ProtoLens.fieldDefault,
         _RequestIdentity'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RequestIdentity
          -> Data.ProtoLens.Encoding.Bytes.Parser RequestIdentity
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
                                       "client_instance"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"clientInstance") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
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
          (do loop Data.ProtoLens.defMessage) "RequestIdentity"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view
                      (Data.ProtoLens.Field.field @"clientInstance") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"requestId") _x
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
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData RequestIdentity where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RequestIdentity'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RequestIdentity'clientInstance x__)
                (Control.DeepSeq.deepseq (_RequestIdentity'requestId x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.identity' @:: Lens' RetainWarmRequest RequestIdentity@
         * 'Proto.Inference.V1.Inference_Fields.maybe'identity' @:: Lens' RetainWarmRequest (Prelude.Maybe RequestIdentity)@
         * 'Proto.Inference.V1.Inference_Fields.context' @:: Lens' RetainWarmRequest Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.latencyProfile' @:: Lens' RetainWarmRequest Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.expiresAtMs' @:: Lens' RetainWarmRequest Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.idleKv' @:: Lens' RetainWarmRequest IdleKvPolicy@
         * 'Proto.Inference.V1.Inference_Fields.maybe'idleKv' @:: Lens' RetainWarmRequest (Prelude.Maybe IdleKvPolicy)@ -}
data RetainWarmRequest
  = RetainWarmRequest'_constructor {_RetainWarmRequest'identity :: !(Prelude.Maybe RequestIdentity),
                                    _RetainWarmRequest'context :: !Data.ByteString.ByteString,
                                    _RetainWarmRequest'latencyProfile :: !Data.ByteString.ByteString,
                                    _RetainWarmRequest'expiresAtMs :: !Data.Word.Word64,
                                    _RetainWarmRequest'idleKv :: !(Prelude.Maybe IdleKvPolicy),
                                    _RetainWarmRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RetainWarmRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RetainWarmRequest "identity" RequestIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'identity
           (\ x__ y__ -> x__ {_RetainWarmRequest'identity = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RetainWarmRequest "maybe'identity" (Prelude.Maybe RequestIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'identity
           (\ x__ y__ -> x__ {_RetainWarmRequest'identity = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetainWarmRequest "context" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'context
           (\ x__ y__ -> x__ {_RetainWarmRequest'context = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetainWarmRequest "latencyProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'latencyProfile
           (\ x__ y__ -> x__ {_RetainWarmRequest'latencyProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetainWarmRequest "expiresAtMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'expiresAtMs
           (\ x__ y__ -> x__ {_RetainWarmRequest'expiresAtMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetainWarmRequest "idleKv" IdleKvPolicy where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'idleKv
           (\ x__ y__ -> x__ {_RetainWarmRequest'idleKv = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RetainWarmRequest "maybe'idleKv" (Prelude.Maybe IdleKvPolicy) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetainWarmRequest'idleKv
           (\ x__ y__ -> x__ {_RetainWarmRequest'idleKv = y__}))
        Prelude.id
instance Data.ProtoLens.Message RetainWarmRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.RetainWarmRequest"
  packedMessageDescriptor _
    = "\n\
      \\DC1RetainWarmRequest\DC2H\n\
      \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\RS\n\
      \\acontext\CAN\STX \SOH(\fR\acontextB\EOT\200\243\CAN \DC2'\n\
      \\SIlatency_profile\CAN\ETX \SOH(\fR\SOlatencyProfile\DC2\"\n\
      \\rexpires_at_ms\CAN\EOT \SOH(\EOTR\vexpiresAtMs\DC2<\n\
      \\aidle_kv\CAN\ENQ \SOH(\v2#.inference.customer.v1.IdleKvPolicyR\ACKidleKv"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        identity__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "identity"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RequestIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'identity")) ::
              Data.ProtoLens.FieldDescriptor RetainWarmRequest
        context__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "context"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"context")) ::
              Data.ProtoLens.FieldDescriptor RetainWarmRequest
        latencyProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "latency_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"latencyProfile")) ::
              Data.ProtoLens.FieldDescriptor RetainWarmRequest
        expiresAtMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expires_at_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expiresAtMs")) ::
              Data.ProtoLens.FieldDescriptor RetainWarmRequest
        idleKv__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idle_kv"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdleKvPolicy)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idleKv")) ::
              Data.ProtoLens.FieldDescriptor RetainWarmRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, identity__field_descriptor),
           (Data.ProtoLens.Tag 2, context__field_descriptor),
           (Data.ProtoLens.Tag 3, latencyProfile__field_descriptor),
           (Data.ProtoLens.Tag 4, expiresAtMs__field_descriptor),
           (Data.ProtoLens.Tag 5, idleKv__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RetainWarmRequest'_unknownFields
        (\ x__ y__ -> x__ {_RetainWarmRequest'_unknownFields = y__})
  defMessage
    = RetainWarmRequest'_constructor
        {_RetainWarmRequest'identity = Prelude.Nothing,
         _RetainWarmRequest'context = Data.ProtoLens.fieldDefault,
         _RetainWarmRequest'latencyProfile = Data.ProtoLens.fieldDefault,
         _RetainWarmRequest'expiresAtMs = Data.ProtoLens.fieldDefault,
         _RetainWarmRequest'idleKv = Prelude.Nothing,
         _RetainWarmRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RetainWarmRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser RetainWarmRequest
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
                                       "identity"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"identity") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "context"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"context") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "latency_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"latencyProfile") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "expires_at_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expiresAtMs") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idle_kv"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"idleKv") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RetainWarmRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'identity") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"context") _x
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
                            (Data.ProtoLens.Field.field @"latencyProfile") _x
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"expiresAtMs") _x
                       in
                         if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                             Data.Monoid.mempty
                         else
                             (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      ((Data.Monoid.<>)
                         (case
                              Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'idleKv") _x
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
instance Control.DeepSeq.NFData RetainWarmRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RetainWarmRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RetainWarmRequest'identity x__)
                (Control.DeepSeq.deepseq
                   (_RetainWarmRequest'context x__)
                   (Control.DeepSeq.deepseq
                      (_RetainWarmRequest'latencyProfile x__)
                      (Control.DeepSeq.deepseq
                         (_RetainWarmRequest'expiresAtMs x__)
                         (Control.DeepSeq.deepseq (_RetainWarmRequest'idleKv x__) ())))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.profile' @:: Lens' RetentionProfile Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.minimumDurationMs' @:: Lens' RetentionProfile Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maximumDurationMs' @:: Lens' RetentionProfile Data.Word.Word64@ -}
data RetentionProfile
  = RetentionProfile'_constructor {_RetentionProfile'profile :: !Data.ByteString.ByteString,
                                   _RetentionProfile'minimumDurationMs :: !Data.Word.Word64,
                                   _RetentionProfile'maximumDurationMs :: !Data.Word.Word64,
                                   _RetentionProfile'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RetentionProfile where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RetentionProfile "profile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetentionProfile'profile
           (\ x__ y__ -> x__ {_RetentionProfile'profile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetentionProfile "minimumDurationMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetentionProfile'minimumDurationMs
           (\ x__ y__ -> x__ {_RetentionProfile'minimumDurationMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RetentionProfile "maximumDurationMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RetentionProfile'maximumDurationMs
           (\ x__ y__ -> x__ {_RetentionProfile'maximumDurationMs = y__}))
        Prelude.id
instance Data.ProtoLens.Message RetentionProfile where
  messageName _
    = Data.Text.pack "inference.customer.v1.RetentionProfile"
  packedMessageDescriptor _
    = "\n\
      \\DLERetentionProfile\DC2\RS\n\
      \\aprofile\CAN\SOH \SOH(\fR\aprofileB\EOT\200\243\CAN \DC24\n\
      \\DC3minimum_duration_ms\CAN\STX \SOH(\EOTR\DC1minimumDurationMsB\EOT\216\243\CAN\SOH\DC2.\n\
      \\DC3maximum_duration_ms\CAN\ETX \SOH(\EOTR\DC1maximumDurationMs"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        profile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"profile")) ::
              Data.ProtoLens.FieldDescriptor RetentionProfile
        minimumDurationMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "minimum_duration_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"minimumDurationMs")) ::
              Data.ProtoLens.FieldDescriptor RetentionProfile
        maximumDurationMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "maximum_duration_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maximumDurationMs")) ::
              Data.ProtoLens.FieldDescriptor RetentionProfile
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, profile__field_descriptor),
           (Data.ProtoLens.Tag 2, minimumDurationMs__field_descriptor),
           (Data.ProtoLens.Tag 3, maximumDurationMs__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RetentionProfile'_unknownFields
        (\ x__ y__ -> x__ {_RetentionProfile'_unknownFields = y__})
  defMessage
    = RetentionProfile'_constructor
        {_RetentionProfile'profile = Data.ProtoLens.fieldDefault,
         _RetentionProfile'minimumDurationMs = Data.ProtoLens.fieldDefault,
         _RetentionProfile'maximumDurationMs = Data.ProtoLens.fieldDefault,
         _RetentionProfile'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RetentionProfile
          -> Data.ProtoLens.Encoding.Bytes.Parser RetentionProfile
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
                                       "profile"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"profile") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "minimum_duration_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"minimumDurationMs") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "maximum_duration_ms"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"maximumDurationMs") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RetentionProfile"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"profile") _x
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
                         (Data.ProtoLens.Field.field @"minimumDurationMs") _x
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
                            (Data.ProtoLens.Field.field @"maximumDurationMs") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   (Data.ProtoLens.Encoding.Wire.buildFieldSet
                      (Lens.Family2.view Data.ProtoLens.unknownFields _x))))
instance Control.DeepSeq.NFData RetentionProfile where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RetentionProfile'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RetentionProfile'profile x__)
                (Control.DeepSeq.deepseq
                   (_RetentionProfile'minimumDurationMs x__)
                   (Control.DeepSeq.deepseq
                      (_RetentionProfile'maximumDurationMs x__) ())))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.sequence' @:: Lens' RunEvent Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maybe'event' @:: Lens' RunEvent (Prelude.Maybe RunEvent'Event)@
         * 'Proto.Inference.V1.Inference_Fields.maybe'output' @:: Lens' RunEvent (Prelude.Maybe Data.ByteString.ByteString)@
         * 'Proto.Inference.V1.Inference_Fields.output' @:: Lens' RunEvent Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maybe'usage' @:: Lens' RunEvent (Prelude.Maybe LogicalUsage)@
         * 'Proto.Inference.V1.Inference_Fields.usage' @:: Lens' RunEvent LogicalUsage@
         * 'Proto.Inference.V1.Inference_Fields.maybe'terminal' @:: Lens' RunEvent (Prelude.Maybe RunTerminal)@
         * 'Proto.Inference.V1.Inference_Fields.terminal' @:: Lens' RunEvent RunTerminal@
         * 'Proto.Inference.V1.Inference_Fields.maybe'progress' @:: Lens' RunEvent (Prelude.Maybe RunProgress)@
         * 'Proto.Inference.V1.Inference_Fields.progress' @:: Lens' RunEvent RunProgress@ -}
data RunEvent
  = RunEvent'_constructor {_RunEvent'sequence :: !Data.Word.Word64,
                           _RunEvent'event :: !(Prelude.Maybe RunEvent'Event),
                           _RunEvent'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RunEvent where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
data RunEvent'Event
  = RunEvent'Output !Data.ByteString.ByteString |
    RunEvent'Usage !LogicalUsage |
    RunEvent'Terminal !RunTerminal |
    RunEvent'Progress !RunProgress
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.Field.HasField RunEvent "sequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'sequence (\ x__ y__ -> x__ {_RunEvent'sequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunEvent "maybe'event" (Prelude.Maybe RunEvent'Event) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunEvent "maybe'output" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RunEvent'Output x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RunEvent'Output y__))
instance Data.ProtoLens.Field.HasField RunEvent "output" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RunEvent'Output x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RunEvent'Output y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField RunEvent "maybe'usage" (Prelude.Maybe LogicalUsage) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RunEvent'Usage x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RunEvent'Usage y__))
instance Data.ProtoLens.Field.HasField RunEvent "usage" LogicalUsage where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RunEvent'Usage x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RunEvent'Usage y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Field.HasField RunEvent "maybe'terminal" (Prelude.Maybe RunTerminal) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RunEvent'Terminal x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RunEvent'Terminal y__))
instance Data.ProtoLens.Field.HasField RunEvent "terminal" RunTerminal where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RunEvent'Terminal x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RunEvent'Terminal y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault))
instance Data.ProtoLens.Field.HasField RunEvent "maybe'progress" (Prelude.Maybe RunProgress) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        (Lens.Family2.Unchecked.lens
           (\ x__
              -> case x__ of
                   (Prelude.Just (RunEvent'Progress x__val)) -> Prelude.Just x__val
                   _otherwise -> Prelude.Nothing)
           (\ _ y__ -> Prelude.fmap RunEvent'Progress y__))
instance Data.ProtoLens.Field.HasField RunEvent "progress" RunProgress where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunEvent'event (\ x__ y__ -> x__ {_RunEvent'event = y__}))
        ((Prelude..)
           (Lens.Family2.Unchecked.lens
              (\ x__
                 -> case x__ of
                      (Prelude.Just (RunEvent'Progress x__val)) -> Prelude.Just x__val
                      _otherwise -> Prelude.Nothing)
              (\ _ y__ -> Prelude.fmap RunEvent'Progress y__))
           (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage))
instance Data.ProtoLens.Message RunEvent where
  messageName _ = Data.Text.pack "inference.customer.v1.RunEvent"
  packedMessageDescriptor _
    = "\n\
      \\bRunEvent\DC2\SUB\n\
      \\bsequence\CAN\SOH \SOH(\EOTR\bsequence\DC2\CAN\n\
      \\ACKoutput\CAN\STX \SOH(\fH\NULR\ACKoutput\DC2;\n\
      \\ENQusage\CAN\ETX \SOH(\v2#.inference.customer.v1.LogicalUsageH\NULR\ENQusage\DC2F\n\
      \\bterminal\CAN\EOT \SOH(\SO2\".inference.customer.v1.RunTerminalH\NULR\bterminalB\EOT\136\244\CAN\SOH\DC2@\n\
      \\bprogress\CAN\ENQ \SOH(\v2\".inference.customer.v1.RunProgressH\NULR\bprogressB\r\n\
      \\ENQevent\DC2\EOT\224\243\CAN\SOH"
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
              Data.ProtoLens.FieldDescriptor RunEvent
        output__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "output"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'output")) ::
              Data.ProtoLens.FieldDescriptor RunEvent
        usage__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "usage"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor LogicalUsage)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'usage")) ::
              Data.ProtoLens.FieldDescriptor RunEvent
        terminal__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "terminal"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor RunTerminal)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'terminal")) ::
              Data.ProtoLens.FieldDescriptor RunEvent
        progress__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "progress"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RunProgress)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'progress")) ::
              Data.ProtoLens.FieldDescriptor RunEvent
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, sequence__field_descriptor),
           (Data.ProtoLens.Tag 2, output__field_descriptor),
           (Data.ProtoLens.Tag 3, usage__field_descriptor),
           (Data.ProtoLens.Tag 4, terminal__field_descriptor),
           (Data.ProtoLens.Tag 5, progress__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RunEvent'_unknownFields
        (\ x__ y__ -> x__ {_RunEvent'_unknownFields = y__})
  defMessage
    = RunEvent'_constructor
        {_RunEvent'sequence = Data.ProtoLens.fieldDefault,
         _RunEvent'event = Prelude.Nothing, _RunEvent'_unknownFields = []}
  parseMessage
    = let
        loop :: RunEvent -> Data.ProtoLens.Encoding.Bytes.Parser RunEvent
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
                                       "output"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"output") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "usage"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"usage") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "terminal"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"terminal") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "progress"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"progress") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RunEvent"
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'event") _x
                 of
                   Prelude.Nothing -> Data.Monoid.mempty
                   (Prelude.Just (RunEvent'Output v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 18)
                          ((\ bs
                              -> (Data.Monoid.<>)
                                   (Data.ProtoLens.Encoding.Bytes.putVarInt
                                      (Prelude.fromIntegral (Data.ByteString.length bs)))
                                   (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             v)
                   (Prelude.Just (RunEvent'Usage v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 26)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v)
                   (Prelude.Just (RunEvent'Terminal v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                          ((Prelude..)
                             ((Prelude..)
                                Data.ProtoLens.Encoding.Bytes.putVarInt Prelude.fromIntegral)
                             Prelude.fromEnum v)
                   (Prelude.Just (RunEvent'Progress v))
                     -> (Data.Monoid.<>)
                          (Data.ProtoLens.Encoding.Bytes.putVarInt 42)
                          ((Prelude..)
                             (\ bs
                                -> (Data.Monoid.<>)
                                     (Data.ProtoLens.Encoding.Bytes.putVarInt
                                        (Prelude.fromIntegral (Data.ByteString.length bs)))
                                     (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                             Data.ProtoLens.encodeMessage v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData RunEvent where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RunEvent'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RunEvent'sequence x__)
                (Control.DeepSeq.deepseq (_RunEvent'event x__) ()))
instance Control.DeepSeq.NFData RunEvent'Event where
  rnf (RunEvent'Output x__) = Control.DeepSeq.rnf x__
  rnf (RunEvent'Usage x__) = Control.DeepSeq.rnf x__
  rnf (RunEvent'Terminal x__) = Control.DeepSeq.rnf x__
  rnf (RunEvent'Progress x__) = Control.DeepSeq.rnf x__
_RunEvent'Output ::
  Data.ProtoLens.Prism.Prism' RunEvent'Event Data.ByteString.ByteString
_RunEvent'Output
  = Data.ProtoLens.Prism.prism'
      RunEvent'Output
      (\ p__
         -> case p__ of
              (RunEvent'Output p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RunEvent'Usage ::
  Data.ProtoLens.Prism.Prism' RunEvent'Event LogicalUsage
_RunEvent'Usage
  = Data.ProtoLens.Prism.prism'
      RunEvent'Usage
      (\ p__
         -> case p__ of
              (RunEvent'Usage p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RunEvent'Terminal ::
  Data.ProtoLens.Prism.Prism' RunEvent'Event RunTerminal
_RunEvent'Terminal
  = Data.ProtoLens.Prism.prism'
      RunEvent'Terminal
      (\ p__
         -> case p__ of
              (RunEvent'Terminal p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
_RunEvent'Progress ::
  Data.ProtoLens.Prism.Prism' RunEvent'Event RunProgress
_RunEvent'Progress
  = Data.ProtoLens.Prism.prism'
      RunEvent'Progress
      (\ p__
         -> case p__ of
              (RunEvent'Progress p__val) -> Prelude.Just p__val
              _otherwise -> Prelude.Nothing)
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.source' @:: Lens' RunInputProvenance Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.runId' @:: Lens' RunInputProvenance Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maximumOutput' @:: Lens' RunInputProvenance Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.seed' @:: Lens' RunInputProvenance Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.maybe'seed' @:: Lens' RunInputProvenance (Prelude.Maybe Data.Word.Word64)@ -}
data RunInputProvenance
  = RunInputProvenance'_constructor {_RunInputProvenance'source :: !Data.ByteString.ByteString,
                                     _RunInputProvenance'runId :: !Data.ByteString.ByteString,
                                     _RunInputProvenance'maximumOutput :: !Data.Word.Word64,
                                     _RunInputProvenance'seed :: !(Prelude.Maybe Data.Word.Word64),
                                     _RunInputProvenance'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RunInputProvenance where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RunInputProvenance "source" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunInputProvenance'source
           (\ x__ y__ -> x__ {_RunInputProvenance'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunInputProvenance "runId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunInputProvenance'runId
           (\ x__ y__ -> x__ {_RunInputProvenance'runId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunInputProvenance "maximumOutput" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunInputProvenance'maximumOutput
           (\ x__ y__ -> x__ {_RunInputProvenance'maximumOutput = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunInputProvenance "seed" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunInputProvenance'seed
           (\ x__ y__ -> x__ {_RunInputProvenance'seed = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField RunInputProvenance "maybe'seed" (Prelude.Maybe Data.Word.Word64) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunInputProvenance'seed
           (\ x__ y__ -> x__ {_RunInputProvenance'seed = y__}))
        Prelude.id
instance Data.ProtoLens.Message RunInputProvenance where
  messageName _
    = Data.Text.pack "inference.customer.v1.RunInputProvenance"
  packedMessageDescriptor _
    = "\n\
      \\DC2RunInputProvenance\DC2\FS\n\
      \\ACKsource\CAN\SOH \SOH(\fR\ACKsourceB\EOT\200\243\CAN \DC2\ESC\n\
      \\ACKrun_id\CAN\STX \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2+\n\
      \\SOmaximum_output\CAN\ETX \SOH(\EOTR\rmaximumOutputB\EOT\216\243\CAN\SOH\DC2\ETB\n\
      \\EOTseed\CAN\EOT \SOH(\EOTH\NULR\EOTseed\136\SOH\SOHB\a\n\
      \\ENQ_seed"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor RunInputProvenance
        runId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"runId")) ::
              Data.ProtoLens.FieldDescriptor RunInputProvenance
        maximumOutput__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "maximum_output"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"maximumOutput")) ::
              Data.ProtoLens.FieldDescriptor RunInputProvenance
        seed__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "seed"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'seed")) ::
              Data.ProtoLens.FieldDescriptor RunInputProvenance
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, runId__field_descriptor),
           (Data.ProtoLens.Tag 3, maximumOutput__field_descriptor),
           (Data.ProtoLens.Tag 4, seed__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RunInputProvenance'_unknownFields
        (\ x__ y__ -> x__ {_RunInputProvenance'_unknownFields = y__})
  defMessage
    = RunInputProvenance'_constructor
        {_RunInputProvenance'source = Data.ProtoLens.fieldDefault,
         _RunInputProvenance'runId = Data.ProtoLens.fieldDefault,
         _RunInputProvenance'maximumOutput = Data.ProtoLens.fieldDefault,
         _RunInputProvenance'seed = Prelude.Nothing,
         _RunInputProvenance'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RunInputProvenance
          -> Data.ProtoLens.Encoding.Bytes.Parser RunInputProvenance
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
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "run_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"runId") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "maximum_output"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"maximumOutput") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "seed"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"seed") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RunInputProvenance"
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
                      ((\ bs
                          -> (Data.Monoid.<>)
                               (Data.ProtoLens.Encoding.Bytes.putVarInt
                                  (Prelude.fromIntegral (Data.ByteString.length bs)))
                               (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                         _v))
             ((Data.Monoid.<>)
                (let
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"runId") _x
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
                            (Data.ProtoLens.Field.field @"maximumOutput") _x
                    in
                      if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                          Data.Monoid.mempty
                      else
                          (Data.Monoid.<>)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt 24)
                            (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                   ((Data.Monoid.<>)
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'seed") _x
                       of
                         Prelude.Nothing -> Data.Monoid.mempty
                         (Prelude.Just _v)
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt 32)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                      (Data.ProtoLens.Encoding.Wire.buildFieldSet
                         (Lens.Family2.view Data.ProtoLens.unknownFields _x)))))
instance Control.DeepSeq.NFData RunInputProvenance where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RunInputProvenance'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RunInputProvenance'source x__)
                (Control.DeepSeq.deepseq
                   (_RunInputProvenance'runId x__)
                   (Control.DeepSeq.deepseq
                      (_RunInputProvenance'maximumOutput x__)
                      (Control.DeepSeq.deepseq (_RunInputProvenance'seed x__) ()))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.kind' @:: Lens' RunProgress Data.Text.Text@ -}
data RunProgress
  = RunProgress'_constructor {_RunProgress'kind :: !Data.Text.Text,
                              _RunProgress'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RunProgress where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RunProgress "kind" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunProgress'kind (\ x__ y__ -> x__ {_RunProgress'kind = y__}))
        Prelude.id
instance Data.ProtoLens.Message RunProgress where
  messageName _ = Data.Text.pack "inference.customer.v1.RunProgress"
  packedMessageDescriptor _
    = "\n\
      \\vRunProgress\DC2\DC2\n\
      \\EOTkind\CAN\SOH \SOH(\tR\EOTkind"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        kind__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "kind"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"kind")) ::
              Data.ProtoLens.FieldDescriptor RunProgress
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, kind__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RunProgress'_unknownFields
        (\ x__ y__ -> x__ {_RunProgress'_unknownFields = y__})
  defMessage
    = RunProgress'_constructor
        {_RunProgress'kind = Data.ProtoLens.fieldDefault,
         _RunProgress'_unknownFields = []}
  parseMessage
    = let
        loop ::
          RunProgress -> Data.ProtoLens.Encoding.Bytes.Parser RunProgress
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
                                       "kind"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"kind") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RunProgress"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let _v = Lens.Family2.view (Data.ProtoLens.Field.field @"kind") _x
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
instance Control.DeepSeq.NFData RunProgress where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RunProgress'_unknownFields x__)
             (Control.DeepSeq.deepseq (_RunProgress'kind x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.output' @:: Lens' RunResult Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.context' @:: Lens' RunResult ContextView@
         * 'Proto.Inference.V1.Inference_Fields.maybe'context' @:: Lens' RunResult (Prelude.Maybe ContextView)@
         * 'Proto.Inference.V1.Inference_Fields.terminal' @:: Lens' RunResult RunTerminal@
         * 'Proto.Inference.V1.Inference_Fields.receipt' @:: Lens' RunResult UsageReceipt@
         * 'Proto.Inference.V1.Inference_Fields.maybe'receipt' @:: Lens' RunResult (Prelude.Maybe UsageReceipt)@ -}
data RunResult
  = RunResult'_constructor {_RunResult'output :: !Data.ByteString.ByteString,
                            _RunResult'context :: !(Prelude.Maybe ContextView),
                            _RunResult'terminal :: !RunTerminal,
                            _RunResult'receipt :: !(Prelude.Maybe UsageReceipt),
                            _RunResult'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RunResult where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RunResult "output" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunResult'output (\ x__ y__ -> x__ {_RunResult'output = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunResult "context" ContextView where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunResult'context (\ x__ y__ -> x__ {_RunResult'context = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RunResult "maybe'context" (Prelude.Maybe ContextView) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunResult'context (\ x__ y__ -> x__ {_RunResult'context = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunResult "terminal" RunTerminal where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunResult'terminal (\ x__ y__ -> x__ {_RunResult'terminal = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunResult "receipt" UsageReceipt where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunResult'receipt (\ x__ y__ -> x__ {_RunResult'receipt = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RunResult "maybe'receipt" (Prelude.Maybe UsageReceipt) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunResult'receipt (\ x__ y__ -> x__ {_RunResult'receipt = y__}))
        Prelude.id
instance Data.ProtoLens.Message RunResult where
  messageName _ = Data.Text.pack "inference.customer.v1.RunResult"
  packedMessageDescriptor _
    = "\n\
      \\tRunResult\DC2\SYN\n\
      \\ACKoutput\CAN\SOH \SOH(\fR\ACKoutput\DC2A\n\
      \\acontext\CAN\STX \SOH(\v2\".inference.customer.v1.ContextViewH\NULR\acontext\136\SOH\SOH\DC2D\n\
      \\bterminal\CAN\ETX \SOH(\SO2\".inference.customer.v1.RunTerminalR\bterminalB\EOT\136\244\CAN\SOH\DC2B\n\
      \\areceipt\CAN\EOT \SOH(\v2#.inference.customer.v1.UsageReceiptH\SOHR\areceipt\136\SOH\SOHB\n\
      \\n\
      \\b_contextB\n\
      \\n\
      \\b_receipt"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        output__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "output"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"output")) ::
              Data.ProtoLens.FieldDescriptor RunResult
        context__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "context"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ContextView)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'context")) ::
              Data.ProtoLens.FieldDescriptor RunResult
        terminal__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "terminal"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor RunTerminal)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"terminal")) ::
              Data.ProtoLens.FieldDescriptor RunResult
        receipt__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "receipt"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor UsageReceipt)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'receipt")) ::
              Data.ProtoLens.FieldDescriptor RunResult
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, output__field_descriptor),
           (Data.ProtoLens.Tag 2, context__field_descriptor),
           (Data.ProtoLens.Tag 3, terminal__field_descriptor),
           (Data.ProtoLens.Tag 4, receipt__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RunResult'_unknownFields
        (\ x__ y__ -> x__ {_RunResult'_unknownFields = y__})
  defMessage
    = RunResult'_constructor
        {_RunResult'output = Data.ProtoLens.fieldDefault,
         _RunResult'context = Prelude.Nothing,
         _RunResult'terminal = Data.ProtoLens.fieldDefault,
         _RunResult'receipt = Prelude.Nothing,
         _RunResult'_unknownFields = []}
  parseMessage
    = let
        loop :: RunResult -> Data.ProtoLens.Encoding.Bytes.Parser RunResult
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
                                       "output"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"output") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "context"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"context") y x)
                        24
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "terminal"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"terminal") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
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
          (do loop Data.ProtoLens.defMessage) "RunResult"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"output") _x
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
                (case
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'context") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"terminal") _x
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
                      (case
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'receipt") _x
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
instance Control.DeepSeq.NFData RunResult where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RunResult'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RunResult'output x__)
                (Control.DeepSeq.deepseq
                   (_RunResult'context x__)
                   (Control.DeepSeq.deepseq
                      (_RunResult'terminal x__)
                      (Control.DeepSeq.deepseq (_RunResult'receipt x__) ()))))
newtype RunTerminal'UnrecognizedValue
  = RunTerminal'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data RunTerminal
  = RUN_TERMINAL_UNSPECIFIED |
    RUN_TERMINAL_COMPLETED |
    RUN_TERMINAL_OUTPUT_LIMITED |
    RUN_TERMINAL_TOOL_CALL |
    RUN_TERMINAL_REFUSAL |
    RUN_TERMINAL_CANCELLED |
    RUN_TERMINAL_FAILED |
    RUN_TERMINAL_INDETERMINATE |
    RunTerminal'Unrecognized !RunTerminal'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum RunTerminal where
  maybeToEnum 0 = Prelude.Just RUN_TERMINAL_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just RUN_TERMINAL_COMPLETED
  maybeToEnum 2 = Prelude.Just RUN_TERMINAL_OUTPUT_LIMITED
  maybeToEnum 3 = Prelude.Just RUN_TERMINAL_TOOL_CALL
  maybeToEnum 4 = Prelude.Just RUN_TERMINAL_REFUSAL
  maybeToEnum 5 = Prelude.Just RUN_TERMINAL_CANCELLED
  maybeToEnum 6 = Prelude.Just RUN_TERMINAL_FAILED
  maybeToEnum 7 = Prelude.Just RUN_TERMINAL_INDETERMINATE
  maybeToEnum k
    = Prelude.Just
        (RunTerminal'Unrecognized
           (RunTerminal'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum RUN_TERMINAL_UNSPECIFIED = "RUN_TERMINAL_UNSPECIFIED"
  showEnum RUN_TERMINAL_COMPLETED = "RUN_TERMINAL_COMPLETED"
  showEnum RUN_TERMINAL_OUTPUT_LIMITED
    = "RUN_TERMINAL_OUTPUT_LIMITED"
  showEnum RUN_TERMINAL_TOOL_CALL = "RUN_TERMINAL_TOOL_CALL"
  showEnum RUN_TERMINAL_REFUSAL = "RUN_TERMINAL_REFUSAL"
  showEnum RUN_TERMINAL_CANCELLED = "RUN_TERMINAL_CANCELLED"
  showEnum RUN_TERMINAL_FAILED = "RUN_TERMINAL_FAILED"
  showEnum RUN_TERMINAL_INDETERMINATE = "RUN_TERMINAL_INDETERMINATE"
  showEnum
    (RunTerminal'Unrecognized (RunTerminal'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "RUN_TERMINAL_UNSPECIFIED"
    = Prelude.Just RUN_TERMINAL_UNSPECIFIED
    | (Prelude.==) k "RUN_TERMINAL_COMPLETED"
    = Prelude.Just RUN_TERMINAL_COMPLETED
    | (Prelude.==) k "RUN_TERMINAL_OUTPUT_LIMITED"
    = Prelude.Just RUN_TERMINAL_OUTPUT_LIMITED
    | (Prelude.==) k "RUN_TERMINAL_TOOL_CALL"
    = Prelude.Just RUN_TERMINAL_TOOL_CALL
    | (Prelude.==) k "RUN_TERMINAL_REFUSAL"
    = Prelude.Just RUN_TERMINAL_REFUSAL
    | (Prelude.==) k "RUN_TERMINAL_CANCELLED"
    = Prelude.Just RUN_TERMINAL_CANCELLED
    | (Prelude.==) k "RUN_TERMINAL_FAILED"
    = Prelude.Just RUN_TERMINAL_FAILED
    | (Prelude.==) k "RUN_TERMINAL_INDETERMINATE"
    = Prelude.Just RUN_TERMINAL_INDETERMINATE
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded RunTerminal where
  minBound = RUN_TERMINAL_UNSPECIFIED
  maxBound = RUN_TERMINAL_INDETERMINATE
instance Prelude.Enum RunTerminal where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum RunTerminal: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum RUN_TERMINAL_UNSPECIFIED = 0
  fromEnum RUN_TERMINAL_COMPLETED = 1
  fromEnum RUN_TERMINAL_OUTPUT_LIMITED = 2
  fromEnum RUN_TERMINAL_TOOL_CALL = 3
  fromEnum RUN_TERMINAL_REFUSAL = 4
  fromEnum RUN_TERMINAL_CANCELLED = 5
  fromEnum RUN_TERMINAL_FAILED = 6
  fromEnum RUN_TERMINAL_INDETERMINATE = 7
  fromEnum
    (RunTerminal'Unrecognized (RunTerminal'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ RUN_TERMINAL_INDETERMINATE
    = Prelude.error
        "RunTerminal.succ: bad argument RUN_TERMINAL_INDETERMINATE. This value would be out of bounds."
  succ RUN_TERMINAL_UNSPECIFIED = RUN_TERMINAL_COMPLETED
  succ RUN_TERMINAL_COMPLETED = RUN_TERMINAL_OUTPUT_LIMITED
  succ RUN_TERMINAL_OUTPUT_LIMITED = RUN_TERMINAL_TOOL_CALL
  succ RUN_TERMINAL_TOOL_CALL = RUN_TERMINAL_REFUSAL
  succ RUN_TERMINAL_REFUSAL = RUN_TERMINAL_CANCELLED
  succ RUN_TERMINAL_CANCELLED = RUN_TERMINAL_FAILED
  succ RUN_TERMINAL_FAILED = RUN_TERMINAL_INDETERMINATE
  succ (RunTerminal'Unrecognized _)
    = Prelude.error
        "RunTerminal.succ: bad argument: unrecognized value"
  pred RUN_TERMINAL_UNSPECIFIED
    = Prelude.error
        "RunTerminal.pred: bad argument RUN_TERMINAL_UNSPECIFIED. This value would be out of bounds."
  pred RUN_TERMINAL_COMPLETED = RUN_TERMINAL_UNSPECIFIED
  pred RUN_TERMINAL_OUTPUT_LIMITED = RUN_TERMINAL_COMPLETED
  pred RUN_TERMINAL_TOOL_CALL = RUN_TERMINAL_OUTPUT_LIMITED
  pred RUN_TERMINAL_REFUSAL = RUN_TERMINAL_TOOL_CALL
  pred RUN_TERMINAL_CANCELLED = RUN_TERMINAL_REFUSAL
  pred RUN_TERMINAL_FAILED = RUN_TERMINAL_CANCELLED
  pred RUN_TERMINAL_INDETERMINATE = RUN_TERMINAL_FAILED
  pred (RunTerminal'Unrecognized _)
    = Prelude.error
        "RunTerminal.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault RunTerminal where
  fieldDefault = RUN_TERMINAL_UNSPECIFIED
instance Control.DeepSeq.NFData RunTerminal where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.runId' @:: Lens' RunView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.input' @:: Lens' RunView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.model' @:: Lens' RunView Data.Text.Text@
         * 'Proto.Inference.V1.Inference_Fields.lastSequence' @:: Lens' RunView Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.cancellationRequested' @:: Lens' RunView Prelude.Bool@
         * 'Proto.Inference.V1.Inference_Fields.result' @:: Lens' RunView RunResult@
         * 'Proto.Inference.V1.Inference_Fields.maybe'result' @:: Lens' RunView (Prelude.Maybe RunResult)@ -}
data RunView
  = RunView'_constructor {_RunView'runId :: !Data.ByteString.ByteString,
                          _RunView'input :: !Data.ByteString.ByteString,
                          _RunView'model :: !Data.Text.Text,
                          _RunView'lastSequence :: !Data.Word.Word64,
                          _RunView'cancellationRequested :: !Prelude.Bool,
                          _RunView'result :: !(Prelude.Maybe RunResult),
                          _RunView'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show RunView where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField RunView "runId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'runId (\ x__ y__ -> x__ {_RunView'runId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunView "input" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'input (\ x__ y__ -> x__ {_RunView'input = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunView "model" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'model (\ x__ y__ -> x__ {_RunView'model = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunView "lastSequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'lastSequence
           (\ x__ y__ -> x__ {_RunView'lastSequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunView "cancellationRequested" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'cancellationRequested
           (\ x__ y__ -> x__ {_RunView'cancellationRequested = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField RunView "result" RunResult where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'result (\ x__ y__ -> x__ {_RunView'result = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField RunView "maybe'result" (Prelude.Maybe RunResult) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _RunView'result (\ x__ y__ -> x__ {_RunView'result = y__}))
        Prelude.id
instance Data.ProtoLens.Message RunView where
  messageName _ = Data.Text.pack "inference.customer.v1.RunView"
  packedMessageDescriptor _
    = "\n\
      \\aRunView\DC2\ESC\n\
      \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2\SUB\n\
      \\ENQinput\CAN\STX \SOH(\fR\ENQinputB\EOT\200\243\CAN \DC2\ESC\n\
      \\ENQmodel\CAN\ETX \SOH(\tR\ENQmodelB\ENQ\248\243\CAN\128\STX\DC2#\n\
      \\rlast_sequence\CAN\EOT \SOH(\EOTR\flastSequence\DC25\n\
      \\SYNcancellation_requested\CAN\ENQ \SOH(\bR\NAKcancellationRequested\DC2=\n\
      \\ACKresult\CAN\ACK \SOH(\v2 .inference.customer.v1.RunResultH\NULR\ACKresult\136\SOH\SOHB\t\n\
      \\a_result"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        runId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"runId")) ::
              Data.ProtoLens.FieldDescriptor RunView
        input__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "input"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"input")) ::
              Data.ProtoLens.FieldDescriptor RunView
        model__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"model")) ::
              Data.ProtoLens.FieldDescriptor RunView
        lastSequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "last_sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"lastSequence")) ::
              Data.ProtoLens.FieldDescriptor RunView
        cancellationRequested__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "cancellation_requested"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"cancellationRequested")) ::
              Data.ProtoLens.FieldDescriptor RunView
        result__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "result"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor RunResult)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'result")) ::
              Data.ProtoLens.FieldDescriptor RunView
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, runId__field_descriptor),
           (Data.ProtoLens.Tag 2, input__field_descriptor),
           (Data.ProtoLens.Tag 3, model__field_descriptor),
           (Data.ProtoLens.Tag 4, lastSequence__field_descriptor),
           (Data.ProtoLens.Tag 5, cancellationRequested__field_descriptor),
           (Data.ProtoLens.Tag 6, result__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _RunView'_unknownFields
        (\ x__ y__ -> x__ {_RunView'_unknownFields = y__})
  defMessage
    = RunView'_constructor
        {_RunView'runId = Data.ProtoLens.fieldDefault,
         _RunView'input = Data.ProtoLens.fieldDefault,
         _RunView'model = Data.ProtoLens.fieldDefault,
         _RunView'lastSequence = Data.ProtoLens.fieldDefault,
         _RunView'cancellationRequested = Data.ProtoLens.fieldDefault,
         _RunView'result = Prelude.Nothing, _RunView'_unknownFields = []}
  parseMessage
    = let
        loop :: RunView -> Data.ProtoLens.Encoding.Bytes.Parser RunView
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
                                       "run_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"runId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "input"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"input") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "model"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"model") y x)
                        32
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "last_sequence"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"lastSequence") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "cancellation_requested"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"cancellationRequested") y x)
                        50
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "result"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"result") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "RunView"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"runId") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"input") _x
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
                      _v = Lens.Family2.view (Data.ProtoLens.Field.field @"model") _x
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
                           = Lens.Family2.view (Data.ProtoLens.Field.field @"lastSequence") _x
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
                                  (Data.ProtoLens.Field.field @"cancellationRequested") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  ((Prelude..)
                                     Data.ProtoLens.Encoding.Bytes.putVarInt
                                     (\ b -> if b then 1 else 0) _v))
                         ((Data.Monoid.<>)
                            (case
                                 Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'result") _x
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
instance Control.DeepSeq.NFData RunView where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_RunView'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_RunView'runId x__)
                (Control.DeepSeq.deepseq
                   (_RunView'input x__)
                   (Control.DeepSeq.deepseq
                      (_RunView'model x__)
                      (Control.DeepSeq.deepseq
                         (_RunView'lastSequence x__)
                         (Control.DeepSeq.deepseq
                            (_RunView'cancellationRequested x__)
                            (Control.DeepSeq.deepseq (_RunView'result x__) ()))))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.model' @:: Lens' Transfer Data.Text.Text@ -}
data Transfer
  = Transfer'_constructor {_Transfer'model :: !Data.Text.Text,
                           _Transfer'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Transfer where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Transfer "model" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Transfer'model (\ x__ y__ -> x__ {_Transfer'model = y__}))
        Prelude.id
instance Data.ProtoLens.Message Transfer where
  messageName _ = Data.Text.pack "inference.customer.v1.Transfer"
  packedMessageDescriptor _
    = "\n\
      \\bTransfer\DC2\DC4\n\
      \\ENQmodel\CAN\SOH \SOH(\tR\ENQmodel"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        model__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"model")) ::
              Data.ProtoLens.FieldDescriptor Transfer
      in
        Data.Map.fromList [(Data.ProtoLens.Tag 1, model__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Transfer'_unknownFields
        (\ x__ y__ -> x__ {_Transfer'_unknownFields = y__})
  defMessage
    = Transfer'_constructor
        {_Transfer'model = Data.ProtoLens.fieldDefault,
         _Transfer'_unknownFields = []}
  parseMessage
    = let
        loop :: Transfer -> Data.ProtoLens.Encoding.Bytes.Parser Transfer
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
                                       "model"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"model") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Transfer"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"model") _x
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
instance Control.DeepSeq.NFData Transfer where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Transfer'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Transfer'model x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.source' @:: Lens' TransferProvenance Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.reusedCompatibleState' @:: Lens' TransferProvenance Prelude.Bool@ -}
data TransferProvenance
  = TransferProvenance'_constructor {_TransferProvenance'source :: !Data.ByteString.ByteString,
                                     _TransferProvenance'reusedCompatibleState :: !Prelude.Bool,
                                     _TransferProvenance'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show TransferProvenance where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField TransferProvenance "source" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TransferProvenance'source
           (\ x__ y__ -> x__ {_TransferProvenance'source = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField TransferProvenance "reusedCompatibleState" Prelude.Bool where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _TransferProvenance'reusedCompatibleState
           (\ x__ y__
              -> x__ {_TransferProvenance'reusedCompatibleState = y__}))
        Prelude.id
instance Data.ProtoLens.Message TransferProvenance where
  messageName _
    = Data.Text.pack "inference.customer.v1.TransferProvenance"
  packedMessageDescriptor _
    = "\n\
      \\DC2TransferProvenance\DC2\FS\n\
      \\ACKsource\CAN\SOH \SOH(\fR\ACKsourceB\EOT\200\243\CAN \DC26\n\
      \\ETBreused_compatible_state\CAN\STX \SOH(\bR\NAKreusedCompatibleState"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        source__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "source"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"source")) ::
              Data.ProtoLens.FieldDescriptor TransferProvenance
        reusedCompatibleState__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "reused_compatible_state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BoolField ::
                 Data.ProtoLens.FieldTypeDescriptor Prelude.Bool)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"reusedCompatibleState")) ::
              Data.ProtoLens.FieldDescriptor TransferProvenance
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, source__field_descriptor),
           (Data.ProtoLens.Tag 2, reusedCompatibleState__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _TransferProvenance'_unknownFields
        (\ x__ y__ -> x__ {_TransferProvenance'_unknownFields = y__})
  defMessage
    = TransferProvenance'_constructor
        {_TransferProvenance'source = Data.ProtoLens.fieldDefault,
         _TransferProvenance'reusedCompatibleState = Data.ProtoLens.fieldDefault,
         _TransferProvenance'_unknownFields = []}
  parseMessage
    = let
        loop ::
          TransferProvenance
          -> Data.ProtoLens.Encoding.Bytes.Parser TransferProvenance
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
                                       "source"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"source") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          ((Prelude./=) 0) Data.ProtoLens.Encoding.Bytes.getVarInt)
                                       "reused_compatible_state"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"reusedCompatibleState") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "TransferProvenance"
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
                         (Data.ProtoLens.Field.field @"reusedCompatibleState") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         ((Prelude..)
                            Data.ProtoLens.Encoding.Bytes.putVarInt (\ b -> if b then 1 else 0)
                            _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData TransferProvenance where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_TransferProvenance'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_TransferProvenance'source x__)
                (Control.DeepSeq.deepseq
                   (_TransferProvenance'reusedCompatibleState x__) ()))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.through' @:: Lens' Truncate Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.maybe'through' @:: Lens' Truncate (Prelude.Maybe Data.ByteString.ByteString)@ -}
data Truncate
  = Truncate'_constructor {_Truncate'through :: !(Prelude.Maybe Data.ByteString.ByteString),
                           _Truncate'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Truncate where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Truncate "through" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Truncate'through (\ x__ y__ -> x__ {_Truncate'through = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.fieldDefault)
instance Data.ProtoLens.Field.HasField Truncate "maybe'through" (Prelude.Maybe Data.ByteString.ByteString) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Truncate'through (\ x__ y__ -> x__ {_Truncate'through = y__}))
        Prelude.id
instance Data.ProtoLens.Message Truncate where
  messageName _ = Data.Text.pack "inference.customer.v1.Truncate"
  packedMessageDescriptor _
    = "\n\
      \\bTruncate\DC2\GS\n\
      \\athrough\CAN\SOH \SOH(\fH\NULR\athrough\136\SOH\SOHB\n\
      \\n\
      \\b_through"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        through__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "through"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'through")) ::
              Data.ProtoLens.FieldDescriptor Truncate
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, through__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Truncate'_unknownFields
        (\ x__ y__ -> x__ {_Truncate'_unknownFields = y__})
  defMessage
    = Truncate'_constructor
        {_Truncate'through = Prelude.Nothing,
         _Truncate'_unknownFields = []}
  parseMessage
    = let
        loop :: Truncate -> Data.ProtoLens.Encoding.Bytes.Parser Truncate
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
                                       "through"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"through") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "Truncate"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (case
                  Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'through") _x
              of
                Prelude.Nothing -> Data.Monoid.mempty
                (Prelude.Just _v)
                  -> (Data.Monoid.<>)
                       (Data.ProtoLens.Encoding.Bytes.putVarInt 10)
                       ((\ bs
                           -> (Data.Monoid.<>)
                                (Data.ProtoLens.Encoding.Bytes.putVarInt
                                   (Prelude.fromIntegral (Data.ByteString.length bs)))
                                (Data.ProtoLens.Encoding.Bytes.putBytes bs))
                          _v))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData Truncate where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Truncate'_unknownFields x__)
             (Control.DeepSeq.deepseq (_Truncate'through x__) ())
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.receiptId' @:: Lens' UsageReceipt Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.modelProfile' @:: Lens' UsageReceipt Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.meterRevision' @:: Lens' UsageReceipt Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.usage' @:: Lens' UsageReceipt LogicalUsage@
         * 'Proto.Inference.V1.Inference_Fields.maybe'usage' @:: Lens' UsageReceipt (Prelude.Maybe LogicalUsage)@
         * 'Proto.Inference.V1.Inference_Fields.rateCardRevision' @:: Lens' UsageReceipt Data.ByteString.ByteString@ -}
data UsageReceipt
  = UsageReceipt'_constructor {_UsageReceipt'receiptId :: !Data.ByteString.ByteString,
                               _UsageReceipt'modelProfile :: !Data.ByteString.ByteString,
                               _UsageReceipt'meterRevision :: !Data.ByteString.ByteString,
                               _UsageReceipt'usage :: !(Prelude.Maybe LogicalUsage),
                               _UsageReceipt'rateCardRevision :: !Data.ByteString.ByteString,
                               _UsageReceipt'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show UsageReceipt where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField UsageReceipt "receiptId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'receiptId
           (\ x__ y__ -> x__ {_UsageReceipt'receiptId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "modelProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'modelProfile
           (\ x__ y__ -> x__ {_UsageReceipt'modelProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "meterRevision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'meterRevision
           (\ x__ y__ -> x__ {_UsageReceipt'meterRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "usage" LogicalUsage where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'usage (\ x__ y__ -> x__ {_UsageReceipt'usage = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField UsageReceipt "maybe'usage" (Prelude.Maybe LogicalUsage) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'usage (\ x__ y__ -> x__ {_UsageReceipt'usage = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField UsageReceipt "rateCardRevision" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _UsageReceipt'rateCardRevision
           (\ x__ y__ -> x__ {_UsageReceipt'rateCardRevision = y__}))
        Prelude.id
instance Data.ProtoLens.Message UsageReceipt where
  messageName _ = Data.Text.pack "inference.customer.v1.UsageReceipt"
  packedMessageDescriptor _
    = "\n\
      \\fUsageReceipt\DC2#\n\
      \\n\
      \receipt_id\CAN\SOH \SOH(\fR\treceiptIdB\EOT\200\243\CAN \DC2)\n\
      \\rmodel_profile\CAN\STX \SOH(\fR\fmodelProfileB\EOT\200\243\CAN \DC2+\n\
      \\SOmeter_revision\CAN\ETX \SOH(\fR\rmeterRevisionB\EOT\200\243\CAN \DC2?\n\
      \\ENQusage\CAN\EOT \SOH(\v2#.inference.customer.v1.LogicalUsageR\ENQusageB\EOT\208\243\CAN\SOH\DC22\n\
      \\DC2rate_card_revision\CAN\ENQ \SOH(\fR\DLErateCardRevisionB\EOT\200\243\CAN "
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        receiptId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "receipt_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"receiptId")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        modelProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"modelProfile")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        meterRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "meter_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"meterRevision")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        usage__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "usage"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor LogicalUsage)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'usage")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
        rateCardRevision__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "rate_card_revision"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"rateCardRevision")) ::
              Data.ProtoLens.FieldDescriptor UsageReceipt
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, receiptId__field_descriptor),
           (Data.ProtoLens.Tag 2, modelProfile__field_descriptor),
           (Data.ProtoLens.Tag 3, meterRevision__field_descriptor),
           (Data.ProtoLens.Tag 4, usage__field_descriptor),
           (Data.ProtoLens.Tag 5, rateCardRevision__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _UsageReceipt'_unknownFields
        (\ x__ y__ -> x__ {_UsageReceipt'_unknownFields = y__})
  defMessage
    = UsageReceipt'_constructor
        {_UsageReceipt'receiptId = Data.ProtoLens.fieldDefault,
         _UsageReceipt'modelProfile = Data.ProtoLens.fieldDefault,
         _UsageReceipt'meterRevision = Data.ProtoLens.fieldDefault,
         _UsageReceipt'usage = Prelude.Nothing,
         _UsageReceipt'rateCardRevision = Data.ProtoLens.fieldDefault,
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
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "receipt_id"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"receiptId") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "model_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"modelProfile") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "meter_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"meterRevision") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "usage"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"usage") y x)
                        42
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "rate_card_revision"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"rateCardRevision") y x)
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
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"receiptId") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"modelProfile") _x
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
                            (Data.ProtoLens.Field.field @"meterRevision") _x
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
                           Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'usage") _x
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
                                  (Data.ProtoLens.Field.field @"rateCardRevision") _x
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
instance Control.DeepSeq.NFData UsageReceipt where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_UsageReceipt'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_UsageReceipt'receiptId x__)
                (Control.DeepSeq.deepseq
                   (_UsageReceipt'modelProfile x__)
                   (Control.DeepSeq.deepseq
                      (_UsageReceipt'meterRevision x__)
                      (Control.DeepSeq.deepseq
                         (_UsageReceipt'usage x__)
                         (Control.DeepSeq.deepseq
                            (_UsageReceipt'rateCardRevision x__) ())))))
newtype WarmState'UnrecognizedValue
  = WarmState'UnrecognizedValue Data.Int.Int32
  deriving stock (Prelude.Eq, Prelude.Ord, Prelude.Show)
data WarmState
  = WARM_STATE_UNSPECIFIED |
    WARM_STATE_ACTIVE |
    WARM_STATE_EXPIRED |
    WARM_STATE_BREACHED |
    WARM_STATE_RELEASED |
    WarmState'Unrecognized !WarmState'UnrecognizedValue
  deriving stock (Prelude.Show, Prelude.Eq, Prelude.Ord)
instance Data.ProtoLens.MessageEnum WarmState where
  maybeToEnum 0 = Prelude.Just WARM_STATE_UNSPECIFIED
  maybeToEnum 1 = Prelude.Just WARM_STATE_ACTIVE
  maybeToEnum 2 = Prelude.Just WARM_STATE_EXPIRED
  maybeToEnum 3 = Prelude.Just WARM_STATE_BREACHED
  maybeToEnum 4 = Prelude.Just WARM_STATE_RELEASED
  maybeToEnum k
    = Prelude.Just
        (WarmState'Unrecognized
           (WarmState'UnrecognizedValue (Prelude.fromIntegral k)))
  showEnum WARM_STATE_UNSPECIFIED = "WARM_STATE_UNSPECIFIED"
  showEnum WARM_STATE_ACTIVE = "WARM_STATE_ACTIVE"
  showEnum WARM_STATE_EXPIRED = "WARM_STATE_EXPIRED"
  showEnum WARM_STATE_BREACHED = "WARM_STATE_BREACHED"
  showEnum WARM_STATE_RELEASED = "WARM_STATE_RELEASED"
  showEnum (WarmState'Unrecognized (WarmState'UnrecognizedValue k))
    = Prelude.show k
  readEnum k
    | (Prelude.==) k "WARM_STATE_UNSPECIFIED"
    = Prelude.Just WARM_STATE_UNSPECIFIED
    | (Prelude.==) k "WARM_STATE_ACTIVE"
    = Prelude.Just WARM_STATE_ACTIVE
    | (Prelude.==) k "WARM_STATE_EXPIRED"
    = Prelude.Just WARM_STATE_EXPIRED
    | (Prelude.==) k "WARM_STATE_BREACHED"
    = Prelude.Just WARM_STATE_BREACHED
    | (Prelude.==) k "WARM_STATE_RELEASED"
    = Prelude.Just WARM_STATE_RELEASED
    | Prelude.otherwise
    = (Prelude.>>=) (Text.Read.readMaybe k) Data.ProtoLens.maybeToEnum
instance Prelude.Bounded WarmState where
  minBound = WARM_STATE_UNSPECIFIED
  maxBound = WARM_STATE_RELEASED
instance Prelude.Enum WarmState where
  toEnum k__
    = Prelude.maybe
        (Prelude.error
           ((Prelude.++)
              "toEnum: unknown value for enum WarmState: " (Prelude.show k__)))
        Prelude.id (Data.ProtoLens.maybeToEnum k__)
  fromEnum WARM_STATE_UNSPECIFIED = 0
  fromEnum WARM_STATE_ACTIVE = 1
  fromEnum WARM_STATE_EXPIRED = 2
  fromEnum WARM_STATE_BREACHED = 3
  fromEnum WARM_STATE_RELEASED = 4
  fromEnum (WarmState'Unrecognized (WarmState'UnrecognizedValue k))
    = Prelude.fromIntegral k
  succ WARM_STATE_RELEASED
    = Prelude.error
        "WarmState.succ: bad argument WARM_STATE_RELEASED. This value would be out of bounds."
  succ WARM_STATE_UNSPECIFIED = WARM_STATE_ACTIVE
  succ WARM_STATE_ACTIVE = WARM_STATE_EXPIRED
  succ WARM_STATE_EXPIRED = WARM_STATE_BREACHED
  succ WARM_STATE_BREACHED = WARM_STATE_RELEASED
  succ (WarmState'Unrecognized _)
    = Prelude.error "WarmState.succ: bad argument: unrecognized value"
  pred WARM_STATE_UNSPECIFIED
    = Prelude.error
        "WarmState.pred: bad argument WARM_STATE_UNSPECIFIED. This value would be out of bounds."
  pred WARM_STATE_ACTIVE = WARM_STATE_UNSPECIFIED
  pred WARM_STATE_EXPIRED = WARM_STATE_ACTIVE
  pred WARM_STATE_BREACHED = WARM_STATE_EXPIRED
  pred WARM_STATE_RELEASED = WARM_STATE_BREACHED
  pred (WarmState'Unrecognized _)
    = Prelude.error "WarmState.pred: bad argument: unrecognized value"
  enumFrom = Data.ProtoLens.Message.Enum.messageEnumFrom
  enumFromTo = Data.ProtoLens.Message.Enum.messageEnumFromTo
  enumFromThen = Data.ProtoLens.Message.Enum.messageEnumFromThen
  enumFromThenTo = Data.ProtoLens.Message.Enum.messageEnumFromThenTo
instance Data.ProtoLens.FieldDefault WarmState where
  fieldDefault = WARM_STATE_UNSPECIFIED
instance Control.DeepSeq.NFData WarmState where
  rnf x__ = Prelude.seq x__ ()
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.commitment' @:: Lens' WarmView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.context' @:: Lens' WarmView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.modelProfile' @:: Lens' WarmView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.latencyProfile' @:: Lens' WarmView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.expiresAtMs' @:: Lens' WarmView Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.state' @:: Lens' WarmView WarmState@
         * 'Proto.Inference.V1.Inference_Fields.evidenceDigest' @:: Lens' WarmView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.admissionReceiptId' @:: Lens' WarmView Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.sequence' @:: Lens' WarmView Data.Word.Word64@
         * 'Proto.Inference.V1.Inference_Fields.idleKv' @:: Lens' WarmView IdleKvRetention@
         * 'Proto.Inference.V1.Inference_Fields.maybe'idleKv' @:: Lens' WarmView (Prelude.Maybe IdleKvRetention)@ -}
data WarmView
  = WarmView'_constructor {_WarmView'commitment :: !Data.ByteString.ByteString,
                           _WarmView'context :: !Data.ByteString.ByteString,
                           _WarmView'modelProfile :: !Data.ByteString.ByteString,
                           _WarmView'latencyProfile :: !Data.ByteString.ByteString,
                           _WarmView'expiresAtMs :: !Data.Word.Word64,
                           _WarmView'state :: !WarmState,
                           _WarmView'evidenceDigest :: !Data.ByteString.ByteString,
                           _WarmView'admissionReceiptId :: !Data.ByteString.ByteString,
                           _WarmView'sequence :: !Data.Word.Word64,
                           _WarmView'idleKv :: !(Prelude.Maybe IdleKvRetention),
                           _WarmView'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show WarmView where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField WarmView "commitment" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'commitment
           (\ x__ y__ -> x__ {_WarmView'commitment = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "context" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'context (\ x__ y__ -> x__ {_WarmView'context = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "modelProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'modelProfile
           (\ x__ y__ -> x__ {_WarmView'modelProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "latencyProfile" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'latencyProfile
           (\ x__ y__ -> x__ {_WarmView'latencyProfile = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "expiresAtMs" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'expiresAtMs
           (\ x__ y__ -> x__ {_WarmView'expiresAtMs = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "state" WarmState where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'state (\ x__ y__ -> x__ {_WarmView'state = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "evidenceDigest" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'evidenceDigest
           (\ x__ y__ -> x__ {_WarmView'evidenceDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "admissionReceiptId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'admissionReceiptId
           (\ x__ y__ -> x__ {_WarmView'admissionReceiptId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "sequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'sequence (\ x__ y__ -> x__ {_WarmView'sequence = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WarmView "idleKv" IdleKvRetention where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'idleKv (\ x__ y__ -> x__ {_WarmView'idleKv = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField WarmView "maybe'idleKv" (Prelude.Maybe IdleKvRetention) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WarmView'idleKv (\ x__ y__ -> x__ {_WarmView'idleKv = y__}))
        Prelude.id
instance Data.ProtoLens.Message WarmView where
  messageName _ = Data.Text.pack "inference.customer.v1.WarmView"
  packedMessageDescriptor _
    = "\n\
      \\bWarmView\DC2$\n\
      \\n\
      \commitment\CAN\SOH \SOH(\fR\n\
      \commitmentB\EOT\200\243\CAN \DC2\RS\n\
      \\acontext\CAN\STX \SOH(\fR\acontextB\EOT\200\243\CAN \DC2)\n\
      \\rmodel_profile\CAN\ETX \SOH(\fR\fmodelProfileB\EOT\200\243\CAN \DC2'\n\
      \\SIlatency_profile\CAN\EOT \SOH(\fR\SOlatencyProfile\DC2(\n\
      \\rexpires_at_ms\CAN\ENQ \SOH(\EOTR\vexpiresAtMsB\EOT\216\243\CAN\SOH\DC2<\n\
      \\ENQstate\CAN\ACK \SOH(\SO2 .inference.customer.v1.WarmStateR\ENQstateB\EOT\136\244\CAN\SOH\DC2-\n\
      \\SIevidence_digest\CAN\a \SOH(\fR\SOevidenceDigestB\EOT\200\243\CAN \DC26\n\
      \\DC4admission_receipt_id\CAN\b \SOH(\fR\DC2admissionReceiptIdB\EOT\200\243\CAN \DC2 \n\
      \\bsequence\CAN\t \SOH(\EOTR\bsequenceB\EOT\216\243\CAN\SOH\DC2?\n\
      \\aidle_kv\CAN\n\
      \ \SOH(\v2&.inference.customer.v1.IdleKvRetentionR\ACKidleKv"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        commitment__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "commitment"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"commitment")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        context__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "context"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"context")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        modelProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "model_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"modelProfile")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        latencyProfile__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "latency_profile"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"latencyProfile")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        expiresAtMs__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "expires_at_ms"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"expiresAtMs")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        state__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "state"
              (Data.ProtoLens.ScalarField Data.ProtoLens.EnumField ::
                 Data.ProtoLens.FieldTypeDescriptor WarmState)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"state")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        evidenceDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "evidence_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"evidenceDigest")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        admissionReceiptId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "admission_receipt_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"admissionReceiptId")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        sequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"sequence")) ::
              Data.ProtoLens.FieldDescriptor WarmView
        idleKv__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "idle_kv"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor IdleKvRetention)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'idleKv")) ::
              Data.ProtoLens.FieldDescriptor WarmView
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, commitment__field_descriptor),
           (Data.ProtoLens.Tag 2, context__field_descriptor),
           (Data.ProtoLens.Tag 3, modelProfile__field_descriptor),
           (Data.ProtoLens.Tag 4, latencyProfile__field_descriptor),
           (Data.ProtoLens.Tag 5, expiresAtMs__field_descriptor),
           (Data.ProtoLens.Tag 6, state__field_descriptor),
           (Data.ProtoLens.Tag 7, evidenceDigest__field_descriptor),
           (Data.ProtoLens.Tag 8, admissionReceiptId__field_descriptor),
           (Data.ProtoLens.Tag 9, sequence__field_descriptor),
           (Data.ProtoLens.Tag 10, idleKv__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _WarmView'_unknownFields
        (\ x__ y__ -> x__ {_WarmView'_unknownFields = y__})
  defMessage
    = WarmView'_constructor
        {_WarmView'commitment = Data.ProtoLens.fieldDefault,
         _WarmView'context = Data.ProtoLens.fieldDefault,
         _WarmView'modelProfile = Data.ProtoLens.fieldDefault,
         _WarmView'latencyProfile = Data.ProtoLens.fieldDefault,
         _WarmView'expiresAtMs = Data.ProtoLens.fieldDefault,
         _WarmView'state = Data.ProtoLens.fieldDefault,
         _WarmView'evidenceDigest = Data.ProtoLens.fieldDefault,
         _WarmView'admissionReceiptId = Data.ProtoLens.fieldDefault,
         _WarmView'sequence = Data.ProtoLens.fieldDefault,
         _WarmView'idleKv = Prelude.Nothing, _WarmView'_unknownFields = []}
  parseMessage
    = let
        loop :: WarmView -> Data.ProtoLens.Encoding.Bytes.Parser WarmView
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
                                       "commitment"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"commitment") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "context"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"context") y x)
                        26
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "model_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"modelProfile") y x)
                        34
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "latency_profile"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"latencyProfile") y x)
                        40
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "expires_at_ms"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"expiresAtMs") y x)
                        48
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (Prelude.fmap
                                          Prelude.toEnum
                                          (Prelude.fmap
                                             Prelude.fromIntegral
                                             Data.ProtoLens.Encoding.Bytes.getVarInt))
                                       "state"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"state") y x)
                        58
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "evidence_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"evidenceDigest") y x)
                        66
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getBytes
                                             (Prelude.fromIntegral len))
                                       "admission_receipt_id"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"admissionReceiptId") y x)
                        72
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "sequence"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"sequence") y x)
                        82
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.isolate
                                             (Prelude.fromIntegral len) Data.ProtoLens.parseMessage)
                                       "idle_kv"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"idleKv") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "WarmView"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v
                  = Lens.Family2.view (Data.ProtoLens.Field.field @"commitment") _x
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"context") _x
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
                        = Lens.Family2.view (Data.ProtoLens.Field.field @"modelProfile") _x
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
                               (Data.ProtoLens.Field.field @"latencyProfile") _x
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
                         (let
                            _v
                              = Lens.Family2.view (Data.ProtoLens.Field.field @"expiresAtMs") _x
                          in
                            if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                Data.Monoid.mempty
                            else
                                (Data.Monoid.<>)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 40)
                                  (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                         ((Data.Monoid.<>)
                            (let
                               _v = Lens.Family2.view (Data.ProtoLens.Field.field @"state") _x
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
                            ((Data.Monoid.<>)
                               (let
                                  _v
                                    = Lens.Family2.view
                                        (Data.ProtoLens.Field.field @"evidenceDigest") _x
                                in
                                  if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                      Data.Monoid.mempty
                                  else
                                      (Data.Monoid.<>)
                                        (Data.ProtoLens.Encoding.Bytes.putVarInt 58)
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
                                           (Data.ProtoLens.Field.field @"admissionReceiptId") _x
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
                                     (let
                                        _v
                                          = Lens.Family2.view
                                              (Data.ProtoLens.Field.field @"sequence") _x
                                      in
                                        if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                                            Data.Monoid.mempty
                                        else
                                            (Data.Monoid.<>)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt 72)
                                              (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                                     ((Data.Monoid.<>)
                                        (case
                                             Lens.Family2.view
                                               (Data.ProtoLens.Field.field @"maybe'idleKv") _x
                                         of
                                           Prelude.Nothing -> Data.Monoid.mempty
                                           (Prelude.Just _v)
                                             -> (Data.Monoid.<>)
                                                  (Data.ProtoLens.Encoding.Bytes.putVarInt 82)
                                                  ((Prelude..)
                                                     (\ bs
                                                        -> (Data.Monoid.<>)
                                                             (Data.ProtoLens.Encoding.Bytes.putVarInt
                                                                (Prelude.fromIntegral
                                                                   (Data.ByteString.length bs)))
                                                             (Data.ProtoLens.Encoding.Bytes.putBytes
                                                                bs))
                                                     Data.ProtoLens.encodeMessage _v))
                                        (Data.ProtoLens.Encoding.Wire.buildFieldSet
                                           (Lens.Family2.view
                                              Data.ProtoLens.unknownFields _x)))))))))))
instance Control.DeepSeq.NFData WarmView where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_WarmView'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_WarmView'commitment x__)
                (Control.DeepSeq.deepseq
                   (_WarmView'context x__)
                   (Control.DeepSeq.deepseq
                      (_WarmView'modelProfile x__)
                      (Control.DeepSeq.deepseq
                         (_WarmView'latencyProfile x__)
                         (Control.DeepSeq.deepseq
                            (_WarmView'expiresAtMs x__)
                            (Control.DeepSeq.deepseq
                               (_WarmView'state x__)
                               (Control.DeepSeq.deepseq
                                  (_WarmView'evidenceDigest x__)
                                  (Control.DeepSeq.deepseq
                                     (_WarmView'admissionReceiptId x__)
                                     (Control.DeepSeq.deepseq
                                        (_WarmView'sequence x__)
                                        (Control.DeepSeq.deepseq (_WarmView'idleKv x__) ()))))))))))
{- | Fields :
     
         * 'Proto.Inference.V1.Inference_Fields.runId' @:: Lens' WatchRunRequest Data.ByteString.ByteString@
         * 'Proto.Inference.V1.Inference_Fields.fromSequence' @:: Lens' WatchRunRequest Data.Word.Word64@ -}
data WatchRunRequest
  = WatchRunRequest'_constructor {_WatchRunRequest'runId :: !Data.ByteString.ByteString,
                                  _WatchRunRequest'fromSequence :: !Data.Word.Word64,
                                  _WatchRunRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show WatchRunRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField WatchRunRequest "runId" Data.ByteString.ByteString where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WatchRunRequest'runId
           (\ x__ y__ -> x__ {_WatchRunRequest'runId = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField WatchRunRequest "fromSequence" Data.Word.Word64 where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _WatchRunRequest'fromSequence
           (\ x__ y__ -> x__ {_WatchRunRequest'fromSequence = y__}))
        Prelude.id
instance Data.ProtoLens.Message WatchRunRequest where
  messageName _
    = Data.Text.pack "inference.customer.v1.WatchRunRequest"
  packedMessageDescriptor _
    = "\n\
      \\SIWatchRunRequest\DC2\ESC\n\
      \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2#\n\
      \\rfrom_sequence\CAN\STX \SOH(\EOTR\ffromSequence"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        runId__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "run_id"
              (Data.ProtoLens.ScalarField Data.ProtoLens.BytesField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.ByteString.ByteString)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"runId")) ::
              Data.ProtoLens.FieldDescriptor WatchRunRequest
        fromSequence__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "from_sequence"
              (Data.ProtoLens.ScalarField Data.ProtoLens.UInt64Field ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Word.Word64)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"fromSequence")) ::
              Data.ProtoLens.FieldDescriptor WatchRunRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, runId__field_descriptor),
           (Data.ProtoLens.Tag 2, fromSequence__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _WatchRunRequest'_unknownFields
        (\ x__ y__ -> x__ {_WatchRunRequest'_unknownFields = y__})
  defMessage
    = WatchRunRequest'_constructor
        {_WatchRunRequest'runId = Data.ProtoLens.fieldDefault,
         _WatchRunRequest'fromSequence = Data.ProtoLens.fieldDefault,
         _WatchRunRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          WatchRunRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser WatchRunRequest
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
                                       "run_id"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"runId") y x)
                        16
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       Data.ProtoLens.Encoding.Bytes.getVarInt "from_sequence"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"fromSequence") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "WatchRunRequest"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"runId") _x
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
                     = Lens.Family2.view (Data.ProtoLens.Field.field @"fromSequence") _x
                 in
                   if (Prelude.==) _v Data.ProtoLens.fieldDefault then
                       Data.Monoid.mempty
                   else
                       (Data.Monoid.<>)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt 16)
                         (Data.ProtoLens.Encoding.Bytes.putVarInt _v))
                (Data.ProtoLens.Encoding.Wire.buildFieldSet
                   (Lens.Family2.view Data.ProtoLens.unknownFields _x)))
instance Control.DeepSeq.NFData WatchRunRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_WatchRunRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_WatchRunRequest'runId x__)
                (Control.DeepSeq.deepseq (_WatchRunRequest'fromSequence x__) ()))
data ModelsService = ModelsService {}
instance Data.ProtoLens.Service.Types.Service ModelsService where
  type ServiceName ModelsService = "ModelsService"
  type ServicePackage ModelsService = "inference.customer.v1"
  type ServiceMethods ModelsService = '["list"]
  packedServiceDescriptor _
    = "\n\
      \\rModelsService\DC2l\n\
      \\EOTList\DC2(.inference.customer.v1.ListModelsRequest\SUB).inference.customer.v1.ListModelsResponse\"\SI\162\244\CAN\vmodels/list"
instance Data.ProtoLens.Service.Types.HasMethodImpl ModelsService "list" where
  type MethodName ModelsService "list" = "List"
  type MethodInput ModelsService "list" = ListModelsRequest
  type MethodOutput ModelsService "list" = ListModelsResponse
  type MethodStreamingType ModelsService "list" = 'Data.ProtoLens.Service.Types.NonStreaming
data ContextsService = ContextsService {}
instance Data.ProtoLens.Service.Types.Service ContextsService where
  type ServiceName ContextsService = "ContextsService"
  type ServicePackage ContextsService = "inference.customer.v1"
  type ServiceMethods ContextsService = '["create",
                                          "inspect",
                                          "mutate"]
  packedServiceDescriptor _
    = "\n\
      \\SIContextsService\DC2r\n\
      \\ACKCreate\DC2+.inference.customer.v1.CreateContextRequest\SUB&.inference.customer.v1.MutationReceipt\"\DC3\162\244\CAN\SIcontexts/create\DC2q\n\
      \\aInspect\DC2,.inference.customer.v1.InspectContextRequest\SUB\".inference.customer.v1.ContextView\"\DC4\162\244\CAN\DLEcontexts/inspect\DC2r\n\
      \\ACKMutate\DC2+.inference.customer.v1.MutateContextRequest\SUB&.inference.customer.v1.MutationReceipt\"\DC3\162\244\CAN\SIcontexts/mutate"
instance Data.ProtoLens.Service.Types.HasMethodImpl ContextsService "create" where
  type MethodName ContextsService "create" = "Create"
  type MethodInput ContextsService "create" = CreateContextRequest
  type MethodOutput ContextsService "create" = MutationReceipt
  type MethodStreamingType ContextsService "create" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ContextsService "inspect" where
  type MethodName ContextsService "inspect" = "Inspect"
  type MethodInput ContextsService "inspect" = InspectContextRequest
  type MethodOutput ContextsService "inspect" = ContextView
  type MethodStreamingType ContextsService "inspect" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl ContextsService "mutate" where
  type MethodName ContextsService "mutate" = "Mutate"
  type MethodInput ContextsService "mutate" = MutateContextRequest
  type MethodOutput ContextsService "mutate" = MutationReceipt
  type MethodStreamingType ContextsService "mutate" = 'Data.ProtoLens.Service.Types.NonStreaming
data WarmContextsService = WarmContextsService {}
instance Data.ProtoLens.Service.Types.Service WarmContextsService where
  type ServiceName WarmContextsService = "WarmContextsService"
  type ServicePackage WarmContextsService = "inference.customer.v1"
  type ServiceMethods WarmContextsService = '["inspect",
                                              "release",
                                              "renew",
                                              "retain"]
  packedServiceDescriptor _
    = "\n\
      \\DC3WarmContextsService\DC2d\n\
      \\ACKRetain\DC2(.inference.customer.v1.RetainWarmRequest\SUB\US.inference.customer.v1.WarmView\"\SI\162\244\CAN\vwarm/retain\DC2g\n\
      \\aInspect\DC2).inference.customer.v1.InspectWarmRequest\SUB\US.inference.customer.v1.WarmView\"\DLE\162\244\CAN\fwarm/inspect\DC2a\n\
      \\ENQRenew\DC2'.inference.customer.v1.RenewWarmRequest\SUB\US.inference.customer.v1.WarmView\"\SO\162\244\CAN\n\
      \warm/renew\DC2g\n\
      \\aRelease\DC2).inference.customer.v1.ReleaseWarmRequest\SUB\US.inference.customer.v1.WarmView\"\DLE\162\244\CAN\fwarm/release"
instance Data.ProtoLens.Service.Types.HasMethodImpl WarmContextsService "retain" where
  type MethodName WarmContextsService "retain" = "Retain"
  type MethodInput WarmContextsService "retain" = RetainWarmRequest
  type MethodOutput WarmContextsService "retain" = WarmView
  type MethodStreamingType WarmContextsService "retain" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WarmContextsService "inspect" where
  type MethodName WarmContextsService "inspect" = "Inspect"
  type MethodInput WarmContextsService "inspect" = InspectWarmRequest
  type MethodOutput WarmContextsService "inspect" = WarmView
  type MethodStreamingType WarmContextsService "inspect" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WarmContextsService "renew" where
  type MethodName WarmContextsService "renew" = "Renew"
  type MethodInput WarmContextsService "renew" = RenewWarmRequest
  type MethodOutput WarmContextsService "renew" = WarmView
  type MethodStreamingType WarmContextsService "renew" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl WarmContextsService "release" where
  type MethodName WarmContextsService "release" = "Release"
  type MethodInput WarmContextsService "release" = ReleaseWarmRequest
  type MethodOutput WarmContextsService "release" = WarmView
  type MethodStreamingType WarmContextsService "release" = 'Data.ProtoLens.Service.Types.NonStreaming
data RunsService = RunsService {}
instance Data.ProtoLens.Service.Types.Service RunsService where
  type ServiceName RunsService = "RunsService"
  type ServicePackage RunsService = "inference.customer.v1"
  type ServiceMethods RunsService = '["cancel",
                                      "generate",
                                      "inspect",
                                      "watch"]
  packedServiceDescriptor _
    = "\n\
      \\vRunsService\DC2t\n\
      \\bGenerate\DC2).inference.customer.v1.GenerateRunRequest\SUB*.inference.customer.v1.GenerateRunResponse\"\DC1\162\244\CAN\rruns/generate\DC2e\n\
      \\aInspect\DC2(.inference.customer.v1.InspectRunRequest\SUB\RS.inference.customer.v1.RunView\"\DLE\162\244\CAN\fruns/inspect\DC2b\n\
      \\ENQWatch\DC2&.inference.customer.v1.WatchRunRequest\SUB\US.inference.customer.v1.RunEvent\"\SO\162\244\CAN\n\
      \runs/watch0\SOH\DC2c\n\
      \\ACKCancel\DC2(.inference.customer.v1.InspectRunRequest\SUB\RS.inference.customer.v1.RunView\"\SI\162\244\CAN\vruns/cancel"
instance Data.ProtoLens.Service.Types.HasMethodImpl RunsService "generate" where
  type MethodName RunsService "generate" = "Generate"
  type MethodInput RunsService "generate" = GenerateRunRequest
  type MethodOutput RunsService "generate" = GenerateRunResponse
  type MethodStreamingType RunsService "generate" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl RunsService "inspect" where
  type MethodName RunsService "inspect" = "Inspect"
  type MethodInput RunsService "inspect" = InspectRunRequest
  type MethodOutput RunsService "inspect" = RunView
  type MethodStreamingType RunsService "inspect" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl RunsService "watch" where
  type MethodName RunsService "watch" = "Watch"
  type MethodInput RunsService "watch" = WatchRunRequest
  type MethodOutput RunsService "watch" = RunEvent
  type MethodStreamingType RunsService "watch" = 'Data.ProtoLens.Service.Types.ServerStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl RunsService "cancel" where
  type MethodName RunsService "cancel" = "Cancel"
  type MethodInput RunsService "cancel" = InspectRunRequest
  type MethodOutput RunsService "cancel" = RunView
  type MethodStreamingType RunsService "cancel" = 'Data.ProtoLens.Service.Types.NonStreaming
data EvaluationsService = EvaluationsService {}
instance Data.ProtoLens.Service.Types.Service EvaluationsService where
  type ServiceName EvaluationsService = "EvaluationsService"
  type ServicePackage EvaluationsService = "inference.customer.v1"
  type ServiceMethods EvaluationsService = '["create", "inspect"]
  packedServiceDescriptor _
    = "\n\
      \\DC2EvaluationsService\DC2w\n\
      \\ACKCreate\DC2..inference.customer.v1.CreateEvaluationRequest\SUB%.inference.customer.v1.EvaluationView\"\SYN\162\244\CAN\DC2evaluations/create\DC2z\n\
      \\aInspect\DC2/.inference.customer.v1.InspectEvaluationRequest\SUB%.inference.customer.v1.EvaluationView\"\ETB\162\244\CAN\DC3evaluations/inspect"
instance Data.ProtoLens.Service.Types.HasMethodImpl EvaluationsService "create" where
  type MethodName EvaluationsService "create" = "Create"
  type MethodInput EvaluationsService "create" = CreateEvaluationRequest
  type MethodOutput EvaluationsService "create" = EvaluationView
  type MethodStreamingType EvaluationsService "create" = 'Data.ProtoLens.Service.Types.NonStreaming
instance Data.ProtoLens.Service.Types.HasMethodImpl EvaluationsService "inspect" where
  type MethodName EvaluationsService "inspect" = "Inspect"
  type MethodInput EvaluationsService "inspect" = InspectEvaluationRequest
  type MethodOutput EvaluationsService "inspect" = EvaluationView
  type MethodStreamingType EvaluationsService "inspect" = 'Data.ProtoLens.Service.Types.NonStreaming
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\FSinference/v1/inference.proto\DC2\NAKinference.customer.v1\SUB\ESCvalidation/v1/options.proto\"\DC3\n\
    \\DC1ListModelsRequest\"_\n\
    \\DC2ListModelsResponse\DC2I\n\
    \\ACKmodels\CAN\SOH \ETX(\v2&.inference.customer.v1.ModelCapabilityR\ACKmodelsB\t\232\243\CAN\SOH\240\243\CAN\128 \"\158\ETX\n\
    \\SIModelCapability\DC2\ESC\n\
    \\ENQmodel\CAN\SOH \SOH(\tR\ENQmodelB\ENQ\248\243\CAN\128\STX\DC21\n\
    \\DC1execution_profile\CAN\STX \SOH(\fR\DLEexecutionProfileB\EOT\200\243\CAN \DC2-\n\
    \\SImaximum_context\CAN\ETX \SOH(\EOTR\SOmaximumContextB\EOT\216\243\CAN\SOH\DC2+\n\
    \\SOmaximum_output\CAN\EOT \SOH(\EOTR\rmaximumOutputB\EOT\216\243\CAN\SOH\DC2(\n\
    \\bfeatures\CAN\ENQ \ETX(\tR\bfeaturesB\f\232\243\CAN\SOH\240\243\CAN@\144\244\CAN@\DC2\\\n\
    \\DC2retention_profiles\CAN\ACK \ETX(\v2'.inference.customer.v1.RetentionProfileR\DC1retentionProfilesB\EOT\240\243\CAN@\DC2W\n\
    \\DLEidle_kv_profiles\CAN\a \ETX(\v2'.inference.customer.v1.RetentionProfileR\SOidleKvProfilesB\EOT\240\243\CAN@\"\152\SOH\n\
    \\DLERetentionProfile\DC2\RS\n\
    \\aprofile\CAN\SOH \SOH(\fR\aprofileB\EOT\200\243\CAN \DC24\n\
    \\DC3minimum_duration_ms\CAN\STX \SOH(\EOTR\DC1minimumDurationMsB\EOT\216\243\CAN\SOH\DC2.\n\
    \\DC3maximum_duration_ms\CAN\ETX \SOH(\EOTR\DC1maximumDurationMs\"\136\STX\n\
    \\DC1RetainWarmRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\RS\n\
    \\acontext\CAN\STX \SOH(\fR\acontextB\EOT\200\243\CAN \DC2'\n\
    \\SIlatency_profile\CAN\ETX \SOH(\fR\SOlatencyProfile\DC2\"\n\
    \\rexpires_at_ms\CAN\EOT \SOH(\EOTR\vexpiresAtMs\DC2<\n\
    \\aidle_kv\CAN\ENQ \SOH(\v2#.inference.customer.v1.IdleKvPolicyR\ACKidleKv\"\\\n\
    \\fIdleKvPolicy\DC2\RS\n\
    \\aprofile\CAN\SOH \SOH(\fR\aprofileB\EOT\200\243\CAN \DC2,\n\
    \\SIidle_timeout_ms\CAN\STX \SOH(\EOTR\ridleTimeoutMsB\EOT\216\243\CAN\SOH\"\129\STX\n\
    \\SIIdleKvRetention\DC2A\n\
    \\ACKpolicy\CAN\SOH \SOH(\v2#.inference.customer.v1.IdleKvPolicyR\ACKpolicyB\EOT\208\243\CAN\SOH\DC2*\n\
    \\SOretained_at_ms\CAN\STX \SOH(\EOTR\fretainedAtMsB\EOT\216\243\CAN\SOH\DC20\n\
    \\SIlast_used_at_ms\CAN\ETX \SOH(\EOTH\NULR\flastUsedAtMsB\EOT\216\243\CAN\SOH\136\SOH\SOH\DC2)\n\
    \\vlast_run_id\CAN\EOT \SOH(\fH\SOHR\tlastRunIdB\EOT\200\243\CAN\DLE\136\SOH\SOHB\DC2\n\
    \\DLE_last_used_at_msB\SO\n\
    \\f_last_run_id\":\n\
    \\DC2InspectWarmRequest\DC2$\n\
    \\n\
    \commitment\CAN\SOH \SOH(\fR\n\
    \commitmentB\EOT\200\243\CAN \"\237\SOH\n\
    \\DLERenewWarmRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2$\n\
    \\n\
    \commitment\CAN\STX \SOH(\fR\n\
    \commitmentB\EOT\200\243\CAN \DC2\"\n\
    \\rexpires_at_ms\CAN\ETX \SOH(\EOTR\vexpiresAtMs\DC21\n\
    \\SIidle_timeout_ms\CAN\EOT \SOH(\EOTH\NULR\ridleTimeoutMsB\EOT\216\243\CAN\SOH\136\SOH\SOHB\DC2\n\
    \\DLE_idle_timeout_ms\"\132\SOH\n\
    \\DC2ReleaseWarmRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2$\n\
    \\n\
    \commitment\CAN\STX \SOH(\fR\n\
    \commitmentB\EOT\200\243\CAN \"\214\ETX\n\
    \\bWarmView\DC2$\n\
    \\n\
    \commitment\CAN\SOH \SOH(\fR\n\
    \commitmentB\EOT\200\243\CAN \DC2\RS\n\
    \\acontext\CAN\STX \SOH(\fR\acontextB\EOT\200\243\CAN \DC2)\n\
    \\rmodel_profile\CAN\ETX \SOH(\fR\fmodelProfileB\EOT\200\243\CAN \DC2'\n\
    \\SIlatency_profile\CAN\EOT \SOH(\fR\SOlatencyProfile\DC2(\n\
    \\rexpires_at_ms\CAN\ENQ \SOH(\EOTR\vexpiresAtMsB\EOT\216\243\CAN\SOH\DC2<\n\
    \\ENQstate\CAN\ACK \SOH(\SO2 .inference.customer.v1.WarmStateR\ENQstateB\EOT\136\244\CAN\SOH\DC2-\n\
    \\SIevidence_digest\CAN\a \SOH(\fR\SOevidenceDigestB\EOT\200\243\CAN \DC26\n\
    \\DC4admission_receipt_id\CAN\b \SOH(\fR\DC2admissionReceiptIdB\EOT\200\243\CAN \DC2 \n\
    \\bsequence\CAN\t \SOH(\EOTR\bsequenceB\EOT\216\243\CAN\SOH\DC2?\n\
    \\aidle_kv\CAN\n\
    \ \SOH(\v2&.inference.customer.v1.IdleKvRetentionR\ACKidleKv\"\129\SOH\n\
    \\DC2EvaluationArtifact\DC2\FS\n\
    \\ACKdigest\CAN\SOH \SOH(\fR\ACKdigestB\EOT\200\243\CAN \DC2$\n\
    \\n\
    \media_type\CAN\STX \SOH(\tR\tmediaTypeB\ENQ\248\243\CAN\128\STX\DC2'\n\
    \\flogical_size\CAN\ETX \SOH(\EOTR\vlogicalSizeB\EOT\216\243\CAN\SOH\"\158\SOH\n\
    \\SOEvaluationCase\DC2\GS\n\
    \\acase_id\CAN\SOH \SOH(\fR\ACKcaseIdB\EOT\200\243\CAN\DLE\DC2\DC4\n\
    \\ENQinput\CAN\STX \SOH(\fR\ENQinput\DC2=\n\
    \\NAKinput_artifact_digest\CAN\ETX \SOH(\fH\NULR\DC3inputArtifactDigestB\EOT\200\243\CAN \136\SOH\SOHB\CAN\n\
    \\SYN_input_artifact_digest\"\154\SOH\n\
    \\SIEvaluationSuite\DC2!\n\
    \\bidentity\CAN\SOH \SOH(\tR\bidentityB\ENQ\248\243\CAN\128\STX\DC2\FS\n\
    \\ACKdigest\CAN\STX \SOH(\fR\ACKdigestB\EOT\200\243\CAN \DC2F\n\
    \\ENQcases\CAN\ETX \ETX(\v2%.inference.customer.v1.EvaluationCaseR\ENQcasesB\t\232\243\CAN\SOH\240\243\CAN\128 \"`\n\
    \\DLEEvaluationGrader\DC2\GS\n\
    \\ACKhandle\CAN\SOH \SOH(\fR\ACKhandleB\ENQ\248\243\CAN\128 \DC2-\n\
    \\SIartifact_digest\CAN\STX \SOH(\fR\SOartifactDigestB\EOT\200\243\CAN \"\139\SOH\n\
    \\DLEEvaluationMetric\DC2!\n\
    \\bidentity\CAN\SOH \SOH(\tR\bidentityB\ENQ\248\243\CAN\128\STX\DC2T\n\
    \\vaggregation\CAN\STX \SOH(\SO2,.inference.customer.v1.EvaluationAggregationR\vaggregationB\EOT\136\244\CAN\SOH\"\163\ETX\n\
    \\SOEvaluationSpec\DC2T\n\
    \\n\
    \candidates\CAN\SOH \ETX(\v2).inference.customer.v1.EvaluationArtifactR\n\
    \candidatesB\t\232\243\CAN\SOH\240\243\CAN\128\STX\DC2B\n\
    \\ENQsuite\CAN\STX \SOH(\v2&.inference.customer.v1.EvaluationSuiteR\ENQsuiteB\EOT\208\243\CAN\SOH\DC2E\n\
    \\ACKgrader\CAN\ETX \SOH(\v2'.inference.customer.v1.EvaluationGraderR\ACKgraderB\EOT\208\243\CAN\SOH\DC2K\n\
    \\ametrics\CAN\EOT \ETX(\v2'.inference.customer.v1.EvaluationMetricR\ametricsB\b\232\243\CAN\SOH\240\243\CAN@\DC2<\n\
    \\DC4maximum_case_results\CAN\ENQ \SOH(\EOTR\DC2maximumCaseResultsB\n\
    \\216\243\CAN\SOH\128\244\CAN\128\128\EOT\DC2%\n\
    \\vspec_digest\CAN\ACK \SOH(\fR\n\
    \specDigestB\EOT\200\243\CAN \"\164\SOH\n\
    \\ETBCreateEvaluationRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2?\n\
    \\EOTspec\CAN\STX \SOH(\v2%.inference.customer.v1.EvaluationSpecR\EOTspecB\EOT\208\243\CAN\SOH\"E\n\
    \\CANInspectEvaluationRequest\DC2)\n\
    \\revaluation_id\CAN\SOH \SOH(\fR\fevaluationIdB\EOT\200\243\CAN\DLE\"U\n\
    \\rExactRational\DC2\FS\n\
    \\tnumerator\CAN\SOH \SOH(\DC2R\tnumerator\DC2&\n\
    \\vdenominator\CAN\STX \SOH(\EOTR\vdenominatorB\EOT\216\243\CAN\SOH\"\130\SOH\n\
    \\NAKEvaluationMetricValue\DC2'\n\
    \\SImetric_identity\CAN\SOH \SOH(\tR\SOmetricIdentity\DC2@\n\
    \\ENQvalue\CAN\STX \SOH(\v2$.inference.customer.v1.ExactRationalR\ENQvalueB\EOT\208\243\CAN\SOH\"\216\STX\n\
    \\DC4EvaluationCaseResult\DC2/\n\
    \\DLEcandidate_digest\CAN\SOH \SOH(\fR\SIcandidateDigestB\EOT\200\243\CAN \DC2\GS\n\
    \\acase_id\CAN\STX \SOH(\fR\ACKcaseIdB\EOT\200\243\CAN\DLE\DC2Z\n\
    \\vobservation\CAN\ETX \SOH(\v22.inference.customer.v1.EvaluationGraderObservationR\vobservationB\EOT\208\243\CAN\SOH\DC2F\n\
    \\ametrics\CAN\EOT \ETX(\v2,.inference.customer.v1.EvaluationMetricValueR\ametrics\DC2L\n\
    \\aoutcome\CAN\ENQ \SOH(\SO2,.inference.customer.v1.EvaluationCaseOutcomeR\aoutcomeB\EOT\136\244\CAN\SOH\"\183\SOH\n\
    \\ESCEvaluationGraderObservation\DC26\n\
    \\DC4native_output_digest\CAN\SOH \SOH(\fR\DC2nativeOutputDigestB\EOT\200\243\CAN \DC23\n\
    \\DC2observation_digest\CAN\STX \SOH(\fR\DC1observationDigestB\EOT\200\243\CAN \DC2+\n\
    \\SObinding_digest\CAN\ETX \SOH(\fR\rbindingDigestB\EOT\200\243\CAN \"\135\STX\n\
    \\DC3EvaluationAggregate\DC2/\n\
    \\DLEcandidate_digest\CAN\SOH \SOH(\fR\SIcandidateDigestB\EOT\200\243\CAN \DC2'\n\
    \\SImetric_identity\CAN\STX \SOH(\tR\SOmetricIdentity\DC2T\n\
    \\vaggregation\CAN\ETX \SOH(\SO2,.inference.customer.v1.EvaluationAggregationR\vaggregationB\EOT\136\244\CAN\SOH\DC2@\n\
    \\ENQvalue\CAN\EOT \SOH(\v2$.inference.customer.v1.ExactRationalR\ENQvalueB\EOT\208\243\CAN\SOH\"\128\STX\n\
    \\DLEEvaluationResult\DC2%\n\
    \\vspec_digest\CAN\SOH \SOH(\fR\n\
    \specDigestB\EOT\200\243\CAN \DC2N\n\
    \\fcase_results\CAN\STX \ETX(\v2+.inference.customer.v1.EvaluationCaseResultR\vcaseResults\DC2J\n\
    \\n\
    \aggregates\CAN\ETX \ETX(\v2*.inference.customer.v1.EvaluationAggregateR\n\
    \aggregates\DC2)\n\
    \\rresult_digest\CAN\EOT \SOH(\fR\fresultDigestB\EOT\200\243\CAN \"\179\STX\n\
    \\SOEvaluationView\DC2)\n\
    \\revaluation_id\CAN\SOH \SOH(\fR\fevaluationIdB\EOT\200\243\CAN\DLE\DC2?\n\
    \\EOTspec\CAN\STX \SOH(\v2%.inference.customer.v1.EvaluationSpecR\EOTspecB\EOT\208\243\CAN\SOH\DC2B\n\
    \\ENQstate\CAN\ETX \SOH(\SO2&.inference.customer.v1.EvaluationStateR\ENQstateB\EOT\136\244\CAN\SOH\DC2D\n\
    \\ACKresult\CAN\EOT \SOH(\v2'.inference.customer.v1.EvaluationResultH\NULR\ACKresult\136\SOH\SOH\DC2 \n\
    \\bsequence\CAN\ENQ \SOH(\EOTR\bsequenceB\EOT\216\243\CAN\SOHB\t\n\
    \\a_result\"e\n\
    \\SIRequestIdentity\DC2-\n\
    \\SIclient_instance\CAN\SOH \SOH(\fR\SOclientInstanceB\EOT\200\243\CAN\DLE\DC2#\n\
    \\n\
    \request_id\CAN\STX \SOH(\fR\trequestIdB\EOT\200\243\CAN\DLE\"\172\SOH\n\
    \\EOTItem\DC2\SO\n\
    \\STXid\CAN\SOH \SOH(\fR\STXid\DC23\n\
    \\EOTkind\CAN\STX \SOH(\SO2\US.inference.customer.v1.ItemKindR\EOTkind\DC2\CAN\n\
    \\apayload\CAN\ETX \SOH(\fR\apayload\DC2\DC2\n\
    \\EOTlink\CAN\EOT \SOH(\fR\EOTlink\DC21\n\
    \\DC4continuation_profile\CAN\ENQ \SOH(\fR\DC3continuationProfile\"\169\SOH\n\
    \\DC4CreateContextRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\DC4\n\
    \\ENQmodel\CAN\STX \SOH(\tR\ENQmodel\DC21\n\
    \\ENQitems\CAN\ETX \ETX(\v2\ESC.inference.customer.v1.ItemR\ENQitems\"9\n\
    \\NAKInspectContextRequest\DC2 \n\
    \\brevision\CAN\SOH \SOH(\fR\brevisionB\EOT\200\243\CAN \"\a\n\
    \\ENQEmpty\"Q\n\
    \\ACKInsert\DC2\SYN\n\
    \\ACKtarget\CAN\SOH \SOH(\fR\ACKtarget\DC2/\n\
    \\EOTitem\CAN\STX \SOH(\v2\ESC.inference.customer.v1.ItemR\EOTitem\";\n\
    \\aReplace\DC2\SYN\n\
    \\ACKtarget\CAN\SOH \SOH(\fR\ACKtarget\DC2\CAN\n\
    \\apayload\CAN\STX \SOH(\fR\apayload\"\167\STX\n\
    \\EOTEdit\DC25\n\
    \\ACKappend\CAN\SOH \SOH(\v2\ESC.inference.customer.v1.ItemH\NULR\ACKappend\DC2D\n\
    \\rinsert_before\CAN\STX \SOH(\v2\GS.inference.customer.v1.InsertH\NULR\finsertBefore\DC2B\n\
    \\finsert_after\CAN\ETX \SOH(\v2\GS.inference.customer.v1.InsertH\NULR\vinsertAfter\DC2:\n\
    \\areplace\CAN\EOT \SOH(\v2\RS.inference.customer.v1.ReplaceH\NULR\areplace\DC2\CAN\n\
    \\ACKdelete\CAN\ENQ \SOH(\fH\NULR\ACKdeleteB\b\n\
    \\ACKaction\":\n\
    \\ENQEdits\DC21\n\
    \\ENQedits\CAN\SOH \ETX(\v2\ESC.inference.customer.v1.EditR\ENQedits\"5\n\
    \\bTruncate\DC2\GS\n\
    \\athrough\CAN\SOH \SOH(\fH\NULR\athrough\136\SOH\SOHB\n\
    \\n\
    \\b_through\"d\n\
    \\aCompact\DC2\SUB\n\
    \\bselected\CAN\SOH \ETX(\fR\bselected\DC2=\n\
    \\vreplacement\CAN\STX \ETX(\v2\ESC.inference.customer.v1.ItemR\vreplacement\" \n\
    \\bTransfer\DC2\DC4\n\
    \\ENQmodel\CAN\SOH \SOH(\tR\ENQmodel\"\234\ETX\n\
    \\DC4MutateContextRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\FS\n\
    \\ACKsource\CAN\STX \SOH(\fR\ACKsourceB\EOT\200\243\CAN \DC22\n\
    \\EOTedit\CAN\ETX \SOH(\v2\FS.inference.customer.v1.EditsH\NULR\EOTedit\DC22\n\
    \\EOTfork\CAN\EOT \SOH(\v2\FS.inference.customer.v1.EmptyH\NULR\EOTfork\DC2=\n\
    \\btruncate\CAN\ENQ \SOH(\v2\US.inference.customer.v1.TruncateH\NULR\btruncate\DC2:\n\
    \\acompact\CAN\ACK \SOH(\v2\RS.inference.customer.v1.CompactH\NULR\acompact\DC28\n\
    \\arelease\CAN\a \SOH(\v2\FS.inference.customer.v1.EmptyH\NULR\arelease\DC2=\n\
    \\btransfer\CAN\b \SOH(\v2\US.inference.customer.v1.TransferH\NULR\btransferB\SO\n\
    \\ACKaction\DC2\EOT\224\243\CAN\SOH\"\158\SOH\n\
    \\SIMutationReceipt\DC2 \n\
    \\brevision\CAN\SOH \SOH(\fR\brevisionB\EOT\200\243\CAN \DC2+\n\
    \\SOcommand_digest\CAN\STX \SOH(\fR\rcommandDigestB\EOT\200\243\CAN \DC2 \n\
    \\bsequence\CAN\ETX \SOH(\EOTR\bsequenceB\EOT\216\243\CAN\SOH\DC2\SUB\n\
    \\bretained\CAN\EOT \SOH(\bR\bretained\"\253\STX\n\
    \\vContextView\DC2 \n\
    \\brevision\CAN\SOH \SOH(\fR\brevisionB\EOT\200\243\CAN \DC2!\n\
    \\ACKparent\CAN\STX \SOH(\fH\NULR\ACKparentB\EOT\200\243\CAN \136\SOH\SOH\DC2\RS\n\
    \\alineage\CAN\ETX \SOH(\fR\alineageB\EOT\200\243\CAN \DC21\n\
    \\DC1execution_profile\CAN\EOT \SOH(\fR\DLEexecutionProfileB\EOT\200\243\CAN \DC2+\n\
    \\SOcontent_digest\CAN\ENQ \SOH(\fR\rcontentDigestB\EOT\200\243\CAN \DC21\n\
    \\ENQitems\CAN\ACK \ETX(\v2\ESC.inference.customer.v1.ItemR\ENQitems\DC2\ESC\n\
    \\ENQmodel\CAN\a \SOH(\tR\ENQmodelB\ENQ\248\243\CAN\128\STX\DC2N\n\
    \\n\
    \provenance\CAN\b \SOH(\v2(.inference.customer.v1.ContextProvenanceR\n\
    \provenanceB\EOT\208\243\CAN\SOHB\t\n\
    \\a_parent\"\203\ETX\n\
    \\DC1ContextProvenance\DC28\n\
    \\acreated\CAN\SOH \SOH(\v2\FS.inference.customer.v1.EmptyH\NULR\acreated\DC2C\n\
    \\aderived\CAN\STX \SOH(\v2'.inference.customer.v1.ProvenanceSourceH\NULR\aderived\DC2A\n\
    \\ACKforked\CAN\ETX \SOH(\v2'.inference.customer.v1.ProvenanceSourceH\NULR\ACKforked\DC2M\n\
    \\vtransferred\CAN\EOT \SOH(\v2).inference.customer.v1.TransferProvenanceH\NULR\vtransferred\DC2K\n\
    \\tgenerated\CAN\ENQ \SOH(\v2+.inference.customer.v1.GenerationProvenanceH\NULR\tgenerated\DC2H\n\
    \\trun_input\CAN\ACK \SOH(\v2).inference.customer.v1.RunInputProvenanceH\NULR\brunInputB\SO\n\
    \\ACKorigin\DC2\EOT\224\243\CAN\SOH\"0\n\
    \\DLEProvenanceSource\DC2\FS\n\
    \\ACKsource\CAN\SOH \SOH(\fR\ACKsourceB\EOT\200\243\CAN \"j\n\
    \\DC2TransferProvenance\DC2\FS\n\
    \\ACKsource\CAN\SOH \SOH(\fR\ACKsourceB\EOT\200\243\CAN \DC26\n\
    \\ETBreused_compatible_state\CAN\STX \SOH(\bR\NAKreusedCompatibleState\"q\n\
    \\DC4GenerationProvenance\DC2\ESC\n\
    \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2<\n\
    \\ETBterminal_receipt_digest\CAN\STX \SOH(\fR\NAKterminalReceiptDigestB\EOT\200\243\CAN \"\158\SOH\n\
    \\DC2RunInputProvenance\DC2\FS\n\
    \\ACKsource\CAN\SOH \SOH(\fR\ACKsourceB\EOT\200\243\CAN \DC2\ESC\n\
    \\ACKrun_id\CAN\STX \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2+\n\
    \\SOmaximum_output\CAN\ETX \SOH(\EOTR\rmaximumOutputB\EOT\216\243\CAN\SOH\DC2\ETB\n\
    \\EOTseed\CAN\EOT \SOH(\EOTH\NULR\EOTseed\136\SOH\SOHB\a\n\
    \\ENQ_seed\"\134\STX\n\
    \\DC2GenerateRunRequest\DC2H\n\
    \\bidentity\CAN\SOH \SOH(\v2&.inference.customer.v1.RequestIdentityR\bidentityB\EOT\208\243\CAN\SOH\DC2\RS\n\
    \\acontext\CAN\STX \SOH(\fR\acontextB\EOT\200\243\CAN \DC27\n\
    \\ENQinput\CAN\ETX \SOH(\v2\ESC.inference.customer.v1.ItemR\ENQinputB\EOT\208\243\CAN\SOH\DC2+\n\
    \\SOmaximum_output\CAN\EOT \SOH(\EOTR\rmaximumOutputB\EOT\216\243\CAN\SOH\DC2\ETB\n\
    \\EOTseed\CAN\ENQ \SOH(\EOTH\NULR\EOTseed\136\SOH\SOHB\a\n\
    \\ENQ_seed\"M\n\
    \\DC3GenerateRunResponse\DC26\n\
    \\ETXrun\CAN\SOH \SOH(\v2\RS.inference.customer.v1.RunViewR\ETXrunB\EOT\208\243\CAN\SOH\"0\n\
    \\DC1InspectRunRequest\DC2\ESC\n\
    \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\"S\n\
    \\SIWatchRunRequest\DC2\ESC\n\
    \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2#\n\
    \\rfrom_sequence\CAN\STX \SOH(\EOTR\ffromSequence\"\196\SOH\n\
    \\fLogicalUsage\DC2\US\n\
    \\vnew_prefill\CAN\SOH \SOH(\EOTR\n\
    \newPrefill\DC2)\n\
    \\DLEgenerated_output\CAN\STX \SOH(\EOTR\SIgeneratedOutput\DC26\n\
    \\ETBeffective_context_reads\CAN\ETX \SOH(\EOTR\NAKeffectiveContextReads\DC20\n\
    \\DC4retained_byte_millis\CAN\EOT \SOH(\EOTR\DC2retainedByteMillis\"\128\STX\n\
    \\fUsageReceipt\DC2#\n\
    \\n\
    \receipt_id\CAN\SOH \SOH(\fR\treceiptIdB\EOT\200\243\CAN \DC2)\n\
    \\rmodel_profile\CAN\STX \SOH(\fR\fmodelProfileB\EOT\200\243\CAN \DC2+\n\
    \\SOmeter_revision\CAN\ETX \SOH(\fR\rmeterRevisionB\EOT\200\243\CAN \DC2?\n\
    \\ENQusage\CAN\EOT \SOH(\v2#.inference.customer.v1.LogicalUsageR\ENQusageB\EOT\208\243\CAN\SOH\DC22\n\
    \\DC2rate_card_revision\CAN\ENQ \SOH(\fR\DLErateCardRevisionB\EOT\200\243\CAN \"\136\STX\n\
    \\tRunResult\DC2\SYN\n\
    \\ACKoutput\CAN\SOH \SOH(\fR\ACKoutput\DC2A\n\
    \\acontext\CAN\STX \SOH(\v2\".inference.customer.v1.ContextViewH\NULR\acontext\136\SOH\SOH\DC2D\n\
    \\bterminal\CAN\ETX \SOH(\SO2\".inference.customer.v1.RunTerminalR\bterminalB\EOT\136\244\CAN\SOH\DC2B\n\
    \\areceipt\CAN\EOT \SOH(\v2#.inference.customer.v1.UsageReceiptH\SOHR\areceipt\136\SOH\SOHB\n\
    \\n\
    \\b_contextB\n\
    \\n\
    \\b_receipt\"\133\STX\n\
    \\aRunView\DC2\ESC\n\
    \\ACKrun_id\CAN\SOH \SOH(\fR\ENQrunIdB\EOT\200\243\CAN\DLE\DC2\SUB\n\
    \\ENQinput\CAN\STX \SOH(\fR\ENQinputB\EOT\200\243\CAN \DC2\ESC\n\
    \\ENQmodel\CAN\ETX \SOH(\tR\ENQmodelB\ENQ\248\243\CAN\128\STX\DC2#\n\
    \\rlast_sequence\CAN\EOT \SOH(\EOTR\flastSequence\DC25\n\
    \\SYNcancellation_requested\CAN\ENQ \SOH(\bR\NAKcancellationRequested\DC2=\n\
    \\ACKresult\CAN\ACK \SOH(\v2 .inference.customer.v1.RunResultH\NULR\ACKresult\136\SOH\SOHB\t\n\
    \\a_result\"\150\STX\n\
    \\bRunEvent\DC2\SUB\n\
    \\bsequence\CAN\SOH \SOH(\EOTR\bsequence\DC2\CAN\n\
    \\ACKoutput\CAN\STX \SOH(\fH\NULR\ACKoutput\DC2;\n\
    \\ENQusage\CAN\ETX \SOH(\v2#.inference.customer.v1.LogicalUsageH\NULR\ENQusage\DC2F\n\
    \\bterminal\CAN\EOT \SOH(\SO2\".inference.customer.v1.RunTerminalH\NULR\bterminalB\EOT\136\244\CAN\SOH\DC2@\n\
    \\bprogress\CAN\ENQ \SOH(\v2\".inference.customer.v1.RunProgressH\NULR\bprogressB\r\n\
    \\ENQevent\DC2\EOT\224\243\CAN\SOH\"!\n\
    \\vRunProgress\DC2\DC2\n\
    \\EOTkind\CAN\SOH \SOH(\tR\EOTkind*\136\SOH\n\
    \\tWarmState\DC2\SUB\n\
    \\SYNWARM_STATE_UNSPECIFIED\DLE\NUL\DC2\NAK\n\
    \\DC1WARM_STATE_ACTIVE\DLE\SOH\DC2\SYN\n\
    \\DC2WARM_STATE_EXPIRED\DLE\STX\DC2\ETB\n\
    \\DC3WARM_STATE_BREACHED\DLE\ETX\DC2\ETB\n\
    \\DC3WARM_STATE_RELEASED\DLE\EOT*\200\SOH\n\
    \\NAKEvaluationAggregation\DC2&\n\
    \\"EVALUATION_AGGREGATION_UNSPECIFIED\DLE\NUL\DC2\US\n\
    \\ESCEVALUATION_AGGREGATION_MEAN\DLE\SOH\DC2\RS\n\
    \\SUBEVALUATION_AGGREGATION_SUM\DLE\STX\DC2\"\n\
    \\RSEVALUATION_AGGREGATION_MINIMUM\DLE\ETX\DC2\"\n\
    \\RSEVALUATION_AGGREGATION_MAXIMUM\DLE\EOT*\189\SOH\n\
    \\NAKEvaluationCaseOutcome\DC2'\n\
    \#EVALUATION_CASE_OUTCOME_UNSPECIFIED\DLE\NUL\DC2\"\n\
    \\RSEVALUATION_CASE_OUTCOME_SCORED\DLE\SOH\DC2,\n\
    \(EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED\DLE\STX\DC2)\n\
    \%EVALUATION_CASE_OUTCOME_GRADER_FAILED\DLE\ETX*\205\SOH\n\
    \\SIEvaluationState\DC2 \n\
    \\FSEVALUATION_STATE_UNSPECIFIED\DLE\NUL\DC2\GS\n\
    \\EMEVALUATION_STATE_ADMITTED\DLE\SOH\DC2\FS\n\
    \\CANEVALUATION_STATE_RUNNING\DLE\STX\DC2\RS\n\
    \\SUBEVALUATION_STATE_COMPLETED\DLE\ETX\DC2\ESC\n\
    \\ETBEVALUATION_STATE_FAILED\DLE\EOT\DC2\RS\n\
    \\SUBEVALUATION_STATE_CANCELLED\DLE\ENQ*\201\STX\n\
    \\bItemKind\DC2\EM\n\
    \\NAKITEM_KIND_UNSPECIFIED\DLE\NUL\DC2\EM\n\
    \\NAKITEM_KIND_INSTRUCTION\DLE\SOH\DC2\DC4\n\
    \\DLEITEM_KIND_SYSTEM\DLE\STX\DC2\ETB\n\
    \\DC3ITEM_KIND_DEVELOPER\DLE\ETX\DC2\DC2\n\
    \\SOITEM_KIND_USER\DLE\EOT\DC2\ETB\n\
    \\DC3ITEM_KIND_ASSISTANT\DLE\ENQ\DC2\GS\n\
    \\EMITEM_KIND_TOOL_DEFINITION\DLE\ACK\DC2\ETB\n\
    \\DC3ITEM_KIND_TOOL_CALL\DLE\a\DC2\EM\n\
    \\NAKITEM_KIND_TOOL_RESULT\DLE\b\DC2\DC3\n\
    \\SIITEM_KIND_IMAGE\DLE\t\DC2\DC3\n\
    \\SIITEM_KIND_AUDIO\DLE\n\
    \\DC2\DC2\n\
    \\SOITEM_KIND_FILE\DLE\v\DC2\SUB\n\
    \\SYNITEM_KIND_CONTINUATION\DLE\f*\133\STX\n\
    \\vRunTerminal\DC2\FS\n\
    \\CANRUN_TERMINAL_UNSPECIFIED\DLE\NUL\DC2\SUB\n\
    \\SYNRUN_TERMINAL_COMPLETED\DLE\SOH\DC2\US\n\
    \\ESCRUN_TERMINAL_OUTPUT_LIMITED\DLE\STX\DC2\SUB\n\
    \\SYNRUN_TERMINAL_TOOL_CALL\DLE\ETX\DC2\CAN\n\
    \\DC4RUN_TERMINAL_REFUSAL\DLE\EOT\DC2 \n\
    \\SYNRUN_TERMINAL_CANCELLED\DLE\ENQ\SUB\EOT\152\244\CAN\SOH\DC2\GS\n\
    \\DC3RUN_TERMINAL_FAILED\DLE\ACK\SUB\EOT\152\244\CAN\SOH\DC2$\n\
    \\SUBRUN_TERMINAL_INDETERMINATE\DLE\a\SUB\EOT\152\244\CAN\SOH2}\n\
    \\rModelsService\DC2l\n\
    \\EOTList\DC2(.inference.customer.v1.ListModelsRequest\SUB).inference.customer.v1.ListModelsResponse\"\SI\162\244\CAN\vmodels/list2\236\STX\n\
    \\SIContextsService\DC2r\n\
    \\ACKCreate\DC2+.inference.customer.v1.CreateContextRequest\SUB&.inference.customer.v1.MutationReceipt\"\DC3\162\244\CAN\SIcontexts/create\DC2q\n\
    \\aInspect\DC2,.inference.customer.v1.InspectContextRequest\SUB\".inference.customer.v1.ContextView\"\DC4\162\244\CAN\DLEcontexts/inspect\DC2r\n\
    \\ACKMutate\DC2+.inference.customer.v1.MutateContextRequest\SUB&.inference.customer.v1.MutationReceipt\"\DC3\162\244\CAN\SIcontexts/mutate2\176\ETX\n\
    \\DC3WarmContextsService\DC2d\n\
    \\ACKRetain\DC2(.inference.customer.v1.RetainWarmRequest\SUB\US.inference.customer.v1.WarmView\"\SI\162\244\CAN\vwarm/retain\DC2g\n\
    \\aInspect\DC2).inference.customer.v1.InspectWarmRequest\SUB\US.inference.customer.v1.WarmView\"\DLE\162\244\CAN\fwarm/inspect\DC2a\n\
    \\ENQRenew\DC2'.inference.customer.v1.RenewWarmRequest\SUB\US.inference.customer.v1.WarmView\"\SO\162\244\CAN\n\
    \warm/renew\DC2g\n\
    \\aRelease\DC2).inference.customer.v1.ReleaseWarmRequest\SUB\US.inference.customer.v1.WarmView\"\DLE\162\244\CAN\fwarm/release2\179\ETX\n\
    \\vRunsService\DC2t\n\
    \\bGenerate\DC2).inference.customer.v1.GenerateRunRequest\SUB*.inference.customer.v1.GenerateRunResponse\"\DC1\162\244\CAN\rruns/generate\DC2e\n\
    \\aInspect\DC2(.inference.customer.v1.InspectRunRequest\SUB\RS.inference.customer.v1.RunView\"\DLE\162\244\CAN\fruns/inspect\DC2b\n\
    \\ENQWatch\DC2&.inference.customer.v1.WatchRunRequest\SUB\US.inference.customer.v1.RunEvent\"\SO\162\244\CAN\n\
    \runs/watch0\SOH\DC2c\n\
    \\ACKCancel\DC2(.inference.customer.v1.InspectRunRequest\SUB\RS.inference.customer.v1.RunView\"\SI\162\244\CAN\vruns/cancel2\137\STX\n\
    \\DC2EvaluationsService\DC2w\n\
    \\ACKCreate\DC2..inference.customer.v1.CreateEvaluationRequest\SUB%.inference.customer.v1.EvaluationView\"\SYN\162\244\CAN\DC2evaluations/create\DC2z\n\
    \\aInspect\DC2/.inference.customer.v1.InspectEvaluationRequest\SUB%.inference.customer.v1.EvaluationView\"\ETB\162\244\CAN\DC3evaluations/inspectB=Z;github.com/acyclic-labs/sdk/go/gen/inference/v1;inferencev1J\234\184\SOH\n\
    \\a\DC2\ENQ\NUL\NUL\146\EOT\SOH\n\
    \\b\n\
    \\SOH\f\DC2\ETX\NUL\NUL\DC2\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\SOH\NUL\RS\n\
    \\t\n\
    \\STX\ETX\NUL\DC2\ETX\ETX\NUL%\n\
    \\b\n\
    \\SOH\b\DC2\ETX\ENQ\NULR\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\ENQ\NULR\n\
    \\n\
    \\n\
    \\STX\ACK\NUL\DC2\EOT\a\NUL\v\SOH\n\
    \\n\
    \\n\
    \\ETX\ACK\NUL\SOH\DC2\ETX\a\b\NAK\n\
    \\f\n\
    \\EOT\ACK\NUL\STX\NUL\DC2\EOT\b\STX\n\
    \\ETX\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\SOH\DC2\ETX\b\ACK\n\
    \\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\STX\DC2\ETX\b\v\FS\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\ETX\DC2\ETX\b'9\n\
    \\f\n\
    \\ENQ\ACK\NUL\STX\NUL\EOT\DC2\ETX\t\EOT=\n\
    \\SI\n\
    \\b\ACK\NUL\STX\NUL\EOT\196\142\ETX\DC2\ETX\t\EOT=\n\
    \\t\n\
    \\STX\EOT\NUL\DC2\ETX\r\NUL\FS\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\r\b\EM\n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\SO\NUL\DC3\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\SO\b\SUB\n\
    \\f\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\EOT\SI\STX\DC2\EOT\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\EOT\DC2\ETX\SI\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ACK\DC2\ETX\SI\v\SUB\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\SI\ESC!\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\SI$%\n\
    \\r\n\
    \\ENQ\EOT\SOH\STX\NUL\b\DC2\EOT\SI&\DC2\ETX\n\
    \\SI\n\
    \\b\EOT\SOH\STX\NUL\b\189\142\ETX\DC2\ETX\DLE\EOT)\n\
    \\SI\n\
    \\b\EOT\SOH\STX\NUL\b\190\142\ETX\DC2\ETX\DC1\EOT,\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\DC4\NUL!\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\DC4\b\ETB\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\NAK\STXF\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ENQ\DC2\ETX\NAK\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\NAK\t\SO\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\NAK\DC1\DC2\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\b\DC2\ETX\NAK\DC3E\n\
    \\SI\n\
    \\b\EOT\STX\STX\NUL\b\191\142\ETX\DC2\ETX\NAK\DC4D\n\
    \\v\n\
    \\EOT\EOT\STX\STX\SOH\DC2\ETX\SYN\STXQ\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ENQ\DC2\ETX\SYN\STX\a\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\SOH\DC2\ETX\SYN\b\EM\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\ETX\DC2\ETX\SYN\FS\GS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\SOH\b\DC2\ETX\SYN\RSP\n\
    \\SI\n\
    \\b\EOT\STX\STX\SOH\b\185\142\ETX\DC2\ETX\SYN\USO\n\
    \\v\n\
    \\EOT\EOT\STX\STX\STX\DC2\ETX\ETB\STXN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ENQ\DC2\ETX\ETB\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\SOH\DC2\ETX\ETB\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\ETX\DC2\ETX\ETB\ESC\FS\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\STX\b\DC2\ETX\ETB\GSM\n\
    \\SI\n\
    \\b\EOT\STX\STX\STX\b\187\142\ETX\DC2\ETX\ETB\RSL\n\
    \\v\n\
    \\EOT\EOT\STX\STX\ETX\DC2\ETX\CAN\STXM\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\ENQ\DC2\ETX\CAN\STX\b\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\SOH\DC2\ETX\CAN\t\ETB\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\ETX\DC2\ETX\CAN\SUB\ESC\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ETX\b\DC2\ETX\CAN\FSL\n\
    \\SI\n\
    \\b\EOT\STX\STX\ETX\b\187\142\ETX\DC2\ETX\CAN\GSK\n\
    \\f\n\
    \\EOT\EOT\STX\STX\EOT\DC2\EOT\EM\STX\GS\EOT\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\EOT\DC2\ETX\EM\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\ENQ\DC2\ETX\EM\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\SOH\DC2\ETX\EM\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\EOT\ETX\DC2\ETX\EM\GS\RS\n\
    \\r\n\
    \\ENQ\EOT\STX\STX\EOT\b\DC2\EOT\EM\US\GS\ETX\n\
    \\SI\n\
    \\b\EOT\STX\STX\EOT\b\189\142\ETX\DC2\ETX\SUB\EOT)\n\
    \\SI\n\
    \\b\EOT\STX\STX\EOT\b\190\142\ETX\DC2\ETX\ESC\EOT*\n\
    \\SI\n\
    \\b\EOT\STX\STX\EOT\b\194\142\ETX\DC2\ETX\FS\EOT8\n\
    \\v\n\
    \\EOT\EOT\STX\STX\ENQ\DC2\ETX\RS\STX\\\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\EOT\DC2\ETX\RS\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\ACK\DC2\ETX\RS\v\ESC\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\SOH\DC2\ETX\RS\FS.\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\ETX\DC2\ETX\RS12\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ENQ\b\DC2\ETX\RS3[\n\
    \\SI\n\
    \\b\EOT\STX\STX\ENQ\b\190\142\ETX\DC2\ETX\RS4Z\n\
    \N\n\
    \\EOT\EOT\STX\STX\ACK\DC2\ETX \STXZ\SUBA Paid KV pin policies; duration bounds apply to idle_timeout_ms.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\EOT\DC2\ETX \STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\ACK\DC2\ETX \v\ESC\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\SOH\DC2\ETX \FS,\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\ETX\DC2\ETX /0\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\ACK\b\DC2\ETX 1Y\n\
    \\SI\n\
    \\b\EOT\STX\STX\ACK\b\190\142\ETX\DC2\ETX 2X\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT#\NUL'\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX#\b\CAN\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX$\STXG\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ENQ\DC2\ETX$\STX\a\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX$\b\SI\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX$\DC2\DC3\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\b\DC2\ETX$\DC4F\n\
    \\SI\n\
    \\b\EOT\ETX\STX\NUL\b\185\142\ETX\DC2\ETX$\NAKE\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\SOH\DC2\ETX%\STXR\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ENQ\DC2\ETX%\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\SOH\DC2\ETX%\t\FS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ETX\DC2\ETX%\US \n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\b\DC2\ETX%!Q\n\
    \\SI\n\
    \\b\EOT\ETX\STX\SOH\b\187\142\ETX\DC2\ETX%\"P\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\STX\DC2\ETX&\STX!\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\ENQ\DC2\ETX&\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\SOH\DC2\ETX&\t\FS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\STX\ETX\DC2\ETX&\US \n\
    \\165\SOH\n\
    \\STX\ACK\SOH\DC2\EOT+\NUL5\SOH\SUB\152\SOH Immutable canonical content operations. Execution/retention guarantees are\n\
    \ advertised separately; these methods do not admit a Run or a warm promise.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\ACK\SOH\SOH\DC2\ETX+\b\ETB\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\NUL\DC2\EOT,\STX.\ETX\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\NUL\SOH\DC2\ETX,\ACK\f\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\NUL\STX\DC2\ETX,\r!\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\NUL\ETX\DC2\ETX,,;\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\NUL\EOT\DC2\ETX-\EOTA\n\
    \\SI\n\
    \\b\ACK\SOH\STX\NUL\EOT\196\142\ETX\DC2\ETX-\EOTA\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\SOH\DC2\EOT/\STX1\ETX\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\SOH\SOH\DC2\ETX/\ACK\r\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\SOH\STX\DC2\ETX/\SO#\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\SOH\ETX\DC2\ETX/.9\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\SOH\EOT\DC2\ETX0\EOTB\n\
    \\SI\n\
    \\b\ACK\SOH\STX\SOH\EOT\196\142\ETX\DC2\ETX0\EOTB\n\
    \\f\n\
    \\EOT\ACK\SOH\STX\STX\DC2\EOT2\STX4\ETX\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\STX\SOH\DC2\ETX2\ACK\f\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\STX\STX\DC2\ETX2\r!\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\STX\ETX\DC2\ETX2,;\n\
    \\f\n\
    \\ENQ\ACK\SOH\STX\STX\EOT\DC2\ETX3\EOTA\n\
    \\SI\n\
    \\b\ACK\SOH\STX\STX\EOT\196\142\ETX\DC2\ETX3\EOTA\n\
    \\147\SOH\n\
    \\STX\ACK\STX\DC2\EOT9\NULF\SOH\SUB\134\SOH Customer warm-retention commitments. Placement, workers, allocations,\n\
    \ migration, rebalancing, and cleanup mechanics remain private.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\ACK\STX\SOH\DC2\ETX9\b\ESC\n\
    \\f\n\
    \\EOT\ACK\STX\STX\NUL\DC2\EOT:\STX<\ETX\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\NUL\SOH\DC2\ETX:\ACK\f\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\NUL\STX\DC2\ETX:\r\RS\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\NUL\ETX\DC2\ETX:)1\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\NUL\EOT\DC2\ETX;\EOT=\n\
    \\SI\n\
    \\b\ACK\STX\STX\NUL\EOT\196\142\ETX\DC2\ETX;\EOT=\n\
    \\f\n\
    \\EOT\ACK\STX\STX\SOH\DC2\EOT=\STX?\ETX\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\SOH\SOH\DC2\ETX=\ACK\r\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\SOH\STX\DC2\ETX=\SO \n\
    \\f\n\
    \\ENQ\ACK\STX\STX\SOH\ETX\DC2\ETX=+3\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\SOH\EOT\DC2\ETX>\EOT>\n\
    \\SI\n\
    \\b\ACK\STX\STX\SOH\EOT\196\142\ETX\DC2\ETX>\EOT>\n\
    \\f\n\
    \\EOT\ACK\STX\STX\STX\DC2\EOT@\STXB\ETX\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\STX\SOH\DC2\ETX@\ACK\v\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\STX\STX\DC2\ETX@\f\FS\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\STX\ETX\DC2\ETX@'/\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\STX\EOT\DC2\ETXA\EOT<\n\
    \\SI\n\
    \\b\ACK\STX\STX\STX\EOT\196\142\ETX\DC2\ETXA\EOT<\n\
    \\f\n\
    \\EOT\ACK\STX\STX\ETX\DC2\EOTC\STXE\ETX\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\ETX\SOH\DC2\ETXC\ACK\r\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\ETX\STX\DC2\ETXC\SO \n\
    \\f\n\
    \\ENQ\ACK\STX\STX\ETX\ETX\DC2\ETXC+3\n\
    \\f\n\
    \\ENQ\ACK\STX\STX\ETX\EOT\DC2\ETXD\EOT>\n\
    \\SI\n\
    \\b\ACK\STX\STX\ETX\EOT\196\142\ETX\DC2\ETXD\EOT>\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOTH\NULO\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETXH\b\EM\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETXI\STXQ\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ACK\DC2\ETXI\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETXI\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETXI\GS\RS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\b\DC2\ETXI\USP\n\
    \\SI\n\
    \\b\EOT\EOT\STX\NUL\b\186\142\ETX\DC2\ETXI O\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\SOH\DC2\ETXJ\STXG\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ENQ\DC2\ETXJ\STX\a\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\SOH\DC2\ETXJ\b\SI\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ETX\DC2\ETXJ\DC2\DC3\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\b\DC2\ETXJ\DC4F\n\
    \\SI\n\
    \\b\EOT\EOT\STX\SOH\b\185\142\ETX\DC2\ETXJ\NAKE\n\
    \N\n\
    \\EOT\EOT\EOT\STX\STX\DC2\ETXL\STX\FS\SUBA Legacy absolute-expiry policy. Mutually exclusive with idle_kv.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ENQ\DC2\ETXL\STX\a\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\SOH\DC2\ETXL\b\ETB\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\STX\ETX\DC2\ETXL\SUB\ESC\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\ETX\DC2\ETXM\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\ENQ\DC2\ETXM\STX\b\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\SOH\DC2\ETXM\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\ETX\ETX\DC2\ETXM\EM\SUB\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\EOT\DC2\ETXN\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\EOT\ACK\DC2\ETXN\STX\SO\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\EOT\SOH\DC2\ETXN\SI\SYN\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\EOT\ETX\DC2\ETXN\EM\SUB\n\
    \\202\STX\n\
    \\STX\EOT\ENQ\DC2\EOTU\NULX\SOH\SUB\189\STX Paid retention of verified KV, without capacity, throughput or latency guarantees.\n\
    \ Only verified actual Run reuse of the pinned revision or descendant prefix\n\
    \ advances last-use. Fork, edit, admission, inspect and recovery do not move the\n\
    \ pin or reset its idle window. Retried identities return committed receipts.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\ENQ\SOH\DC2\ETXU\b\DC4\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\NUL\DC2\ETXV\STXG\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ENQ\DC2\ETXV\STX\a\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\SOH\DC2\ETXV\b\SI\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\ETX\DC2\ETXV\DC2\DC3\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\NUL\b\DC2\ETXV\DC4F\n\
    \\SI\n\
    \\b\EOT\ENQ\STX\NUL\b\185\142\ETX\DC2\ETXV\NAKE\n\
    \\v\n\
    \\EOT\EOT\ENQ\STX\SOH\DC2\ETXW\STXN\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ENQ\DC2\ETXW\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\SOH\DC2\ETXW\t\CAN\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\ETX\DC2\ETXW\ESC\FS\n\
    \\f\n\
    \\ENQ\EOT\ENQ\STX\SOH\b\DC2\ETXW\GSM\n\
    \\SI\n\
    \\b\EOT\ENQ\STX\SOH\b\187\142\ETX\DC2\ETXW\RSL\n\
    \\n\
    \\n\
    \\STX\EOT\ACK\DC2\EOTZ\NULc\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ACK\SOH\DC2\ETXZ\b\ETB\n\
    \\v\n\
    \\EOT\EOT\ACK\STX\NUL\DC2\ETX[\STXL\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ACK\DC2\ETX[\STX\SO\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\SOH\DC2\ETX[\SI\NAK\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\ETX\DC2\ETX[\CAN\EM\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\NUL\b\DC2\ETX[\SUBK\n\
    \\SI\n\
    \\b\EOT\ACK\STX\NUL\b\186\142\ETX\DC2\ETX[\ESCJ\n\
    \O\n\
    \\EOT\EOT\ACK\STX\SOH\DC2\ETX]\STXM\SUBB Trusted service Unix milliseconds after verified initial KV pin.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ENQ\DC2\ETX]\STX\b\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\SOH\DC2\ETX]\t\ETB\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\ETX\DC2\ETX]\SUB\ESC\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\SOH\b\DC2\ETX]\FSL\n\
    \\SI\n\
    \\b\EOT\ACK\STX\SOH\b\187\142\ETX\DC2\ETX]\GSK\n\
    \Q\n\
    \\EOT\EOT\ACK\STX\STX\DC2\ETX_\STXW\SUBD Absent until verified actual reuse; never inferred from admission.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\EOT\DC2\ETX_\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\ENQ\DC2\ETX_\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\SOH\DC2\ETX_\DC2!\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\ETX\DC2\ETX_$%\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\STX\b\DC2\ETX_&V\n\
    \\SI\n\
    \\b\EOT\ACK\STX\STX\b\187\142\ETX\DC2\ETX_'U\n\
    \\154\SOH\n\
    \\EOT\EOT\ACK\STX\ETX\DC2\ETXb\STXT\SUB\140\SOH The authoritative Run that verified actual reuse of this pinned revision\n\
    \ or its descendant prefix, in the same authenticated owner scope.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\EOT\DC2\ETXb\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\ENQ\DC2\ETXb\v\DLE\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\SOH\DC2\ETXb\DC1\FS\n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\ETX\DC2\ETXb\US \n\
    \\f\n\
    \\ENQ\EOT\ACK\STX\ETX\b\DC2\ETXb!S\n\
    \\SI\n\
    \\b\EOT\ACK\STX\ETX\b\185\142\ETX\DC2\ETXb\"R\n\
    \\n\
    \\n\
    \\STX\EOT\a\DC2\EOTe\NULg\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\a\SOH\DC2\ETXe\b\SUB\n\
    \\v\n\
    \\EOT\EOT\a\STX\NUL\DC2\ETXf\STXJ\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ENQ\DC2\ETXf\STX\a\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\SOH\DC2\ETXf\b\DC2\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\ETX\DC2\ETXf\NAK\SYN\n\
    \\f\n\
    \\ENQ\EOT\a\STX\NUL\b\DC2\ETXf\ETBI\n\
    \\SI\n\
    \\b\EOT\a\STX\NUL\b\185\142\ETX\DC2\ETXf\CANH\n\
    \\n\
    \\n\
    \\STX\EOT\b\DC2\EOTi\NULq\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\b\SOH\DC2\ETXi\b\CAN\n\
    \\v\n\
    \\EOT\EOT\b\STX\NUL\DC2\ETXj\STXQ\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ACK\DC2\ETXj\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\SOH\DC2\ETXj\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\ETX\DC2\ETXj\GS\RS\n\
    \\f\n\
    \\ENQ\EOT\b\STX\NUL\b\DC2\ETXj\USP\n\
    \\SI\n\
    \\b\EOT\b\STX\NUL\b\186\142\ETX\DC2\ETXj O\n\
    \\v\n\
    \\EOT\EOT\b\STX\SOH\DC2\ETXk\STXJ\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ENQ\DC2\ETXk\STX\a\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\SOH\DC2\ETXk\b\DC2\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\ETX\DC2\ETXk\NAK\SYN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\SOH\b\DC2\ETXk\ETBI\n\
    \\SI\n\
    \\b\EOT\b\STX\SOH\b\185\142\ETX\DC2\ETXk\CANH\n\
    \\v\n\
    \\EOT\EOT\b\STX\STX\DC2\ETXl\STX\ESC\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\ENQ\DC2\ETXl\STX\b\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\SOH\DC2\ETXl\t\SYN\n\
    \\f\n\
    \\ENQ\EOT\b\STX\STX\ETX\DC2\ETXl\EM\SUB\n\
    \\243\SOH\n\
    \\EOT\EOT\b\STX\ETX\DC2\ETXp\STXW\SUB\229\SOH Changes timeout from last actual use, or retained_at_ms before first use.\n\
    \ Does not reset the idle window. Expired/released pins require a new Retain\n\
    \ identity; renew/replay cannot resurrect them. Inspect reports current state.\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\EOT\DC2\ETXp\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\ENQ\DC2\ETXp\v\DC1\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\SOH\DC2\ETXp\DC2!\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\ETX\DC2\ETXp$%\n\
    \\f\n\
    \\ENQ\EOT\b\STX\ETX\b\DC2\ETXp&V\n\
    \\SI\n\
    \\b\EOT\b\STX\ETX\b\187\142\ETX\DC2\ETXp'U\n\
    \\n\
    \\n\
    \\STX\EOT\t\DC2\EOTs\NULv\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\t\SOH\DC2\ETXs\b\SUB\n\
    \\v\n\
    \\EOT\EOT\t\STX\NUL\DC2\ETXt\STXQ\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ACK\DC2\ETXt\STX\DC1\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\SOH\DC2\ETXt\DC2\SUB\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\ETX\DC2\ETXt\GS\RS\n\
    \\f\n\
    \\ENQ\EOT\t\STX\NUL\b\DC2\ETXt\USP\n\
    \\SI\n\
    \\b\EOT\t\STX\NUL\b\186\142\ETX\DC2\ETXt O\n\
    \\v\n\
    \\EOT\EOT\t\STX\SOH\DC2\ETXu\STXJ\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ENQ\DC2\ETXu\STX\a\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\SOH\DC2\ETXu\b\DC2\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\ETX\DC2\ETXu\NAK\SYN\n\
    \\f\n\
    \\ENQ\EOT\t\STX\SOH\b\DC2\ETXu\ETBI\n\
    \\SI\n\
    \\b\EOT\t\STX\SOH\b\185\142\ETX\DC2\ETXu\CANH\n\
    \\n\
    \\n\
    \\STX\ENQ\NUL\DC2\EOTx\NUL~\SOH\n\
    \\n\
    \\n\
    \\ETX\ENQ\NUL\SOH\DC2\ETXx\ENQ\SO\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\NUL\DC2\ETXy\STX\GS\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\SOH\DC2\ETXy\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\NUL\STX\DC2\ETXy\ESC\FS\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\SOH\DC2\ETXz\STX\CAN\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\SOH\DC2\ETXz\STX\DC3\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\SOH\STX\DC2\ETXz\SYN\ETB\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\STX\DC2\ETX{\STX\EM\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\SOH\DC2\ETX{\STX\DC4\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\STX\STX\DC2\ETX{\ETB\CAN\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\ETX\DC2\ETX|\STX\SUB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\SOH\DC2\ETX|\STX\NAK\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\ETX\STX\DC2\ETX|\CAN\EM\n\
    \\v\n\
    \\EOT\ENQ\NUL\STX\EOT\DC2\ETX}\STX\SUB\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\SOH\DC2\ETX}\STX\NAK\n\
    \\f\n\
    \\ENQ\ENQ\NUL\STX\EOT\STX\DC2\ETX}\CAN\EM\n\
    \\f\n\
    \\STX\EOT\n\
    \\DC2\ACK\128\SOH\NUL\141\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\n\
    \\SOH\DC2\EOT\128\SOH\b\DLE\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\NUL\DC2\EOT\129\SOH\STXJ\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ENQ\DC2\EOT\129\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\NUL\SOH\DC2\EOT\129\SOH\b\DC2\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\NUL\ETX\DC2\EOT\129\SOH\NAK\SYN\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\NUL\b\DC2\EOT\129\SOH\ETBI\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\NUL\b\185\142\ETX\DC2\EOT\129\SOH\CANH\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\SOH\DC2\EOT\130\SOH\STXG\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ENQ\DC2\EOT\130\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\SOH\SOH\DC2\EOT\130\SOH\b\SI\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\SOH\ETX\DC2\EOT\130\SOH\DC2\DC3\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\SOH\b\DC2\EOT\130\SOH\DC4F\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\SOH\b\185\142\ETX\DC2\EOT\130\SOH\NAKE\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\STX\DC2\EOT\131\SOH\STXM\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\STX\ENQ\DC2\EOT\131\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\STX\SOH\DC2\EOT\131\SOH\b\NAK\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\STX\ETX\DC2\EOT\131\SOH\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\STX\b\DC2\EOT\131\SOH\SUBL\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\STX\b\185\142\ETX\DC2\EOT\131\SOH\ESCK\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\ETX\DC2\EOT\132\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ETX\ENQ\DC2\EOT\132\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ETX\SOH\DC2\EOT\132\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ETX\ETX\DC2\EOT\132\SOH\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\EOT\DC2\EOT\133\SOH\STXL\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\EOT\ENQ\DC2\EOT\133\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\EOT\SOH\DC2\EOT\133\SOH\t\SYN\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\EOT\ETX\DC2\EOT\133\SOH\EM\SUB\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\EOT\b\DC2\EOT\133\SOH\ESCK\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\EOT\b\187\142\ETX\DC2\EOT\133\SOH\FSJ\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\ENQ\DC2\EOT\134\SOH\STXJ\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ENQ\ACK\DC2\EOT\134\SOH\STX\v\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ENQ\SOH\DC2\EOT\134\SOH\f\DC1\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ENQ\ETX\DC2\EOT\134\SOH\DC4\NAK\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ENQ\b\DC2\EOT\134\SOH\SYNI\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\ENQ\b\193\142\ETX\DC2\EOT\134\SOH\ETBH\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\ACK\DC2\EOT\135\SOH\STXO\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ACK\ENQ\DC2\EOT\135\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ACK\SOH\DC2\EOT\135\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ACK\ETX\DC2\EOT\135\SOH\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\ACK\b\DC2\EOT\135\SOH\FSN\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\ACK\b\185\142\ETX\DC2\EOT\135\SOH\GSM\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\a\DC2\EOT\136\SOH\STXT\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\a\ENQ\DC2\EOT\136\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\a\SOH\DC2\EOT\136\SOH\b\FS\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\a\ETX\DC2\EOT\136\SOH\US \n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\a\b\DC2\EOT\136\SOH!S\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\a\b\185\142\ETX\DC2\EOT\136\SOH\"R\n\
    \\f\n\
    \\EOT\EOT\n\
    \\STX\b\DC2\EOT\137\SOH\STXG\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\b\ENQ\DC2\EOT\137\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\b\SOH\DC2\EOT\137\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\b\ETX\DC2\EOT\137\SOH\DC4\NAK\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\b\b\DC2\EOT\137\SOH\SYNF\n\
    \\DLE\n\
    \\b\EOT\n\
    \\STX\b\b\187\142\ETX\DC2\EOT\137\SOH\ETBE\n\
    \\163\SOH\n\
    \\EOT\EOT\n\
    \\STX\t\DC2\EOT\140\SOH\STX\US\SUB\148\SOH Present only for idle KV pins; latency_profile is then empty. expires_at_ms\n\
    \ equals checked (last_used_at_ms or retained_at_ms) + idle_timeout_ms.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\t\ACK\DC2\EOT\140\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\t\SOH\DC2\EOT\140\SOH\DC2\EM\n\
    \\r\n\
    \\ENQ\EOT\n\
    \\STX\t\ETX\DC2\EOT\140\SOH\FS\RS\n\
    \\139\SOH\n\
    \\STX\ACK\ETX\DC2\ACK\145\SOH\NUL\158\SOH\SOH\SUB} Recoverable logical generation. Distribution, placement, migration, cache,\n\
    \ and worker identities are intentionally absent.\n\
    \\n\
    \\v\n\
    \\ETX\ACK\ETX\SOH\DC2\EOT\145\SOH\b\DC3\n\
    \\SO\n\
    \\EOT\ACK\ETX\STX\NUL\DC2\ACK\146\SOH\STX\148\SOH\ETX\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\SOH\DC2\EOT\146\SOH\ACK\SO\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\STX\DC2\EOT\146\SOH\SI!\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\ETX\DC2\EOT\146\SOH,?\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\NUL\EOT\DC2\EOT\147\SOH\EOT?\n\
    \\DLE\n\
    \\b\ACK\ETX\STX\NUL\EOT\196\142\ETX\DC2\EOT\147\SOH\EOT?\n\
    \\SO\n\
    \\EOT\ACK\ETX\STX\SOH\DC2\ACK\149\SOH\STX\151\SOH\ETX\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\SOH\DC2\EOT\149\SOH\ACK\r\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\STX\DC2\EOT\149\SOH\SO\US\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\ETX\DC2\EOT\149\SOH*1\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\SOH\EOT\DC2\EOT\150\SOH\EOT>\n\
    \\DLE\n\
    \\b\ACK\ETX\STX\SOH\EOT\196\142\ETX\DC2\EOT\150\SOH\EOT>\n\
    \\SO\n\
    \\EOT\ACK\ETX\STX\STX\DC2\ACK\152\SOH\STX\154\SOH\ETX\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\SOH\DC2\EOT\152\SOH\ACK\v\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\STX\DC2\EOT\152\SOH\f\ESC\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\ACK\DC2\EOT\152\SOH&,\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\ETX\DC2\EOT\152\SOH-5\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\STX\EOT\DC2\EOT\153\SOH\EOT<\n\
    \\DLE\n\
    \\b\ACK\ETX\STX\STX\EOT\196\142\ETX\DC2\EOT\153\SOH\EOT<\n\
    \\SO\n\
    \\EOT\ACK\ETX\STX\ETX\DC2\ACK\155\SOH\STX\157\SOH\ETX\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\SOH\DC2\EOT\155\SOH\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\STX\DC2\EOT\155\SOH\r\RS\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\ETX\DC2\EOT\155\SOH)0\n\
    \\r\n\
    \\ENQ\ACK\ETX\STX\ETX\EOT\DC2\EOT\156\SOH\EOT=\n\
    \\DLE\n\
    \\b\ACK\ETX\STX\ETX\EOT\196\142\ETX\DC2\EOT\156\SOH\EOT=\n\
    \\250\SOH\n\
    \\STX\ACK\EOT\DC2\ACK\163\SOH\NUL\170\SOH\SOH\SUB\235\SOH Immutable, recoverable evaluation admissions. Candidate execution and grader\n\
    \ placement remain private; the customer contract contains only exact artifacts,\n\
    \ bounded suite inputs, metric semantics, and content-addressed observations.\n\
    \\n\
    \\v\n\
    \\ETX\ACK\EOT\SOH\DC2\EOT\163\SOH\b\SUB\n\
    \\SO\n\
    \\EOT\ACK\EOT\STX\NUL\DC2\ACK\164\SOH\STX\166\SOH\ETX\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\NUL\SOH\DC2\EOT\164\SOH\ACK\f\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\NUL\STX\DC2\EOT\164\SOH\r$\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\NUL\ETX\DC2\EOT\164\SOH/=\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\NUL\EOT\DC2\EOT\165\SOH\EOTD\n\
    \\DLE\n\
    \\b\ACK\EOT\STX\NUL\EOT\196\142\ETX\DC2\EOT\165\SOH\EOTD\n\
    \\SO\n\
    \\EOT\ACK\EOT\STX\SOH\DC2\ACK\167\SOH\STX\169\SOH\ETX\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\SOH\SOH\DC2\EOT\167\SOH\ACK\r\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\SOH\STX\DC2\EOT\167\SOH\SO&\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\SOH\ETX\DC2\EOT\167\SOH1?\n\
    \\r\n\
    \\ENQ\ACK\EOT\STX\SOH\EOT\DC2\EOT\168\SOH\EOTE\n\
    \\DLE\n\
    \\b\ACK\EOT\STX\SOH\EOT\196\142\ETX\DC2\EOT\168\SOH\EOTE\n\
    \\f\n\
    \\STX\EOT\v\DC2\ACK\172\SOH\NUL\176\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\v\SOH\DC2\EOT\172\SOH\b\SUB\n\
    \\f\n\
    \\EOT\EOT\v\STX\NUL\DC2\EOT\173\SOH\STXF\n\
    \\r\n\
    \\ENQ\EOT\v\STX\NUL\ENQ\DC2\EOT\173\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\v\STX\NUL\SOH\DC2\EOT\173\SOH\b\SO\n\
    \\r\n\
    \\ENQ\EOT\v\STX\NUL\ETX\DC2\EOT\173\SOH\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT\v\STX\NUL\b\DC2\EOT\173\SOH\DC3E\n\
    \\DLE\n\
    \\b\EOT\v\STX\NUL\b\185\142\ETX\DC2\EOT\173\SOH\DC4D\n\
    \\f\n\
    \\EOT\EOT\v\STX\SOH\DC2\EOT\174\SOH\STXK\n\
    \\r\n\
    \\ENQ\EOT\v\STX\SOH\ENQ\DC2\EOT\174\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\v\STX\SOH\SOH\DC2\EOT\174\SOH\t\DC3\n\
    \\r\n\
    \\ENQ\EOT\v\STX\SOH\ETX\DC2\EOT\174\SOH\SYN\ETB\n\
    \\r\n\
    \\ENQ\EOT\v\STX\SOH\b\DC2\EOT\174\SOH\CANJ\n\
    \\DLE\n\
    \\b\EOT\v\STX\SOH\b\191\142\ETX\DC2\EOT\174\SOH\EMI\n\
    \\f\n\
    \\EOT\EOT\v\STX\STX\DC2\EOT\175\SOH\STXK\n\
    \\r\n\
    \\ENQ\EOT\v\STX\STX\ENQ\DC2\EOT\175\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\v\STX\STX\SOH\DC2\EOT\175\SOH\t\NAK\n\
    \\r\n\
    \\ENQ\EOT\v\STX\STX\ETX\DC2\EOT\175\SOH\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\v\STX\STX\b\DC2\EOT\175\SOH\SUBJ\n\
    \\DLE\n\
    \\b\EOT\v\STX\STX\b\187\142\ETX\DC2\EOT\175\SOH\ESCI\n\
    \\f\n\
    \\STX\EOT\f\DC2\ACK\178\SOH\NUL\182\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\f\SOH\DC2\EOT\178\SOH\b\SYN\n\
    \\f\n\
    \\EOT\EOT\f\STX\NUL\DC2\EOT\179\SOH\STXG\n\
    \\r\n\
    \\ENQ\EOT\f\STX\NUL\ENQ\DC2\EOT\179\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\f\STX\NUL\SOH\DC2\EOT\179\SOH\b\SI\n\
    \\r\n\
    \\ENQ\EOT\f\STX\NUL\ETX\DC2\EOT\179\SOH\DC2\DC3\n\
    \\r\n\
    \\ENQ\EOT\f\STX\NUL\b\DC2\EOT\179\SOH\DC4F\n\
    \\DLE\n\
    \\b\EOT\f\STX\NUL\b\185\142\ETX\DC2\EOT\179\SOH\NAKE\n\
    \\f\n\
    \\EOT\EOT\f\STX\SOH\DC2\EOT\180\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\f\STX\SOH\ENQ\DC2\EOT\180\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\f\STX\SOH\SOH\DC2\EOT\180\SOH\b\r\n\
    \\r\n\
    \\ENQ\EOT\f\STX\SOH\ETX\DC2\EOT\180\SOH\DLE\DC1\n\
    \\f\n\
    \\EOT\EOT\f\STX\STX\DC2\EOT\181\SOH\STX^\n\
    \\r\n\
    \\ENQ\EOT\f\STX\STX\EOT\DC2\EOT\181\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\f\STX\STX\ENQ\DC2\EOT\181\SOH\v\DLE\n\
    \\r\n\
    \\ENQ\EOT\f\STX\STX\SOH\DC2\EOT\181\SOH\DC1&\n\
    \\r\n\
    \\ENQ\EOT\f\STX\STX\ETX\DC2\EOT\181\SOH)*\n\
    \\r\n\
    \\ENQ\EOT\f\STX\STX\b\DC2\EOT\181\SOH+]\n\
    \\DLE\n\
    \\b\EOT\f\STX\STX\b\185\142\ETX\DC2\EOT\181\SOH,\\\n\
    \\f\n\
    \\STX\EOT\r\DC2\ACK\184\SOH\NUL\191\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\r\SOH\DC2\EOT\184\SOH\b\ETB\n\
    \\f\n\
    \\EOT\EOT\r\STX\NUL\DC2\EOT\185\SOH\STXI\n\
    \\r\n\
    \\ENQ\EOT\r\STX\NUL\ENQ\DC2\EOT\185\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\r\STX\NUL\SOH\DC2\EOT\185\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\r\STX\NUL\ETX\DC2\EOT\185\SOH\DC4\NAK\n\
    \\r\n\
    \\ENQ\EOT\r\STX\NUL\b\DC2\EOT\185\SOH\SYNH\n\
    \\DLE\n\
    \\b\EOT\r\STX\NUL\b\191\142\ETX\DC2\EOT\185\SOH\ETBG\n\
    \\f\n\
    \\EOT\EOT\r\STX\SOH\DC2\EOT\186\SOH\STXF\n\
    \\r\n\
    \\ENQ\EOT\r\STX\SOH\ENQ\DC2\EOT\186\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\r\STX\SOH\SOH\DC2\EOT\186\SOH\b\SO\n\
    \\r\n\
    \\ENQ\EOT\r\STX\SOH\ETX\DC2\EOT\186\SOH\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT\r\STX\SOH\b\DC2\EOT\186\SOH\DC3E\n\
    \\DLE\n\
    \\b\EOT\r\STX\SOH\b\185\142\ETX\DC2\EOT\186\SOH\DC4D\n\
    \\SO\n\
    \\EOT\EOT\r\STX\STX\DC2\ACK\187\SOH\STX\190\SOH\EOT\n\
    \\r\n\
    \\ENQ\EOT\r\STX\STX\EOT\DC2\EOT\187\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\r\STX\STX\ACK\DC2\EOT\187\SOH\v\EM\n\
    \\r\n\
    \\ENQ\EOT\r\STX\STX\SOH\DC2\EOT\187\SOH\SUB\US\n\
    \\r\n\
    \\ENQ\EOT\r\STX\STX\ETX\DC2\EOT\187\SOH\"#\n\
    \\SI\n\
    \\ENQ\EOT\r\STX\STX\b\DC2\ACK\187\SOH$\190\SOH\ETX\n\
    \\DLE\n\
    \\b\EOT\r\STX\STX\b\189\142\ETX\DC2\EOT\188\SOH\EOT)\n\
    \\DLE\n\
    \\b\EOT\r\STX\STX\b\190\142\ETX\DC2\EOT\189\SOH\EOT,\n\
    \\f\n\
    \\STX\EOT\SO\DC2\ACK\193\SOH\NUL\196\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SO\SOH\DC2\EOT\193\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\SO\STX\NUL\DC2\EOT\194\SOH\STXG\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\NUL\ENQ\DC2\EOT\194\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\NUL\SOH\DC2\EOT\194\SOH\b\SO\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\NUL\ETX\DC2\EOT\194\SOH\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\NUL\b\DC2\EOT\194\SOH\DC3F\n\
    \\DLE\n\
    \\b\EOT\SO\STX\NUL\b\191\142\ETX\DC2\EOT\194\SOH\DC4E\n\
    \\f\n\
    \\EOT\EOT\SO\STX\SOH\DC2\EOT\195\SOH\STXO\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\SOH\ENQ\DC2\EOT\195\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\SOH\SOH\DC2\EOT\195\SOH\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\SOH\ETX\DC2\EOT\195\SOH\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT\SO\STX\SOH\b\DC2\EOT\195\SOH\FSN\n\
    \\DLE\n\
    \\b\EOT\SO\STX\SOH\b\185\142\ETX\DC2\EOT\195\SOH\GSM\n\
    \\f\n\
    \\STX\ENQ\SOH\DC2\ACK\198\SOH\NUL\204\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\SOH\SOH\DC2\EOT\198\SOH\ENQ\SUB\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\NUL\DC2\EOT\199\SOH\STX)\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\SOH\DC2\EOT\199\SOH\STX$\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\NUL\STX\DC2\EOT\199\SOH'(\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\SOH\DC2\EOT\200\SOH\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\SOH\DC2\EOT\200\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\SOH\STX\DC2\EOT\200\SOH !\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\STX\DC2\EOT\201\SOH\STX!\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\SOH\DC2\EOT\201\SOH\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\STX\STX\DC2\EOT\201\SOH\US \n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\ETX\DC2\EOT\202\SOH\STX%\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ETX\SOH\DC2\EOT\202\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\ETX\STX\DC2\EOT\202\SOH#$\n\
    \\f\n\
    \\EOT\ENQ\SOH\STX\EOT\DC2\EOT\203\SOH\STX%\n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\EOT\SOH\DC2\EOT\203\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\SOH\STX\EOT\STX\DC2\EOT\203\SOH#$\n\
    \\f\n\
    \\STX\EOT\SI\DC2\ACK\206\SOH\NUL\209\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\SI\SOH\DC2\EOT\206\SOH\b\CAN\n\
    \\f\n\
    \\EOT\EOT\SI\STX\NUL\DC2\EOT\207\SOH\STXI\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\NUL\ENQ\DC2\EOT\207\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\NUL\SOH\DC2\EOT\207\SOH\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\NUL\ETX\DC2\EOT\207\SOH\DC4\NAK\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\NUL\b\DC2\EOT\207\SOH\SYNH\n\
    \\DLE\n\
    \\b\EOT\SI\STX\NUL\b\191\142\ETX\DC2\EOT\207\SOH\ETBG\n\
    \\f\n\
    \\EOT\EOT\SI\STX\SOH\DC2\EOT\208\SOH\STX\\\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\SOH\ACK\DC2\EOT\208\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\SOH\SOH\DC2\EOT\208\SOH\CAN#\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\SOH\ETX\DC2\EOT\208\SOH&'\n\
    \\r\n\
    \\ENQ\EOT\SI\STX\SOH\b\DC2\EOT\208\SOH([\n\
    \\DLE\n\
    \\b\EOT\SI\STX\SOH\b\193\142\ETX\DC2\EOT\208\SOH)Z\n\
    \\f\n\
    \\STX\EOT\DLE\DC2\ACK\211\SOH\NUL\227\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DLE\SOH\DC2\EOT\211\SOH\b\SYN\n\
    \\SO\n\
    \\EOT\EOT\DLE\STX\NUL\DC2\ACK\212\SOH\STX\215\SOH\EOT\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\EOT\DC2\EOT\212\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\ACK\DC2\EOT\212\SOH\v\GS\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\SOH\DC2\EOT\212\SOH\RS(\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\NUL\ETX\DC2\EOT\212\SOH+,\n\
    \\SI\n\
    \\ENQ\EOT\DLE\STX\NUL\b\DC2\ACK\212\SOH-\215\SOH\ETX\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\NUL\b\189\142\ETX\DC2\EOT\213\SOH\EOT)\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\NUL\b\190\142\ETX\DC2\EOT\214\SOH\EOT+\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\SOH\DC2\EOT\216\SOH\STXN\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\ACK\DC2\EOT\216\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\SOH\DC2\EOT\216\SOH\DC2\ETB\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\ETX\DC2\EOT\216\SOH\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\SOH\b\DC2\EOT\216\SOH\FSM\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\SOH\b\186\142\ETX\DC2\EOT\216\SOH\GSL\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\STX\DC2\EOT\217\SOH\STXP\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\ACK\DC2\EOT\217\SOH\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\SOH\DC2\EOT\217\SOH\DC3\EM\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\ETX\DC2\EOT\217\SOH\FS\GS\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\STX\b\DC2\EOT\217\SOH\RSO\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\STX\b\186\142\ETX\DC2\EOT\217\SOH\USN\n\
    \\SO\n\
    \\EOT\EOT\DLE\STX\ETX\DC2\ACK\218\SOH\STX\221\SOH\EOT\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\EOT\DC2\EOT\218\SOH\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\ACK\DC2\EOT\218\SOH\v\ESC\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\SOH\DC2\EOT\218\SOH\FS#\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ETX\ETX\DC2\EOT\218\SOH&'\n\
    \\SI\n\
    \\ENQ\EOT\DLE\STX\ETX\b\DC2\ACK\218\SOH(\221\SOH\ETX\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\ETX\b\189\142\ETX\DC2\EOT\219\SOH\EOT)\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\ETX\b\190\142\ETX\DC2\EOT\220\SOH\EOT*\n\
    \\SO\n\
    \\EOT\EOT\DLE\STX\EOT\DC2\ACK\222\SOH\STX\225\SOH\EOT\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\EOT\ENQ\DC2\EOT\222\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\EOT\SOH\DC2\EOT\222\SOH\t\GS\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\EOT\ETX\DC2\EOT\222\SOH !\n\
    \\SI\n\
    \\ENQ\EOT\DLE\STX\EOT\b\DC2\ACK\222\SOH\"\225\SOH\ETX\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\EOT\b\187\142\ETX\DC2\EOT\223\SOH\EOT2\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\EOT\b\192\142\ETX\DC2\EOT\224\SOH\EOT.\n\
    \\f\n\
    \\EOT\EOT\DLE\STX\ENQ\DC2\EOT\226\SOH\STXK\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ENQ\ENQ\DC2\EOT\226\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ENQ\SOH\DC2\EOT\226\SOH\b\DC3\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ENQ\ETX\DC2\EOT\226\SOH\SYN\ETB\n\
    \\r\n\
    \\ENQ\EOT\DLE\STX\ENQ\b\DC2\EOT\226\SOH\CANJ\n\
    \\DLE\n\
    \\b\EOT\DLE\STX\ENQ\b\185\142\ETX\DC2\EOT\226\SOH\EMI\n\
    \\f\n\
    \\STX\EOT\DC1\DC2\ACK\229\SOH\NUL\232\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC1\SOH\DC2\EOT\229\SOH\b\US\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\NUL\DC2\EOT\230\SOH\STXQ\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\ACK\DC2\EOT\230\SOH\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\SOH\DC2\EOT\230\SOH\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\ETX\DC2\EOT\230\SOH\GS\RS\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\NUL\b\DC2\EOT\230\SOH\USP\n\
    \\DLE\n\
    \\b\EOT\DC1\STX\NUL\b\186\142\ETX\DC2\EOT\230\SOH O\n\
    \\f\n\
    \\EOT\EOT\DC1\STX\SOH\DC2\EOT\231\SOH\STXL\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\ACK\DC2\EOT\231\SOH\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\SOH\DC2\EOT\231\SOH\DC1\NAK\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\ETX\DC2\EOT\231\SOH\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\DC1\STX\SOH\b\DC2\EOT\231\SOH\SUBK\n\
    \\DLE\n\
    \\b\EOT\DC1\STX\SOH\b\186\142\ETX\DC2\EOT\231\SOH\ESCJ\n\
    \\f\n\
    \\STX\EOT\DC2\DC2\ACK\234\SOH\NUL\236\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC2\SOH\DC2\EOT\234\SOH\b \n\
    \\f\n\
    \\EOT\EOT\DC2\STX\NUL\DC2\EOT\235\SOH\STXM\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\ENQ\DC2\EOT\235\SOH\STX\a\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\SOH\DC2\EOT\235\SOH\b\NAK\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\ETX\DC2\EOT\235\SOH\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\DC2\STX\NUL\b\DC2\EOT\235\SOH\SUBL\n\
    \\DLE\n\
    \\b\EOT\DC2\STX\NUL\b\185\142\ETX\DC2\EOT\235\SOH\ESCK\n\
    \\f\n\
    \\STX\EOT\DC3\DC2\ACK\238\SOH\NUL\241\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC3\SOH\DC2\EOT\238\SOH\b\NAK\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\NUL\DC2\EOT\239\SOH\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ENQ\DC2\EOT\239\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\SOH\DC2\EOT\239\SOH\t\DC2\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\NUL\ETX\DC2\EOT\239\SOH\NAK\SYN\n\
    \\f\n\
    \\EOT\EOT\DC3\STX\SOH\DC2\EOT\240\SOH\STXJ\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\ENQ\DC2\EOT\240\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\SOH\DC2\EOT\240\SOH\t\DC4\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\ETX\DC2\EOT\240\SOH\ETB\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC3\STX\SOH\b\DC2\EOT\240\SOH\EMI\n\
    \\DLE\n\
    \\b\EOT\DC3\STX\SOH\b\187\142\ETX\DC2\EOT\240\SOH\SUBH\n\
    \\f\n\
    \\STX\EOT\DC4\DC2\ACK\243\SOH\NUL\246\SOH\SOH\n\
    \\v\n\
    \\ETX\EOT\DC4\SOH\DC2\EOT\243\SOH\b\GS\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\NUL\DC2\EOT\244\SOH\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ENQ\DC2\EOT\244\SOH\STX\b\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\SOH\DC2\EOT\244\SOH\t\CAN\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\NUL\ETX\DC2\EOT\244\SOH\ESC\FS\n\
    \\f\n\
    \\EOT\EOT\DC4\STX\SOH\DC2\EOT\245\SOH\STXL\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ACK\DC2\EOT\245\SOH\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\SOH\DC2\EOT\245\SOH\DLE\NAK\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\ETX\DC2\EOT\245\SOH\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\DC4\STX\SOH\b\DC2\EOT\245\SOH\SUBK\n\
    \\DLE\n\
    \\b\EOT\DC4\STX\SOH\b\186\142\ETX\DC2\EOT\245\SOH\ESCJ\n\
    \\f\n\
    \\STX\ENQ\STX\DC2\ACK\248\SOH\NUL\253\SOH\SOH\n\
    \\v\n\
    \\ETX\ENQ\STX\SOH\DC2\EOT\248\SOH\ENQ\SUB\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\NUL\DC2\EOT\249\SOH\STX*\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\NUL\SOH\DC2\EOT\249\SOH\STX%\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\NUL\STX\DC2\EOT\249\SOH()\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\SOH\DC2\EOT\250\SOH\STX%\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\SOH\SOH\DC2\EOT\250\SOH\STX \n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\SOH\STX\DC2\EOT\250\SOH#$\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\STX\DC2\EOT\251\SOH\STX/\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\STX\SOH\DC2\EOT\251\SOH\STX*\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\STX\STX\DC2\EOT\251\SOH-.\n\
    \\f\n\
    \\EOT\ENQ\STX\STX\ETX\DC2\EOT\252\SOH\STX,\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ETX\SOH\DC2\EOT\252\SOH\STX'\n\
    \\r\n\
    \\ENQ\ENQ\STX\STX\ETX\STX\DC2\EOT\252\SOH*+\n\
    \\f\n\
    \\STX\EOT\NAK\DC2\ACK\255\SOH\NUL\133\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\NAK\SOH\DC2\EOT\255\SOH\b\FS\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\NUL\DC2\EOT\128\STX\STXP\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ENQ\DC2\EOT\128\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\SOH\DC2\EOT\128\STX\b\CAN\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\ETX\DC2\EOT\128\STX\ESC\FS\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\NUL\b\DC2\EOT\128\STX\GSO\n\
    \\DLE\n\
    \\b\EOT\NAK\STX\NUL\b\185\142\ETX\DC2\EOT\128\STX\RSN\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\SOH\DC2\EOT\129\STX\STXG\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ENQ\DC2\EOT\129\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\SOH\DC2\EOT\129\STX\b\SI\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\ETX\DC2\EOT\129\STX\DC2\DC3\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\SOH\b\DC2\EOT\129\STX\DC4F\n\
    \\DLE\n\
    \\b\EOT\NAK\STX\SOH\b\185\142\ETX\DC2\EOT\129\STX\NAKE\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\STX\DC2\EOT\130\STX\STX`\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ACK\DC2\EOT\130\STX\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\SOH\DC2\EOT\130\STX\RS)\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\ETX\DC2\EOT\130\STX,-\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\STX\b\DC2\EOT\130\STX._\n\
    \\DLE\n\
    \\b\EOT\NAK\STX\STX\b\186\142\ETX\DC2\EOT\130\STX/^\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\ETX\DC2\EOT\131\STX\STX-\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\EOT\DC2\EOT\131\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ACK\DC2\EOT\131\STX\v \n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\SOH\DC2\EOT\131\STX!(\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\ETX\ETX\DC2\EOT\131\STX+,\n\
    \\f\n\
    \\EOT\EOT\NAK\STX\EOT\DC2\EOT\132\STX\STXX\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\ACK\DC2\EOT\132\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\SOH\DC2\EOT\132\STX\CAN\US\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\ETX\DC2\EOT\132\STX\"#\n\
    \\r\n\
    \\ENQ\EOT\NAK\STX\EOT\b\DC2\EOT\132\STX$W\n\
    \\DLE\n\
    \\b\EOT\NAK\STX\EOT\b\193\142\ETX\DC2\EOT\132\STX%V\n\
    \\229\SOH\n\
    \\STX\EOT\SYN\DC2\ACK\138\STX\NUL\142\STX\SOH\SUB\214\SOH The binding is SHA-256(\"acyclic.inference.grader-observation.v1\\0\" ||\n\
    \ native_output_digest || observation_digest). It proves which exact native\n\
    \ device output the grader observed without exposing either payload.\n\
    \\n\
    \\v\n\
    \\ETX\EOT\SYN\SOH\DC2\EOT\138\STX\b#\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\NUL\DC2\EOT\139\STX\STXT\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ENQ\DC2\EOT\139\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\SOH\DC2\EOT\139\STX\b\FS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\ETX\DC2\EOT\139\STX\US \n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\NUL\b\DC2\EOT\139\STX!S\n\
    \\DLE\n\
    \\b\EOT\SYN\STX\NUL\b\185\142\ETX\DC2\EOT\139\STX\"R\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\SOH\DC2\EOT\140\STX\STXR\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ENQ\DC2\EOT\140\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\SOH\DC2\EOT\140\STX\b\SUB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\ETX\DC2\EOT\140\STX\GS\RS\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\SOH\b\DC2\EOT\140\STX\USQ\n\
    \\DLE\n\
    \\b\EOT\SYN\STX\SOH\b\185\142\ETX\DC2\EOT\140\STX P\n\
    \\f\n\
    \\EOT\EOT\SYN\STX\STX\DC2\EOT\141\STX\STXN\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ENQ\DC2\EOT\141\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\SOH\DC2\EOT\141\STX\b\SYN\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\ETX\DC2\EOT\141\STX\EM\SUB\n\
    \\r\n\
    \\ENQ\EOT\SYN\STX\STX\b\DC2\EOT\141\STX\ESCM\n\
    \\DLE\n\
    \\b\EOT\SYN\STX\STX\b\185\142\ETX\DC2\EOT\141\STX\FSL\n\
    \\f\n\
    \\STX\EOT\ETB\DC2\ACK\144\STX\NUL\149\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\ETB\SOH\DC2\EOT\144\STX\b\ESC\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\NUL\DC2\EOT\145\STX\STXP\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ENQ\DC2\EOT\145\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\SOH\DC2\EOT\145\STX\b\CAN\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\ETX\DC2\EOT\145\STX\ESC\FS\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\NUL\b\DC2\EOT\145\STX\GSO\n\
    \\DLE\n\
    \\b\EOT\ETB\STX\NUL\b\185\142\ETX\DC2\EOT\145\STX\RSN\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\SOH\DC2\EOT\146\STX\STX\GS\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ENQ\DC2\EOT\146\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\SOH\DC2\EOT\146\STX\t\CAN\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\SOH\ETX\DC2\EOT\146\STX\ESC\FS\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\STX\DC2\EOT\147\STX\STX\\\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ACK\DC2\EOT\147\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\SOH\DC2\EOT\147\STX\CAN#\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\ETX\DC2\EOT\147\STX&'\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\STX\b\DC2\EOT\147\STX([\n\
    \\DLE\n\
    \\b\EOT\ETB\STX\STX\b\193\142\ETX\DC2\EOT\147\STX)Z\n\
    \\f\n\
    \\EOT\EOT\ETB\STX\ETX\DC2\EOT\148\STX\STXL\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\ACK\DC2\EOT\148\STX\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\SOH\DC2\EOT\148\STX\DLE\NAK\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\ETX\DC2\EOT\148\STX\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\ETB\STX\ETX\b\DC2\EOT\148\STX\SUBK\n\
    \\DLE\n\
    \\b\EOT\ETB\STX\ETX\b\186\142\ETX\DC2\EOT\148\STX\ESCJ\n\
    \\f\n\
    \\STX\EOT\CAN\DC2\ACK\151\STX\NUL\156\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\CAN\SOH\DC2\EOT\151\STX\b\CAN\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\NUL\DC2\EOT\152\STX\STXK\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ENQ\DC2\EOT\152\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\SOH\DC2\EOT\152\STX\b\DC3\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\ETX\DC2\EOT\152\STX\SYN\ETB\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\NUL\b\DC2\EOT\152\STX\CANJ\n\
    \\DLE\n\
    \\b\EOT\CAN\STX\NUL\b\185\142\ETX\DC2\EOT\152\STX\EMI\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\SOH\DC2\EOT\153\STX\STX1\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\EOT\DC2\EOT\153\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ACK\DC2\EOT\153\STX\v\US\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\SOH\DC2\EOT\153\STX ,\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\SOH\ETX\DC2\EOT\153\STX/0\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\STX\DC2\EOT\154\STX\STX.\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\EOT\DC2\EOT\154\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\ACK\DC2\EOT\154\STX\v\RS\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\SOH\DC2\EOT\154\STX\US)\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\STX\ETX\DC2\EOT\154\STX,-\n\
    \\f\n\
    \\EOT\EOT\CAN\STX\ETX\DC2\EOT\155\STX\STXM\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\ENQ\DC2\EOT\155\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\SOH\DC2\EOT\155\STX\b\NAK\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\ETX\DC2\EOT\155\STX\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\CAN\STX\ETX\b\DC2\EOT\155\STX\SUBL\n\
    \\DLE\n\
    \\b\EOT\CAN\STX\ETX\b\185\142\ETX\DC2\EOT\155\STX\ESCK\n\
    \\f\n\
    \\STX\ENQ\ETX\DC2\ACK\158\STX\NUL\165\STX\SOH\n\
    \\v\n\
    \\ETX\ENQ\ETX\SOH\DC2\EOT\158\STX\ENQ\DC4\n\
    \\f\n\
    \\EOT\ENQ\ETX\STX\NUL\DC2\EOT\159\STX\STX#\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\NUL\SOH\DC2\EOT\159\STX\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\NUL\STX\DC2\EOT\159\STX!\"\n\
    \\f\n\
    \\EOT\ENQ\ETX\STX\SOH\DC2\EOT\160\STX\STX \n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\SOH\SOH\DC2\EOT\160\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\SOH\STX\DC2\EOT\160\STX\RS\US\n\
    \\f\n\
    \\EOT\ENQ\ETX\STX\STX\DC2\EOT\161\STX\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\STX\SOH\DC2\EOT\161\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\STX\STX\DC2\EOT\161\STX\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\ETX\STX\ETX\DC2\EOT\162\STX\STX!\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\ETX\SOH\DC2\EOT\162\STX\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\ETX\STX\DC2\EOT\162\STX\US \n\
    \\f\n\
    \\EOT\ENQ\ETX\STX\EOT\DC2\EOT\163\STX\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\EOT\SOH\DC2\EOT\163\STX\STX\EM\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\EOT\STX\DC2\EOT\163\STX\FS\GS\n\
    \\f\n\
    \\EOT\ENQ\ETX\STX\ENQ\DC2\EOT\164\STX\STX!\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\ENQ\SOH\DC2\EOT\164\STX\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\ETX\STX\ENQ\STX\DC2\EOT\164\STX\US \n\
    \\f\n\
    \\STX\EOT\EM\DC2\ACK\167\STX\NUL\173\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\EM\SOH\DC2\EOT\167\STX\b\SYN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\NUL\DC2\EOT\168\STX\STXM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ENQ\DC2\EOT\168\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\SOH\DC2\EOT\168\STX\b\NAK\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\ETX\DC2\EOT\168\STX\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\NUL\b\DC2\EOT\168\STX\SUBL\n\
    \\DLE\n\
    \\b\EOT\EM\STX\NUL\b\185\142\ETX\DC2\EOT\168\STX\ESCK\n\
    \\f\n\
    \\EOT\EOT\EM\STX\SOH\DC2\EOT\169\STX\STXL\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ACK\DC2\EOT\169\STX\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\SOH\DC2\EOT\169\STX\DC1\NAK\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\ETX\DC2\EOT\169\STX\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\SOH\b\DC2\EOT\169\STX\SUBK\n\
    \\DLE\n\
    \\b\EOT\EM\STX\SOH\b\186\142\ETX\DC2\EOT\169\STX\ESCJ\n\
    \\f\n\
    \\EOT\EOT\EM\STX\STX\DC2\EOT\170\STX\STXP\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ACK\DC2\EOT\170\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\SOH\DC2\EOT\170\STX\DC2\ETB\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\ETX\DC2\EOT\170\STX\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\STX\b\DC2\EOT\170\STX\FSO\n\
    \\DLE\n\
    \\b\EOT\EM\STX\STX\b\193\142\ETX\DC2\EOT\170\STX\GSN\n\
    \\f\n\
    \\EOT\EOT\EM\STX\ETX\DC2\EOT\171\STX\STX'\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\EOT\DC2\EOT\171\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ACK\DC2\EOT\171\STX\v\ESC\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\SOH\DC2\EOT\171\STX\FS\"\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\ETX\ETX\DC2\EOT\171\STX%&\n\
    \\f\n\
    \\EOT\EOT\EM\STX\EOT\DC2\EOT\172\STX\STXG\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\ENQ\DC2\EOT\172\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\SOH\DC2\EOT\172\STX\t\DC1\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\ETX\DC2\EOT\172\STX\DC4\NAK\n\
    \\r\n\
    \\ENQ\EOT\EM\STX\EOT\b\DC2\EOT\172\STX\SYNF\n\
    \\DLE\n\
    \\b\EOT\EM\STX\EOT\b\187\142\ETX\DC2\EOT\172\STX\ETBE\n\
    \\f\n\
    \\STX\EOT\SUB\DC2\ACK\175\STX\NUL\178\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\SUB\SOH\DC2\EOT\175\STX\b\ETB\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\NUL\DC2\EOT\176\STX\STXO\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ENQ\DC2\EOT\176\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\SOH\DC2\EOT\176\STX\b\ETB\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\ETX\DC2\EOT\176\STX\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\NUL\b\DC2\EOT\176\STX\FSN\n\
    \\DLE\n\
    \\b\EOT\SUB\STX\NUL\b\185\142\ETX\DC2\EOT\176\STX\GSM\n\
    \\f\n\
    \\EOT\EOT\SUB\STX\SOH\DC2\EOT\177\STX\STXJ\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ENQ\DC2\EOT\177\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\SOH\DC2\EOT\177\STX\b\DC2\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\ETX\DC2\EOT\177\STX\NAK\SYN\n\
    \\r\n\
    \\ENQ\EOT\SUB\STX\SOH\b\DC2\EOT\177\STX\ETBI\n\
    \\DLE\n\
    \\b\EOT\SUB\STX\SOH\b\185\142\ETX\DC2\EOT\177\STX\CANH\n\
    \\f\n\
    \\STX\ENQ\EOT\DC2\ACK\180\STX\NUL\194\STX\SOH\n\
    \\v\n\
    \\ETX\ENQ\EOT\SOH\DC2\EOT\180\STX\ENQ\r\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\NUL\DC2\EOT\181\STX\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\NUL\SOH\DC2\EOT\181\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\NUL\STX\DC2\EOT\181\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\SOH\DC2\EOT\182\STX\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\SOH\SOH\DC2\EOT\182\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\SOH\STX\DC2\EOT\182\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\STX\DC2\EOT\183\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\STX\SOH\DC2\EOT\183\STX\STX\DC2\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\STX\STX\DC2\EOT\183\STX\NAK\SYN\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\ETX\DC2\EOT\184\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ETX\SOH\DC2\EOT\184\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ETX\STX\DC2\EOT\184\STX\CAN\EM\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\EOT\DC2\EOT\185\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\EOT\SOH\DC2\EOT\185\STX\STX\DLE\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\EOT\STX\DC2\EOT\185\STX\DC3\DC4\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\ENQ\DC2\EOT\186\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ENQ\SOH\DC2\EOT\186\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ENQ\STX\DC2\EOT\186\STX\CAN\EM\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\ACK\DC2\EOT\187\STX\STX \n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ACK\SOH\DC2\EOT\187\STX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\ACK\STX\DC2\EOT\187\STX\RS\US\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\a\DC2\EOT\188\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\a\SOH\DC2\EOT\188\STX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\a\STX\DC2\EOT\188\STX\CAN\EM\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\b\DC2\EOT\189\STX\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\b\SOH\DC2\EOT\189\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\b\STX\DC2\EOT\189\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\t\DC2\EOT\190\STX\STX\SYN\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\t\SOH\DC2\EOT\190\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\t\STX\DC2\EOT\190\STX\DC4\NAK\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\n\
    \\DC2\EOT\191\STX\STX\ETB\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\n\
    \\SOH\DC2\EOT\191\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\n\
    \\STX\DC2\EOT\191\STX\DC4\SYN\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\v\DC2\EOT\192\STX\STX\SYN\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\v\SOH\DC2\EOT\192\STX\STX\DLE\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\v\STX\DC2\EOT\192\STX\DC3\NAK\n\
    \\f\n\
    \\EOT\ENQ\EOT\STX\f\DC2\EOT\193\STX\STX\RS\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\f\SOH\DC2\EOT\193\STX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\EOT\STX\f\STX\DC2\EOT\193\STX\ESC\GS\n\
    \\f\n\
    \\STX\EOT\ESC\DC2\ACK\196\STX\NUL\204\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\ESC\SOH\DC2\EOT\196\STX\b\f\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\NUL\DC2\EOT\197\STX\STX\SI\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ENQ\DC2\EOT\197\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\SOH\DC2\EOT\197\STX\b\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\NUL\ETX\DC2\EOT\197\STX\r\SO\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\SOH\DC2\EOT\198\STX\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ACK\DC2\EOT\198\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\SOH\DC2\EOT\198\STX\v\SI\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\SOH\ETX\DC2\EOT\198\STX\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT\ESC\STX\STX\DC2\EOT\199\STX\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ENQ\DC2\EOT\199\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\SOH\DC2\EOT\199\STX\b\SI\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\STX\ETX\DC2\EOT\199\STX\DC2\DC3\n\
    \Z\n\
    \\EOT\EOT\ESC\STX\ETX\DC2\EOT\201\STX\STX\DC1\SUBL Exactly one logical link for tool call/result; absent for all other kinds.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\ENQ\DC2\EOT\201\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\SOH\DC2\EOT\201\STX\b\f\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\ETX\ETX\DC2\EOT\201\STX\SI\DLE\n\
    \X\n\
    \\EOT\EOT\ESC\STX\EOT\DC2\EOT\203\STX\STX!\SUBJ Only a continuation may contain its exact originating execution profile.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\ENQ\DC2\EOT\203\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\SOH\DC2\EOT\203\STX\b\FS\n\
    \\r\n\
    \\ENQ\EOT\ESC\STX\EOT\ETX\DC2\EOT\203\STX\US \n\
    \\f\n\
    \\STX\EOT\FS\DC2\ACK\206\STX\NUL\210\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\FS\SOH\DC2\EOT\206\STX\b\FS\n\
    \\f\n\
    \\EOT\EOT\FS\STX\NUL\DC2\EOT\207\STX\STXQ\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ACK\DC2\EOT\207\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\SOH\DC2\EOT\207\STX\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\ETX\DC2\EOT\207\STX\GS\RS\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\NUL\b\DC2\EOT\207\STX\USP\n\
    \\DLE\n\
    \\b\EOT\FS\STX\NUL\b\186\142\ETX\DC2\EOT\207\STX O\n\
    \\f\n\
    \\EOT\EOT\FS\STX\SOH\DC2\EOT\208\STX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ENQ\DC2\EOT\208\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\SOH\DC2\EOT\208\STX\t\SO\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\SOH\ETX\DC2\EOT\208\STX\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT\FS\STX\STX\DC2\EOT\209\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\EOT\DC2\EOT\209\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ACK\DC2\EOT\209\STX\v\SI\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\SOH\DC2\EOT\209\STX\DLE\NAK\n\
    \\r\n\
    \\ENQ\EOT\FS\STX\STX\ETX\DC2\EOT\209\STX\CAN\EM\n\
    \\f\n\
    \\STX\EOT\GS\DC2\ACK\212\STX\NUL\214\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\GS\SOH\DC2\EOT\212\STX\b\GS\n\
    \\f\n\
    \\EOT\EOT\GS\STX\NUL\DC2\EOT\213\STX\STXH\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ENQ\DC2\EOT\213\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\SOH\DC2\EOT\213\STX\b\DLE\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\ETX\DC2\EOT\213\STX\DC3\DC4\n\
    \\r\n\
    \\ENQ\EOT\GS\STX\NUL\b\DC2\EOT\213\STX\NAKG\n\
    \\DLE\n\
    \\b\EOT\GS\STX\NUL\b\185\142\ETX\DC2\EOT\213\STX\SYNF\n\
    \\n\
    \\n\
    \\STX\EOT\RS\DC2\EOT\215\STX\NUL\DLE\n\
    \\v\n\
    \\ETX\EOT\RS\SOH\DC2\EOT\215\STX\b\r\n\
    \\f\n\
    \\STX\EOT\US\DC2\ACK\216\STX\NUL\219\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\US\SOH\DC2\EOT\216\STX\b\SO\n\
    \\f\n\
    \\EOT\EOT\US\STX\NUL\DC2\EOT\217\STX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ENQ\DC2\EOT\217\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\SOH\DC2\EOT\217\STX\b\SO\n\
    \\r\n\
    \\ENQ\EOT\US\STX\NUL\ETX\DC2\EOT\217\STX\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT\US\STX\SOH\DC2\EOT\218\STX\STX\DLE\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ACK\DC2\EOT\218\STX\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\SOH\DC2\EOT\218\STX\a\v\n\
    \\r\n\
    \\ENQ\EOT\US\STX\SOH\ETX\DC2\EOT\218\STX\SO\SI\n\
    \\f\n\
    \\STX\EOT \DC2\ACK\220\STX\NUL\223\STX\SOH\n\
    \\v\n\
    \\ETX\EOT \SOH\DC2\EOT\220\STX\b\SI\n\
    \\f\n\
    \\EOT\EOT \STX\NUL\DC2\EOT\221\STX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ENQ\DC2\EOT\221\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\SOH\DC2\EOT\221\STX\b\SO\n\
    \\r\n\
    \\ENQ\EOT \STX\NUL\ETX\DC2\EOT\221\STX\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT \STX\SOH\DC2\EOT\222\STX\STX\DC4\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ENQ\DC2\EOT\222\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\SOH\DC2\EOT\222\STX\b\SI\n\
    \\r\n\
    \\ENQ\EOT \STX\SOH\ETX\DC2\EOT\222\STX\DC2\DC3\n\
    \\f\n\
    \\STX\EOT!\DC2\ACK\224\STX\NUL\232\STX\SOH\n\
    \\v\n\
    \\ETX\EOT!\SOH\DC2\EOT\224\STX\b\f\n\
    \\SO\n\
    \\EOT\EOT!\b\NUL\DC2\ACK\225\STX\STX\231\STX\ETX\n\
    \\r\n\
    \\ENQ\EOT!\b\NUL\SOH\DC2\EOT\225\STX\b\SO\n\
    \\f\n\
    \\EOT\EOT!\STX\NUL\DC2\EOT\226\STX\EOT\DC4\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ACK\DC2\EOT\226\STX\EOT\b\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\SOH\DC2\EOT\226\STX\t\SI\n\
    \\r\n\
    \\ENQ\EOT!\STX\NUL\ETX\DC2\EOT\226\STX\DC2\DC3\n\
    \\f\n\
    \\EOT\EOT!\STX\SOH\DC2\EOT\227\STX\EOT\GS\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ACK\DC2\EOT\227\STX\EOT\n\
    \\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\SOH\DC2\EOT\227\STX\v\CAN\n\
    \\r\n\
    \\ENQ\EOT!\STX\SOH\ETX\DC2\EOT\227\STX\ESC\FS\n\
    \\f\n\
    \\EOT\EOT!\STX\STX\DC2\EOT\228\STX\EOT\FS\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\ACK\DC2\EOT\228\STX\EOT\n\
    \\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\SOH\DC2\EOT\228\STX\v\ETB\n\
    \\r\n\
    \\ENQ\EOT!\STX\STX\ETX\DC2\EOT\228\STX\SUB\ESC\n\
    \\f\n\
    \\EOT\EOT!\STX\ETX\DC2\EOT\229\STX\EOT\CAN\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\ACK\DC2\EOT\229\STX\EOT\v\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\SOH\DC2\EOT\229\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT!\STX\ETX\ETX\DC2\EOT\229\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT!\STX\EOT\DC2\EOT\230\STX\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT!\STX\EOT\ENQ\DC2\EOT\230\STX\EOT\t\n\
    \\r\n\
    \\ENQ\EOT!\STX\EOT\SOH\DC2\EOT\230\STX\n\
    \\DLE\n\
    \\r\n\
    \\ENQ\EOT!\STX\EOT\ETX\DC2\EOT\230\STX\DC3\DC4\n\
    \\f\n\
    \\STX\EOT\"\DC2\ACK\233\STX\NUL\235\STX\SOH\n\
    \\v\n\
    \\ETX\EOT\"\SOH\DC2\EOT\233\STX\b\r\n\
    \\f\n\
    \\EOT\EOT\"\STX\NUL\DC2\EOT\234\STX\STX\SUB\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\EOT\DC2\EOT\234\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ACK\DC2\EOT\234\STX\v\SI\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\SOH\DC2\EOT\234\STX\DLE\NAK\n\
    \\r\n\
    \\ENQ\EOT\"\STX\NUL\ETX\DC2\EOT\234\STX\CAN\EM\n\
    \\f\n\
    \\STX\EOT#\DC2\ACK\236\STX\NUL\238\STX\SOH\n\
    \\v\n\
    \\ETX\EOT#\SOH\DC2\EOT\236\STX\b\DLE\n\
    \\f\n\
    \\EOT\EOT#\STX\NUL\DC2\EOT\237\STX\STX\GS\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\EOT\DC2\EOT\237\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ENQ\DC2\EOT\237\STX\v\DLE\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\SOH\DC2\EOT\237\STX\DC1\CAN\n\
    \\r\n\
    \\ENQ\EOT#\STX\NUL\ETX\DC2\EOT\237\STX\ESC\FS\n\
    \\f\n\
    \\STX\EOT$\DC2\ACK\239\STX\NUL\242\STX\SOH\n\
    \\v\n\
    \\ETX\EOT$\SOH\DC2\EOT\239\STX\b\SI\n\
    \\f\n\
    \\EOT\EOT$\STX\NUL\DC2\EOT\240\STX\STX\RS\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\EOT\DC2\EOT\240\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ENQ\DC2\EOT\240\STX\v\DLE\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\SOH\DC2\EOT\240\STX\DC1\EM\n\
    \\r\n\
    \\ENQ\EOT$\STX\NUL\ETX\DC2\EOT\240\STX\FS\GS\n\
    \\f\n\
    \\EOT\EOT$\STX\SOH\DC2\EOT\241\STX\STX \n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\EOT\DC2\EOT\241\STX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\ACK\DC2\EOT\241\STX\v\SI\n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\SOH\DC2\EOT\241\STX\DLE\ESC\n\
    \\r\n\
    \\ENQ\EOT$\STX\SOH\ETX\DC2\EOT\241\STX\RS\US\n\
    \\f\n\
    \\STX\EOT%\DC2\ACK\243\STX\NUL\245\STX\SOH\n\
    \\v\n\
    \\ETX\EOT%\SOH\DC2\EOT\243\STX\b\DLE\n\
    \\f\n\
    \\EOT\EOT%\STX\NUL\DC2\EOT\244\STX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ENQ\DC2\EOT\244\STX\STX\b\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\SOH\DC2\EOT\244\STX\t\SO\n\
    \\r\n\
    \\ENQ\EOT%\STX\NUL\ETX\DC2\EOT\244\STX\DC1\DC2\n\
    \\f\n\
    \\STX\EOT&\DC2\ACK\246\STX\NUL\130\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT&\SOH\DC2\EOT\246\STX\b\FS\n\
    \\f\n\
    \\EOT\EOT&\STX\NUL\DC2\EOT\247\STX\STXQ\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ACK\DC2\EOT\247\STX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\SOH\DC2\EOT\247\STX\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\ETX\DC2\EOT\247\STX\GS\RS\n\
    \\r\n\
    \\ENQ\EOT&\STX\NUL\b\DC2\EOT\247\STX\USP\n\
    \\DLE\n\
    \\b\EOT&\STX\NUL\b\186\142\ETX\DC2\EOT\247\STX O\n\
    \\f\n\
    \\EOT\EOT&\STX\SOH\DC2\EOT\248\STX\STXF\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ENQ\DC2\EOT\248\STX\STX\a\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\SOH\DC2\EOT\248\STX\b\SO\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\ETX\DC2\EOT\248\STX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT&\STX\SOH\b\DC2\EOT\248\STX\DC3E\n\
    \\DLE\n\
    \\b\EOT&\STX\SOH\b\185\142\ETX\DC2\EOT\248\STX\DC4D\n\
    \\SO\n\
    \\EOT\EOT&\b\NUL\DC2\ACK\249\STX\STX\129\ETX\ETX\n\
    \\r\n\
    \\ENQ\EOT&\b\NUL\SOH\DC2\EOT\249\STX\b\SO\n\
    \\r\n\
    \\ENQ\EOT&\b\NUL\STX\DC2\EOT\250\STX\EOT9\n\
    \\DLE\n\
    \\b\EOT&\b\NUL\STX\188\142\ETX\DC2\EOT\250\STX\EOT9\n\
    \\f\n\
    \\EOT\EOT&\STX\STX\DC2\EOT\251\STX\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\ACK\DC2\EOT\251\STX\EOT\t\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\SOH\DC2\EOT\251\STX\n\
    \\SO\n\
    \\r\n\
    \\ENQ\EOT&\STX\STX\ETX\DC2\EOT\251\STX\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT&\STX\ETX\DC2\EOT\252\STX\EOT\DC3\n\
    \\r\n\
    \\ENQ\EOT&\STX\ETX\ACK\DC2\EOT\252\STX\EOT\t\n\
    \\r\n\
    \\ENQ\EOT&\STX\ETX\SOH\DC2\EOT\252\STX\n\
    \\SO\n\
    \\r\n\
    \\ENQ\EOT&\STX\ETX\ETX\DC2\EOT\252\STX\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT&\STX\EOT\DC2\EOT\253\STX\EOT\SUB\n\
    \\r\n\
    \\ENQ\EOT&\STX\EOT\ACK\DC2\EOT\253\STX\EOT\f\n\
    \\r\n\
    \\ENQ\EOT&\STX\EOT\SOH\DC2\EOT\253\STX\r\NAK\n\
    \\r\n\
    \\ENQ\EOT&\STX\EOT\ETX\DC2\EOT\253\STX\CAN\EM\n\
    \\f\n\
    \\EOT\EOT&\STX\ENQ\DC2\EOT\254\STX\EOT\CAN\n\
    \\r\n\
    \\ENQ\EOT&\STX\ENQ\ACK\DC2\EOT\254\STX\EOT\v\n\
    \\r\n\
    \\ENQ\EOT&\STX\ENQ\SOH\DC2\EOT\254\STX\f\DC3\n\
    \\r\n\
    \\ENQ\EOT&\STX\ENQ\ETX\DC2\EOT\254\STX\SYN\ETB\n\
    \\f\n\
    \\EOT\EOT&\STX\ACK\DC2\EOT\255\STX\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT&\STX\ACK\ACK\DC2\EOT\255\STX\EOT\t\n\
    \\r\n\
    \\ENQ\EOT&\STX\ACK\SOH\DC2\EOT\255\STX\n\
    \\DC1\n\
    \\r\n\
    \\ENQ\EOT&\STX\ACK\ETX\DC2\EOT\255\STX\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT&\STX\a\DC2\EOT\128\ETX\EOT\SUB\n\
    \\r\n\
    \\ENQ\EOT&\STX\a\ACK\DC2\EOT\128\ETX\EOT\f\n\
    \\r\n\
    \\ENQ\EOT&\STX\a\SOH\DC2\EOT\128\ETX\r\NAK\n\
    \\r\n\
    \\ENQ\EOT&\STX\a\ETX\DC2\EOT\128\ETX\CAN\EM\n\
    \\f\n\
    \\STX\EOT'\DC2\ACK\132\ETX\NUL\138\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT'\SOH\DC2\EOT\132\ETX\b\ETB\n\
    \\f\n\
    \\EOT\EOT'\STX\NUL\DC2\EOT\133\ETX\STXH\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\ENQ\DC2\EOT\133\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\SOH\DC2\EOT\133\ETX\b\DLE\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\ETX\DC2\EOT\133\ETX\DC3\DC4\n\
    \\r\n\
    \\ENQ\EOT'\STX\NUL\b\DC2\EOT\133\ETX\NAKG\n\
    \\DLE\n\
    \\b\EOT'\STX\NUL\b\185\142\ETX\DC2\EOT\133\ETX\SYNF\n\
    \\f\n\
    \\EOT\EOT'\STX\SOH\DC2\EOT\134\ETX\STXN\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\ENQ\DC2\EOT\134\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\SOH\DC2\EOT\134\ETX\b\SYN\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\ETX\DC2\EOT\134\ETX\EM\SUB\n\
    \\r\n\
    \\ENQ\EOT'\STX\SOH\b\DC2\EOT\134\ETX\ESCM\n\
    \\DLE\n\
    \\b\EOT'\STX\SOH\b\185\142\ETX\DC2\EOT\134\ETX\FSL\n\
    \\f\n\
    \\EOT\EOT'\STX\STX\DC2\EOT\135\ETX\STXG\n\
    \\r\n\
    \\ENQ\EOT'\STX\STX\ENQ\DC2\EOT\135\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT'\STX\STX\SOH\DC2\EOT\135\ETX\t\DC1\n\
    \\r\n\
    \\ENQ\EOT'\STX\STX\ETX\DC2\EOT\135\ETX\DC4\NAK\n\
    \\r\n\
    \\ENQ\EOT'\STX\STX\b\DC2\EOT\135\ETX\SYNF\n\
    \\DLE\n\
    \\b\EOT'\STX\STX\b\187\142\ETX\DC2\EOT\135\ETX\ETBE\n\
    \Z\n\
    \\EOT\EOT'\STX\ETX\DC2\EOT\137\ETX\STX\DC4\SUBL The recorded command's effect, not a promise of current or warm retention.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT'\STX\ETX\ENQ\DC2\EOT\137\ETX\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT'\STX\ETX\SOH\DC2\EOT\137\ETX\a\SI\n\
    \\r\n\
    \\ENQ\EOT'\STX\ETX\ETX\DC2\EOT\137\ETX\DC2\DC3\n\
    \\f\n\
    \\STX\EOT(\DC2\ACK\139\ETX\NUL\148\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT(\SOH\DC2\EOT\139\ETX\b\DC3\n\
    \\f\n\
    \\EOT\EOT(\STX\NUL\DC2\EOT\140\ETX\STXH\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\ENQ\DC2\EOT\140\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\SOH\DC2\EOT\140\ETX\b\DLE\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\ETX\DC2\EOT\140\ETX\DC3\DC4\n\
    \\r\n\
    \\ENQ\EOT(\STX\NUL\b\DC2\EOT\140\ETX\NAKG\n\
    \\DLE\n\
    \\b\EOT(\STX\NUL\b\185\142\ETX\DC2\EOT\140\ETX\SYNF\n\
    \\f\n\
    \\EOT\EOT(\STX\SOH\DC2\EOT\141\ETX\STXO\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\EOT\DC2\EOT\141\ETX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\ENQ\DC2\EOT\141\ETX\v\DLE\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\SOH\DC2\EOT\141\ETX\DC1\ETB\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\ETX\DC2\EOT\141\ETX\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT(\STX\SOH\b\DC2\EOT\141\ETX\FSN\n\
    \\DLE\n\
    \\b\EOT(\STX\SOH\b\185\142\ETX\DC2\EOT\141\ETX\GSM\n\
    \\f\n\
    \\EOT\EOT(\STX\STX\DC2\EOT\142\ETX\STXG\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\ENQ\DC2\EOT\142\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\SOH\DC2\EOT\142\ETX\b\SI\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\ETX\DC2\EOT\142\ETX\DC2\DC3\n\
    \\r\n\
    \\ENQ\EOT(\STX\STX\b\DC2\EOT\142\ETX\DC4F\n\
    \\DLE\n\
    \\b\EOT(\STX\STX\b\185\142\ETX\DC2\EOT\142\ETX\NAKE\n\
    \\f\n\
    \\EOT\EOT(\STX\ETX\DC2\EOT\143\ETX\STXQ\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\ENQ\DC2\EOT\143\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\SOH\DC2\EOT\143\ETX\b\EM\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\ETX\DC2\EOT\143\ETX\FS\GS\n\
    \\r\n\
    \\ENQ\EOT(\STX\ETX\b\DC2\EOT\143\ETX\RSP\n\
    \\DLE\n\
    \\b\EOT(\STX\ETX\b\185\142\ETX\DC2\EOT\143\ETX\USO\n\
    \\f\n\
    \\EOT\EOT(\STX\EOT\DC2\EOT\144\ETX\STXN\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\ENQ\DC2\EOT\144\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\SOH\DC2\EOT\144\ETX\b\SYN\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\ETX\DC2\EOT\144\ETX\EM\SUB\n\
    \\r\n\
    \\ENQ\EOT(\STX\EOT\b\DC2\EOT\144\ETX\ESCM\n\
    \\DLE\n\
    \\b\EOT(\STX\EOT\b\185\142\ETX\DC2\EOT\144\ETX\FSL\n\
    \\f\n\
    \\EOT\EOT(\STX\ENQ\DC2\EOT\145\ETX\STX\SUB\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\EOT\DC2\EOT\145\ETX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\ACK\DC2\EOT\145\ETX\v\SI\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\SOH\DC2\EOT\145\ETX\DLE\NAK\n\
    \\r\n\
    \\ENQ\EOT(\STX\ENQ\ETX\DC2\EOT\145\ETX\CAN\EM\n\
    \\f\n\
    \\EOT\EOT(\STX\ACK\DC2\EOT\146\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\ENQ\DC2\EOT\146\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\SOH\DC2\EOT\146\ETX\t\SO\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\ETX\DC2\EOT\146\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT(\STX\ACK\b\DC2\EOT\146\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT(\STX\ACK\b\191\142\ETX\DC2\EOT\146\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT(\STX\a\DC2\EOT\147\ETX\STXU\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\ACK\DC2\EOT\147\ETX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\SOH\DC2\EOT\147\ETX\DC4\RS\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\ETX\DC2\EOT\147\ETX!\"\n\
    \\r\n\
    \\ENQ\EOT(\STX\a\b\DC2\EOT\147\ETX#T\n\
    \\DLE\n\
    \\b\EOT(\STX\a\b\186\142\ETX\DC2\EOT\147\ETX$S\n\
    \\f\n\
    \\STX\EOT)\DC2\ACK\150\ETX\NUL\160\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT)\SOH\DC2\EOT\150\ETX\b\EM\n\
    \\SO\n\
    \\EOT\EOT)\b\NUL\DC2\ACK\151\ETX\STX\159\ETX\ETX\n\
    \\r\n\
    \\ENQ\EOT)\b\NUL\SOH\DC2\EOT\151\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT)\b\NUL\STX\DC2\EOT\152\ETX\EOT9\n\
    \\DLE\n\
    \\b\EOT)\b\NUL\STX\188\142\ETX\DC2\EOT\152\ETX\EOT9\n\
    \\f\n\
    \\EOT\EOT)\STX\NUL\DC2\EOT\153\ETX\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\ACK\DC2\EOT\153\ETX\EOT\t\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\SOH\DC2\EOT\153\ETX\n\
    \\DC1\n\
    \\r\n\
    \\ENQ\EOT)\STX\NUL\ETX\DC2\EOT\153\ETX\DC4\NAK\n\
    \\f\n\
    \\EOT\EOT)\STX\SOH\DC2\EOT\154\ETX\EOT!\n\
    \\r\n\
    \\ENQ\EOT)\STX\SOH\ACK\DC2\EOT\154\ETX\EOT\DC4\n\
    \\r\n\
    \\ENQ\EOT)\STX\SOH\SOH\DC2\EOT\154\ETX\NAK\FS\n\
    \\r\n\
    \\ENQ\EOT)\STX\SOH\ETX\DC2\EOT\154\ETX\US \n\
    \\f\n\
    \\EOT\EOT)\STX\STX\DC2\EOT\155\ETX\EOT \n\
    \\r\n\
    \\ENQ\EOT)\STX\STX\ACK\DC2\EOT\155\ETX\EOT\DC4\n\
    \\r\n\
    \\ENQ\EOT)\STX\STX\SOH\DC2\EOT\155\ETX\NAK\ESC\n\
    \\r\n\
    \\ENQ\EOT)\STX\STX\ETX\DC2\EOT\155\ETX\RS\US\n\
    \\f\n\
    \\EOT\EOT)\STX\ETX\DC2\EOT\156\ETX\EOT'\n\
    \\r\n\
    \\ENQ\EOT)\STX\ETX\ACK\DC2\EOT\156\ETX\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT)\STX\ETX\SOH\DC2\EOT\156\ETX\ETB\"\n\
    \\r\n\
    \\ENQ\EOT)\STX\ETX\ETX\DC2\EOT\156\ETX%&\n\
    \\f\n\
    \\EOT\EOT)\STX\EOT\DC2\EOT\157\ETX\EOT'\n\
    \\r\n\
    \\ENQ\EOT)\STX\EOT\ACK\DC2\EOT\157\ETX\EOT\CAN\n\
    \\r\n\
    \\ENQ\EOT)\STX\EOT\SOH\DC2\EOT\157\ETX\EM\"\n\
    \\r\n\
    \\ENQ\EOT)\STX\EOT\ETX\DC2\EOT\157\ETX%&\n\
    \\f\n\
    \\EOT\EOT)\STX\ENQ\DC2\EOT\158\ETX\EOT%\n\
    \\r\n\
    \\ENQ\EOT)\STX\ENQ\ACK\DC2\EOT\158\ETX\EOT\SYN\n\
    \\r\n\
    \\ENQ\EOT)\STX\ENQ\SOH\DC2\EOT\158\ETX\ETB \n\
    \\r\n\
    \\ENQ\EOT)\STX\ENQ\ETX\DC2\EOT\158\ETX#$\n\
    \\f\n\
    \\STX\EOT*\DC2\ACK\161\ETX\NUL\163\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT*\SOH\DC2\EOT\161\ETX\b\CAN\n\
    \\f\n\
    \\EOT\EOT*\STX\NUL\DC2\EOT\162\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\ENQ\DC2\EOT\162\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\SOH\DC2\EOT\162\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\ETX\DC2\EOT\162\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT*\STX\NUL\b\DC2\EOT\162\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT*\STX\NUL\b\185\142\ETX\DC2\EOT\162\ETX\DC4D\n\
    \\f\n\
    \\STX\EOT+\DC2\ACK\164\ETX\NUL\167\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT+\SOH\DC2\EOT\164\ETX\b\SUB\n\
    \\f\n\
    \\EOT\EOT+\STX\NUL\DC2\EOT\165\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\ENQ\DC2\EOT\165\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\SOH\DC2\EOT\165\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\ETX\DC2\EOT\165\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT+\STX\NUL\b\DC2\EOT\165\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT+\STX\NUL\b\185\142\ETX\DC2\EOT\165\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT+\STX\SOH\DC2\EOT\166\ETX\STX#\n\
    \\r\n\
    \\ENQ\EOT+\STX\SOH\ENQ\DC2\EOT\166\ETX\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT+\STX\SOH\SOH\DC2\EOT\166\ETX\a\RS\n\
    \\r\n\
    \\ENQ\EOT+\STX\SOH\ETX\DC2\EOT\166\ETX!\"\n\
    \\f\n\
    \\STX\EOT,\DC2\ACK\168\ETX\NUL\171\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT,\SOH\DC2\EOT\168\ETX\b\FS\n\
    \\f\n\
    \\EOT\EOT,\STX\NUL\DC2\EOT\169\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\ENQ\DC2\EOT\169\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\SOH\DC2\EOT\169\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\ETX\DC2\EOT\169\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT,\STX\NUL\b\DC2\EOT\169\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT,\STX\NUL\b\185\142\ETX\DC2\EOT\169\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT,\STX\SOH\DC2\EOT\170\ETX\STXW\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\ENQ\DC2\EOT\170\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\SOH\DC2\EOT\170\ETX\b\US\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\ETX\DC2\EOT\170\ETX\"#\n\
    \\r\n\
    \\ENQ\EOT,\STX\SOH\b\DC2\EOT\170\ETX$V\n\
    \\DLE\n\
    \\b\EOT,\STX\SOH\b\185\142\ETX\DC2\EOT\170\ETX%U\n\
    \\f\n\
    \\STX\EOT-\DC2\ACK\172\ETX\NUL\177\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT-\SOH\DC2\EOT\172\ETX\b\SUB\n\
    \\f\n\
    \\EOT\EOT-\STX\NUL\DC2\EOT\173\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\ENQ\DC2\EOT\173\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\SOH\DC2\EOT\173\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\ETX\DC2\EOT\173\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT-\STX\NUL\b\DC2\EOT\173\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT-\STX\NUL\b\185\142\ETX\DC2\EOT\173\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT-\STX\SOH\DC2\EOT\174\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\ENQ\DC2\EOT\174\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\SOH\DC2\EOT\174\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\ETX\DC2\EOT\174\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT-\STX\SOH\b\DC2\EOT\174\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT-\STX\SOH\b\185\142\ETX\DC2\EOT\174\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT-\STX\STX\DC2\EOT\175\ETX\STXM\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\ENQ\DC2\EOT\175\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\SOH\DC2\EOT\175\ETX\t\ETB\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\ETX\DC2\EOT\175\ETX\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT-\STX\STX\b\DC2\EOT\175\ETX\FSL\n\
    \\DLE\n\
    \\b\EOT-\STX\STX\b\187\142\ETX\DC2\EOT\175\ETX\GSK\n\
    \\f\n\
    \\EOT\EOT-\STX\ETX\DC2\EOT\176\ETX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\EOT\DC2\EOT\176\ETX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\ENQ\DC2\EOT\176\ETX\v\DC1\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\SOH\DC2\EOT\176\ETX\DC2\SYN\n\
    \\r\n\
    \\ENQ\EOT-\STX\ETX\ETX\DC2\EOT\176\ETX\EM\SUB\n\
    \\f\n\
    \\STX\EOT.\DC2\ACK\179\ETX\NUL\185\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT.\SOH\DC2\EOT\179\ETX\b\SUB\n\
    \\f\n\
    \\EOT\EOT.\STX\NUL\DC2\EOT\180\ETX\STXQ\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\ACK\DC2\EOT\180\ETX\STX\DC1\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\SOH\DC2\EOT\180\ETX\DC2\SUB\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\ETX\DC2\EOT\180\ETX\GS\RS\n\
    \\r\n\
    \\ENQ\EOT.\STX\NUL\b\DC2\EOT\180\ETX\USP\n\
    \\DLE\n\
    \\b\EOT.\STX\NUL\b\186\142\ETX\DC2\EOT\180\ETX O\n\
    \\f\n\
    \\EOT\EOT.\STX\SOH\DC2\EOT\181\ETX\STXG\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\ENQ\DC2\EOT\181\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\SOH\DC2\EOT\181\ETX\b\SI\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\ETX\DC2\EOT\181\ETX\DC2\DC3\n\
    \\r\n\
    \\ENQ\EOT.\STX\SOH\b\DC2\EOT\181\ETX\DC4F\n\
    \\DLE\n\
    \\b\EOT.\STX\SOH\b\185\142\ETX\DC2\EOT\181\ETX\NAKE\n\
    \\f\n\
    \\EOT\EOT.\STX\STX\DC2\EOT\182\ETX\STXC\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\ACK\DC2\EOT\182\ETX\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\SOH\DC2\EOT\182\ETX\a\f\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\ETX\DC2\EOT\182\ETX\SI\DLE\n\
    \\r\n\
    \\ENQ\EOT.\STX\STX\b\DC2\EOT\182\ETX\DC1B\n\
    \\DLE\n\
    \\b\EOT.\STX\STX\b\186\142\ETX\DC2\EOT\182\ETX\DC2A\n\
    \\f\n\
    \\EOT\EOT.\STX\ETX\DC2\EOT\183\ETX\STXM\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\ENQ\DC2\EOT\183\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\SOH\DC2\EOT\183\ETX\t\ETB\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\ETX\DC2\EOT\183\ETX\SUB\ESC\n\
    \\r\n\
    \\ENQ\EOT.\STX\ETX\b\DC2\EOT\183\ETX\FSL\n\
    \\DLE\n\
    \\b\EOT.\STX\ETX\b\187\142\ETX\DC2\EOT\183\ETX\GSK\n\
    \\f\n\
    \\EOT\EOT.\STX\EOT\DC2\EOT\184\ETX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\EOT\DC2\EOT\184\ETX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\ENQ\DC2\EOT\184\ETX\v\DC1\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\SOH\DC2\EOT\184\ETX\DC2\SYN\n\
    \\r\n\
    \\ENQ\EOT.\STX\EOT\ETX\DC2\EOT\184\ETX\EM\SUB\n\
    \\f\n\
    \\STX\EOT/\DC2\ACK\187\ETX\NUL\189\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT/\SOH\DC2\EOT\187\ETX\b\ESC\n\
    \\f\n\
    \\EOT\EOT/\STX\NUL\DC2\EOT\188\ETX\STXD\n\
    \\r\n\
    \\ENQ\EOT/\STX\NUL\ACK\DC2\EOT\188\ETX\STX\t\n\
    \\r\n\
    \\ENQ\EOT/\STX\NUL\SOH\DC2\EOT\188\ETX\n\
    \\r\n\
    \\r\n\
    \\ENQ\EOT/\STX\NUL\ETX\DC2\EOT\188\ETX\DLE\DC1\n\
    \\r\n\
    \\ENQ\EOT/\STX\NUL\b\DC2\EOT\188\ETX\DC2C\n\
    \\DLE\n\
    \\b\EOT/\STX\NUL\b\186\142\ETX\DC2\EOT\188\ETX\DC3B\n\
    \\f\n\
    \\STX\EOT0\DC2\ACK\190\ETX\NUL\192\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT0\SOH\DC2\EOT\190\ETX\b\EM\n\
    \\f\n\
    \\EOT\EOT0\STX\NUL\DC2\EOT\191\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT0\STX\NUL\ENQ\DC2\EOT\191\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT0\STX\NUL\SOH\DC2\EOT\191\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT0\STX\NUL\ETX\DC2\EOT\191\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT0\STX\NUL\b\DC2\EOT\191\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT0\STX\NUL\b\185\142\ETX\DC2\EOT\191\ETX\DC4D\n\
    \\f\n\
    \\STX\EOT1\DC2\ACK\193\ETX\NUL\197\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT1\SOH\DC2\EOT\193\ETX\b\ETB\n\
    \\f\n\
    \\EOT\EOT1\STX\NUL\DC2\EOT\194\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT1\STX\NUL\ENQ\DC2\EOT\194\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT1\STX\NUL\SOH\DC2\EOT\194\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT1\STX\NUL\ETX\DC2\EOT\194\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT1\STX\NUL\b\DC2\EOT\194\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT1\STX\NUL\b\185\142\ETX\DC2\EOT\194\ETX\DC4D\n\
    \4\n\
    \\EOT\EOT1\STX\SOH\DC2\EOT\196\ETX\STX\ESC\SUB& Inclusive, zero-based public cursor.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT1\STX\SOH\ENQ\DC2\EOT\196\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT1\STX\SOH\SOH\DC2\EOT\196\ETX\t\SYN\n\
    \\r\n\
    \\ENQ\EOT1\STX\SOH\ETX\DC2\EOT\196\ETX\EM\SUB\n\
    \\f\n\
    \\STX\ENQ\ENQ\DC2\ACK\199\ETX\NUL\208\ETX\SOH\n\
    \\v\n\
    \\ETX\ENQ\ENQ\SOH\DC2\EOT\199\ETX\ENQ\DLE\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\NUL\DC2\EOT\200\ETX\STX\US\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\NUL\SOH\DC2\EOT\200\ETX\STX\SUB\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\NUL\STX\DC2\EOT\200\ETX\GS\RS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\SOH\DC2\EOT\201\ETX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\SOH\SOH\DC2\EOT\201\ETX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\SOH\STX\DC2\EOT\201\ETX\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\STX\DC2\EOT\202\ETX\STX\"\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\STX\SOH\DC2\EOT\202\ETX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\STX\STX\DC2\EOT\202\ETX !\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\ETX\DC2\EOT\203\ETX\STX\GS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ETX\SOH\DC2\EOT\203\ETX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ETX\STX\DC2\EOT\203\ETX\ESC\FS\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\EOT\DC2\EOT\204\ETX\STX\ESC\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\EOT\SOH\DC2\EOT\204\ETX\STX\SYN\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\EOT\STX\DC2\EOT\204\ETX\EM\SUB\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\ENQ\DC2\EOT\205\ETX\STXO\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ENQ\SOH\DC2\EOT\205\ETX\STX\CAN\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ENQ\STX\DC2\EOT\205\ETX\ESC\FS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ENQ\ETX\DC2\EOT\205\ETX\GSN\n\
    \\DLE\n\
    \\b\ENQ\ENQ\STX\ENQ\ETX\195\142\ETX\DC2\EOT\205\ETX\RSM\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\ACK\DC2\EOT\206\ETX\STXL\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ACK\SOH\DC2\EOT\206\ETX\STX\NAK\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ACK\STX\DC2\EOT\206\ETX\CAN\EM\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\ACK\ETX\DC2\EOT\206\ETX\SUBK\n\
    \\DLE\n\
    \\b\ENQ\ENQ\STX\ACK\ETX\195\142\ETX\DC2\EOT\206\ETX\ESCJ\n\
    \\f\n\
    \\EOT\ENQ\ENQ\STX\a\DC2\EOT\207\ETX\STXS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\a\SOH\DC2\EOT\207\ETX\STX\FS\n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\a\STX\DC2\EOT\207\ETX\US \n\
    \\r\n\
    \\ENQ\ENQ\ENQ\STX\a\ETX\DC2\EOT\207\ETX!R\n\
    \\DLE\n\
    \\b\ENQ\ENQ\STX\a\ETX\195\142\ETX\DC2\EOT\207\ETX\"Q\n\
    \\f\n\
    \\STX\EOT2\DC2\ACK\210\ETX\NUL\231\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT2\SOH\DC2\EOT\210\ETX\b\DC4\n\
    \\186\STX\n\
    \\EOT\EOT2\STX\NUL\DC2\EOT\215\ETX\STX\EM\SUB\171\STX Prompt tokens newly computed at this Run's first verified execution.\n\
    \ Frozen with effective_context_reads as a partition of the exact rendered\n\
    \ prompt-token total, bound by native tokenizer/render/model/runtime/KV proof.\n\
    \ Recovery, retry and replay never reclassify or add to this input partition.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT2\STX\NUL\ENQ\DC2\EOT\215\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT2\STX\NUL\SOH\DC2\EOT\215\ETX\t\DC4\n\
    \\r\n\
    \\ENQ\EOT2\STX\NUL\ETX\DC2\EOT\215\ETX\ETB\CAN\n\
    \\142\ETX\n\
    \\EOT\EOT2\STX\SOH\DC2\EOT\221\ETX\STX\RS\SUB\255\STX Uniquely committed native output-token records, counted once. Decoded UTF-8\n\
    \ bytes or re-tokenization cannot establish this count. A non-output EOS\n\
    \ sentinel is excluded; persisted special/stop output records require explicit\n\
    \ meter-revision semantics. Failed/discarded pre-checkpoint device work has no\n\
    \ agreed eligibility rule here and must not be inferred as zero eligible work.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT2\STX\SOH\ENQ\DC2\EOT\221\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT2\STX\SOH\SOH\DC2\EOT\221\ETX\t\EM\n\
    \\r\n\
    \\ENQ\EOT2\STX\SOH\ETX\DC2\EOT\221\ETX\FS\GS\n\
    \\238\SOH\n\
    \\EOT\EOT2\STX\STX\DC2\EOT\225\ETX\STX%\SUB\223\SOH Prompt tokens actually served from verified KV reuse in the same frozen\n\
    \ first-execution partition. Together with new_prefill this equals the exact\n\
    \ rendered prompt-token total; repeated admission/watch adds no new units.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT2\STX\STX\ENQ\DC2\EOT\225\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT2\STX\STX\SOH\DC2\EOT\225\ETX\t \n\
    \\r\n\
    \\ENQ\EOT2\STX\STX\ETX\DC2\EOT\225\ETX#$\n\
    \\176\STX\n\
    \\EOT\EOT2\STX\ETX\DC2\EOT\230\ETX\STX\"\SUB\161\STX Logical Context retention measure. Logical custody identity,\n\
    \ interval events, pending-intent eligibility and dedup scope remain unagreed.\n\
    \ Neither device/cache allocations nor Objects physical storage establish\n\
    \ this measure; absent lifecycle evidence must not imply zero eligible work.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT2\STX\ETX\ENQ\DC2\EOT\230\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT2\STX\ETX\SOH\DC2\EOT\230\ETX\t\GS\n\
    \\r\n\
    \\ENQ\EOT2\STX\ETX\ETX\DC2\EOT\230\ETX !\n\
    \\f\n\
    \\STX\EOT3\DC2\ACK\233\ETX\NUL\241\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT3\SOH\DC2\EOT\233\ETX\b\DC4\n\
    \\f\n\
    \\EOT\EOT3\STX\NUL\DC2\EOT\234\ETX\STXJ\n\
    \\r\n\
    \\ENQ\EOT3\STX\NUL\ENQ\DC2\EOT\234\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT3\STX\NUL\SOH\DC2\EOT\234\ETX\b\DC2\n\
    \\r\n\
    \\ENQ\EOT3\STX\NUL\ETX\DC2\EOT\234\ETX\NAK\SYN\n\
    \\r\n\
    \\ENQ\EOT3\STX\NUL\b\DC2\EOT\234\ETX\ETBI\n\
    \\DLE\n\
    \\b\EOT3\STX\NUL\b\185\142\ETX\DC2\EOT\234\ETX\CANH\n\
    \\f\n\
    \\EOT\EOT3\STX\SOH\DC2\EOT\235\ETX\STXM\n\
    \\r\n\
    \\ENQ\EOT3\STX\SOH\ENQ\DC2\EOT\235\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT3\STX\SOH\SOH\DC2\EOT\235\ETX\b\NAK\n\
    \\r\n\
    \\ENQ\EOT3\STX\SOH\ETX\DC2\EOT\235\ETX\CAN\EM\n\
    \\r\n\
    \\ENQ\EOT3\STX\SOH\b\DC2\EOT\235\ETX\SUBL\n\
    \\DLE\n\
    \\b\EOT3\STX\SOH\b\185\142\ETX\DC2\EOT\235\ETX\ESCK\n\
    \\f\n\
    \\EOT\EOT3\STX\STX\DC2\EOT\236\ETX\STXN\n\
    \\r\n\
    \\ENQ\EOT3\STX\STX\ENQ\DC2\EOT\236\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT3\STX\STX\SOH\DC2\EOT\236\ETX\b\SYN\n\
    \\r\n\
    \\ENQ\EOT3\STX\STX\ETX\DC2\EOT\236\ETX\EM\SUB\n\
    \\r\n\
    \\ENQ\EOT3\STX\STX\b\DC2\EOT\236\ETX\ESCM\n\
    \\DLE\n\
    \\b\EOT3\STX\STX\b\185\142\ETX\DC2\EOT\236\ETX\FSL\n\
    \\163\SOH\n\
    \\EOT\EOT3\STX\ETX\DC2\EOT\239\ETX\STXK\SUB\148\SOH Immutable final totals for this receipt, never an incremental charge delta.\n\
    \ Meter semantics are independent of pricing or charging authorization.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT3\STX\ETX\ACK\DC2\EOT\239\ETX\STX\SO\n\
    \\r\n\
    \\ENQ\EOT3\STX\ETX\SOH\DC2\EOT\239\ETX\SI\DC4\n\
    \\r\n\
    \\ENQ\EOT3\STX\ETX\ETX\DC2\EOT\239\ETX\ETB\CAN\n\
    \\r\n\
    \\ENQ\EOT3\STX\ETX\b\DC2\EOT\239\ETX\EMJ\n\
    \\DLE\n\
    \\b\EOT3\STX\ETX\b\186\142\ETX\DC2\EOT\239\ETX\SUBI\n\
    \\f\n\
    \\EOT\EOT3\STX\EOT\DC2\EOT\240\ETX\STXR\n\
    \\r\n\
    \\ENQ\EOT3\STX\EOT\ENQ\DC2\EOT\240\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT3\STX\EOT\SOH\DC2\EOT\240\ETX\b\SUB\n\
    \\r\n\
    \\ENQ\EOT3\STX\EOT\ETX\DC2\EOT\240\ETX\GS\RS\n\
    \\r\n\
    \\ENQ\EOT3\STX\EOT\b\DC2\EOT\240\ETX\USQ\n\
    \\DLE\n\
    \\b\EOT3\STX\EOT\b\185\142\ETX\DC2\EOT\240\ETX P\n\
    \\f\n\
    \\STX\EOT4\DC2\ACK\243\ETX\NUL\248\ETX\SOH\n\
    \\v\n\
    \\ETX\EOT4\SOH\DC2\EOT\243\ETX\b\DC1\n\
    \\f\n\
    \\EOT\EOT4\STX\NUL\DC2\EOT\244\ETX\STX\DC3\n\
    \\r\n\
    \\ENQ\EOT4\STX\NUL\ENQ\DC2\EOT\244\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT4\STX\NUL\SOH\DC2\EOT\244\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT4\STX\NUL\ETX\DC2\EOT\244\ETX\DC1\DC2\n\
    \\f\n\
    \\EOT\EOT4\STX\SOH\DC2\EOT\245\ETX\STX#\n\
    \\r\n\
    \\ENQ\EOT4\STX\SOH\EOT\DC2\EOT\245\ETX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT4\STX\SOH\ACK\DC2\EOT\245\ETX\v\SYN\n\
    \\r\n\
    \\ENQ\EOT4\STX\SOH\SOH\DC2\EOT\245\ETX\ETB\RS\n\
    \\r\n\
    \\ENQ\EOT4\STX\SOH\ETX\DC2\EOT\245\ETX!\"\n\
    \\f\n\
    \\EOT\EOT4\STX\STX\DC2\EOT\246\ETX\STXO\n\
    \\r\n\
    \\ENQ\EOT4\STX\STX\ACK\DC2\EOT\246\ETX\STX\r\n\
    \\r\n\
    \\ENQ\EOT4\STX\STX\SOH\DC2\EOT\246\ETX\SO\SYN\n\
    \\r\n\
    \\ENQ\EOT4\STX\STX\ETX\DC2\EOT\246\ETX\EM\SUB\n\
    \\r\n\
    \\ENQ\EOT4\STX\STX\b\DC2\EOT\246\ETX\ESCN\n\
    \\DLE\n\
    \\b\EOT4\STX\STX\b\193\142\ETX\DC2\EOT\246\ETX\FSM\n\
    \\f\n\
    \\EOT\EOT4\STX\ETX\DC2\EOT\247\ETX\STX$\n\
    \\r\n\
    \\ENQ\EOT4\STX\ETX\EOT\DC2\EOT\247\ETX\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT4\STX\ETX\ACK\DC2\EOT\247\ETX\v\ETB\n\
    \\r\n\
    \\ENQ\EOT4\STX\ETX\SOH\DC2\EOT\247\ETX\CAN\US\n\
    \\r\n\
    \\ENQ\EOT4\STX\ETX\ETX\DC2\EOT\247\ETX\"#\n\
    \\f\n\
    \\STX\EOT5\DC2\ACK\250\ETX\NUL\129\EOT\SOH\n\
    \\v\n\
    \\ETX\EOT5\SOH\DC2\EOT\250\ETX\b\SI\n\
    \\f\n\
    \\EOT\EOT5\STX\NUL\DC2\EOT\251\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT5\STX\NUL\ENQ\DC2\EOT\251\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT5\STX\NUL\SOH\DC2\EOT\251\ETX\b\SO\n\
    \\r\n\
    \\ENQ\EOT5\STX\NUL\ETX\DC2\EOT\251\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT5\STX\NUL\b\DC2\EOT\251\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT5\STX\NUL\b\185\142\ETX\DC2\EOT\251\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT5\STX\SOH\DC2\EOT\252\ETX\STXE\n\
    \\r\n\
    \\ENQ\EOT5\STX\SOH\ENQ\DC2\EOT\252\ETX\STX\a\n\
    \\r\n\
    \\ENQ\EOT5\STX\SOH\SOH\DC2\EOT\252\ETX\b\r\n\
    \\r\n\
    \\ENQ\EOT5\STX\SOH\ETX\DC2\EOT\252\ETX\DLE\DC1\n\
    \\r\n\
    \\ENQ\EOT5\STX\SOH\b\DC2\EOT\252\ETX\DC2D\n\
    \\DLE\n\
    \\b\EOT5\STX\SOH\b\185\142\ETX\DC2\EOT\252\ETX\DC3C\n\
    \\f\n\
    \\EOT\EOT5\STX\STX\DC2\EOT\253\ETX\STXF\n\
    \\r\n\
    \\ENQ\EOT5\STX\STX\ENQ\DC2\EOT\253\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT5\STX\STX\SOH\DC2\EOT\253\ETX\t\SO\n\
    \\r\n\
    \\ENQ\EOT5\STX\STX\ETX\DC2\EOT\253\ETX\DC1\DC2\n\
    \\r\n\
    \\ENQ\EOT5\STX\STX\b\DC2\EOT\253\ETX\DC3E\n\
    \\DLE\n\
    \\b\EOT5\STX\STX\b\191\142\ETX\DC2\EOT\253\ETX\DC4D\n\
    \\f\n\
    \\EOT\EOT5\STX\ETX\DC2\EOT\254\ETX\STX\ESC\n\
    \\r\n\
    \\ENQ\EOT5\STX\ETX\ENQ\DC2\EOT\254\ETX\STX\b\n\
    \\r\n\
    \\ENQ\EOT5\STX\ETX\SOH\DC2\EOT\254\ETX\t\SYN\n\
    \\r\n\
    \\ENQ\EOT5\STX\ETX\ETX\DC2\EOT\254\ETX\EM\SUB\n\
    \\f\n\
    \\EOT\EOT5\STX\EOT\DC2\EOT\255\ETX\STX\"\n\
    \\r\n\
    \\ENQ\EOT5\STX\EOT\ENQ\DC2\EOT\255\ETX\STX\ACK\n\
    \\r\n\
    \\ENQ\EOT5\STX\EOT\SOH\DC2\EOT\255\ETX\a\GS\n\
    \\r\n\
    \\ENQ\EOT5\STX\EOT\ETX\DC2\EOT\255\ETX !\n\
    \\f\n\
    \\EOT\EOT5\STX\ENQ\DC2\EOT\128\EOT\STX \n\
    \\r\n\
    \\ENQ\EOT5\STX\ENQ\EOT\DC2\EOT\128\EOT\STX\n\
    \\n\
    \\r\n\
    \\ENQ\EOT5\STX\ENQ\ACK\DC2\EOT\128\EOT\v\DC4\n\
    \\r\n\
    \\ENQ\EOT5\STX\ENQ\SOH\DC2\EOT\128\EOT\NAK\ESC\n\
    \\r\n\
    \\ENQ\EOT5\STX\ENQ\ETX\DC2\EOT\128\EOT\RS\US\n\
    \\f\n\
    \\STX\EOT6\DC2\ACK\131\EOT\NUL\142\EOT\SOH\n\
    \\v\n\
    \\ETX\EOT6\SOH\DC2\EOT\131\EOT\b\DLE\n\
    \\f\n\
    \\EOT\EOT6\STX\NUL\DC2\EOT\132\EOT\STX\SYN\n\
    \\r\n\
    \\ENQ\EOT6\STX\NUL\ENQ\DC2\EOT\132\EOT\STX\b\n\
    \\r\n\
    \\ENQ\EOT6\STX\NUL\SOH\DC2\EOT\132\EOT\t\DC1\n\
    \\r\n\
    \\ENQ\EOT6\STX\NUL\ETX\DC2\EOT\132\EOT\DC4\NAK\n\
    \\SO\n\
    \\EOT\EOT6\b\NUL\DC2\ACK\133\EOT\STX\141\EOT\ETX\n\
    \\r\n\
    \\ENQ\EOT6\b\NUL\SOH\DC2\EOT\133\EOT\b\r\n\
    \\r\n\
    \\ENQ\EOT6\b\NUL\STX\DC2\EOT\134\EOT\EOT9\n\
    \\DLE\n\
    \\b\EOT6\b\NUL\STX\188\142\ETX\DC2\EOT\134\EOT\EOT9\n\
    \\f\n\
    \\EOT\EOT6\STX\SOH\DC2\EOT\135\EOT\EOT\NAK\n\
    \\r\n\
    \\ENQ\EOT6\STX\SOH\ENQ\DC2\EOT\135\EOT\EOT\t\n\
    \\r\n\
    \\ENQ\EOT6\STX\SOH\SOH\DC2\EOT\135\EOT\n\
    \\DLE\n\
    \\r\n\
    \\ENQ\EOT6\STX\SOH\ETX\DC2\EOT\135\EOT\DC3\DC4\n\
    \\163\SOH\n\
    \\EOT\EOT6\STX\STX\DC2\EOT\138\EOT\EOT\ESC\SUB\148\SOH Authoritative cumulative snapshot for this Run. Recovered/replayed watch\n\
    \ events are snapshots of the same units, never incremental charge deltas.\n\
    \\n\
    \\r\n\
    \\ENQ\EOT6\STX\STX\ACK\DC2\EOT\138\EOT\EOT\DLE\n\
    \\r\n\
    \\ENQ\EOT6\STX\STX\SOH\DC2\EOT\138\EOT\DC1\SYN\n\
    \\r\n\
    \\ENQ\EOT6\STX\STX\ETX\DC2\EOT\138\EOT\EM\SUB\n\
    \\f\n\
    \\EOT\EOT6\STX\ETX\DC2\EOT\139\EOT\EOTQ\n\
    \\r\n\
    \\ENQ\EOT6\STX\ETX\ACK\DC2\EOT\139\EOT\EOT\SI\n\
    \\r\n\
    \\ENQ\EOT6\STX\ETX\SOH\DC2\EOT\139\EOT\DLE\CAN\n\
    \\r\n\
    \\ENQ\EOT6\STX\ETX\ETX\DC2\EOT\139\EOT\ESC\FS\n\
    \\r\n\
    \\ENQ\EOT6\STX\ETX\b\DC2\EOT\139\EOT\GSP\n\
    \\DLE\n\
    \\b\EOT6\STX\ETX\b\193\142\ETX\DC2\EOT\139\EOT\RSO\n\
    \\f\n\
    \\EOT\EOT6\STX\EOT\DC2\EOT\140\EOT\EOT\GS\n\
    \\r\n\
    \\ENQ\EOT6\STX\EOT\ACK\DC2\EOT\140\EOT\EOT\SI\n\
    \\r\n\
    \\ENQ\EOT6\STX\EOT\SOH\DC2\EOT\140\EOT\DLE\CAN\n\
    \\r\n\
    \\ENQ\EOT6\STX\EOT\ETX\DC2\EOT\140\EOT\ESC\FS\n\
    \\f\n\
    \\STX\EOT7\DC2\ACK\144\EOT\NUL\146\EOT\SOH\n\
    \\v\n\
    \\ETX\EOT7\SOH\DC2\EOT\144\EOT\b\DC3\n\
    \\f\n\
    \\EOT\EOT7\STX\NUL\DC2\EOT\145\EOT\STX\DC2\n\
    \\r\n\
    \\ENQ\EOT7\STX\NUL\ENQ\DC2\EOT\145\EOT\STX\b\n\
    \\r\n\
    \\ENQ\EOT7\STX\NUL\SOH\DC2\EOT\145\EOT\t\r\n\
    \\r\n\
    \\ENQ\EOT7\STX\NUL\ETX\DC2\EOT\145\EOT\DLE\DC1b\ACKproto3"