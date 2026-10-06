{- This file was auto-generated from protocol/v1/protocol.proto by the proto-lens-protoc program. -}
{-# LANGUAGE ScopedTypeVariables, DataKinds, TypeFamilies, UndecidableInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, PatternSynonyms, MagicHash, NoImplicitPrelude, DataKinds, BangPatterns, TypeApplications, OverloadedStrings, DerivingStrategies#-}
{-# OPTIONS_GHC -Wno-unused-imports#-}
{-# OPTIONS_GHC -Wno-duplicate-exports#-}
{-# OPTIONS_GHC -Wno-dodgy-exports#-}
module Proto.Protocol.V1.Protocol (
        Capability(), CapabilitySet(), HandshakeRequest(),
        HandshakeResponse(), ProtocolIdentity()
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
     
         * 'Proto.Protocol.V1.Protocol_Fields.name' @:: Lens' Capability Data.Text.Text@
         * 'Proto.Protocol.V1.Protocol_Fields.version' @:: Lens' Capability Data.Text.Text@ -}
data Capability
  = Capability'_constructor {_Capability'name :: !Data.Text.Text,
                             _Capability'version :: !Data.Text.Text,
                             _Capability'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show Capability where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField Capability "name" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Capability'name (\ x__ y__ -> x__ {_Capability'name = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField Capability "version" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _Capability'version (\ x__ y__ -> x__ {_Capability'version = y__}))
        Prelude.id
instance Data.ProtoLens.Message Capability where
  messageName _ = Data.Text.pack "acyclic.protocol.v1.Capability"
  packedMessageDescriptor _
    = "\n\
      \\n\
      \Capability\DC2\DC2\n\
      \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\CAN\n\
      \\aversion\CAN\STX \SOH(\tR\aversion"
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
              Data.ProtoLens.FieldDescriptor Capability
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"version")) ::
              Data.ProtoLens.FieldDescriptor Capability
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, name__field_descriptor),
           (Data.ProtoLens.Tag 2, version__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _Capability'_unknownFields
        (\ x__ y__ -> x__ {_Capability'_unknownFields = y__})
  defMessage
    = Capability'_constructor
        {_Capability'name = Data.ProtoLens.fieldDefault,
         _Capability'version = Data.ProtoLens.fieldDefault,
         _Capability'_unknownFields = []}
  parseMessage
    = let
        loop ::
          Capability -> Data.ProtoLens.Encoding.Bytes.Parser Capability
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
          (do loop Data.ProtoLens.defMessage) "Capability"
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
                   _v = Lens.Family2.view (Data.ProtoLens.Field.field @"version") _x
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
instance Control.DeepSeq.NFData Capability where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_Capability'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_Capability'name x__)
                (Control.DeepSeq.deepseq (_Capability'version x__) ()))
{- | Fields :
     
         * 'Proto.Protocol.V1.Protocol_Fields.capabilities' @:: Lens' CapabilitySet [Capability]@
         * 'Proto.Protocol.V1.Protocol_Fields.vec'capabilities' @:: Lens' CapabilitySet (Data.Vector.Vector Capability)@ -}
data CapabilitySet
  = CapabilitySet'_constructor {_CapabilitySet'capabilities :: !(Data.Vector.Vector Capability),
                                _CapabilitySet'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show CapabilitySet where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField CapabilitySet "capabilities" [Capability] where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CapabilitySet'capabilities
           (\ x__ y__ -> x__ {_CapabilitySet'capabilities = y__}))
        (Lens.Family2.Unchecked.lens
           Data.Vector.Generic.toList
           (\ _ y__ -> Data.Vector.Generic.fromList y__))
instance Data.ProtoLens.Field.HasField CapabilitySet "vec'capabilities" (Data.Vector.Vector Capability) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _CapabilitySet'capabilities
           (\ x__ y__ -> x__ {_CapabilitySet'capabilities = y__}))
        Prelude.id
instance Data.ProtoLens.Message CapabilitySet where
  messageName _ = Data.Text.pack "acyclic.protocol.v1.CapabilitySet"
  packedMessageDescriptor _
    = "\n\
      \\rCapabilitySet\DC2C\n\
      \\fcapabilities\CAN\SOH \ETX(\v2\US.acyclic.protocol.v1.CapabilityR\fcapabilities"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        capabilities__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "capabilities"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor Capability)
              (Data.ProtoLens.RepeatedField
                 Data.ProtoLens.Unpacked
                 (Data.ProtoLens.Field.field @"capabilities")) ::
              Data.ProtoLens.FieldDescriptor CapabilitySet
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, capabilities__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _CapabilitySet'_unknownFields
        (\ x__ y__ -> x__ {_CapabilitySet'_unknownFields = y__})
  defMessage
    = CapabilitySet'_constructor
        {_CapabilitySet'capabilities = Data.Vector.Generic.empty,
         _CapabilitySet'_unknownFields = []}
  parseMessage
    = let
        loop ::
          CapabilitySet
          -> Data.ProtoLens.Encoding.Growing.Growing Data.Vector.Vector Data.ProtoLens.Encoding.Growing.RealWorld Capability
             -> Data.ProtoLens.Encoding.Bytes.Parser CapabilitySet
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
                          -> do !y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                        (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                            Data.ProtoLens.Encoding.Bytes.isolate
                                              (Prelude.fromIntegral len)
                                              Data.ProtoLens.parseMessage)
                                        "capabilities"
                                v <- Data.ProtoLens.Encoding.Parser.Unsafe.unsafeLiftIO
                                       (Data.ProtoLens.Encoding.Growing.append
                                          mutable'capabilities y)
                                loop x v
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
          "CapabilitySet"
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
                   (Data.ProtoLens.Field.field @"vec'capabilities") _x))
             (Data.ProtoLens.Encoding.Wire.buildFieldSet
                (Lens.Family2.view Data.ProtoLens.unknownFields _x))
instance Control.DeepSeq.NFData CapabilitySet where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_CapabilitySet'_unknownFields x__)
             (Control.DeepSeq.deepseq (_CapabilitySet'capabilities x__) ())
{- | Fields :
     
         * 'Proto.Protocol.V1.Protocol_Fields.protocol' @:: Lens' HandshakeRequest ProtocolIdentity@
         * 'Proto.Protocol.V1.Protocol_Fields.maybe'protocol' @:: Lens' HandshakeRequest (Prelude.Maybe ProtocolIdentity)@
         * 'Proto.Protocol.V1.Protocol_Fields.required' @:: Lens' HandshakeRequest CapabilitySet@
         * 'Proto.Protocol.V1.Protocol_Fields.maybe'required' @:: Lens' HandshakeRequest (Prelude.Maybe CapabilitySet)@ -}
data HandshakeRequest
  = HandshakeRequest'_constructor {_HandshakeRequest'protocol :: !(Prelude.Maybe ProtocolIdentity),
                                   _HandshakeRequest'required :: !(Prelude.Maybe CapabilitySet),
                                   _HandshakeRequest'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show HandshakeRequest where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField HandshakeRequest "protocol" ProtocolIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeRequest'protocol
           (\ x__ y__ -> x__ {_HandshakeRequest'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HandshakeRequest "maybe'protocol" (Prelude.Maybe ProtocolIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeRequest'protocol
           (\ x__ y__ -> x__ {_HandshakeRequest'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HandshakeRequest "required" CapabilitySet where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeRequest'required
           (\ x__ y__ -> x__ {_HandshakeRequest'required = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HandshakeRequest "maybe'required" (Prelude.Maybe CapabilitySet) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeRequest'required
           (\ x__ y__ -> x__ {_HandshakeRequest'required = y__}))
        Prelude.id
instance Data.ProtoLens.Message HandshakeRequest where
  messageName _
    = Data.Text.pack "acyclic.protocol.v1.HandshakeRequest"
  packedMessageDescriptor _
    = "\n\
      \\DLEHandshakeRequest\DC2A\n\
      \\bprotocol\CAN\SOH \SOH(\v2%.acyclic.protocol.v1.ProtocolIdentityR\bprotocol\DC2>\n\
      \\brequired\CAN\STX \SOH(\v2\".acyclic.protocol.v1.CapabilitySetR\brequired"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor HandshakeRequest
        required__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "required"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CapabilitySet)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'required")) ::
              Data.ProtoLens.FieldDescriptor HandshakeRequest
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, required__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HandshakeRequest'_unknownFields
        (\ x__ y__ -> x__ {_HandshakeRequest'_unknownFields = y__})
  defMessage
    = HandshakeRequest'_constructor
        {_HandshakeRequest'protocol = Prelude.Nothing,
         _HandshakeRequest'required = Prelude.Nothing,
         _HandshakeRequest'_unknownFields = []}
  parseMessage
    = let
        loop ::
          HandshakeRequest
          -> Data.ProtoLens.Encoding.Bytes.Parser HandshakeRequest
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
                                       "required"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"required") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "HandshakeRequest"
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
                     Lens.Family2.view (Data.ProtoLens.Field.field @"maybe'required") _x
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
instance Control.DeepSeq.NFData HandshakeRequest where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_HandshakeRequest'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_HandshakeRequest'protocol x__)
                (Control.DeepSeq.deepseq (_HandshakeRequest'required x__) ()))
{- | Fields :
     
         * 'Proto.Protocol.V1.Protocol_Fields.protocol' @:: Lens' HandshakeResponse ProtocolIdentity@
         * 'Proto.Protocol.V1.Protocol_Fields.maybe'protocol' @:: Lens' HandshakeResponse (Prelude.Maybe ProtocolIdentity)@
         * 'Proto.Protocol.V1.Protocol_Fields.supported' @:: Lens' HandshakeResponse CapabilitySet@
         * 'Proto.Protocol.V1.Protocol_Fields.maybe'supported' @:: Lens' HandshakeResponse (Prelude.Maybe CapabilitySet)@ -}
data HandshakeResponse
  = HandshakeResponse'_constructor {_HandshakeResponse'protocol :: !(Prelude.Maybe ProtocolIdentity),
                                    _HandshakeResponse'supported :: !(Prelude.Maybe CapabilitySet),
                                    _HandshakeResponse'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show HandshakeResponse where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField HandshakeResponse "protocol" ProtocolIdentity where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeResponse'protocol
           (\ x__ y__ -> x__ {_HandshakeResponse'protocol = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HandshakeResponse "maybe'protocol" (Prelude.Maybe ProtocolIdentity) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeResponse'protocol
           (\ x__ y__ -> x__ {_HandshakeResponse'protocol = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField HandshakeResponse "supported" CapabilitySet where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeResponse'supported
           (\ x__ y__ -> x__ {_HandshakeResponse'supported = y__}))
        (Data.ProtoLens.maybeLens Data.ProtoLens.defMessage)
instance Data.ProtoLens.Field.HasField HandshakeResponse "maybe'supported" (Prelude.Maybe CapabilitySet) where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _HandshakeResponse'supported
           (\ x__ y__ -> x__ {_HandshakeResponse'supported = y__}))
        Prelude.id
instance Data.ProtoLens.Message HandshakeResponse where
  messageName _
    = Data.Text.pack "acyclic.protocol.v1.HandshakeResponse"
  packedMessageDescriptor _
    = "\n\
      \\DC1HandshakeResponse\DC2A\n\
      \\bprotocol\CAN\SOH \SOH(\v2%.acyclic.protocol.v1.ProtocolIdentityR\bprotocol\DC2@\n\
      \\tsupported\CAN\STX \SOH(\v2\".acyclic.protocol.v1.CapabilitySetR\tsupported"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        protocol__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "protocol"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor ProtocolIdentity)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'protocol")) ::
              Data.ProtoLens.FieldDescriptor HandshakeResponse
        supported__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "supported"
              (Data.ProtoLens.MessageField Data.ProtoLens.MessageType ::
                 Data.ProtoLens.FieldTypeDescriptor CapabilitySet)
              (Data.ProtoLens.OptionalField
                 (Data.ProtoLens.Field.field @"maybe'supported")) ::
              Data.ProtoLens.FieldDescriptor HandshakeResponse
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, protocol__field_descriptor),
           (Data.ProtoLens.Tag 2, supported__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _HandshakeResponse'_unknownFields
        (\ x__ y__ -> x__ {_HandshakeResponse'_unknownFields = y__})
  defMessage
    = HandshakeResponse'_constructor
        {_HandshakeResponse'protocol = Prelude.Nothing,
         _HandshakeResponse'supported = Prelude.Nothing,
         _HandshakeResponse'_unknownFields = []}
  parseMessage
    = let
        loop ::
          HandshakeResponse
          -> Data.ProtoLens.Encoding.Bytes.Parser HandshakeResponse
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
                                       "supported"
                                loop
                                  (Lens.Family2.set (Data.ProtoLens.Field.field @"supported") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "HandshakeResponse"
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
                       (Data.ProtoLens.Field.field @"maybe'supported") _x
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
instance Control.DeepSeq.NFData HandshakeResponse where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_HandshakeResponse'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_HandshakeResponse'protocol x__)
                (Control.DeepSeq.deepseq (_HandshakeResponse'supported x__) ()))
{- | Fields :
     
         * 'Proto.Protocol.V1.Protocol_Fields.version' @:: Lens' ProtocolIdentity Data.Text.Text@
         * 'Proto.Protocol.V1.Protocol_Fields.descriptorDigest' @:: Lens' ProtocolIdentity Data.Text.Text@ -}
data ProtocolIdentity
  = ProtocolIdentity'_constructor {_ProtocolIdentity'version :: !Data.Text.Text,
                                   _ProtocolIdentity'descriptorDigest :: !Data.Text.Text,
                                   _ProtocolIdentity'_unknownFields :: !Data.ProtoLens.FieldSet}
  deriving stock (Prelude.Eq, Prelude.Ord)
instance Prelude.Show ProtocolIdentity where
  showsPrec _ __x __s
    = Prelude.showChar
        '{'
        (Prelude.showString
           (Data.ProtoLens.showMessageShort __x) (Prelude.showChar '}' __s))
instance Data.ProtoLens.Field.HasField ProtocolIdentity "version" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ProtocolIdentity'version
           (\ x__ y__ -> x__ {_ProtocolIdentity'version = y__}))
        Prelude.id
instance Data.ProtoLens.Field.HasField ProtocolIdentity "descriptorDigest" Data.Text.Text where
  fieldOf _
    = (Prelude..)
        (Lens.Family2.Unchecked.lens
           _ProtocolIdentity'descriptorDigest
           (\ x__ y__ -> x__ {_ProtocolIdentity'descriptorDigest = y__}))
        Prelude.id
instance Data.ProtoLens.Message ProtocolIdentity where
  messageName _
    = Data.Text.pack "acyclic.protocol.v1.ProtocolIdentity"
  packedMessageDescriptor _
    = "\n\
      \\DLEProtocolIdentity\DC2\CAN\n\
      \\aversion\CAN\SOH \SOH(\tR\aversion\DC2+\n\
      \\DC1descriptor_digest\CAN\STX \SOH(\tR\DLEdescriptorDigest"
  packedFileDescriptor _ = packedFileDescriptor
  fieldsByTag
    = let
        version__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "version"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional (Data.ProtoLens.Field.field @"version")) ::
              Data.ProtoLens.FieldDescriptor ProtocolIdentity
        descriptorDigest__field_descriptor
          = Data.ProtoLens.FieldDescriptor
              "descriptor_digest"
              (Data.ProtoLens.ScalarField Data.ProtoLens.StringField ::
                 Data.ProtoLens.FieldTypeDescriptor Data.Text.Text)
              (Data.ProtoLens.PlainField
                 Data.ProtoLens.Optional
                 (Data.ProtoLens.Field.field @"descriptorDigest")) ::
              Data.ProtoLens.FieldDescriptor ProtocolIdentity
      in
        Data.Map.fromList
          [(Data.ProtoLens.Tag 1, version__field_descriptor),
           (Data.ProtoLens.Tag 2, descriptorDigest__field_descriptor)]
  unknownFields
    = Lens.Family2.Unchecked.lens
        _ProtocolIdentity'_unknownFields
        (\ x__ y__ -> x__ {_ProtocolIdentity'_unknownFields = y__})
  defMessage
    = ProtocolIdentity'_constructor
        {_ProtocolIdentity'version = Data.ProtoLens.fieldDefault,
         _ProtocolIdentity'descriptorDigest = Data.ProtoLens.fieldDefault,
         _ProtocolIdentity'_unknownFields = []}
  parseMessage
    = let
        loop ::
          ProtocolIdentity
          -> Data.ProtoLens.Encoding.Bytes.Parser ProtocolIdentity
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
                                       "version"
                                loop (Lens.Family2.set (Data.ProtoLens.Field.field @"version") y x)
                        18
                          -> do y <- (Data.ProtoLens.Encoding.Bytes.<?>)
                                       (do len <- Data.ProtoLens.Encoding.Bytes.getVarInt
                                           Data.ProtoLens.Encoding.Bytes.getText
                                             (Prelude.fromIntegral len))
                                       "descriptor_digest"
                                loop
                                  (Lens.Family2.set
                                     (Data.ProtoLens.Field.field @"descriptorDigest") y x)
                        wire
                          -> do !y <- Data.ProtoLens.Encoding.Wire.parseTaggedValueFromWire
                                        wire
                                loop
                                  (Lens.Family2.over
                                     Data.ProtoLens.unknownFields (\ !t -> (:) y t) x)
      in
        (Data.ProtoLens.Encoding.Bytes.<?>)
          (do loop Data.ProtoLens.defMessage) "ProtocolIdentity"
  buildMessage
    = \ _x
        -> (Data.Monoid.<>)
             (let
                _v = Lens.Family2.view (Data.ProtoLens.Field.field @"version") _x
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
                         (Data.ProtoLens.Field.field @"descriptorDigest") _x
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
instance Control.DeepSeq.NFData ProtocolIdentity where
  rnf
    = \ x__
        -> Control.DeepSeq.deepseq
             (_ProtocolIdentity'_unknownFields x__)
             (Control.DeepSeq.deepseq
                (_ProtocolIdentity'version x__)
                (Control.DeepSeq.deepseq
                   (_ProtocolIdentity'descriptorDigest x__) ()))
packedFileDescriptor :: Data.ByteString.ByteString
packedFileDescriptor
  = "\n\
    \\SUBprotocol/v1/protocol.proto\DC2\DC3acyclic.protocol.v1\"Y\n\
    \\DLEProtocolIdentity\DC2\CAN\n\
    \\aversion\CAN\SOH \SOH(\tR\aversion\DC2+\n\
    \\DC1descriptor_digest\CAN\STX \SOH(\tR\DLEdescriptorDigest\":\n\
    \\n\
    \Capability\DC2\DC2\n\
    \\EOTname\CAN\SOH \SOH(\tR\EOTname\DC2\CAN\n\
    \\aversion\CAN\STX \SOH(\tR\aversion\"T\n\
    \\rCapabilitySet\DC2C\n\
    \\fcapabilities\CAN\SOH \ETX(\v2\US.acyclic.protocol.v1.CapabilityR\fcapabilities\"\149\SOH\n\
    \\DLEHandshakeRequest\DC2A\n\
    \\bprotocol\CAN\SOH \SOH(\v2%.acyclic.protocol.v1.ProtocolIdentityR\bprotocol\DC2>\n\
    \\brequired\CAN\STX \SOH(\v2\".acyclic.protocol.v1.CapabilitySetR\brequired\"\152\SOH\n\
    \\DC1HandshakeResponse\DC2A\n\
    \\bprotocol\CAN\SOH \SOH(\v2%.acyclic.protocol.v1.ProtocolIdentityR\bprotocol\DC2@\n\
    \\tsupported\CAN\STX \SOH(\v2\".acyclic.protocol.v1.CapabilitySetR\tsupportedB;Z9github.com/acyclic-labs/sdk/go/gen/protocol/v1;protocolv1J\189\ACK\n\
    \\ACK\DC2\EOT\NUL\NUL\SUB\SOH\n\
    \\b\n\
    \\SOH\f\DC2\ETX\NUL\NUL\DC2\n\
    \\b\n\
    \\SOH\STX\DC2\ETX\SOH\NUL\FS\n\
    \\b\n\
    \\SOH\b\DC2\ETX\ETX\NULP\n\
    \\t\n\
    \\STX\b\v\DC2\ETX\ETX\NULP\n\
    \\160\SOH\n\
    \\STX\EOT\NUL\DC2\EOT\b\NUL\v\SOH2\147\SOH Version negotiation shared by every Acyclic service family. Each family\n\
    \ names its own contract in ProtocolIdentity; nothing here depends on one.\n\
    \\n\
    \\n\
    \\n\
    \\ETX\EOT\NUL\SOH\DC2\ETX\b\b\CAN\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\NUL\DC2\ETX\t\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ENQ\DC2\ETX\t\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\SOH\DC2\ETX\t\t\DLE\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\NUL\ETX\DC2\ETX\t\DC3\DC4\n\
    \\v\n\
    \\EOT\EOT\NUL\STX\SOH\DC2\ETX\n\
    \\STX\US\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ENQ\DC2\ETX\n\
    \\STX\b\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\SOH\DC2\ETX\n\
    \\t\SUB\n\
    \\f\n\
    \\ENQ\EOT\NUL\STX\SOH\ETX\DC2\ETX\n\
    \\GS\RS\n\
    \\n\
    \\n\
    \\STX\EOT\SOH\DC2\EOT\f\NUL\SI\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\SOH\SOH\DC2\ETX\f\b\DC2\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\NUL\DC2\ETX\r\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ENQ\DC2\ETX\r\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\SOH\DC2\ETX\r\t\r\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\NUL\ETX\DC2\ETX\r\DLE\DC1\n\
    \\v\n\
    \\EOT\EOT\SOH\STX\SOH\DC2\ETX\SO\STX\NAK\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ENQ\DC2\ETX\SO\STX\b\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\SOH\DC2\ETX\SO\t\DLE\n\
    \\f\n\
    \\ENQ\EOT\SOH\STX\SOH\ETX\DC2\ETX\SO\DC3\DC4\n\
    \\n\
    \\n\
    \\STX\EOT\STX\DC2\EOT\DLE\NUL\DC2\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\STX\SOH\DC2\ETX\DLE\b\NAK\n\
    \\v\n\
    \\EOT\EOT\STX\STX\NUL\DC2\ETX\DC1\STX'\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\EOT\DC2\ETX\DC1\STX\n\
    \\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ACK\DC2\ETX\DC1\v\NAK\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\SOH\DC2\ETX\DC1\SYN\"\n\
    \\f\n\
    \\ENQ\EOT\STX\STX\NUL\ETX\DC2\ETX\DC1%&\n\
    \\n\
    \\n\
    \\STX\EOT\ETX\DC2\EOT\DC3\NUL\SYN\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\ETX\SOH\DC2\ETX\DC3\b\CAN\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\NUL\DC2\ETX\DC4\STX \n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ACK\DC2\ETX\DC4\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\SOH\DC2\ETX\DC4\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\NUL\ETX\DC2\ETX\DC4\RS\US\n\
    \\v\n\
    \\EOT\EOT\ETX\STX\SOH\DC2\ETX\NAK\STX\GS\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ACK\DC2\ETX\NAK\STX\SI\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\SOH\DC2\ETX\NAK\DLE\CAN\n\
    \\f\n\
    \\ENQ\EOT\ETX\STX\SOH\ETX\DC2\ETX\NAK\ESC\FS\n\
    \\n\
    \\n\
    \\STX\EOT\EOT\DC2\EOT\ETB\NUL\SUB\SOH\n\
    \\n\
    \\n\
    \\ETX\EOT\EOT\SOH\DC2\ETX\ETB\b\EM\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\NUL\DC2\ETX\CAN\STX \n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ACK\DC2\ETX\CAN\STX\DC2\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\SOH\DC2\ETX\CAN\DC3\ESC\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\NUL\ETX\DC2\ETX\CAN\RS\US\n\
    \\v\n\
    \\EOT\EOT\EOT\STX\SOH\DC2\ETX\EM\STX\RS\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ACK\DC2\ETX\EM\STX\SI\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\SOH\DC2\ETX\EM\DLE\EM\n\
    \\f\n\
    \\ENQ\EOT\EOT\STX\SOH\ETX\DC2\ETX\EM\FS\GSb\ACKproto3"