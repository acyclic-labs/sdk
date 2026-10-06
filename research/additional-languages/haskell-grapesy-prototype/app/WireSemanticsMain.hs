{-# LANGUAGE OverloadedStrings #-}

-- Generated-protocol conformance proof.  Every value below is built through
-- the proto-lens API emitted from the Rust-owned protobuf descriptors.
module Main where

import Control.Lens ((.~), (^.))
import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import Data.ProtoLens (defMessage)
import Data.ProtoLens.Encoding (decodeMessage, encodeMessage)
import Data.Word (Word64)
import qualified Proto.Filesystem.V2.Filesystem as Filesystem
import qualified Proto.Filesystem.V2.Filesystem_Fields as FilesystemF
import qualified Proto.Objects.V2.Objects as Objects
import qualified Proto.Objects.V2.Objects_Fields as ObjectsF

assert :: Bool -> String -> IO ()
assert True _ = pure ()
assert False message = fail message

assertWire :: String -> BS.ByteString -> BS.ByteString -> IO ()
assertWire label expected actual =
  assert (expected == actual) (label <> " wire mismatch: expected " <> show expected <> ", got " <> show actual)

decodeOrFail :: String -> Either String value -> IO value
decodeOrFail label result = case result of
  Left errorValue -> fail (label <> " decode failed: " <> errorValue)
  Right value -> pure value

main :: IO ()
main = do
  let maxU64 = maxBound :: Word64

  -- A non-oneof uint64 uses the complete unsigned 64-bit range.
  let snapshot :: Filesystem.WorkspaceContextSnapshot
      snapshot = defMessage & FilesystemF.revision .~ maxU64
      snapshotWire = encodeMessage snapshot
  assertWire "workspace snapshot max uint64" (BS.pack (16 : replicate 9 255 <> [1])) snapshotWire
  snapshotDecoded <- decodeOrFail "workspace snapshot" (decodeMessage snapshotWire :: Either String Filesystem.WorkspaceContextSnapshot)
  assert (snapshotDecoded ^. FilesystemF.revision == maxU64) "workspace snapshot lost max uint64"

  -- OptionalU64 is a Rust-owned oneof: present and unavailable must remain
  -- distinguishable, while an absent value emits no wire bytes.
  let optionalAbsent = defMessage :: Filesystem.OptionalU64
      optionalPresent :: Filesystem.OptionalU64
      optionalPresent = defMessage & FilesystemF.present .~ maxU64
      optionalUnavailable :: Filesystem.OptionalU64
      optionalUnavailable = defMessage & FilesystemF.unavailable .~ True
      absentWire = encodeMessage optionalAbsent
      presentWire = encodeMessage optionalPresent
      unavailableWire = encodeMessage optionalUnavailable
  assert (BS.null absentWire) "absent optional uint64 emitted wire bytes"
  assert (not (BS.null presentWire)) "present optional uint64 emitted no wire bytes"
  assert (not (BS.null unavailableWire)) "unavailable optional uint64 emitted no wire bytes"
  assertWire "present optional uint64 max" (BS.pack (8 : replicate 9 255 <> [1])) presentWire
  assertWire "unavailable optional uint64" (BS.pack [16, 1]) unavailableWire
  absentDecoded <- decodeOrFail "absent optional uint64" (decodeMessage absentWire :: Either String Filesystem.OptionalU64)
  presentDecoded <- decodeOrFail "present optional uint64" (decodeMessage presentWire :: Either String Filesystem.OptionalU64)
  unavailableDecoded <- decodeOrFail "unavailable optional uint64" (decodeMessage unavailableWire :: Either String Filesystem.OptionalU64)
  assert (absentDecoded ^. FilesystemF.maybe'value == Nothing) "absent optional uint64 became present"
  assert (presentDecoded ^. FilesystemF.maybe'value == Just (Filesystem.OptionalU64'Present maxU64)) "present optional uint64 changed arm or value"
  assert (unavailableDecoded ^. FilesystemF.maybe'value == Just (Filesystem.OptionalU64'Unavailable True)) "unavailable optional uint64 changed arm"

  -- A signed oneof must use proto-lens' generated SInt64 field and preserve
  -- negative values through zig-zag wire encoding and decode.
  let signedPresent :: Filesystem.OptionalI64
      signedPresent = defMessage & FilesystemF.present .~ (-42 :: Int64)
      signedWire = encodeMessage signedPresent
  assertWire "signed optional int64" (BS.pack [8, 83]) signedWire
  signedDecoded <- decodeOrFail "signed optional int64" (decodeMessage signedWire :: Either String Filesystem.OptionalI64)
  assert (signedDecoded ^. FilesystemF.maybe'value == Just (Filesystem.OptionalI64'Present (-42))) "signed optional int64 changed value"

  -- A second generated oneof exercises a nested message arm and its uint64
  -- suffix arm, proving oneof identity survives encode/decode.
  let byteRange :: Objects.ByteRange
      byteRange = defMessage & ObjectsF.suffixLength .~ maxU64
      byteRangeWire = encodeMessage byteRange
  assertWire "byte range suffix max uint64" (BS.pack (16 : replicate 9 255 <> [1])) byteRangeWire
  byteRangeDecoded <- decodeOrFail "byte range suffix oneof" (decodeMessage byteRangeWire :: Either String Objects.ByteRange)
  assert (byteRangeDecoded ^. ObjectsF.maybe'selection == Just (Objects.ByteRange'SuffixLength maxU64)) "byte range oneof changed arm or value"

  -- Proto3 optional presence is observable in the wire representation even
  -- when the scalar carries its default value.
  let rangeAbsentEnd :: Objects.InclusiveRange
      rangeAbsentEnd = defMessage & ObjectsF.start .~ (7 :: Word64)
      rangePresentEnd = rangeAbsentEnd & ObjectsF.maybe'end .~ Just maxU64
      rangeAbsentWire = encodeMessage rangeAbsentEnd
      rangePresentWire = encodeMessage rangePresentEnd
  assert (rangeAbsentWire /= rangePresentWire) "optional end presence was erased"
  rangeAbsentDecoded <- decodeOrFail "absent optional end" (decodeMessage rangeAbsentWire :: Either String Objects.InclusiveRange)
  rangePresentDecoded <- decodeOrFail "present optional end" (decodeMessage rangePresentWire :: Either String Objects.InclusiveRange)
  assert (rangeAbsentDecoded ^. ObjectsF.maybe'end == Nothing) "absent optional end became present"
  assert (rangePresentDecoded ^. ObjectsF.maybe'end == Just maxU64) "present optional end changed value"

  -- Unknown enum values must be retained as an explicit typed constructor.
  let unknownCode = Objects.ErrorCode'Unrecognized (Objects.ErrorCode'UnrecognizedValue (99 :: Int32))
      errorDetail :: Objects.ErrorDetail
      errorDetail = defMessage & ObjectsF.code .~ unknownCode
      errorDetailWire = encodeMessage errorDetail
  assertWire "unknown enum" (BS.pack [8, 99]) errorDetailWire
  errorDetailDecoded <- decodeOrFail "unknown enum" (decodeMessage errorDetailWire :: Either String Objects.ErrorDetail)
  assert (errorDetailDecoded ^. ObjectsF.code == unknownCode) "unknown enum value was normalized or discarded"

  putStrLn "PASS:rust-owned-haskell-wire-semantics=max-u64,optional-presence,oneof,unknown-enum"
