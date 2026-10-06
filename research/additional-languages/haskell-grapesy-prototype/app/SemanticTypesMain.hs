module Main where

import qualified Acyclic.Semantics as Semantics
import Data.ProtoLens (defMessage)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.Int (Int32, Int64)
import Data.Word (Word32, Word64)
import qualified Proto.Filesystem.V2.Filesystem as FilesystemV2
import qualified Proto.Inference.V1.Inference as InferenceV1
import qualified Proto.Machines.V1.Machines as MachinesV1
import qualified Proto.Objects.V2.Objects as ObjectsV2
import qualified Proto.Stream.V2.Stream as StreamV2
import qualified Proto.Workers.V1.Workers as WorkersV1

expectRight :: String -> Either String value -> IO value
expectRight label result = case result of
  Left errorValue -> fail (label <> ": " <> errorValue)
  Right value -> pure value

expectLeft :: String -> Either String value -> IO ()
expectLeft label result = case result of
  Left _ -> pure ()
  Right _ -> fail (label <> ": invalid value was accepted")

main :: IO ()
main = do
  actor <- expectRight "actor" (Semantics.mkActorId "actor-1")
  operation <- expectRight "operation" (Semantics.mkOperationId (BS8.pack "0123456789abcdef"))
  workspace <- expectRight "workspace" (Semantics.mkWorkspaceId (BS8.pack "fedcba9876543210"))
  _ <- expectRight "idempotency" (Semantics.mkIdempotencyKey (BS8.pack "0011223344556677"))
  expectLeft "empty actor" (Semantics.mkActorId "")
  expectLeft "short operation" (Semantics.mkOperationId (BS8.pack "short"))
  expectLeft "short workspace" (Semantics.mkWorkspaceId (BS8.pack "short"))
  let _known :: Semantics.WireChoice
      _known = Semantics.KnownOneof (Semantics.KnownAcyclicActorsV1SubscriptionStartCursorN1 (0 :: Word64))
      _knownMaxU64 :: Semantics.WireChoice
      _knownMaxU64 = Semantics.KnownOneof (Semantics.KnownAcyclicFilesystemV2OptionalU64PresentN1 (maxBound :: Word64))
      _knownUnavailable :: Semantics.WireChoice
      _knownUnavailable = Semantics.KnownOneof (Semantics.KnownAcyclicFilesystemV2OptionalU64UnavailableN2 True)
      _knownSigned :: Semantics.WireChoice
      _knownSigned = Semantics.KnownOneof (Semantics.KnownAcyclicFilesystemV2OptionalI64PresentN1 (-42 :: Int64))
      _knownSignedUnavailable :: Semantics.WireChoice
      _knownSignedUnavailable = Semantics.KnownOneof (Semantics.KnownAcyclicFilesystemV2OptionalI64UnavailableN2 False)
      _unknown :: Semantics.WireChoice
      _enum :: Semantics.ActorsActorStateEnum
      _enum = Semantics.actorsActorStateEnumFromWire 0
      _unknownEnum :: Semantics.ActorsActorStateEnum
      _unknownEnum = Semantics.actorsActorStateEnumFromWire 99
      _unknown = Semantics.UnknownOneof (Semantics.workspaceIdValue workspace)
  if Semantics.actorsActorStateEnumValue _unknownEnum /= 99
    then fail "unknown enum value was not retained by Rust-owned semantics"
    else pure ()
  putStrLn ("PASS:rust-owned-haskell-semantic-types=" <> show (Semantics.actorIdValue actor))
  putStrLn "PASS:rust-owned-haskell-semantic-vectors=max-u64,optional-i64-negative,optional-unavailable;unknown-enum=99"
