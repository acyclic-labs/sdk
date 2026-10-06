module Main where

import qualified Acyclic.Semantics as Semantics
import Data.ProtoLens (defMessage)
import qualified Proto.Objects.V2.Objects as ObjectsV2
import qualified Data.ByteString.Char8 as BS8

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
  let _known :: Semantics.WireChoice ObjectsV2.ObjectInfo
      _known = Semantics.KnownOneof (defMessage :: ObjectsV2.ObjectInfo)
      _unknown :: Semantics.WireChoice ObjectsV2.ObjectInfo
      _unknown = Semantics.UnknownOneof (Semantics.workspaceIdValue workspace)
  putStrLn ("PASS:rust-owned-haskell-semantic-types=" <> show (Semantics.actorIdValue actor))
