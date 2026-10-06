{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
module Main where

import Data.Proxy (Proxy(..))
import Data.ProtoLens.Service.Types (Service, ServiceMethods)
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import qualified Proto.Actors.V1.Actors as Actors
import qualified Proto.Filesystem.V2.Filesystem as Filesystem
import qualified Proto.Harness.V2.Harness as Harness
import qualified Proto.Inference.V1.Inference as Inference
import qualified Proto.Machines.V1.Machines as Machines
import qualified Proto.Objects.V1.Objects as ObjectsV1
import qualified Proto.Objects.V2.Objects as ObjectsV2
import qualified Proto.Stream.V2.Stream as Stream
import qualified Proto.Workers.V1.Workers as Workers

class SymbolList (symbols :: [Symbol]) where
  symbolList :: Proxy symbols -> [String]

instance SymbolList '[] where
  symbolList _ = []

instance (KnownSymbol symbol, SymbolList symbols) => SymbolList (symbol ': symbols) where
  symbolList _ = symbolVal (Proxy @symbol) : symbolList (Proxy @symbols)

serviceCount :: forall service. (Service service, SymbolList (ServiceMethods service)) => Int
serviceCount = length (symbolList (Proxy @(ServiceMethods service)))

serviceRow :: forall service. (Service service, SymbolList (ServiceMethods service)) => String -> (String, Int)
serviceRow name = (name, serviceCount @service)

main :: IO ()
main = do
  let activeRows =
        [ serviceRow @Actors.ActorsService "actors"
        , serviceRow @Filesystem.FilesystemService "filesystem"
        , serviceRow @Harness.HarnessService "harness"
        , serviceRow @Inference.ModelsService "inference.models"
        , serviceRow @Inference.ContextsService "inference.contexts"
        , serviceRow @Inference.WarmContextsService "inference.warmContexts"
        , serviceRow @Inference.RunsService "inference.runs"
        , serviceRow @Inference.EvaluationsService "inference.evaluations"
        , serviceRow @Machines.MachinesService "machines"
        , serviceRow @ObjectsV2.BucketsService "objects.v2.buckets"
        , serviceRow @ObjectsV2.ObjectsService "objects.v2.objects"
        , serviceRow @ObjectsV2.MultipartService "objects.v2.multipart"
        , serviceRow @Workers.WorkersService "workers"
        , serviceRow @Stream.StreamService "stream"
        ]
      archivedRows =
        [ serviceRow @ObjectsV1.BucketsService "objects.v1.buckets"
        , serviceRow @ObjectsV1.ObjectsService "objects.v1.objects"
        , serviceRow @ObjectsV1.MultipartService "objects.v1.multipart"
        , serviceRow @ObjectsV1.SnapshotsService "objects.v1.snapshots"
        ]
      rows = activeRows <> archivedRows
      activeTotal = sum (map snd activeRows)
      serviceTotal = length rows
  mapM_ print rows
  if activeTotal == 106 && serviceTotal == 18
    then do
      putStrLn "PASS:rust-owned-typed-rpc-surface=106"
      putStrLn "PASS:rust-owned-typed-rpc-services=18;archived=4"
    else fail ("Rust-owned typed RPC surface expected 106 active methods across 18 services, observed " <> show activeTotal <> " methods across " <> show serviceTotal <> " services")
