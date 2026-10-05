{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import Prelude hiding (Read)
import qualified Data.ByteString as BS
import Data.ProtoLens (defMessage, encodeMessage)
import Data.Proxy (Proxy(..))
import Control.Lens ((.~))
import Acyclic.Stream.Api
import Proto.Stream.V2.Stream
import Proto.Stream.V2.Stream_Fields

main :: IO ()
main = do
  let request :: AppendRequest
      request = defMessage
        & path .~ "haskell/prototype"
        & records .~ ["rust-owned-record"]
      wire = encodeMessage request
  putStrLn "PASS:rust-owned-proto-lens-encoding"
  putStrLn ("append-request-bytes=" <> show (BS.length wire))
  putStrLn ("rpc-types=" <> show [showType (Proxy @Append), showType (Proxy @Read), showType (Proxy @Follow)])
  where
    showType :: Proxy a -> String
    showType _ = "generated-grapesy-protobuf"

infixl 1 &
(&) :: a -> (a -> b) -> b
x & f = f x
