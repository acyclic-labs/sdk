{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Main where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Data.List (stripPrefix)
import Data.Maybe (fromMaybe)
import Network.GRPC.Client qualified as Client
import Network.GRPC.Common
import Network.GRPC.Common.Protobuf (Proto)
import Acyclic.Stream.Api (Append, append, appendRequest)
import Proto.Stream.V2.Stream
import System.Environment (lookupEnv)
import System.Timeout (timeout)
import Text.Read (readMaybe)

data Endpoint = Endpoint String Int

parseEndpoint :: String -> Endpoint
parseEndpoint raw =
  let withoutScheme = fromMaybe raw (stripPrefix "http://" raw)
      withoutTls = fromMaybe withoutScheme (stripPrefix "https://" withoutScheme)
      (host, portText) = break (== ':') withoutTls
      port = fromMaybe 80 (readMaybe (drop 1 portText))
  in Endpoint host port

request :: Proto AppendRequest
request = appendRequest "haskell/remote" ["rust-owned-haskell-record"] "haskell-remote-append"

main :: IO ()
main = do
  endpointText <- fromMaybe "http://127.0.0.1:50051" <$> lookupEnv "ACYCLIC_HASKELL_GRPC_ENDPOINT"
  tls <- parseBool <$> lookupEnv "ACYCLIC_HASKELL_GRPC_TLS"
  cancelMs <- fromMaybe 1000 . (>>= readMaybe) <$> lookupEnv "ACYCLIC_HASKELL_CANCEL_MS"
  let Endpoint host port = parseEndpoint endpointText
      address = Client.Address host (fromIntegral port) Nothing
      server = if tls
        then Client.ServerSecure Client.NoServerValidation SslKeyLogNone address
        else Client.ServerInsecure address
      params = def {
          Client.connReconnectPolicy =
            Client.exponentialBackoff reconnectWait 1.5 (0.05, 0.1) 3
        }
  putStrLn "typed-consumer=acyclic-haskell-grapesy-remote"
  putStrLn ("endpoint=" <> endpointText)
  putStrLn ("tls=" <> show tls)
  putStrLn "request-manifest=AppendRequest(path,records,idempotencyKey)"
  result <- try (Client.withConnection params server $ \conn -> do
      response <- append conn request
      putStrLn ("append-response=" <> show response)
      cancelResult <- timeout (cancelMs * 1000) $
        append conn request
      putStrLn ("cancel-probe=" <> maybe "timed-out-and-cancelled" (const "completed-before-deadline") cancelResult)
      recovered <- append conn request
      putStrLn ("recovery-response=" <> show recovered)
    ) :: IO (Either SomeException ())
  case result of
    Left errorValue -> do
      putStrLn ("remote-error=" <> show errorValue)
      fail "typed Haskell remote consumer failed"
    Right () -> do
      putStrLn "remote-status=passed"
      putStrLn "h2=passed"
      putStrLn "reconnect-policy=configured"

parseBool :: Maybe String -> Bool
parseBool value = any (== value) [Just "1", Just "true", Just "TRUE", Just "yes"]

reconnectWait :: Int -> IO ()
reconnectWait micros = do
  putStrLn ("reconnect-delay-us=" <> show micros)
  threadDelay micros
