#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) stop("usage: qualify-r-runtime.R <base-url> <receipt-path>")
base <- args[[1]]
receipt_path <- args[[2]]

library(jsonlite)
library(httr)
library(acyclic.actors.r)
library(acyclic.workers.r)
library(acyclic.stream.r)
library(acyclic.objects.r)
library(acyclic.inference.r)

json_body <- function(value) jsonlite::fromJSON(value, simplifyVector = FALSE)
check <- list()
record <- function(name, status, detail) {
  check[[name]] <<- list(status = status, detail = detail)
}
expect <- function(name, expr, detail) {
  value <- tryCatch(force(expr), error = function(e) e)
  if (inherits(value, "error")) {
    record(name, "fail", paste(detail, conditionMessage(value)))
    return(NULL)
  }
  record(name, "pass", detail)
  value
}

actor_client <- acyclic.actors.r::ApiClient$new(base_path = base, bearer_token = "bash-fixture-token")
actor_api <- acyclic.actors.r::DefaultApi$new(actor_client)
worker_client <- acyclic.workers.r::ApiClient$new(base_path = base, bearer_token = "bash-fixture-token")
worker_api <- acyclic.workers.r::DefaultApi$new(worker_client)
stream_client <- acyclic.stream.r::ApiClient$new(base_path = base, bearer_token = "bash-fixture-token")
stream_api <- acyclic.stream.r::DefaultApi$new(stream_client)
objects_client <- acyclic.objects.r::ApiClient$new(base_path = base, bearer_token = "bash-fixture-token")
objects_api <- acyclic.objects.r::DefaultApi$new(objects_client)
inference_client <- acyclic.inference.r::ApiClient$new(base_path = base, bearer_token = "bash-fixture-token")
inference_api <- acyclic.inference.r::DefaultApi$new(inference_client)

actor_request <- AcyclicActorsV1InvokeActorRequest$new(
  actorId = "actor-1", body = "AQID", method = "POST", url = "https://example.test"
)
actor_json <- expect("actors.json_roundtrip", actor_request$toJSONString(), "R6 model serialized")
if (!is.null(actor_json)) {
  parsed <- json_body(actor_json)
  if (identical(parsed$body, "AQID")) record("actors.json_body", "pass", "base64 bytes retained as canonical JSON")
  else record("actors.json_body", "fail", "canonical body was not preserved")
}
actor_response <- expect("actors.transport", actor_api$InvokeActorWithHttpInfo(actor_request, .parse = FALSE), "fixture request completed")
if (!is.null(actor_response) && identical(actor_response$status_code, 200L)) record("actors.transport_status", "pass", "HTTP 200")

worker_request <- AcyclicWorkersV1InvokeDeploymentRequest$new(
  alias = "prod", body = "AQID", method = "POST", url = "https://example.test"
)
worker_response <- expect("workers.transport", worker_api$InvokeDeploymentWithHttpInfo("prod", worker_request, .parse = FALSE), "fixture request completed")
if (!is.null(worker_response) && identical(worker_response$status_code, 200L)) record("workers.transport_status", "pass", "HTTP 200")

stream_request <- AcyclicStreamV2ReadRequest$new(from = "", limit = 0, path = "root")
stream_error <- expect("stream.recovery_error", stream_api$ReadWithHttpInfo(stream_request, .parse = FALSE), "retryable fixture response returned")
if (!is.null(stream_error) && identical(stream_error$status_code, 503L)) record("stream.recovery_status", "pass", "HTTP 503 retryable response observed")
stream_request$limit <- 1
stream_response <- expect("stream.recovery_retry", stream_api$ReadWithHttpInfo(stream_request, .parse = FALSE), "retry request completed")
if (!is.null(stream_response) && identical(stream_response$status_code, 200L)) record("stream.retry_status", "pass", "HTTP 200 after retry")

objects_request <- AcyclicObjectsV2PutObjectRequest$new(body = "AQID", complete = TRUE)
objects_response <- expect("objects.transport", objects_api$PutObjectWithHttpInfo(objects_request, .parse = FALSE), "fixture request completed")
if (!is.null(objects_response) && identical(objects_response$status_code, 200L)) record("objects.transport_status", "pass", "HTTP 200")

inference_request <- InferenceCustomerV1GenerateRunRequest$new(context = "AQID", maximumOutput = "18446744073709551615")
inference_json <- expect("inference.json_roundtrip", inference_request$toJSONString(), "R6 model serialized")
if (!is.null(inference_json)) {
  parsed <- json_body(inference_json)
  if (identical(parsed$maximumOutput, "18446744073709551615")) record("inference.uint64", "pass", "maximumOutput preserved as exact decimal string")
  else record("inference.uint64", "fail", "maximumOutput changed during JSON serialization")
}
inference_response <- expect("inference.transport", inference_api$RunsGenerateWithHttpInfo(inference_request, .parse = FALSE), "fixture request completed")
if (!is.null(inference_response) && identical(inference_response$status_code, 200L)) record("inference.transport_status", "pass", "HTTP 200")

unauth_client <- acyclic.actors.r::ApiClient$new(base_path = base, bearer_token = "wrong")
unauth_api <- acyclic.actors.r::DefaultApi$new(unauth_client)
unauth_response <- expect("auth.negative", unauth_api$InvokeActorWithHttpInfo(actor_request, .parse = FALSE), "unauthenticated fixture response returned")
if (!is.null(unauth_response) && identical(unauth_response$status_code, 401L)) record("auth.negative_status", "pass", "HTTP 401")

statuses <- vapply(check, function(item) item$status, character(1))
receipt <- list(
  schema = "acyclic.sdk.openapi.runtime-scenario.v1",
  target = "r",
  runtime = list(r = R.version$version.string,
                 packages = list(actors = as.character(packageVersion("acyclic.actors.r")),
                                 workers = as.character(packageVersion("acyclic.workers.r")),
                                 stream = as.character(packageVersion("acyclic.stream.r")),
                                 objects = as.character(packageVersion("acyclic.objects.r")),
                                 inference = as.character(packageVersion("acyclic.inference.r")))),
  fixture = list(base_url = base, auth = "bearer", families = c("actors", "workers", "stream", "objects", "inference")),
  checks = check,
  passed = sum(statuses == "pass"),
  failed = sum(statuses == "fail"),
  status = if (all(statuses == "pass")) "pass" else "fail"
)
writeLines(jsonlite::toJSON(receipt, auto_unbox = TRUE, pretty = TRUE), receipt_path)
if (receipt$status != "pass") quit(status = 1)
