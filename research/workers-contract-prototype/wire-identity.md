# Workers v1 wire identity map

Input: `proto/workers/v1/workers.proto`, SHA-256 `8D3904F1645D795678B234E65165E36FEC468D5BF563BADA2F2E2BFDF4751B51`.

Rust field types below are the source declaration types. Protify emits the corresponding prost wire type and protobuf JSON name from the Rust identifier. No field uses a custom `json_name` override.

## Messages

| Message | Field | Tag | Rust declaration | Wire / JSON identity |
|---|---|---:|---|---|
| CodeVersion | sha256 | 1 | `Bytes` | `bytes`, `sha256` |
| CodeVersion | size_bytes | 2 | `u64` | `uint64`, `size_bytes` |
| Deployment | alias | 1 | `String` | `string`, `alias` |
| Deployment | version | 2 | `Option<CodeVersion>` + `message` | message presence, `version` |
| Deployment | revision | 3 | `u64` | `uint64`, `revision` |
| PublishVersionRequest | javascript_module | 1 | `Bytes` | `bytes`, `javascript_module` |
| PublishVersionRequest | expected_sha256 | 2 | `Bytes` | `bytes`, `expected_sha256` |
| PublishVersionRequest | idempotency_key | 3 | `String` | `string`, `idempotency_key` |
| PublishVersionResponse | version | 1 | `Option<CodeVersion>` + `message` | message presence, `version` |
| SelectDeploymentRequest | alias | 1 | `String` | `string`, `alias` |
| SelectDeploymentRequest | version_sha256 | 2 | `Bytes` | `bytes`, `version_sha256` |
| SelectDeploymentRequest | expected_revision | 3 | `Option<u64>` | **proto3 optional**, `expected_revision` |
| SelectDeploymentRequest | idempotency_key | 4 | `String` | `string`, `idempotency_key` |
| SelectDeploymentResponse | deployment | 1 | `Option<Deployment>` + `message` | message presence, `deployment` |
| ObjectRef | bucket | 1 | `String` | `string`, `bucket` |
| ObjectRef | key | 2 | `String` | `string`, `key` |
| ObjectRef | reserved | 3 | — | reserved number `3` |
| ObjectRef | reserved | — | — | reserved name `version_id` |
| Payload | source.inline_bytes | 1 | `payload::Source::InlineBytes(Bytes)` | oneof `source`, `bytes`, `inline_bytes` |
| Payload | source.object | 2 | `payload::Source::Object(ObjectRef)` | oneof `source`, message, `object` |
| JobResult | body | 1 | `Bytes` | `bytes`, `body` |
| JobResult | reserved | 2 | — | reserved number `2` |
| JobResult | reserved | — | — | reserved name `object_version` |
| JobLimits | timeout_millis | 1 | `u64` | `uint64`, `timeout_millis` |
| JobLimits | memory_bytes | 2 | `u64` | `uint64`, `memory_bytes` |
| JobLimits | output_bytes | 3 | `u64` | `uint64`, `output_bytes` |
| RetryPolicy | max_attempts | 1 | `u32` | `uint32`, `max_attempts` |
| RetryPolicy | backoff_millis | 2 | `u64` | `uint64`, `backoff_millis` |
| JobTarget | target.deployment_alias | 1 | `job_target::Target::DeploymentAlias(String)` | oneof `target`, `string`, `deployment_alias` |
| JobTarget | target.version_sha256 | 2 | `job_target::Target::VersionSha256(Bytes)` | oneof `target`, `bytes`, `version_sha256` |
| SubmitJobRequest | target | 1 | `Option<JobTarget>` + `message` | message presence, `target` |
| SubmitJobRequest | input | 2 | `Option<Payload>` + `message` | message presence, `input` |
| SubmitJobRequest | limits | 3 | `Option<JobLimits>` + `message` | message presence, `limits` |
| SubmitJobRequest | retry | 4 | `Option<RetryPolicy>` + `message` | message presence, `retry` |
| SubmitJobRequest | idempotency_key | 5 | `String` | `string`, `idempotency_key` |
| SubmitJobResponse | job | 1 | `Option<JobObservation>` + `message` | message presence, `job` |
| JobObservation | job_id | 1 | `String` | `string`, `job_id` |
| JobObservation | state | 2 | `i32` + `enum_(JobState)` | enum, `state` |
| JobObservation | resolved_sha256 | 3 | `Bytes` | `bytes`, `resolved_sha256` |
| JobObservation | attempt | 4 | `u32` | `uint32`, `attempt` |
| JobObservation | result | 5 | `Option<JobResult>` + `message` | message presence, `result` |
| JobObservation | failure_code | 6 | `String` | `string`, `failure_code` |
| JobObservation | cancellation_requested | 7 | `bool` | `bool`, `cancellation_requested` |
| InspectJobRequest | job_id | 1 | `String` | `string`, `job_id` |
| InspectJobResponse | job | 1 | `Option<JobObservation>` + `message` | message presence, `job` |
| CancelJobRequest | job_id | 1 | `String` | `string`, `job_id` |
| CancelJobRequest | idempotency_key | 2 | `String` | `string`, `idempotency_key` |
| CancelJobResponse | job | 1 | `Option<JobObservation>` + `message` | message presence, `job` |
| Header | name | 1 | `String` | `string`, `name` |
| Header | value | 2 | `String` | `string`, `value` |
| InvokeVersionRequest | version_sha256 | 1 | `Bytes` | `bytes`, `version_sha256` |
| InvokeVersionRequest | method | 2 | `String` | `string`, `method` |
| InvokeVersionRequest | url | 3 | `String` | `string`, `url` |
| InvokeVersionRequest | headers | 4 | `Vec<Header>` + `repeated(message)` | repeated message, `headers` |
| InvokeVersionRequest | body | 5 | `Bytes` | `bytes`, `body` |
| InvokeDeploymentRequest | alias | 1 | `String` | `string`, `alias` |
| InvokeDeploymentRequest | method | 2 | `String` | `string`, `method` |
| InvokeDeploymentRequest | url | 3 | `String` | `string`, `url` |
| InvokeDeploymentRequest | headers | 4 | `Vec<Header>` + `repeated(message)` | repeated message, `headers` |
| InvokeDeploymentRequest | body | 5 | `Bytes` | `bytes`, `body` |
| InvokeResponse | status | 1 | `u32` | `uint32`, `status` |
| InvokeResponse | headers | 2 | `Vec<Header>` + `repeated(message)` | repeated message, `headers` |
| InvokeResponse | body | 3 | `Bytes` | `bytes`, `body` |
| InvokeResponse | resolved_sha256 | 4 | `Bytes` | `bytes`, `resolved_sha256` |
| InvokeResponse | resolved_revision | 5 | `Option<u64>` | **proto3 optional**, `resolved_revision` |
| Error | code | 1 | `i32` + `enum_(ErrorCode)` | enum, `code` |
| Error | message | 2 | `String` | `string`, `message` |

## Enums

`JobState`: `UNSPECIFIED=0`, `ACCEPTED=1`, `RUNNING=2`, `SUCCEEDED=3`, `FAILED=4`, `CANCELLED=5`.

`ErrorCode`: `UNSPECIFIED=0`, `INVALID_ARGUMENT=1`, `CAPABILITY_DENIED=2`, `CAPABILITY_EXPIRED=3`, `VERSION_NOT_FOUND=4`, `DEPLOYMENT_NOT_FOUND=5`, `JOB_NOT_FOUND=6`, `IDEMPOTENCY_MISMATCH=7`, `REVISION_CONFLICT=8`, `OVERLOADED=9`, `TERMINAL_JOB_FAILURE=10`.

Protify's enum renderer prefixes Rust variants with the enum's protobuf name, so these declarations produce the exact enum value names above.

## Service

`WorkersService` contains seven unary methods, in the existing order, with the existing request/response identities:

- `PublishVersion(PublishVersionRequest) -> PublishVersionResponse`
- `SelectDeployment(SelectDeploymentRequest) -> SelectDeploymentResponse`
- `SubmitJob(SubmitJobRequest) -> SubmitJobResponse`
- `InspectJob(InspectJobRequest) -> InspectJobResponse`
- `CancelJob(CancelJobRequest) -> CancelJobResponse`
- `InvokeVersion(InvokeVersionRequest) -> InvokeResponse`
- `InvokeDeployment(InvokeDeploymentRequest) -> InvokeResponse`

The package is `acyclic.workers.v1`, the file is `workers/v1/workers.proto`, and the file option is the existing Go package value.
