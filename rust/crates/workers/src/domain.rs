// Executable Workers semantic declarations. Wire shadows and schema come from these types.
use crate::contract::WORKERS_FILE;
use protify::*;
use ts_rs::{Config, ExportError, TS};

#[derive(Clone, Debug, Default, Eq, PartialEq, thiserror::Error)]
pub enum DomainError {
    #[default]
    #[error("required Workers message or selector is absent")]
    MissingMessage,
    #[error(transparent)]
    Contract(#[from] crate::ContractError),
    #[error("unknown JobState value {0}")]
    UnknownJobState(i32),
    #[error("unknown ErrorCode value {0}")]
    UnknownErrorCode(i32),
}
impl From<std::convert::Infallible> for DomainError {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

/// Known `JobState` values; raw shadows retain unknown protobuf integers.
#[acyclic_protify_proc_macro::proto_enum(error = DomainError, unknown = DomainError::UnknownJobState)]
#[proto(file = WORKERS_FILE)]
#[derive(TS)]
#[ts(export_to = "workers/JobState.ts", repr(enum))]
pub enum JobState {
    /// No job state was reported.
    Unspecified = 0,
    /// The durable job was accepted.
    Accepted = 1,
    /// An attempt is executing the run handler.
    Running = 2,
    /// Execution produced a successful result.
    Succeeded = 3,
    /// Execution finished with a failure.
    Failed = 4,
    /// The job was cancelled.
    Cancelled = 5,
}
fn parse_jobstate(value: i32) -> Result<JobState, DomainError> {
    JobState::try_from(value)
}
fn encode_jobstate(value: JobState) -> i32 {
    value as i32
}

/// Known `ErrorCode` values; raw shadows retain unknown protobuf integers.
#[acyclic_protify_proc_macro::proto_enum(error = DomainError, unknown = DomainError::UnknownErrorCode)]
#[proto(file = WORKERS_FILE)]
#[derive(TS)]
#[ts(export_to = "workers/ErrorCode.ts", repr(enum))]
pub enum ErrorCode {
    /// No service error code was reported.
    Unspecified = 0,
    /// The request failed contract admission.
    InvalidArgument = 1,
    /// The credential does not admit the requested operation.
    CapabilityDenied = 2,
    /// The credential has expired.
    CapabilityExpired = 3,
    /// The immutable version was not found.
    VersionNotFound = 4,
    /// The deployment alias was not found.
    DeploymentNotFound = 5,
    /// The durable job was not found.
    JobNotFound = 6,
    /// The idempotency key identifies a different request.
    IdempotencyMismatch = 7,
    /// The deployment revision precondition did not match.
    RevisionConflict = 8,
    /// The service could not admit work at its current load.
    Overloaded = 9,
    /// Durable execution ended with a terminal failure.
    TerminalJobFailure = 10,
}
fn parse_errorcode(value: i32) -> Result<ErrorCode, DomainError> {
    ErrorCode::try_from(value)
}
fn encode_errorcode(value: ErrorCode) -> i32 {
    value as i32
}

/// Semantic Source cases preserving the published oneof.
pub mod payload {
    use super::*;
    #[acyclic_protify_proc_macro::proto_oneof(proxied, fallible = DomainError)]
    #[derive(Clone, Debug, Eq, PartialEq, TS)]
    #[ts(
        export_to = "workers/Source.ts",
        tag = "case",
        content = "value",
        rename_all = "camelCase"
    )]
    pub enum Source {
        /// Inline input bytes; an empty byte sequence is a present input.
        #[proto(tag = 1, bytes)]
        InlineBytes(#[ts(type = "Uint8Array")] Vec<u8>),
        /// Logical object privately retained when the job is accepted.
        #[proto(tag = 2, message(proxied))]
        Object(ObjectRef),
    }
}

/// Semantic Target cases preserving the published oneof.
pub mod job_target {
    use super::*;
    #[acyclic_protify_proc_macro::proto_oneof(proxied, fallible = DomainError)]
    #[derive(Clone, Debug, Eq, PartialEq, TS)]
    #[ts(
        export_to = "workers/Target.ts",
        tag = "case",
        content = "value",
        rename_all = "camelCase"
    )]
    pub enum Target {
        /// Deployment alias resolved at job acceptance.
        #[proto(tag = 1, string)]
        DeploymentAlias(String),
        /// Exact immutable version selected for the job.
        #[proto(tag = 2, bytes)]
        VersionSha256(#[ts(type = "Uint8Array")] Vec<u8>),
    }
}

/// A digest identifies exact immutable JavaScript module bytes.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/CodeVersion.ts", rename_all = "camelCase")]
pub struct CodeVersion {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Uint8Array")]
    /// SHA-256 identifying the exact immutable module bytes.
    pub sha256: Vec<u8>,
    #[proto(tag = 2, uint64)]
    #[ts(type = "bigint")]
    /// Size of the module in bytes.
    pub size_bytes: u64,
}

/// A deployment alias may change; revision increases on every selection.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Deployment.ts", rename_all = "camelCase")]
pub struct Deployment {
    #[proto(tag = 1)]
    /// Mutable deployment alias.
    pub alias: String,
    #[proto(tag = 2, message(proxied))]
    #[ts(optional)]
    /// Selected immutable version, when reported by the service.
    pub version: Option<CodeVersion>,
    #[proto(tag = 3, uint64)]
    #[ts(type = "bigint")]
    /// Deployment revision, advanced by each successful selection.
    pub revision: u64,
}

/// Publish exact module bytes with their expected SHA-256 and idempotency key.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, post_from_proto = PublishVersionRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/PublishVersionRequest.ts",
    rename_all = "camelCase"
)]
pub struct PublishVersionRequest {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Uint8Array")]
    /// Exact module bytes; publication requires a nonempty module within `MAX_MODULE_BYTES`.
    javascript_module: Vec<u8>,
    #[proto(tag = 2, bytes)]
    #[ts(type = "Uint8Array")]
    /// Expected SHA-256, checked against the exact module bytes at publication.
    expected_sha256: Vec<u8>,
    #[proto(tag = 3)]
    /// Caller-provided key for idempotent admission.
    idempotency_key: String,
}
impl PublishVersionRequest {
    /// Creates a request using the canonical Workers admission predicate.
    pub fn new(
        javascript_module: Vec<u8>,
        expected_sha256: Vec<u8>,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let value = Self {
            javascript_module,
            expected_sha256,
            idempotency_key,
        };
        value.validate_from_proto()?;
        Ok(value)
    }
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        crate::validate_publish(&self.clone().into()).map_err(DomainError::Contract)
    }
    /// Exact module bytes; publication requires a nonempty module within `MAX_MODULE_BYTES`.
    #[must_use]
    pub fn javascript_module(&self) -> &[u8] {
        &self.javascript_module
    }
    /// Expected SHA-256, checked against the exact module bytes at publication.
    #[must_use]
    pub fn expected_sha256(&self) -> &[u8] {
        &self.expected_sha256
    }
    /// Caller-provided key for idempotent admission.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// The immutable version accepted by module publication.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/PublishVersionResponse.ts",
    rename_all = "camelCase"
)]
pub struct PublishVersionResponse {
    #[proto(tag = 1, message(proxied))]
    #[ts(optional)]
    /// Selected immutable version, when reported by the service.
    pub version: Option<CodeVersion>,
}

/// Select an immutable version for a deployment alias using revision preconditions.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, post_from_proto = SelectDeploymentRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/SelectDeploymentRequest.ts",
    rename_all = "camelCase"
)]
pub struct SelectDeploymentRequest {
    #[proto(tag = 1)]
    /// Mutable deployment alias.
    alias: String,
    #[proto(tag = 2, bytes)]
    #[ts(type = "Uint8Array")]
    /// SHA-256 selecting the immutable version.
    version_sha256: Vec<u8>,
    #[proto(tag = 3, optional(uint64))]
    #[ts(optional)]
    /// Omitted means create only if absent. A present positive value selects only
    /// when it matches the current revision; every successful selection advances it.
    expected_revision: Option<u64>,
    #[proto(tag = 4)]
    /// Caller-provided key for idempotent admission.
    idempotency_key: String,
}
impl SelectDeploymentRequest {
    /// Creates a request using the canonical Workers admission predicate.
    pub fn new(
        alias: String,
        version_sha256: Vec<u8>,
        expected_revision: Option<u64>,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let value = Self {
            alias,
            version_sha256,
            expected_revision,
            idempotency_key,
        };
        value.validate_from_proto()?;
        Ok(value)
    }
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        crate::validate_select(&self.clone().into()).map_err(DomainError::Contract)
    }
    /// Mutable deployment alias.
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }
    /// SHA-256 selecting the immutable version.
    #[must_use]
    pub fn version_sha256(&self) -> &[u8] {
        &self.version_sha256
    }
    /// Omitted means create only if absent. A present positive value selects only
    /// when it matches the current revision; every successful selection advances it.
    #[must_use]
    pub fn expected_revision(&self) -> &Option<u64> {
        &self.expected_revision
    }
    /// Caller-provided key for idempotent admission.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// The deployment selected by the alias mutation.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/SelectDeploymentResponse.ts",
    rename_all = "camelCase"
)]
pub struct SelectDeploymentResponse {
    #[proto(tag = 1, message(proxied))]
    #[ts(optional)]
    /// Selected deployment, when reported by the service.
    pub deployment: Option<Deployment>,
}

/// Logical S3 object selected and privately retained at durable job acceptance.
/// Retries read the same retained bytes even if this public key is replaced.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, reserved_numbers(3), reserved_names("version_id"))]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/ObjectRef.ts", rename_all = "camelCase")]
pub struct ObjectRef {
    #[proto(tag = 1)]
    /// Logical object bucket; job admission checks nonempty text and at most 63 bytes.
    pub bucket: String,
    #[proto(tag = 2)]
    /// Logical object key; job admission checks nonempty text and at most 1024 bytes.
    pub key: String,
}

/// Durable job input selected from inline bytes or a retained logical object.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Payload.ts", rename_all = "camelCase")]
pub struct Payload {
    #[proto(tag = 1, oneof(proxied, tags(1, 2), required))]
    /// Inline bytes or the logical object retained at job acceptance.
    pub source: payload::Source,
}

/// Exact accepted job output, bounded by `JobLimits.output_bytes`. Storage and
/// retention are service-owned; no replaceable public Object pointer is exposed.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, reserved_numbers(2), reserved_names("object_version"))]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobResult.ts", rename_all = "camelCase")]
pub struct JobResult {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Uint8Array")]
    /// Exact accepted job output bytes.
    pub body: Vec<u8>,
}

/// Resource budgets applied to the accepted durable job.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobLimits.ts", rename_all = "camelCase")]
pub struct JobLimits {
    #[proto(tag = 1, uint64)]
    #[ts(type = "bigint")]
    /// Execution timeout in milliseconds; job submission requires a positive value.
    pub timeout_millis: u64,
    #[proto(tag = 2, uint64)]
    #[ts(type = "bigint")]
    /// Memory budget in bytes; job submission requires a positive value.
    pub memory_bytes: u64,
    #[proto(tag = 3, uint64)]
    #[ts(type = "bigint")]
    /// Result budget in bytes; job submission requires a positive value at most `MAX_INLINE_BYTES`.
    pub output_bytes: u64,
}

/// Bounded attempts and retry delay for the accepted durable job.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/RetryPolicy.ts", rename_all = "camelCase")]
pub struct RetryPolicy {
    #[proto(tag = 1, uint32)]
    /// Number of attempts, including the first; job submission admits 1 through `MAX_JOB_ATTEMPTS`.
    pub max_attempts: u32,
    #[proto(tag = 2, uint64)]
    #[ts(type = "bigint")]
    /// Delay between attempts in milliseconds; zero is admitted.
    pub backoff_millis: u64,
}

/// Resolve a deployment alias or select an exact immutable version at job acceptance.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobTarget.ts", rename_all = "camelCase")]
pub struct JobTarget {
    #[proto(tag = 1, oneof(proxied, tags(1, 2), required))]
    /// Deployment alias or immutable version selected for job admission.
    pub target: job_target::Target,
}

/// Accepted input is delivered to `default.run`, never to `default.fetch`.
/// The same job ID and input recur on retry; attempt numbering starts at one.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, post_from_proto = SubmitJobRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/SubmitJobRequest.ts", rename_all = "camelCase")]
pub struct SubmitJobRequest {
    #[proto(tag = 1, message(proxied, required))]
    /// Deployment alias or immutable version selected for job admission.
    target: JobTarget,
    #[proto(tag = 2, message(proxied, required))]
    /// Input delivered to the durable run handler.
    input: Payload,
    #[proto(tag = 3, message(proxied, required))]
    /// Accepted execution and result budgets.
    limits: JobLimits,
    #[proto(tag = 4, message(proxied, required))]
    /// Accepted attempt count and backoff policy.
    retry: RetryPolicy,
    #[proto(tag = 5)]
    /// Caller-provided key for idempotent admission.
    idempotency_key: String,
}
impl SubmitJobRequest {
    /// Creates a request using the canonical Workers admission predicate.
    pub fn new(
        target: JobTarget,
        input: Payload,
        limits: JobLimits,
        retry: RetryPolicy,
        idempotency_key: String,
    ) -> Result<Self, DomainError> {
        let value = Self {
            target,
            input,
            limits,
            retry,
            idempotency_key,
        };
        value.validate_from_proto()?;
        Ok(value)
    }
    fn validate_from_proto(&self) -> Result<(), DomainError> {
        crate::validate_submit(&self.clone().into()).map_err(DomainError::Contract)
    }
    /// Deployment alias or immutable version selected for job admission.
    #[must_use]
    pub fn target(&self) -> &JobTarget {
        &self.target
    }
    /// Input delivered to the durable run handler.
    #[must_use]
    pub fn input(&self) -> &Payload {
        &self.input
    }
    /// Accepted execution and result budgets.
    #[must_use]
    pub fn limits(&self) -> &JobLimits {
        &self.limits
    }
    /// Accepted attempt count and backoff policy.
    #[must_use]
    pub fn retry(&self) -> &RetryPolicy {
        &self.retry
    }
    /// Caller-provided key for idempotent admission.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// The durable job observation returned at acceptance.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/SubmitJobResponse.ts", rename_all = "camelCase")]
pub struct SubmitJobResponse {
    #[proto(tag = 1, message(proxied))]
    #[ts(optional)]
    /// Durable job observation, when reported by the service.
    pub job: Option<JobObservation>,
}

/// Current execution state, resolved version and result of a durable job.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobObservation.ts", rename_all = "camelCase")]
pub struct JobObservation {
    #[proto(tag = 1)]
    /// Identity of the accepted durable job.
    pub job_id: String,
    #[proto(tag = 2, enum_(JobState), from_proto = parse_jobstate, into_proto = encode_jobstate)]
    /// Current execution state reported for the job.
    pub state: JobState,
    #[proto(tag = 3, bytes)]
    #[ts(type = "Uint8Array")]
    /// SHA-256 of the immutable version resolved for this operation.
    pub resolved_sha256: Vec<u8>,
    #[proto(tag = 4, uint32)]
    /// Attempt number, starting at one for accepted jobs.
    pub attempt: u32,
    #[proto(tag = 5, message(proxied))]
    #[ts(optional)]
    /// Accepted result bytes, when reported by execution.
    pub result: Option<JobResult>,
    #[proto(tag = 6)]
    /// Failure code reported by job execution.
    pub failure_code: String,
    #[proto(tag = 7)]
    /// Whether cancellation has been requested for the job.
    pub cancellation_requested: bool,
}

/// Inspect the current observation of an accepted durable job.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/InspectJobRequest.ts", rename_all = "camelCase")]
pub struct InspectJobRequest {
    #[proto(tag = 1)]
    /// Identity of the accepted durable job.
    pub job_id: String,
}

/// The durable job observation returned by inspection.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/InspectJobResponse.ts", rename_all = "camelCase")]
pub struct InspectJobResponse {
    #[proto(tag = 1, message(proxied))]
    #[ts(optional)]
    /// Durable job observation, when reported by the service.
    pub job: Option<JobObservation>,
}

/// Request cancellation of an accepted job with an idempotency key.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/CancelJobRequest.ts", rename_all = "camelCase")]
pub struct CancelJobRequest {
    #[proto(tag = 1)]
    /// Identity of the accepted durable job.
    pub job_id: String,
    #[proto(tag = 2)]
    /// Caller-provided key for idempotent admission.
    pub idempotency_key: String,
}

/// The durable job observation returned after requesting cancellation.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/CancelJobResponse.ts", rename_all = "camelCase")]
pub struct CancelJobResponse {
    #[proto(tag = 1, message(proxied))]
    #[ts(optional)]
    /// Durable job observation, when reported by the service.
    pub job: Option<JobObservation>,
}

/// A name and value carried by the ordinary HTTP invocation.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Header.ts", rename_all = "camelCase")]
pub struct Header {
    #[proto(tag = 1)]
    /// HTTP header name.
    pub name: String,
    #[proto(tag = 2)]
    /// HTTP header value.
    pub value: String,
}

/// Invocation is ordinary HTTP work, not durable job acceptance.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/InvokeVersionRequest.ts",
    rename_all = "camelCase"
)]
pub struct InvokeVersionRequest {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Uint8Array")]
    /// SHA-256 selecting the immutable version.
    pub version_sha256: Vec<u8>,
    #[proto(tag = 2)]
    /// HTTP method passed to the invoked worker.
    pub method: String,
    #[proto(tag = 3)]
    /// HTTP URL passed to the invoked worker.
    pub url: String,
    #[proto(tag = 4, repeated(message(proxied)))]
    /// HTTP headers in their supplied order.
    pub headers: Vec<Header>,
    #[proto(tag = 5, bytes)]
    #[ts(type = "Uint8Array")]
    /// HTTP request body bytes.
    pub body: Vec<u8>,
}

/// Resolve a deployment alias for an ordinary HTTP invocation.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/InvokeDeploymentRequest.ts",
    rename_all = "camelCase"
)]
pub struct InvokeDeploymentRequest {
    #[proto(tag = 1)]
    /// Mutable deployment alias.
    pub alias: String,
    #[proto(tag = 2)]
    /// HTTP method passed to the invoked worker.
    pub method: String,
    #[proto(tag = 3)]
    /// HTTP URL passed to the invoked worker.
    pub url: String,
    #[proto(tag = 4, repeated(message(proxied)))]
    /// HTTP headers in their supplied order.
    pub headers: Vec<Header>,
    #[proto(tag = 5, bytes)]
    #[ts(type = "Uint8Array")]
    /// HTTP request body bytes.
    pub body: Vec<u8>,
}

/// HTTP response and the immutable version resolved for the invocation.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/InvokeResponse.ts", rename_all = "camelCase")]
pub struct InvokeResponse {
    #[proto(tag = 1, uint32)]
    /// HTTP status returned by the invocation.
    pub status: u32,
    #[proto(tag = 2, repeated(message(proxied)))]
    /// HTTP headers in their supplied order.
    pub headers: Vec<Header>,
    #[proto(tag = 3, bytes)]
    #[ts(type = "Uint8Array")]
    /// HTTP response body bytes.
    pub body: Vec<u8>,
    #[proto(tag = 4, bytes)]
    #[ts(type = "Uint8Array")]
    /// SHA-256 of the immutable version resolved for this operation.
    pub resolved_sha256: Vec<u8>,
    #[proto(tag = 5, optional(uint64))]
    #[ts(optional)]
    /// Deployment revision when an alias was resolved; absent for a direct version invocation.
    pub resolved_revision: Option<u64>,
}

/// Workers service rejection with its known code and original message.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Error.ts", rename_all = "camelCase")]
pub struct Error {
    #[proto(tag = 1, enum_(ErrorCode), from_proto = parse_errorcode, into_proto = encode_errorcode)]
    /// Known Workers service error code.
    pub code: ErrorCode,
    #[proto(tag = 2)]
    /// Original service error message.
    pub message: String,
}

/// Export the actual Rust declaration registry and its readonly public aliases.
pub fn export_typescript(path: impl AsRef<std::path::Path>) -> Result<(), ExportError> {
    export_typescript_with_metadata(path, Vec::new()).map(drop)
}
/// Exported TypeScript identifiers and their compiler-linked protobuf identities.
pub type TypeScriptMetadata = (Vec<String>, Vec<(String, String)>);
/// Export actual declarations and compiler-linked wire/TS identities from one root registry.
pub fn export_typescript_with_metadata(path: impl AsRef<std::path::Path>, additional: Vec<(String, std::path::PathBuf)>) -> Result<TypeScriptMetadata, ExportError> {
    let config = Config::default()
        .with_out_dir(path.as_ref())
        .with_import_extension(Some("js"));
    let mut exports = additional;
    let mut messages = Vec::new();
    fn paired<D, W>(config: &Config) -> (String, String)
    where D: TS + TryFrom<W>, W: protify::ProtoMessage + From<D> {
        (W::full_name().to_owned(), D::ident(config))
    }
    macro_rules! export_roots {
        ($($root:ty),+ $(,)?) => { $(
            <$root as TS>::export_all(&config)?;
            exports.push((<$root as TS>::ident(&config), <$root as TS>::output_path().ok_or(ExportError::CannotBeExported(std::any::type_name::<$root>()))?));
        )+ };
    }
    macro_rules! export_messages {
        ($($root:ident),+ $(,)?) => {
            export_roots!($($root),+);
            $(messages.push(paired::<$root, crate::wire::$root>(&config));)+
        };
    }
    export_messages!(
        CodeVersion,
        Deployment,
        PublishVersionRequest,
        PublishVersionResponse,
        SelectDeploymentRequest,
        SelectDeploymentResponse,
        ObjectRef,
        Payload,
        JobResult,
        JobLimits,
        RetryPolicy,
        JobTarget,
        SubmitJobRequest,
        SubmitJobResponse,
        JobObservation,
        InspectJobRequest,
        InspectJobResponse,
        CancelJobRequest,
        CancelJobResponse,
        Header,
        InvokeVersionRequest,
        InvokeDeploymentRequest,
        InvokeResponse,
        Error
    );
    export_roots!(
        JobState,
        ErrorCode,
        payload::Source,
        job_target::Target
    );
    std::fs::write(
        path.as_ref().join("workers/JobState.ts"),
        format!(
            "// Generated from Rust discriminants. Do not edit.\nexport type {} = {};\n",
            <JobState as TS>::ident(&config),
            <JobState as TS>::inline(&config)
        ),
    )?;
    std::fs::write(
        path.as_ref().join("workers/ErrorCode.ts"),
        format!(
            "// Generated from Rust discriminants. Do not edit.\nexport type {} = {};\n",
            <ErrorCode as TS>::ident(&config),
            <ErrorCode as TS>::inline(&config)
        ),
    )?;
    exports.sort_by(|left, right| left.0.cmp(&right.0));
    let raw = exports
        .iter()
        .map(|(_, output)| {
            Ok(format!(
                "export * from \"./{}\";",
                output
                    .file_name()
                    .ok_or(ExportError::CannotBeExported("TypeScript output filename"))?
                    .to_string_lossy()
                    .replace(".ts", ".js")
            ))
        })
        .collect::<Result<Vec<_>, ExportError>>()?
        .join("\n");
    let aliases = exports
        .iter()
        .map(|(name, _)| format!("export type {name} = ReadonlySemantic<Semantic.{name}>;"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        path.as_ref().join("workers/index.ts"),
        format!("// Generated from Rust ts-rs metadata. Do not edit.\n{raw}\n"),
    )?;
    std::fs::write(
        path.as_ref().join("workers/readonly.ts"),
        format!(
            "// Generated from Rust ts-rs metadata. Do not edit.\nimport type * as Semantic from \"./index.js\";\nimport type {{ ReadonlySemantic }} from \"../../readonly.js\";\n{aliases}\n"
        ),
    )?;
    Ok((exports.into_iter().map(|(name, _)| name).collect(), messages))
}
