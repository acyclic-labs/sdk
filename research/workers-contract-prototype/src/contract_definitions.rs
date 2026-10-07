use protify::*;

proto_package!(
    WORKERS_PACKAGE,
    name = "acyclic.workers.v1",
    files = [WORKERS_FILE]
);

define_proto_file!(
    WORKERS_FILE,
    name = "workers/v1/workers.proto",
    package = WORKERS_PACKAGE,
    options =
        [proto_option!("go_package" => "github.com/acyclic-labs/sdk/go/gen/workers/v1;workersv1")],
    messages = [
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
        Error,
    ],
    enums = [JobState, ErrorCode],
    services = [WorkersService],
);

/// Immutable JavaScript module digest and size.
#[proto_message]
pub struct CodeVersion {
    #[proto(name = "sha256", tag = 1, bytes)]
    /// SHA-256 digest of the exact module bytes.
    pub sha256: Bytes,
    #[proto(name = "size_bytes", tag = 2)]
    /// Module size in bytes.
    pub size_bytes: u64,
}

/// A deployment alias and its selected immutable version.
#[proto_message]
pub struct Deployment {
    #[proto(name = "alias", tag = 1)]
    /// Mutable deployment alias.
    pub alias: String,
    #[proto(name = "version", tag = 2, message)]
    /// Selected immutable version.
    pub version: Option<CodeVersion>,
    #[proto(name = "revision", tag = 3)]
    /// Monotonically increasing selection revision.
    pub revision: u64,
}

/// Uploads an immutable JavaScript module version.
#[proto_message]
pub struct PublishVersionRequest {
    #[proto(name = "javascript_module", tag = 1, bytes)]
    /// JavaScript module bytes.
    pub javascript_module: Bytes,
    #[proto(name = "expected_sha256", tag = 2, bytes)]
    /// Optional expected module digest.
    pub expected_sha256: Bytes,
    #[proto(name = "idempotency_key", tag = 3)]
    /// Idempotency key for the publish operation.
    pub idempotency_key: String,
}

/// Result of publishing a module version.
#[proto_message]
pub struct PublishVersionResponse {
    #[proto(name = "version", tag = 1, message)]
    /// Published immutable version.
    pub version: Option<CodeVersion>,
}

/// Selects a version for a deployment alias.
#[proto_message]
pub struct SelectDeploymentRequest {
    #[proto(name = "alias", tag = 1)]
    /// Deployment alias.
    pub alias: String,
    #[proto(name = "version_sha256", tag = 2, bytes)]
    /// Version digest to select.
    pub version_sha256: Bytes,
    /// Optional compare-and-set revision. Its presence is wire significant.
    #[proto(name = "expected_revision", tag = 3)]
    pub expected_revision: Option<u64>,
    #[proto(name = "idempotency_key", tag = 4)]
    /// Idempotency key for the selection operation.
    pub idempotency_key: String,
}

/// Result of selecting a deployment version.
#[proto_message]
pub struct SelectDeploymentResponse {
    #[proto(name = "deployment", tag = 1, message)]
    /// Updated deployment.
    pub deployment: Option<Deployment>,
}

/// Durable object reference used as job input.
#[proto_message]
#[proto(reserved_numbers(3), reserved_names("version_id"))]
pub struct ObjectRef {
    #[proto(name = "bucket", tag = 1)]
    /// Object storage bucket.
    pub bucket: String,
    #[proto(name = "key", tag = 2)]
    /// Object key.
    pub key: String,
}

/// Job input source.
pub mod payload {
    use super::*;

    /// Mutually exclusive inline or durable input source.
    #[proto_oneof]
    pub enum Source {
        #[proto(name = "inline_bytes", tag = 1)]
        /// Inline bytes.
        InlineBytes(Bytes),
        #[proto(name = "object", message, tag = 2)]
        /// Durable object reference.
        Object(ObjectRef),
    }
}

/// Accepted job input.
#[proto_message]
pub struct Payload {
    #[proto(name = "source", oneof(tags(1, 2)))]
    /// Selected input source.
    pub source: Option<payload::Source>,
}

/// Exact bounded output retained for a job.
#[proto_message]
#[proto(reserved_numbers(2), reserved_names("object_version"))]
pub struct JobResult {
    #[proto(name = "body", tag = 1, bytes)]
    /// Output body bytes.
    pub body: Bytes,
}

/// Execution resource limits.
#[proto_message]
pub struct JobLimits {
    #[proto(name = "timeout_millis", tag = 1)]
    /// Timeout in milliseconds.
    pub timeout_millis: u64,
    #[proto(name = "memory_bytes", tag = 2)]
    /// Memory limit in bytes.
    pub memory_bytes: u64,
    #[proto(name = "output_bytes", tag = 3)]
    /// Output limit in bytes.
    pub output_bytes: u64,
}

/// Retry behavior for a durable job.
#[proto_message]
pub struct RetryPolicy {
    #[proto(name = "max_attempts", tag = 1)]
    /// Maximum number of attempts.
    pub max_attempts: u32,
    #[proto(name = "backoff_millis", tag = 2)]
    /// Backoff between attempts in milliseconds.
    pub backoff_millis: u64,
}

/// Job target selector.
pub mod job_target {
    use super::*;

    /// Mutually exclusive deployment or immutable version target.
    #[proto_oneof]
    pub enum Target {
        #[proto(name = "deployment_alias", tag = 1)]
        /// Mutable deployment alias.
        DeploymentAlias(String),
        #[proto(name = "version_sha256", tag = 2)]
        /// Immutable version digest.
        VersionSha256(Bytes),
    }
}

/// Target selector for a durable job.
#[proto_message]
pub struct JobTarget {
    #[proto(name = "target", oneof(tags(1, 2)))]
    /// Selected job target.
    pub target: Option<job_target::Target>,
}

/// Submits a durable job.
#[proto_message]
pub struct SubmitJobRequest {
    #[proto(name = "target", tag = 1, message)]
    /// Target selector.
    pub target: Option<JobTarget>,
    #[proto(name = "input", tag = 2, message)]
    /// Job input.
    pub input: Option<Payload>,
    #[proto(name = "limits", tag = 3, message)]
    /// Execution limits.
    pub limits: Option<JobLimits>,
    #[proto(name = "retry", tag = 4, message)]
    /// Retry policy.
    pub retry: Option<RetryPolicy>,
    #[proto(name = "idempotency_key", tag = 5)]
    /// Idempotency key for the submission.
    pub idempotency_key: String,
}

/// Result of submitting a durable job.
#[proto_message]
pub struct SubmitJobResponse {
    #[proto(name = "job", tag = 1, message)]
    /// Accepted job observation.
    pub job: Option<JobObservation>,
}

/// Durable job lifecycle state.
#[proto_enum]
pub enum JobState {
    /// No state was supplied.
    #[proto(name = "JOB_STATE_UNSPECIFIED")]
    Unspecified = 0,
    /// Job accepted for execution.
    #[proto(name = "JOB_STATE_ACCEPTED")]
    Accepted = 1,
    /// Job currently executing.
    #[proto(name = "JOB_STATE_RUNNING")]
    Running = 2,
    /// Job completed successfully.
    #[proto(name = "JOB_STATE_SUCCEEDED")]
    Succeeded = 3,
    /// Job reached terminal failure.
    #[proto(name = "JOB_STATE_FAILED")]
    Failed = 4,
    /// Job was cancelled.
    #[proto(name = "JOB_STATE_CANCELLED")]
    Cancelled = 5,
}

/// Observed durable job state and result.
#[proto_message]
pub struct JobObservation {
    #[proto(name = "job_id", tag = 1)]
    /// Stable job identity.
    pub job_id: String,
    #[proto(name = "state", tag = 2, enum_(JobState))]
    /// Encoded lifecycle state.
    pub state: i32,
    #[proto(name = "resolved_sha256", tag = 3, bytes)]
    /// Resolved version digest.
    pub resolved_sha256: Bytes,
    #[proto(name = "attempt", tag = 4)]
    /// One-based execution attempt.
    pub attempt: u32,
    #[proto(name = "result", tag = 5, message)]
    /// Retained result when available.
    pub result: Option<JobResult>,
    #[proto(name = "failure_code", tag = 6)]
    /// Stable failure code when terminally failed.
    pub failure_code: String,
    #[proto(name = "cancellation_requested", tag = 7)]
    /// Whether cancellation has been requested.
    pub cancellation_requested: bool,
}

/// Inspects a durable job.
#[proto_message]
pub struct InspectJobRequest {
    #[proto(name = "job_id", tag = 1)]
    /// Stable job identity.
    pub job_id: String,
}

/// Result of inspecting a durable job.
#[proto_message]
pub struct InspectJobResponse {
    #[proto(name = "job", tag = 1, message)]
    /// Current job observation.
    pub job: Option<JobObservation>,
}

/// Cancels a durable job.
#[proto_message]
pub struct CancelJobRequest {
    #[proto(name = "job_id", tag = 1)]
    /// Stable job identity.
    pub job_id: String,
    #[proto(name = "idempotency_key", tag = 2)]
    /// Idempotency key for the cancellation operation.
    pub idempotency_key: String,
}

/// Result of requesting cancellation.
#[proto_message]
pub struct CancelJobResponse {
    #[proto(name = "job", tag = 1, message)]
    /// Updated job observation.
    pub job: Option<JobObservation>,
}

/// HTTP header pair.
#[proto_message]
pub struct Header {
    #[proto(name = "name", tag = 1)]
    /// Header name.
    pub name: String,
    #[proto(name = "value", tag = 2)]
    /// Header value.
    pub value: String,
}

/// Invokes an immutable version using ordinary HTTP semantics.
#[proto_message]
pub struct InvokeVersionRequest {
    #[proto(name = "version_sha256", tag = 1, bytes)]
    /// Version digest.
    pub version_sha256: Bytes,
    #[proto(name = "method", tag = 2)]
    /// HTTP method.
    pub method: String,
    #[proto(name = "url", tag = 3)]
    /// Request URL.
    pub url: String,
    #[proto(name = "headers", tag = 4, repeated(message))]
    /// Request headers.
    pub headers: Vec<Header>,
    #[proto(name = "body", tag = 5, bytes)]
    /// Request body.
    pub body: Bytes,
}

/// Invokes a deployment alias using ordinary HTTP semantics.
#[proto_message]
pub struct InvokeDeploymentRequest {
    #[proto(name = "alias", tag = 1)]
    /// Deployment alias.
    pub alias: String,
    #[proto(name = "method", tag = 2)]
    /// HTTP method.
    pub method: String,
    #[proto(name = "url", tag = 3)]
    /// Request URL.
    pub url: String,
    #[proto(name = "headers", tag = 4, repeated(message))]
    /// Request headers.
    pub headers: Vec<Header>,
    #[proto(name = "body", tag = 5, bytes)]
    /// Request body.
    pub body: Bytes,
}

/// Response from an ordinary HTTP invocation.
#[proto_message]
pub struct InvokeResponse {
    #[proto(name = "status", tag = 1)]
    /// HTTP status code.
    pub status: u32,
    #[proto(name = "headers", tag = 2, repeated(message))]
    /// Response headers.
    pub headers: Vec<Header>,
    #[proto(name = "body", tag = 3, bytes)]
    /// Response body.
    pub body: Bytes,
    #[proto(name = "resolved_sha256", tag = 4, bytes)]
    /// Resolved version digest.
    pub resolved_sha256: Bytes,
    /// Optional resolved deployment revision. Its presence is wire significant.
    #[proto(name = "resolved_revision", tag = 5)]
    pub resolved_revision: Option<u64>,
}

/// Error classification returned by the Workers service.
#[proto_enum]
pub enum ErrorCode {
    /// No error classification was supplied.
    #[proto(name = "ERROR_CODE_UNSPECIFIED")]
    Unspecified = 0,
    /// One or more request values are invalid.
    #[proto(name = "ERROR_CODE_INVALID_ARGUMENT")]
    InvalidArgument = 1,
    /// Capability denied.
    #[proto(name = "ERROR_CODE_CAPABILITY_DENIED")]
    CapabilityDenied = 2,
    /// Capability expired.
    #[proto(name = "ERROR_CODE_CAPABILITY_EXPIRED")]
    CapabilityExpired = 3,
    /// Version was not found.
    #[proto(name = "ERROR_CODE_VERSION_NOT_FOUND")]
    VersionNotFound = 4,
    /// Deployment was not found.
    #[proto(name = "ERROR_CODE_DEPLOYMENT_NOT_FOUND")]
    DeploymentNotFound = 5,
    /// Job was not found.
    #[proto(name = "ERROR_CODE_JOB_NOT_FOUND")]
    JobNotFound = 6,
    /// Idempotency key did not match.
    #[proto(name = "ERROR_CODE_IDEMPOTENCY_MISMATCH")]
    IdempotencyMismatch = 7,
    /// Deployment revision conflicted.
    #[proto(name = "ERROR_CODE_REVISION_CONFLICT")]
    RevisionConflict = 8,
    /// Service is overloaded.
    #[proto(name = "ERROR_CODE_OVERLOADED")]
    Overloaded = 9,
    /// Job reached terminal failure.
    #[proto(name = "ERROR_CODE_TERMINAL_JOB_FAILURE")]
    TerminalJobFailure = 10,
}

/// Structured Workers service error.
#[proto_message]
pub struct Error {
    #[proto(name = "code", tag = 1, enum_(ErrorCode))]
    /// Encoded error classification.
    pub code: i32,
    #[proto(name = "message", tag = 2)]
    /// Human-readable message.
    pub message: String,
}

/// Workers RPC operations.
#[proto_service]
pub enum WorkersService {
    /// Publishes an immutable module version.
    PublishVersion {
        request: PublishVersionRequest,
        response: PublishVersionResponse,
    },
    /// Selects a version for an alias.
    SelectDeployment {
        request: SelectDeploymentRequest,
        response: SelectDeploymentResponse,
    },
    /// Submits a durable job.
    SubmitJob {
        request: SubmitJobRequest,
        response: SubmitJobResponse,
    },
    /// Inspects a durable job.
    InspectJob {
        request: InspectJobRequest,
        response: InspectJobResponse,
    },
    /// Requests cancellation of a durable job.
    CancelJob {
        request: CancelJobRequest,
        response: CancelJobResponse,
    },
    /// Invokes an immutable version.
    InvokeVersion {
        request: InvokeVersionRequest,
        response: InvokeResponse,
    },
    /// Invokes a deployment alias.
    InvokeDeployment {
        request: InvokeDeploymentRequest,
        response: InvokeResponse,
    },
}
