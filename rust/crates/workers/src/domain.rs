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

/// Known JobState values; raw shadows retain unknown protobuf integers.
#[acyclic_protify_proc_macro::proto_enum(error = DomainError, unknown = DomainError::UnknownJobState)]
#[proto(file = WORKERS_FILE)]
#[derive(TS)]
#[ts(export_to = "workers/JobState.ts", repr(enum))]
pub enum JobState {
    /// Published discriminant 0.
    Unspecified = 0,
    /// Published discriminant 1.
    Accepted = 1,
    /// Published discriminant 2.
    Running = 2,
    /// Published discriminant 3.
    Succeeded = 3,
    /// Published discriminant 4.
    Failed = 4,
    /// Published discriminant 5.
    Cancelled = 5,
}
fn parse_jobstate(value: i32) -> Result<JobState, DomainError> {
    JobState::try_from(value)
}
fn encode_jobstate(value: JobState) -> i32 {
    value as i32
}

/// Known ErrorCode values; raw shadows retain unknown protobuf integers.
#[acyclic_protify_proc_macro::proto_enum(error = DomainError, unknown = DomainError::UnknownErrorCode)]
#[proto(file = WORKERS_FILE)]
#[derive(TS)]
#[ts(export_to = "workers/ErrorCode.ts", repr(enum))]
pub enum ErrorCode {
    /// Published discriminant 0.
    Unspecified = 0,
    /// Published discriminant 1.
    InvalidArgument = 1,
    /// Published discriminant 2.
    CapabilityDenied = 2,
    /// Published discriminant 3.
    CapabilityExpired = 3,
    /// Published discriminant 4.
    VersionNotFound = 4,
    /// Published discriminant 5.
    DeploymentNotFound = 5,
    /// Published discriminant 6.
    JobNotFound = 6,
    /// Published discriminant 7.
    IdempotencyMismatch = 7,
    /// Published discriminant 8.
    RevisionConflict = 8,
    /// Published discriminant 9.
    Overloaded = 9,
    /// Published discriminant 10.
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
    #[ts(tag = "case", content = "value", rename_all = "camelCase")]
    pub enum Source {
        /// Published selector 1.
        #[proto(tag = 1, bytes)]
        #[ts(type = "Readonly<Uint8Array>")]
        InlineBytes(Vec<u8>),
        /// Published selector 2.
        #[proto(tag = 2, message(proxied))]
        Object(ObjectRef),
    }
}

/// Semantic Target cases preserving the published oneof.
pub mod job_target {
    use super::*;
    #[acyclic_protify_proc_macro::proto_oneof(proxied, fallible = DomainError)]
    #[derive(Clone, Debug, Eq, PartialEq, TS)]
    #[ts(tag = "case", content = "value", rename_all = "camelCase")]
    pub enum Target {
        /// Published selector 1.
        #[proto(tag = 1, string)]
        DeploymentAlias(String),
        /// Published selector 2.
        #[proto(tag = 2, bytes)]
        #[ts(type = "Readonly<Uint8Array>")]
        VersionSha256(Vec<u8>),
    }
}

/// Workers CodeVersion with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/CodeVersion.ts", rename_all = "camelCase")]
pub struct CodeVersion {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub sha256: Vec<u8>,
    #[proto(tag = 2, uint64)]
    #[ts(type = "bigint")]
    /// Published semantic field.
    pub size_bytes: u64,
}

/// Workers Deployment with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Deployment.ts", rename_all = "camelCase")]
pub struct Deployment {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub alias: String,
    #[proto(tag = 2, message(proxied))]
    /// Published semantic field.
    pub version: Option<CodeVersion>,
    #[proto(tag = 3, uint64)]
    #[ts(type = "bigint")]
    /// Published semantic field.
    pub revision: u64,
}

/// Workers PublishVersionRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, post_from_proto = PublishVersionRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/PublishVersionRequest.ts",
    rename_all = "camelCase"
)]
pub struct PublishVersionRequest {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    javascript_module: Vec<u8>,
    #[proto(tag = 2, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    expected_sha256: Vec<u8>,
    #[proto(tag = 3)]
    idempotency_key: String,
}
impl PublishVersionRequest {
    /// Creates a semantic value without changing its wire representation.
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
    /// Returns the exact javascript_module value.
    #[must_use]
    pub fn javascript_module(&self) -> &[u8] {
        &self.javascript_module
    }
    /// Returns the exact expected_sha256 value.
    #[must_use]
    pub fn expected_sha256(&self) -> &[u8] {
        &self.expected_sha256
    }
    /// Returns the exact idempotency_key value.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Workers PublishVersionResponse with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/PublishVersionResponse.ts",
    rename_all = "camelCase"
)]
pub struct PublishVersionResponse {
    #[proto(tag = 1, message(proxied))]
    /// Published semantic field.
    pub version: Option<CodeVersion>,
}

/// Workers SelectDeploymentRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, post_from_proto = SelectDeploymentRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/SelectDeploymentRequest.ts",
    rename_all = "camelCase"
)]
pub struct SelectDeploymentRequest {
    #[proto(tag = 1)]
    alias: String,
    #[proto(tag = 2, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    version_sha256: Vec<u8>,
    #[proto(tag = 3, optional(uint64))]
    expected_revision: Option<u64>,
    #[proto(tag = 4)]
    idempotency_key: String,
}
impl SelectDeploymentRequest {
    /// Creates a semantic value without changing its wire representation.
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
    /// Returns the exact alias value.
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }
    /// Returns the exact version_sha256 value.
    #[must_use]
    pub fn version_sha256(&self) -> &[u8] {
        &self.version_sha256
    }
    /// Returns the exact expected_revision value.
    #[must_use]
    pub fn expected_revision(&self) -> &Option<u64> {
        &self.expected_revision
    }
    /// Returns the exact idempotency_key value.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Workers SelectDeploymentResponse with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/SelectDeploymentResponse.ts",
    rename_all = "camelCase"
)]
pub struct SelectDeploymentResponse {
    #[proto(tag = 1, message(proxied))]
    /// Published semantic field.
    pub deployment: Option<Deployment>,
}

/// Workers ObjectRef with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, reserved_numbers(3), reserved_names("version_id"))]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/ObjectRef.ts", rename_all = "camelCase")]
pub struct ObjectRef {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub bucket: String,
    #[proto(tag = 2)]
    /// Published semantic field.
    pub key: String,
}

/// Workers Payload with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Payload.ts", rename_all = "camelCase")]
pub struct Payload {
    #[proto(tag = 1, oneof(proxied, tags(1, 2), required))]
    /// Published semantic field.
    pub source: payload::Source,
}

/// Workers JobResult with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, reserved_numbers(2), reserved_names("object_version"))]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobResult.ts", rename_all = "camelCase")]
pub struct JobResult {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub body: Vec<u8>,
}

/// Workers JobLimits with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobLimits.ts", rename_all = "camelCase")]
pub struct JobLimits {
    #[proto(tag = 1, uint64)]
    #[ts(type = "bigint")]
    /// Published semantic field.
    pub timeout_millis: u64,
    #[proto(tag = 2, uint64)]
    #[ts(type = "bigint")]
    /// Published semantic field.
    pub memory_bytes: u64,
    #[proto(tag = 3, uint64)]
    #[ts(type = "bigint")]
    /// Published semantic field.
    pub output_bytes: u64,
}

/// Workers RetryPolicy with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/RetryPolicy.ts", rename_all = "camelCase")]
pub struct RetryPolicy {
    #[proto(tag = 1, uint32)]
    /// Published semantic field.
    pub max_attempts: u32,
    #[proto(tag = 2, uint64)]
    #[ts(type = "bigint")]
    /// Published semantic field.
    pub backoff_millis: u64,
}

/// Workers JobTarget with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobTarget.ts", rename_all = "camelCase")]
pub struct JobTarget {
    #[proto(tag = 1, oneof(proxied, tags(1, 2), required))]
    /// Published semantic field.
    pub target: job_target::Target,
}

/// Workers SubmitJobRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE, post_from_proto = SubmitJobRequest::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/SubmitJobRequest.ts", rename_all = "camelCase")]
pub struct SubmitJobRequest {
    #[proto(tag = 1, message(proxied, required))]
    target: JobTarget,
    #[proto(tag = 2, message(proxied, required))]
    input: Payload,
    #[proto(tag = 3, message(proxied, required))]
    limits: JobLimits,
    #[proto(tag = 4, message(proxied, required))]
    retry: RetryPolicy,
    #[proto(tag = 5)]
    idempotency_key: String,
}
impl SubmitJobRequest {
    /// Creates a semantic value without changing its wire representation.
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
    /// Returns the exact target value.
    #[must_use]
    pub fn target(&self) -> &JobTarget {
        &self.target
    }
    /// Returns the exact input value.
    #[must_use]
    pub fn input(&self) -> &Payload {
        &self.input
    }
    /// Returns the exact limits value.
    #[must_use]
    pub fn limits(&self) -> &JobLimits {
        &self.limits
    }
    /// Returns the exact retry value.
    #[must_use]
    pub fn retry(&self) -> &RetryPolicy {
        &self.retry
    }
    /// Returns the exact idempotency_key value.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

/// Workers SubmitJobResponse with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/SubmitJobResponse.ts", rename_all = "camelCase")]
pub struct SubmitJobResponse {
    #[proto(tag = 1, message(proxied))]
    /// Published semantic field.
    pub job: Option<JobObservation>,
}

/// Workers JobObservation with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/JobObservation.ts", rename_all = "camelCase")]
pub struct JobObservation {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub job_id: String,
    #[proto(tag = 2, enum_(JobState), from_proto = parse_jobstate, into_proto = encode_jobstate)]
    /// Published semantic field.
    pub state: JobState,
    #[proto(tag = 3, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub resolved_sha256: Vec<u8>,
    #[proto(tag = 4, uint32)]
    /// Published semantic field.
    pub attempt: u32,
    #[proto(tag = 5, message(proxied))]
    /// Published semantic field.
    pub result: Option<JobResult>,
    #[proto(tag = 6)]
    /// Published semantic field.
    pub failure_code: String,
    #[proto(tag = 7)]
    /// Published semantic field.
    pub cancellation_requested: bool,
}

/// Workers InspectJobRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/InspectJobRequest.ts", rename_all = "camelCase")]
pub struct InspectJobRequest {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub job_id: String,
}

/// Workers InspectJobResponse with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/InspectJobResponse.ts", rename_all = "camelCase")]
pub struct InspectJobResponse {
    #[proto(tag = 1, message(proxied))]
    /// Published semantic field.
    pub job: Option<JobObservation>,
}

/// Workers CancelJobRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/CancelJobRequest.ts", rename_all = "camelCase")]
pub struct CancelJobRequest {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub job_id: String,
    #[proto(tag = 2)]
    /// Published semantic field.
    pub idempotency_key: String,
}

/// Workers CancelJobResponse with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/CancelJobResponse.ts", rename_all = "camelCase")]
pub struct CancelJobResponse {
    #[proto(tag = 1, message(proxied))]
    /// Published semantic field.
    pub job: Option<JobObservation>,
}

/// Workers Header with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Header.ts", rename_all = "camelCase")]
pub struct Header {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub name: String,
    #[proto(tag = 2)]
    /// Published semantic field.
    pub value: String,
}

/// Workers InvokeVersionRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/InvokeVersionRequest.ts",
    rename_all = "camelCase"
)]
pub struct InvokeVersionRequest {
    #[proto(tag = 1, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub version_sha256: Vec<u8>,
    #[proto(tag = 2)]
    /// Published semantic field.
    pub method: String,
    #[proto(tag = 3)]
    /// Published semantic field.
    pub url: String,
    #[proto(tag = 4, repeated(message(proxied)))]
    /// Published semantic field.
    pub headers: Vec<Header>,
    #[proto(tag = 5, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub body: Vec<u8>,
}

/// Workers InvokeDeploymentRequest with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(
    export_to = "workers/InvokeDeploymentRequest.ts",
    rename_all = "camelCase"
)]
pub struct InvokeDeploymentRequest {
    #[proto(tag = 1)]
    /// Published semantic field.
    pub alias: String,
    #[proto(tag = 2)]
    /// Published semantic field.
    pub method: String,
    #[proto(tag = 3)]
    /// Published semantic field.
    pub url: String,
    #[proto(tag = 4, repeated(message(proxied)))]
    /// Published semantic field.
    pub headers: Vec<Header>,
    #[proto(tag = 5, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub body: Vec<u8>,
}

/// Workers InvokeResponse with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/InvokeResponse.ts", rename_all = "camelCase")]
pub struct InvokeResponse {
    #[proto(tag = 1, uint32)]
    /// Published semantic field.
    pub status: u32,
    #[proto(tag = 2, repeated(message(proxied)))]
    /// Published semantic field.
    pub headers: Vec<Header>,
    #[proto(tag = 3, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub body: Vec<u8>,
    #[proto(tag = 4, bytes)]
    #[ts(type = "Readonly<Uint8Array>")]
    /// Published semantic field.
    pub resolved_sha256: Vec<u8>,
    #[proto(tag = 5, optional(uint64))]
    /// Published semantic field.
    pub resolved_revision: Option<u64>,
}

/// Workers Error with Rust-owned semantic fields.
#[acyclic_protify_proc_macro::proto_message(proxied, fallible = DomainError)]
#[proto(file = WORKERS_FILE)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "workers/Error.ts", rename_all = "camelCase")]
pub struct Error {
    #[proto(tag = 1, enum_(ErrorCode), from_proto = parse_errorcode, into_proto = encode_errorcode)]
    /// Published semantic field.
    pub code: ErrorCode,
    #[proto(tag = 2)]
    /// Published semantic field.
    pub message: String,
}

/// Export semantic types through maintained ts-rs dependency traversal.
pub fn export_typescript(path: impl AsRef<std::path::Path>) -> Result<(), ExportError> {
    let config = Config::default()
        .with_out_dir(path.as_ref())
        .with_import_extension(Some("js"));
    <CodeVersion as TS>::export_all(&config)?;
    <Deployment as TS>::export_all(&config)?;
    <PublishVersionRequest as TS>::export_all(&config)?;
    <PublishVersionResponse as TS>::export_all(&config)?;
    <SelectDeploymentRequest as TS>::export_all(&config)?;
    <SelectDeploymentResponse as TS>::export_all(&config)?;
    <ObjectRef as TS>::export_all(&config)?;
    <Payload as TS>::export_all(&config)?;
    <JobResult as TS>::export_all(&config)?;
    <JobLimits as TS>::export_all(&config)?;
    <RetryPolicy as TS>::export_all(&config)?;
    <JobTarget as TS>::export_all(&config)?;
    <SubmitJobRequest as TS>::export_all(&config)?;
    <SubmitJobResponse as TS>::export_all(&config)?;
    <JobObservation as TS>::export_all(&config)?;
    <InspectJobRequest as TS>::export_all(&config)?;
    <InspectJobResponse as TS>::export_all(&config)?;
    <CancelJobRequest as TS>::export_all(&config)?;
    <CancelJobResponse as TS>::export_all(&config)?;
    <Header as TS>::export_all(&config)?;
    <InvokeVersionRequest as TS>::export_all(&config)?;
    <InvokeDeploymentRequest as TS>::export_all(&config)?;
    <InvokeResponse as TS>::export_all(&config)?;
    <Error as TS>::export_all(&config)?;
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
    Ok(())
}
