//! Rust-owned Workers v1 contract metadata.
//!
//! The checked-in protobuf is a one-time migration and compatibility oracle;
//! normal descriptor, OpenAPI, SDK, and documentation generation consumes the
//! structured model below. Field numbers, JSON names, presence, oneofs,
//! reserved identities, and RPC identities are explicit in Rust.

use prost::Message;

use super::{
    Cardinality, ContractSpec, EnumSpec, EnumValueSpec, FieldSpec, FieldType, MessageSpec,
    MethodSpec, OneofSpec, RouteSpec, ServiceSpec,
};

const fn field(
    name: &'static str,
    number: u32,
    cardinality: Cardinality,
    field_type: FieldType,
    json_name: &'static str,
) -> FieldSpec {
    super::field(name, number, cardinality, field_type, json_name)
}

const fn optional(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    super::optional_field(name, number, field_type, json_name, oneof)
}

const fn oneof_field(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    super::oneof_field(name, number, field_type, json_name, oneof)
}

const fn oneof(name: &'static str, synthetic: bool) -> OneofSpec {
    super::oneof(name, synthetic)
}

macro_rules! message {
    ($name:ident, $wire_name:literal, [$($field:expr),* $(,)?], [$($oneof:expr),* $(,)?], [$($range:expr),* $(,)?], [$($reserved:expr),* $(,)?]) => {
        #[allow(non_upper_case_globals)]
        const $name: MessageSpec = MessageSpec {
            name: $wire_name,
            fields: &[$($field),*],
            oneofs: &[$($oneof),*],
            nested_messages: &[],
            is_map_entry: false,
            reserved_ranges: &[$($range),*],
            reserved_names: &[$($reserved),*],
        };
    };
}

message!(
    CodeVersion,
    "CodeVersion",
    [
        field(
            "sha256",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "sha256"
        ),
        field(
            "size_bytes",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "sizeBytes"
        ),
    ],
    [],
    [],
    []
);
message!(
    Deployment,
    "Deployment",
    [
        field(
            "alias",
            1,
            Cardinality::Singular,
            FieldType::String,
            "alias"
        ),
        field(
            "version",
            2,
            Cardinality::Singular,
            FieldType::Message("CodeVersion"),
            "version"
        ),
        field(
            "revision",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "revision"
        ),
    ],
    [],
    [],
    []
);
message!(
    PublishVersionRequest,
    "PublishVersionRequest",
    [
        field(
            "javascript_module",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "javascriptModule"
        ),
        field(
            "expected_sha256",
            2,
            Cardinality::Singular,
            FieldType::Bytes,
            "expectedSha256"
        ),
        field(
            "idempotency_key",
            3,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    [],
    [],
    []
);
message!(
    PublishVersionResponse,
    "PublishVersionResponse",
    [field(
        "version",
        1,
        Cardinality::Singular,
        FieldType::Message("CodeVersion"),
        "version"
    )],
    [],
    [],
    []
);
message!(
    SelectDeploymentRequest,
    "SelectDeploymentRequest",
    [
        field(
            "alias",
            1,
            Cardinality::Singular,
            FieldType::String,
            "alias"
        ),
        field(
            "version_sha256",
            2,
            Cardinality::Singular,
            FieldType::Bytes,
            "versionSha256"
        ),
        optional(
            "expected_revision",
            3,
            FieldType::Uint64,
            "expectedRevision",
            "_expected_revision"
        ),
        field(
            "idempotency_key",
            4,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    [oneof("_expected_revision", true)],
    [],
    []
);
message!(
    SelectDeploymentResponse,
    "SelectDeploymentResponse",
    [field(
        "deployment",
        1,
        Cardinality::Singular,
        FieldType::Message("Deployment"),
        "deployment"
    )],
    [],
    [],
    []
);
message!(
    ObjectRef,
    "ObjectRef",
    [
        field(
            "bucket",
            1,
            Cardinality::Singular,
            FieldType::String,
            "bucket"
        ),
        field("key", 2, Cardinality::Singular, FieldType::String, "key"),
    ],
    [],
    [(3, 3)],
    ["version_id"]
);
message!(
    Payload,
    "Payload",
    [
        oneof_field("inline_bytes", 1, FieldType::Bytes, "inlineBytes", "source"),
        oneof_field(
            "object",
            2,
            FieldType::Message("ObjectRef"),
            "object",
            "source"
        ),
    ],
    [oneof("source", false)],
    [],
    []
);
message!(
    JobResult,
    "JobResult",
    [field(
        "body",
        1,
        Cardinality::Singular,
        FieldType::Bytes,
        "body"
    )],
    [],
    [(2, 2)],
    ["object_version"]
);
message!(
    JobLimits,
    "JobLimits",
    [
        field(
            "timeout_millis",
            1,
            Cardinality::Singular,
            FieldType::Uint64,
            "timeoutMillis"
        ),
        field(
            "memory_bytes",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "memoryBytes"
        ),
        field(
            "output_bytes",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "outputBytes"
        ),
    ],
    [],
    [],
    []
);
message!(
    RetryPolicy,
    "RetryPolicy",
    [
        field(
            "max_attempts",
            1,
            Cardinality::Singular,
            FieldType::Uint32,
            "maxAttempts"
        ),
        field(
            "backoff_millis",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "backoffMillis"
        ),
    ],
    [],
    [],
    []
);
message!(
    JobTarget,
    "JobTarget",
    [
        oneof_field(
            "deployment_alias",
            1,
            FieldType::String,
            "deploymentAlias",
            "target"
        ),
        oneof_field(
            "version_sha256",
            2,
            FieldType::Bytes,
            "versionSha256",
            "target"
        ),
    ],
    [oneof("target", false)],
    [],
    []
);
message!(
    SubmitJobRequest,
    "SubmitJobRequest",
    [
        field(
            "target",
            1,
            Cardinality::Singular,
            FieldType::Message("JobTarget"),
            "target"
        ),
        field(
            "input",
            2,
            Cardinality::Singular,
            FieldType::Message("Payload"),
            "input"
        ),
        field(
            "limits",
            3,
            Cardinality::Singular,
            FieldType::Message("JobLimits"),
            "limits"
        ),
        field(
            "retry",
            4,
            Cardinality::Singular,
            FieldType::Message("RetryPolicy"),
            "retry"
        ),
        field(
            "idempotency_key",
            5,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    [],
    [],
    []
);
message!(
    SubmitJobResponse,
    "SubmitJobResponse",
    [field(
        "job",
        1,
        Cardinality::Singular,
        FieldType::Message("JobObservation"),
        "job"
    )],
    [],
    [],
    []
);
message!(
    JobObservation,
    "JobObservation",
    [
        field(
            "job_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "jobId"
        ),
        field(
            "state",
            2,
            Cardinality::Singular,
            FieldType::Enum("JobState"),
            "state"
        ),
        field(
            "resolved_sha256",
            3,
            Cardinality::Singular,
            FieldType::Bytes,
            "resolvedSha256"
        ),
        field(
            "attempt",
            4,
            Cardinality::Singular,
            FieldType::Uint32,
            "attempt"
        ),
        field(
            "result",
            5,
            Cardinality::Singular,
            FieldType::Message("JobResult"),
            "result"
        ),
        field(
            "failure_code",
            6,
            Cardinality::Singular,
            FieldType::String,
            "failureCode"
        ),
        field(
            "cancellation_requested",
            7,
            Cardinality::Singular,
            FieldType::Bool,
            "cancellationRequested"
        ),
    ],
    [],
    [],
    []
);
message!(
    InspectJobRequest,
    "InspectJobRequest",
    [field(
        "job_id",
        1,
        Cardinality::Singular,
        FieldType::String,
        "jobId"
    )],
    [],
    [],
    []
);
message!(
    InspectJobResponse,
    "InspectJobResponse",
    [field(
        "job",
        1,
        Cardinality::Singular,
        FieldType::Message("JobObservation"),
        "job"
    )],
    [],
    [],
    []
);
message!(
    CancelJobRequest,
    "CancelJobRequest",
    [
        field(
            "job_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "jobId"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    [],
    [],
    []
);
message!(
    CancelJobResponse,
    "CancelJobResponse",
    [field(
        "job",
        1,
        Cardinality::Singular,
        FieldType::Message("JobObservation"),
        "job"
    )],
    [],
    [],
    []
);
message!(
    Header,
    "Header",
    [
        field("name", 1, Cardinality::Singular, FieldType::String, "name"),
        field(
            "value",
            2,
            Cardinality::Singular,
            FieldType::String,
            "value"
        ),
    ],
    [],
    [],
    []
);
message!(
    InvokeVersionRequest,
    "InvokeVersionRequest",
    [
        field(
            "version_sha256",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "versionSha256"
        ),
        field(
            "method",
            2,
            Cardinality::Singular,
            FieldType::String,
            "method"
        ),
        field("url", 3, Cardinality::Singular, FieldType::String, "url"),
        field(
            "headers",
            4,
            Cardinality::Repeated,
            FieldType::Message("Header"),
            "headers"
        ),
        field("body", 5, Cardinality::Singular, FieldType::Bytes, "body"),
    ],
    [],
    [],
    []
);
message!(
    InvokeDeploymentRequest,
    "InvokeDeploymentRequest",
    [
        field(
            "alias",
            1,
            Cardinality::Singular,
            FieldType::String,
            "alias"
        ),
        field(
            "method",
            2,
            Cardinality::Singular,
            FieldType::String,
            "method"
        ),
        field("url", 3, Cardinality::Singular, FieldType::String, "url"),
        field(
            "headers",
            4,
            Cardinality::Repeated,
            FieldType::Message("Header"),
            "headers"
        ),
        field("body", 5, Cardinality::Singular, FieldType::Bytes, "body"),
    ],
    [],
    [],
    []
);
message!(
    InvokeResponse,
    "InvokeResponse",
    [
        field(
            "status",
            1,
            Cardinality::Singular,
            FieldType::Uint32,
            "status"
        ),
        field(
            "headers",
            2,
            Cardinality::Repeated,
            FieldType::Message("Header"),
            "headers"
        ),
        field("body", 3, Cardinality::Singular, FieldType::Bytes, "body"),
        field(
            "resolved_sha256",
            4,
            Cardinality::Singular,
            FieldType::Bytes,
            "resolvedSha256"
        ),
        optional(
            "resolved_revision",
            5,
            FieldType::Uint64,
            "resolvedRevision",
            "_resolved_revision"
        ),
    ],
    [oneof("_resolved_revision", true)],
    [],
    []
);
message!(
    Error,
    "Error",
    [
        field(
            "code",
            1,
            Cardinality::Singular,
            FieldType::Enum("ErrorCode"),
            "code"
        ),
        field(
            "message",
            2,
            Cardinality::Singular,
            FieldType::String,
            "message"
        ),
    ],
    [],
    [],
    []
);

pub const JOB_STATE: EnumSpec = EnumSpec {
    name: "JobState",
    values: &[
        EnumValueSpec {
            name: "JOB_STATE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "JOB_STATE_ACCEPTED",
            number: 1,
        },
        EnumValueSpec {
            name: "JOB_STATE_RUNNING",
            number: 2,
        },
        EnumValueSpec {
            name: "JOB_STATE_SUCCEEDED",
            number: 3,
        },
        EnumValueSpec {
            name: "JOB_STATE_FAILED",
            number: 4,
        },
        EnumValueSpec {
            name: "JOB_STATE_CANCELLED",
            number: 5,
        },
    ],
};

pub const ERROR_CODE: EnumSpec = EnumSpec {
    name: "ErrorCode",
    values: &[
        EnumValueSpec {
            name: "ERROR_CODE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "ERROR_CODE_INVALID_ARGUMENT",
            number: 1,
        },
        EnumValueSpec {
            name: "ERROR_CODE_CAPABILITY_DENIED",
            number: 2,
        },
        EnumValueSpec {
            name: "ERROR_CODE_CAPABILITY_EXPIRED",
            number: 3,
        },
        EnumValueSpec {
            name: "ERROR_CODE_VERSION_NOT_FOUND",
            number: 4,
        },
        EnumValueSpec {
            name: "ERROR_CODE_DEPLOYMENT_NOT_FOUND",
            number: 5,
        },
        EnumValueSpec {
            name: "ERROR_CODE_JOB_NOT_FOUND",
            number: 6,
        },
        EnumValueSpec {
            name: "ERROR_CODE_IDEMPOTENCY_MISMATCH",
            number: 7,
        },
        EnumValueSpec {
            name: "ERROR_CODE_REVISION_CONFLICT",
            number: 8,
        },
        EnumValueSpec {
            name: "ERROR_CODE_OVERLOADED",
            number: 9,
        },
        EnumValueSpec {
            name: "ERROR_CODE_TERMINAL_JOB_FAILURE",
            number: 10,
        },
    ],
};

pub const WORKERS_SERVICE: ServiceSpec = ServiceSpec {
    name: "WorkersService",
    methods: &[
        MethodSpec {
            name: "PublishVersion",
            input: "PublishVersionRequest",
            output: "PublishVersionResponse",
            docs: "Publishes an immutable JavaScript module version.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "SelectDeployment",
            input: "SelectDeploymentRequest",
            output: "SelectDeploymentResponse",
            docs: "Selects a version for a deployment alias with revision compare-and-swap.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "SubmitJob",
            input: "SubmitJobRequest",
            output: "SubmitJobResponse",
            docs: "Accepts durable input and returns the accepted job.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InspectJob",
            input: "InspectJobRequest",
            output: "InspectJobResponse",
            docs: "Returns the current durable job observation.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "CancelJob",
            input: "CancelJobRequest",
            output: "CancelJobResponse",
            docs: "Requests cancellation of a durable job.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InvokeVersion",
            input: "InvokeVersionRequest",
            output: "InvokeResponse",
            docs: "Invokes an immutable Worker version as ordinary HTTP work.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InvokeDeployment",
            input: "InvokeDeploymentRequest",
            output: "InvokeResponse",
            docs: "Invokes the version selected by a deployment alias.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

/// Rust-owned HTTP projection for the hosted Workers transport.
pub const WORKERS_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        method: "POST",
        path: "/v1/workers/versions/publish",
        operation_id: "publishVersion",
        rpc: "acyclic.workers.v1.WorkersService/PublishVersion",
        request: "PublishVersionRequest",
        response: "PublishVersionResponse",
        docs: "Publishes an immutable JavaScript module version.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/workers/deployments/select",
        operation_id: "selectDeployment",
        rpc: "acyclic.workers.v1.WorkersService/SelectDeployment",
        request: "SelectDeploymentRequest",
        response: "SelectDeploymentResponse",
        docs: "Selects a version for a deployment alias with revision compare-and-swap.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/workers/jobs/submit",
        operation_id: "submitJob",
        rpc: "acyclic.workers.v1.WorkersService/SubmitJob",
        request: "SubmitJobRequest",
        response: "SubmitJobResponse",
        docs: "Accepts durable input and returns the accepted job.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/workers/jobs/inspect",
        operation_id: "inspectJob",
        rpc: "acyclic.workers.v1.WorkersService/InspectJob",
        request: "InspectJobRequest",
        response: "InspectJobResponse",
        docs: "Returns the current durable job observation.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/workers/jobs/cancel",
        operation_id: "cancelJob",
        rpc: "acyclic.workers.v1.WorkersService/CancelJob",
        request: "CancelJobRequest",
        response: "CancelJobResponse",
        docs: "Requests cancellation of a durable job.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/workers/versions/{sha256hex}/invoke",
        operation_id: "invokeVersion",
        rpc: "acyclic.workers.v1.WorkersService/InvokeVersion",
        request: "InvokeVersionRequest",
        response: "InvokeResponse",
        docs: "Invokes an immutable Worker version as ordinary HTTP work.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/workers/deployments/{alias}/invoke",
        operation_id: "invokeDeployment",
        rpc: "acyclic.workers.v1.WorkersService/InvokeDeployment",
        request: "InvokeDeploymentRequest",
        response: "InvokeResponse",
        docs: "Invokes the version selected by a deployment alias.",
    },
];

pub const WORKERS: ContractSpec = ContractSpec {
    file_name: "workers/v1/workers.proto",
    syntax: "proto3",
    package: "acyclic.workers.v1",
    dependencies: &[],
    options: super::FileOptionsSpec {
        go_package: "github.com/acyclic-labs/sdk/go/gen/workers/v1;workersv1",
    },
    messages: &[
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
    enums: &[JOB_STATE, ERROR_CODE],
    services: &[WORKERS_SERVICE],
    routes: WORKERS_ROUTES,
};

/// Documentation owned by the Workers model and consumed by exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkersDoc {
    pub name: &'static str,
    pub text: &'static str,
}

pub const WORKERS_SERVICE_DOC: WorkersDoc = WorkersDoc {
    name: "WorkersService",
    text: "Publishes immutable Worker versions, selects deployments, and executes jobs.",
};

pub const WORKERS_MESSAGE_DOCS: &[WorkersDoc] = &[
    WorkersDoc {
        name: "CodeVersion",
        text: "An immutable JavaScript module identified by its SHA-256 digest.",
    },
    WorkersDoc {
        name: "Deployment",
        text: "A mutable alias selection with a monotonic revision.",
    },
    WorkersDoc {
        name: "PublishVersionRequest",
        text: "Publishes exact JavaScript module bytes after optional digest validation.",
    },
    WorkersDoc {
        name: "PublishVersionResponse",
        text: "The immutable version identity created by publication.",
    },
    WorkersDoc {
        name: "SelectDeploymentRequest",
        text: "Selects an immutable version for an alias with optional compare-and-swap revision.",
    },
    WorkersDoc {
        name: "SelectDeploymentResponse",
        text: "The deployment alias and selected version after a successful selection.",
    },
    WorkersDoc {
        name: "ObjectRef",
        text: "A logical S3 object retained privately as durable job input.",
    },
    WorkersDoc {
        name: "Payload",
        text: "Job input supplied inline or by a retained object reference.",
    },
    WorkersDoc {
        name: "JobResult",
        text: "The exact accepted job output, bounded by the job limits.",
    },
    WorkersDoc {
        name: "JobLimits",
        text: "Execution and output bounds applied to a durable job.",
    },
    WorkersDoc {
        name: "RetryPolicy",
        text: "The bounded retry schedule for a durable job.",
    },
    WorkersDoc {
        name: "JobTarget",
        text: "The deployment alias or immutable version that receives a job.",
    },
    WorkersDoc {
        name: "SubmitJobRequest",
        text: "Accepts durable input for execution by the selected Worker version.",
    },
    WorkersDoc {
        name: "SubmitJobResponse",
        text: "The accepted job observation.",
    },
    WorkersDoc {
        name: "JobObservation",
        text: "The current durable job state, attempt, output, and failure details.",
    },
    WorkersDoc {
        name: "InspectJobRequest",
        text: "Identifies a durable job to inspect.",
    },
    WorkersDoc {
        name: "InspectJobResponse",
        text: "The current observation for a durable job.",
    },
    WorkersDoc {
        name: "CancelJobRequest",
        text: "Requests cancellation of a durable job.",
    },
    WorkersDoc {
        name: "CancelJobResponse",
        text: "The job observation after a cancellation request.",
    },
    WorkersDoc {
        name: "Header",
        text: "An HTTP header forwarded to or returned from a Worker invocation.",
    },
    WorkersDoc {
        name: "InvokeVersionRequest",
        text: "Invokes an immutable Worker version as ordinary HTTP work.",
    },
    WorkersDoc {
        name: "InvokeDeploymentRequest",
        text: "Invokes the version currently selected by a deployment alias.",
    },
    WorkersDoc {
        name: "InvokeResponse",
        text: "The ordinary HTTP response and the resolved Worker identity.",
    },
    WorkersDoc {
        name: "Error",
        text: "A typed Workers service error.",
    },
];

pub const WORKERS_ENUM_DOCS: &[WorkersDoc] = &[
    WorkersDoc {
        name: "JobState",
        text: "Lifecycle state of a durable Worker job.",
    },
    WorkersDoc {
        name: "ErrorCode",
        text: "Stable error categories returned by the Workers service.",
    },
];

pub const WORKERS_METHOD_DOCS: &[WorkersDoc] = &[
    WorkersDoc {
        name: "PublishVersion",
        text: "Publishes an immutable JavaScript module version.",
    },
    WorkersDoc {
        name: "SelectDeployment",
        text: "Selects a version for a deployment alias with revision compare-and-swap.",
    },
    WorkersDoc {
        name: "SubmitJob",
        text: "Accepts durable input and returns the accepted job.",
    },
    WorkersDoc {
        name: "InspectJob",
        text: "Returns the current durable job observation.",
    },
    WorkersDoc {
        name: "CancelJob",
        text: "Requests cancellation of a durable job.",
    },
    WorkersDoc {
        name: "InvokeVersion",
        text: "Invokes an immutable Worker version as ordinary HTTP work.",
    },
    WorkersDoc {
        name: "InvokeDeployment",
        text: "Invokes the version selected by a deployment alias.",
    },
];

pub const WORKERS_FIELD_DOCS: &[WorkersDoc] = &[
    WorkersDoc {
        name: "job",
        text: "Accepted durable job observation.",
    },
    WorkersDoc {
        name: "deployment",
        text: "Deployment alias and selected version after a successful selection.",
    },
    WorkersDoc {
        name: "sha256",
        text: "SHA-256 digest of immutable module bytes.",
    },
    WorkersDoc {
        name: "size_bytes",
        text: "Size of the immutable module in bytes.",
    },
    WorkersDoc {
        name: "alias",
        text: "Mutable deployment alias.",
    },
    WorkersDoc {
        name: "version",
        text: "Immutable version selected by the deployment.",
    },
    WorkersDoc {
        name: "revision",
        text: "Monotonic deployment selection revision.",
    },
    WorkersDoc {
        name: "javascript_module",
        text: "Exact JavaScript module bytes to publish.",
    },
    WorkersDoc {
        name: "expected_sha256",
        text: "Optional digest that must match the published bytes.",
    },
    WorkersDoc {
        name: "idempotency_key",
        text: "Client key used to make a mutation idempotent.",
    },
    WorkersDoc {
        name: "version_sha256",
        text: "Immutable version digest.",
    },
    WorkersDoc {
        name: "expected_revision",
        text: "Optional current deployment revision required for selection.",
    },
    WorkersDoc {
        name: "bucket",
        text: "Private object-store bucket for retained input.",
    },
    WorkersDoc {
        name: "key",
        text: "Private object-store key for retained input.",
    },
    WorkersDoc {
        name: "inline_bytes",
        text: "Input bytes retained directly with the job.",
    },
    WorkersDoc {
        name: "object",
        text: "Input object retained at durable job acceptance.",
    },
    WorkersDoc {
        name: "body",
        text: "Exact job output or ordinary invocation body.",
    },
    WorkersDoc {
        name: "timeout_millis",
        text: "Maximum Worker execution time.",
    },
    WorkersDoc {
        name: "memory_bytes",
        text: "Maximum Worker memory use.",
    },
    WorkersDoc {
        name: "output_bytes",
        text: "Maximum durable job output size.",
    },
    WorkersDoc {
        name: "max_attempts",
        text: "Maximum number of delivery attempts.",
    },
    WorkersDoc {
        name: "backoff_millis",
        text: "Delay between retry attempts.",
    },
    WorkersDoc {
        name: "deployment_alias",
        text: "Deployment alias selected for execution.",
    },
    WorkersDoc {
        name: "target",
        text: "Version or deployment target for the job.",
    },
    WorkersDoc {
        name: "input",
        text: "Input retained for the durable job.",
    },
    WorkersDoc {
        name: "limits",
        text: "Execution and output limits.",
    },
    WorkersDoc {
        name: "retry",
        text: "Retry policy applied to the job.",
    },
    WorkersDoc {
        name: "job_id",
        text: "Stable durable job identifier.",
    },
    WorkersDoc {
        name: "state",
        text: "Current durable job state.",
    },
    WorkersDoc {
        name: "resolved_sha256",
        text: "Digest of the version resolved for execution.",
    },
    WorkersDoc {
        name: "attempt",
        text: "Current delivery attempt, starting at one.",
    },
    WorkersDoc {
        name: "result",
        text: "Exact accepted job output after success.",
    },
    WorkersDoc {
        name: "failure_code",
        text: "Stable terminal failure code.",
    },
    WorkersDoc {
        name: "cancellation_requested",
        text: "Whether cancellation has been requested.",
    },
    WorkersDoc {
        name: "name",
        text: "HTTP header name.",
    },
    WorkersDoc {
        name: "value",
        text: "HTTP header value.",
    },
    WorkersDoc {
        name: "method",
        text: "HTTP method passed to the Worker.",
    },
    WorkersDoc {
        name: "url",
        text: "URL passed to the Worker.",
    },
    WorkersDoc {
        name: "headers",
        text: "Headers forwarded to or returned from the Worker.",
    },
    WorkersDoc {
        name: "status",
        text: "HTTP status returned by the Worker.",
    },
    WorkersDoc {
        name: "resolved_revision",
        text: "Optional deployment revision resolved for invocation.",
    },
    WorkersDoc {
        name: "code",
        text: "Typed Workers error code.",
    },
    WorkersDoc {
        name: "message",
        text: "Human-readable error detail.",
    },
];

pub fn workers_descriptor() -> Vec<u8> {
    WORKERS.descriptor_set().encode_to_vec()
}

pub fn workers_proto() -> String {
    WORKERS.render_proto()
}
