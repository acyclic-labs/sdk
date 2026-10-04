//! Rust-owned metadata for the Actors v1 wire contract.
//!
//! This is deliberately a structured model rather than generated Rust or an
//! embedded `.proto` document. The model emits both a canonical descriptor set
//! and source text; the checked-in protobuf is retained only as a compatibility
//! fixture for tests.

use acyclic_sdk_contract_options::RawOptions;
use prost::Message;

pub mod bindings;
pub mod credential;
pub mod embedded_facades;
pub mod facades;
pub mod family_registry;
pub mod filesystem;
pub mod harness;
pub mod inference;
pub mod machines;
pub mod objects;
pub mod protocol;
pub mod semantic_oracle;
pub mod stream;
pub mod type_policy;
pub mod transport;
pub mod transport_control;
pub mod wire_semantics;
pub mod workers;

pub use bindings::{
    BindingFamily, BindingGenerationError, BindingOutput, BindingTransport, NativeBindingBoundary,
    ProductBindingConfig, descriptor_set_with_docs, generate_product_bindings,
    generate_rust_bindings, native_binding_boundary,
};
pub use credential::{BEARER_NO_CRLF, CredentialPolicy};
pub use embedded_facades::{EmbeddedFacadeOutput, generate_embedded_facades};

pub use facades::{
    CancellationKind, FACADE_SELECTION_POLICY, FacadeLanguage, FacadeOperationPolicy, FacadeOutput,
    FacadeSelectionPolicy, all_facade_operations, facade_operations, generate_remote_facade,
    generate_remote_facades,
};
pub use family_registry::{
    FAMILY_VIEWS, FamilyModel, FamilyView, HttpProjection, NativeMethodBoundary,
    explicit_http_family_views, family_view, native_method_boundaries_for_family,
};
pub use filesystem::{
    FILESYSTEM, FILESYSTEM_OPERATION_POLICIES, FILESYSTEM_SERVICE_AVAILABILITY,
    ServiceAvailability as FilesystemServiceAvailability, filesystem_descriptor, filesystem_proto,
};
pub use harness::{
    HARNESS, HARNESS_OPERATION_POLICIES, HARNESS_SERVICE_AVAILABILITY,
    ServiceAvailability as HarnessServiceAvailability, harness_descriptor, harness_proto,
};
pub use inference::{INFERENCE, inference_descriptor, inference_proto};
pub use machines::{MACHINES, machines_descriptor, machines_proto};
pub use objects::{OBJECTS_V2, objects_descriptor, objects_proto};
pub use stream::{STREAM, STREAM_ROUTES, STREAM_SERVICE, stream_descriptor, stream_proto};
pub use type_policy::{
    FIELD_SEMANTIC_TYPES, SEMANTIC_TYPES, TYPE_PROJECTION_PROFILES, FieldSemanticType,
    SemanticRule, SemanticType, TypePolicyLanguage, TypeProjectionProfile,
    WireValueKind, field_semantic_type, semantic_type, type_projection_profile,
};
pub use transport::{
    ClientRuntime, FamilyTransportPolicy, RuntimeTransportPolicy, TransportAvailability,
    TransportKind, TransportOption, TransportRequirements, TransportSelection,
    TransportSelectionError, TransportSelectionRequest, select_transport, select_transport_by_name,
};
pub use wire_semantics::{
    compare_family_rpc_message, compare_message, compare_message_with_options, family_rpc_streaming,
    compare_rpc_message, compare_rpc_message_with_options, CompareOptions, FloatPolicy,
    rpc_streaming, RpcDirection, RpcSemanticError, RpcStreaming, SemanticMismatch,
    UnknownFieldPolicy,
};
pub use workers::{WORKERS, WORKERS_ROUTES, WORKERS_SERVICE, workers_descriptor, workers_proto};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    Singular,
    Optional,
    Repeated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    String,
    Bytes,
    Bool,
    Int64,
    Sint64,
    Uint32,
    Uint64,
    Message(&'static str),
    /// A message declared by an imported protobuf file.
    ExternalMessage(&'static str),
    Enum(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapSpec {
    pub key: FieldType,
    pub value: FieldType,
    pub entry_name: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSpec {
    pub name: &'static str,
    pub number: u32,
    pub cardinality: Cardinality,
    pub field_type: FieldType,
    pub json_name: &'static str,
    pub oneof: Option<&'static str>,
    pub proto3_optional: bool,
    pub map: Option<MapSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OneofSpec {
    pub name: &'static str,
    pub synthetic: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageSpec {
    pub name: &'static str,
    pub fields: &'static [FieldSpec],
    pub oneofs: &'static [OneofSpec],
    pub nested_messages: &'static [MessageSpec],
    pub is_map_entry: bool,
    /// Inclusive reserved field-number ranges retained for wire evolution.
    pub reserved_ranges: &'static [(u32, u32)],
    pub reserved_names: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnumValueSpec {
    pub name: &'static str,
    pub number: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnumSpec {
    pub name: &'static str,
    pub values: &'static [EnumValueSpec],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MethodSpec {
    pub name: &'static str,
    pub input: &'static str,
    pub output: &'static str,
    /// Rust-owned operation prose rendered into protobuf and documentation projections.
    pub docs: &'static str,
    pub client_streaming: bool,
    pub server_streaming: bool,
}

/// A hosted HTTP projection of one RPC operation.
///
/// Routes are deliberately kept separate from protobuf descriptors: the route
/// path and JSON transport are projections, while `rpc`, `request`, and
/// `response` retain an explicit link to the wire identity. Generators must
/// consume this table instead of reconstructing paths from handwritten maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub operation_id: &'static str,
    pub rpc: &'static str,
    pub request: &'static str,
    pub response: &'static str,
    pub docs: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceSpec {
    pub name: &'static str,
    pub methods: &'static [MethodSpec],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileOptionsSpec {
    pub go_package: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractSpec {
    pub file_name: &'static str,
    pub syntax: &'static str,
    pub package: &'static str,
    pub dependencies: &'static [&'static str],
    pub options: FileOptionsSpec,
    pub messages: &'static [MessageSpec],
    pub enums: &'static [EnumSpec],
    pub services: &'static [ServiceSpec],
    /// Hosted HTTP routes projected from this contract, if any.
    pub routes: &'static [RouteSpec],
}

/// Rust-owned behavior metadata projected alongside a wire operation.
///
/// These policy names are deliberately separate from the protobuf descriptor:
/// they let SDK and HTTP generators preserve capability, error, and validation
/// semantics without inferring them from streaming flags or route spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationPolicy {
    pub rpc: &'static str,
    pub capabilities: &'static [&'static str],
    pub errors: &'static [&'static str],
    pub validations: &'static [&'static str],
}

const ACTOR_MUTATION_CAPABILITIES: &[&str] = &["actors.write", "actors.idempotent_mutation"];
const ACTOR_READ_CAPABILITIES: &[&str] = &["actors.read"];
const ACTOR_INVOKE_CAPABILITIES: &[&str] = &["actors.invoke"];
const ACTOR_ERRORS: &[&str] = &[
    "ERROR_CODE_UNSPECIFIED",
    "ERROR_CODE_INVALID_ARGUMENT",
    "ERROR_CODE_CAPABILITY_DENIED",
    "ERROR_CODE_CAPABILITY_EXPIRED",
    "ERROR_CODE_ACTOR_NOT_FOUND",
    "ERROR_CODE_SUBSCRIPTION_NOT_FOUND",
    "ERROR_CODE_IDEMPOTENCY_MISMATCH",
    "ERROR_CODE_CONFLICT",
    "ERROR_CODE_ADMISSION_DENIED",
    "ERROR_CODE_CHECKPOINT_FAILED",
    "ERROR_CODE_DEPENDENCY_UNAVAILABLE",
];
const ACTOR_IDEMPOTENCY_VALIDATIONS: &[&str] = &["idempotency_key.non_empty_utf8"];

pub(crate) const ACTOR_POLICIES: &[OperationPolicy] = &[
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/CreateActor",
        capabilities: ACTOR_MUTATION_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: ACTOR_IDEMPOTENCY_VALIDATIONS,
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/UpdateActor",
        capabilities: ACTOR_MUTATION_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: &[
            "actor_id.non_empty_utf8",
            "expected_configuration_revision.non_negative",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/InspectActor",
        capabilities: ACTOR_READ_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: &["actor_id.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/AddSubscription",
        capabilities: ACTOR_MUTATION_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: ACTOR_IDEMPOTENCY_VALIDATIONS,
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/RemoveSubscription",
        capabilities: ACTOR_MUTATION_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: ACTOR_IDEMPOTENCY_VALIDATIONS,
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/ResumeSubscription",
        capabilities: ACTOR_MUTATION_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: ACTOR_IDEMPOTENCY_VALIDATIONS,
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/CheckpointActor",
        capabilities: ACTOR_MUTATION_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: &["actor_id.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.actors.v1.ActorsService/InvokeActor",
        capabilities: ACTOR_INVOKE_CAPABILITIES,
        errors: ACTOR_ERRORS,
        validations: &["actor_id.non_empty_utf8", "method.non_empty_utf8"],
    },
];

const STREAM_ERRORS: &[&str] = &[
    "INVALID_ARGUMENT",
    "TAIL_CONFLICT",
    "COMMIT_CONFLICT",
    "IDEMPOTENCY_MISMATCH",
];
const STREAM_WRITE_CAPABILITIES: &[&str] = &["stream.write"];
const STREAM_READ_CAPABILITIES: &[&str] = &["stream.read"];
pub(crate) const STREAM_POLICIES: &[OperationPolicy] = &[
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/InspectIdempotency",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["idempotency_key.non_empty_bytes"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Append",
        capabilities: STREAM_WRITE_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["path.non_empty_utf8", "records.max_bytes"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Tail",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["path.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Fork",
        capabilities: STREAM_WRITE_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["source.non_empty_utf8", "destination.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Read",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["path.non_empty_utf8", "limit.max_stream_items"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Follow",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["path.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Children",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["limit.max_stream_items"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/ChildrenPage",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["limit.max_stream_items"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/Commit",
        capabilities: STREAM_WRITE_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["mutations.max_command_bytes"],
    },
    OperationPolicy {
        rpc: "acyclic.stream.v2.StreamService/ReadCommit",
        capabilities: STREAM_READ_CAPABILITIES,
        errors: STREAM_ERRORS,
        validations: &["commit_id.non_empty_bytes"],
    },
];

const WORKER_ERRORS: &[&str] = &[
    "INVALID_ARGUMENT",
    "VERSION_NOT_FOUND",
    "DEPLOYMENT_NOT_FOUND",
    "JOB_NOT_FOUND",
    "REVISION_CONFLICT",
    "TERMINAL_JOB_FAILURE",
];
const WORKER_MUTATION_CAPABILITIES: &[&str] = &["workers.write", "workers.idempotent_mutation"];
const WORKER_READ_CAPABILITIES: &[&str] = &["workers.read"];
const WORKER_INVOKE_CAPABILITIES: &[&str] = &["workers.invoke"];
pub(crate) const WORKER_POLICIES: &[OperationPolicy] = &[
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/PublishVersion",
        capabilities: WORKER_MUTATION_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &[
            "javascript_module.non_empty_bytes",
            "expected_sha256.length_32",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/SelectDeployment",
        capabilities: WORKER_MUTATION_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &["alias.non_empty_utf8", "version_sha256.length_32"],
    },
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/SubmitJob",
        capabilities: WORKER_MUTATION_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &["idempotency_key.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/InspectJob",
        capabilities: WORKER_READ_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &["job_id.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/CancelJob",
        capabilities: WORKER_MUTATION_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &["job_id.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/InvokeVersion",
        capabilities: WORKER_INVOKE_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &["version_sha256.length_32", "method.non_empty_utf8"],
    },
    OperationPolicy {
        rpc: "acyclic.workers.v1.WorkersService/InvokeDeployment",
        capabilities: WORKER_INVOKE_CAPABILITIES,
        errors: WORKER_ERRORS,
        validations: &["alias.non_empty_utf8", "method.non_empty_utf8"],
    },
];

const OBJECTS_ERRORS: &[&str] = &[
    "ERROR_CODE_INVALID_ARGUMENT",
    "ERROR_CODE_NOT_FOUND",
    "ERROR_CODE_ALREADY_EXISTS",
    "ERROR_CODE_PRECONDITION_FAILED",
    "ERROR_CODE_IDEMPOTENCY_MISMATCH",
    "ERROR_CODE_QUOTA_EXCEEDED",
    "ERROR_CODE_UNSUPPORTED",
    "ERROR_CODE_UNAVAILABLE",
    "ERROR_CODE_ACCESS_DENIED",
    "ERROR_CODE_RANGE_NOT_SATISFIABLE",
    "ERROR_CODE_NOT_MODIFIED",
];
const OBJECTS_BUCKET_READ_CAPABILITIES: &[&str] = &["objects.bucket.read"];
const OBJECTS_BUCKET_WRITE_CAPABILITIES: &[&str] =
    &["objects.bucket.write", "objects.idempotent_mutation"];
const OBJECTS_OBJECT_READ_CAPABILITIES: &[&str] = &["objects.object.read"];
const OBJECTS_OBJECT_WRITE_CAPABILITIES: &[&str] =
    &["objects.object.write", "objects.idempotent_mutation"];
const OBJECTS_MULTIPART_READ_CAPABILITIES: &[&str] = &["objects.multipart.read"];
const OBJECTS_MULTIPART_WRITE_CAPABILITIES: &[&str] =
    &["objects.multipart.write", "objects.idempotent_mutation"];

pub(crate) const OBJECTS_POLICIES: &[OperationPolicy] = &[
    OperationPolicy {
        rpc: "acyclic.objects.v2.BucketsService/CreateBucket",
        capabilities: OBJECTS_BUCKET_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &["bucket.name.non_empty", "idempotency_key.non_empty"],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.BucketsService/HeadBucket",
        capabilities: OBJECTS_BUCKET_READ_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &["bucket.name.non_empty"],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.BucketsService/DeleteBucket",
        capabilities: OBJECTS_BUCKET_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "bucket.name.non_empty",
            "bucket.empty",
            "idempotency_key.non_empty",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.ObjectsService/PutObject",
        capabilities: OBJECTS_OBJECT_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "bucket.name.non_empty",
            "object.key.non_empty",
            "preconditions.atomic",
            "idempotency_key.non_empty",
            "upload.completion_frame",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.ObjectsService/GetObject",
        capabilities: OBJECTS_OBJECT_READ_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "bucket.name.non_empty",
            "object.key.non_empty",
            "range.valid",
            "response.bounded",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.ObjectsService/HeadObject",
        capabilities: OBJECTS_OBJECT_READ_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &["bucket.name.non_empty", "object.key.non_empty"],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.ObjectsService/DeleteObject",
        capabilities: OBJECTS_OBJECT_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "bucket.name.non_empty",
            "object.key.non_empty",
            "preconditions.atomic",
            "idempotency_key.non_empty",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.ObjectsService/ListObjects",
        capabilities: OBJECTS_OBJECT_READ_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &["bucket.name.non_empty", "pagination.bounded"],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.MultipartService/CreateMultipart",
        capabilities: OBJECTS_MULTIPART_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "bucket.name.non_empty",
            "object.key.non_empty",
            "idempotency_key.non_empty",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.MultipartService/UploadPart",
        capabilities: OBJECTS_MULTIPART_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "upload_id.non_empty",
            "part_number.positive",
            "upload.completion_frame",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.MultipartService/ListParts",
        capabilities: OBJECTS_MULTIPART_READ_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &["upload_id.non_empty", "pagination.bounded"],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.MultipartService/CompleteMultipart",
        capabilities: OBJECTS_MULTIPART_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &[
            "upload_id.non_empty",
            "parts.ordered_exact",
            "idempotency_key.non_empty",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.objects.v2.MultipartService/AbortMultipart",
        capabilities: OBJECTS_MULTIPART_WRITE_CAPABILITIES,
        errors: OBJECTS_ERRORS,
        validations: &["upload_id.non_empty", "idempotency_key.non_empty"],
    },
];

// Inference exposes these categories through its Rust client Error enum. The
// customer service remains responsible for its own backend admission details.
const INFERENCE_ERRORS: &[&str] = &[
    "inference.invalid",
    "inference.transport",
    "inference.observation",
];
const INFERENCE_MODELS_READ_CAPABILITIES: &[&str] = &["inference.models.read"];
const INFERENCE_CONTEXT_READ_CAPABILITIES: &[&str] = &["inference.context.read"];
const INFERENCE_CONTEXT_WRITE_CAPABILITIES: &[&str] =
    &["inference.context.write", "inference.idempotent_mutation"];
const INFERENCE_WARM_READ_CAPABILITIES: &[&str] = &["inference.warm.read"];
const INFERENCE_WARM_WRITE_CAPABILITIES: &[&str] =
    &["inference.warm.write", "inference.idempotent_mutation"];
const INFERENCE_RUN_READ_CAPABILITIES: &[&str] = &["inference.runs.read"];
const INFERENCE_RUN_WRITE_CAPABILITIES: &[&str] =
    &["inference.runs.write", "inference.idempotent_mutation"];
const INFERENCE_EVALUATION_READ_CAPABILITIES: &[&str] = &["inference.evaluations.read"];
const INFERENCE_EVALUATION_WRITE_CAPABILITIES: &[&str] = &[
    "inference.evaluations.write",
    "inference.idempotent_mutation",
];

pub(crate) const INFERENCE_POLICIES: &[OperationPolicy] = &[
    OperationPolicy {
        rpc: "inference.customer.v1.ModelsService/List",
        capabilities: INFERENCE_MODELS_READ_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["model_capabilities.bounded", "retention_profiles.valid"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.ContextsService/Create",
        capabilities: INFERENCE_CONTEXT_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &[
            "request_identity.nonzero",
            "model.non_empty",
            "items.bounded",
            "message.bounded",
        ],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.ContextsService/Inspect",
        capabilities: INFERENCE_CONTEXT_READ_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["revision.length_32"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.ContextsService/Mutate",
        capabilities: INFERENCE_CONTEXT_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &[
            "request_identity.nonzero",
            "source.present",
            "action.present",
            "message.bounded",
        ],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.WarmContextsService/Retain",
        capabilities: INFERENCE_WARM_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &[
            "request_identity.nonzero",
            "context.length_32",
            "policy.valid",
        ],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.WarmContextsService/Inspect",
        capabilities: INFERENCE_WARM_READ_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["commitment.length_32"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.WarmContextsService/Renew",
        capabilities: INFERENCE_WARM_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &[
            "request_identity.nonzero",
            "commitment.length_32",
            "policy.valid",
        ],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.WarmContextsService/Release",
        capabilities: INFERENCE_WARM_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["request_identity.nonzero", "commitment.length_32"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.RunsService/Generate",
        capabilities: INFERENCE_RUN_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &[
            "request_identity.nonzero",
            "context.length_32",
            "maximum_output.positive",
            "message.bounded",
        ],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.RunsService/Inspect",
        capabilities: INFERENCE_RUN_READ_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["run_id.length_16"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.RunsService/Watch",
        capabilities: INFERENCE_RUN_READ_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["run_id.length_16", "cursor.monotonic", "terminal.required"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.RunsService/Cancel",
        capabilities: INFERENCE_RUN_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["run_id.length_16"],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.EvaluationsService/Create",
        capabilities: INFERENCE_EVALUATION_WRITE_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &[
            "request_identity.nonzero",
            "candidates.bounded",
            "cases.bounded",
            "metrics.bounded",
            "spec_digest.length_32",
            "message.bounded",
        ],
    },
    OperationPolicy {
        rpc: "inference.customer.v1.EvaluationsService/Inspect",
        capabilities: INFERENCE_EVALUATION_READ_CAPABILITIES,
        errors: INFERENCE_ERRORS,
        validations: &["evaluation_id.length_16"],
    },
];

// Machines exposes these categories through its ProviderError enum. They are
// local/runtime policy categories, not claims about a hosted backend.
const MACHINES_ERRORS: &[&str] = &[
    "invalid",
    "not_found",
    "conflict",
    "unsupported",
    "rejected",
    "unavailable",
    "operation_indeterminate",
    "operation_observation_indeterminate",
    "operation_failed",
    "operation_cancelled",
];
const MACHINES_QUALIFY_CAPABILITIES: &[&str] = &["machines.qualify"];
const MACHINES_READ_CAPABILITIES: &[&str] = &["machines.read"];
const MACHINES_MUTATION_CAPABILITIES: &[&str] = &["machines.write", "machines.idempotent_mutation"];
const MACHINES_FORK_CAPABILITIES: &[&str] = &["machines.fork", "machines.idempotent_mutation"];
const MACHINES_OPERATION_CAPABILITIES: &[&str] = &["machines.operations"];

pub(crate) const MACHINES_POLICIES: &[OperationPolicy] = &[
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/QualifyImage",
        capabilities: MACHINES_QUALIFY_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["image.immutable_digest", "capabilities.proven"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Create",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["idempotency_key.nonzero", "contract.valid", "limits.valid"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Checkpoint",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "idempotency_key.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Fork",
        capabilities: MACHINES_FORK_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &[
            "checkpoint_id.nonzero",
            "count.bounded",
            "idempotency_key.nonzero",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/ForkMachine",
        capabilities: MACHINES_FORK_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "fidelity.declared", "count.bounded"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Suspend",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "idempotency_key.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Wake",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "idempotency_key.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/SetSuspensionPolicy",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &[
            "machine_id.nonzero",
            "policy.valid",
            "idempotency_key.nonzero",
        ],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/DestroyMachine",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "idempotency_key.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/DestroyCheckpoint",
        capabilities: MACHINES_MUTATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["checkpoint_id.nonzero", "idempotency_key.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Recover",
        capabilities: MACHINES_OPERATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["idempotency_key.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/InspectMachine",
        capabilities: MACHINES_READ_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/InspectCheckpoint",
        capabilities: MACHINES_READ_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["checkpoint_id.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/ListMachines",
        capabilities: MACHINES_READ_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["page_limit.bounded", "cursor.valid"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Events",
        capabilities: MACHINES_READ_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "page_limit.bounded", "cursor.valid"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Usage",
        capabilities: MACHINES_READ_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["machine_id.nonzero", "time_range.valid"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/Cancel",
        capabilities: MACHINES_OPERATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["operation_id.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/InspectOperation",
        capabilities: MACHINES_OPERATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &["operation_id.nonzero"],
    },
    OperationPolicy {
        rpc: "acyclic.machines.v1.MachinesService/WatchOperation",
        capabilities: MACHINES_OPERATION_CAPABILITIES,
        errors: MACHINES_ERRORS,
        validations: &[
            "operation_id.nonzero",
            "cursor.monotonic",
            "terminal.required",
        ],
    },
];

impl ContractSpec {
    pub fn raw_options(&self, subject: &str) -> RawOptions {
        match self.package {
            "inference.customer.v1" => inference::inference_raw_options(subject),
            _ => RawOptions::new(),
        }
    }

    /// Return operation policy metadata owned by this contract family.
    pub fn operation_policies(&self) -> &'static [OperationPolicy] {
        match self.package {
            "acyclic.actors.v1" => ACTOR_POLICIES,
            "acyclic.stream.v2" => STREAM_POLICIES,
            "acyclic.workers.v1" => WORKER_POLICIES,
            "acyclic.objects.v2" => OBJECTS_POLICIES,
            "inference.customer.v1" => INFERENCE_POLICIES,
            "acyclic.machines.v1" => MACHINES_POLICIES,
            _ => &[],
        }
    }
}

pub const ACTORS: ContractSpec = ContractSpec {
    file_name: "actors/v1/actors.proto",
    syntax: "proto3",
    package: "acyclic.actors.v1",
    dependencies: &[],
    options: FileOptionsSpec {
        go_package: "github.com/acyclic-labs/sdk/go/gen/actors/v1;actorsv1",
    },
    messages: &[
        Binding::SPEC,
        ActorLimits::SPEC,
        SubscriptionStart::SPEC,
        SubscriptionSpec::SPEC,
        SubscriptionObservation::SPEC,
        ActorObservation::SPEC,
        CreateActorRequest::SPEC,
        CreateActorResponse::SPEC,
        UpdateActorRequest::SPEC,
        UpdateActorResponse::SPEC,
        InspectActorRequest::SPEC,
        InspectActorResponse::SPEC,
        AddSubscriptionRequest::SPEC,
        AddSubscriptionResponse::SPEC,
        RemoveSubscriptionRequest::SPEC,
        RemoveSubscriptionResponse::SPEC,
        ResumeSubscriptionRequest::SPEC,
        ResumeSubscriptionResponse::SPEC,
        CheckpointActorRequest::SPEC,
        CheckpointActorResponse::SPEC,
        Header::SPEC,
        InvokeActorRequest::SPEC,
        InvokeActorResponse::SPEC,
        Error::SPEC,
    ],
    enums: &[SUBSCRIPTION_STATE, ACTOR_STATE, ERROR_CODE],
    services: &[ACTORS_SERVICE],
    routes: ACTORS_ROUTES,
};

impl ContractSpec {
    pub fn message(&self, name: &str) -> Option<&MessageSpec> {
        self.messages.iter().find(|message| message.name == name)
    }
    pub fn enum_(&self, name: &str) -> Option<&EnumSpec> {
        self.enums.iter().find(|enum_| enum_.name == name)
    }
}

const fn field(
    name: &'static str,
    number: u32,
    cardinality: Cardinality,
    field_type: FieldType,
    json_name: &'static str,
) -> FieldSpec {
    FieldSpec {
        name,
        number,
        cardinality,
        field_type,
        json_name,
        oneof: None,
        proto3_optional: false,
        map: None,
    }
}
const fn optional_field(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    FieldSpec {
        name,
        number,
        cardinality: Cardinality::Optional,
        field_type,
        json_name,
        oneof: Some(oneof),
        proto3_optional: true,
        map: None,
    }
}
const fn oneof_field(
    name: &'static str,
    number: u32,
    field_type: FieldType,
    json_name: &'static str,
    oneof: &'static str,
) -> FieldSpec {
    FieldSpec {
        name,
        number,
        cardinality: Cardinality::Singular,
        field_type,
        json_name,
        oneof: Some(oneof),
        proto3_optional: false,
        map: None,
    }
}
const fn oneof(name: &'static str, synthetic: bool) -> OneofSpec {
    OneofSpec { name, synthetic }
}

fn actors_message_docs(name: &str) -> &'static str {
    match name {
        "Binding" => "A capability binding made available to an actor.",
        "ActorLimits" => "Resource and checkpoint limits for an actor.",
        "SubscriptionStart" => "The cursor or head position from which a subscription starts.",
        "SubscriptionSpec" => "A stream subscription attached to an actor.",
        "SubscriptionObservation" => "The current delivery and recovery state of a subscription.",
        "ActorObservation" => "The observable state and configuration revision of an actor.",
        "CreateActorRequest" => "Creates an actor and its initial subscriptions.",
        "CreateActorResponse" => "The actor created by the request.",
        "UpdateActorRequest" => "Replaces actor configuration with compare-and-swap semantics.",
        "UpdateActorResponse" => "The actor after configuration replacement.",
        "InspectActorRequest" => "Identifies an actor to inspect.",
        "InspectActorResponse" => "The inspected actor.",
        "AddSubscriptionRequest" => "Adds a subscription to an actor.",
        "AddSubscriptionResponse" => "The actor after adding a subscription.",
        "RemoveSubscriptionRequest" => "Removes a subscription from an actor.",
        "RemoveSubscriptionResponse" => "The actor after removing a subscription.",
        "ResumeSubscriptionRequest" => "Resumes delivery for an actor subscription.",
        "ResumeSubscriptionResponse" => "The actor after resuming a subscription.",
        "CheckpointActorRequest" => "Requests a durable actor checkpoint.",
        "CheckpointActorResponse" => "The actor after checkpointing.",
        "Header" => "An HTTP-style invocation header.",
        "InvokeActorRequest" => "Invokes an actor method with an HTTP-style request.",
        "InvokeActorResponse" => "The status, body, and headers returned by an invocation.",
        "Error" => "A typed actor service error.",
        _ => "A message in the Actors v1 wire contract.",
    }
}

fn actors_field_docs(name: &str) -> &'static str {
    match name {
        "name" => "The stable name of the binding or header.",
        "capability" => "The capability granted by the binding.",
        "resource" => "The resource selected by the binding.",
        "handler_timeout_millis" => "Maximum handler execution time in milliseconds.",
        "memory_bytes" => "Maximum actor memory in bytes.",
        "checkpoint_bytes" => "Maximum checkpoint size in bytes.",
        "cursor" => "A previously observed stream cursor.",
        "current_head" => "Start at the current stream head.",
        "subscription_id" => "The stable subscription identifier.",
        "stream_path" => "The stream path consumed by the subscription.",
        "start" => "The subscription's starting position.",
        "placement_anchor" => "Whether placement is anchored to the selected resource.",
        "state" => "The current subscription or actor state.",
        "delivered_cursor" => "The latest cursor delivered to the actor.",
        "completed_cursor" => "The latest cursor fully completed by the actor.",
        "recoverable_cursor" => "The earliest cursor from which recovery is possible.",
        "retry_count" => "The number of delivery retries.",
        "failure_code" => "The stable code for the latest delivery failure.",
        "failed_cursor" => "The cursor whose delivery most recently failed.",
        "actor_id" => "The stable actor identifier.",
        "code_sha256" => "SHA-256 digest of the actor code.",
        "home_region" => "The actor's home region.",
        "subscriptions" => "Subscriptions associated with the actor.",
        "checkpoint_unix_millis" => "Time of the latest checkpoint in Unix milliseconds.",
        "checkpoint_epoch" => "Monotonic checkpoint epoch.",
        "configuration_revision" => "Revision used for configuration compare-and-swap.",
        "bindings" => "Capability bindings supplied to the actor.",
        "limits" => "Resource and checkpoint limits supplied to the actor.",
        "idempotency_key" => "Client key used to make a mutation idempotent.",
        "expected_configuration_revision" => "Required current configuration revision.",
        "subscription" => "Subscription to add.",
        "body" => "The invocation request or response body.",
        "method" => "The method name or HTTP method to invoke.",
        "url" => "The URL passed to the actor invocation.",
        "headers" => "Headers passed to or returned from the invocation.",
        "status" => "The invocation status code.",
        "code" => "The typed error code.",
        "message" => "Human-readable error detail.",
        _ => "A field in the Actors v1 wire contract.",
    }
}

fn enum_docs(package: &str, name: &str) -> &'static str {
    match package {
        "acyclic.actors.v1" => match name {
            "SubscriptionState" => "Lifecycle state of a stream subscription.",
            "ActorState" => "Lifecycle state of an actor.",
            "ErrorCode" => "Stable error categories returned by the Actors service.",
            _ => panic!("missing Actors enum docs for {name}"),
        },
        "acyclic.workers.v1" => workers::WORKERS_ENUM_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| panic!("missing Workers enum docs for {name}")),
        "acyclic.objects.v2" => objects::OBJECTS_ENUM_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| panic!("missing Objects enum docs for {name}")),
        "acyclic.stream.v2" => match name {
            "StreamLimit" => "Fixed protocol limits for Stream operations.",
            _ => panic!("missing Stream enum docs for {name}"),
        },
        "acyclic.filesystem.v2" => "An enum in the Filesystem v2 wire contract.",
        "acyclic.harness.v2" => "An enum in the Harness v2 wire contract.",
        "acyclic.machines.v1" => "An enum in the Machines v1 wire contract.",
        "inference.customer.v1" => "An enum in the Inference customer v1 wire contract.",
        "acyclic.protocol.v1" => "An enum in the Protocol v1 wire contract.",
        _ => panic!("missing enum docs for package {package}: {name}"),
    }
}

fn service_docs(package: &str, name: &str) -> &'static str {
    match package {
        "acyclic.actors.v1" => match name {
            "ActorsService" => "Remote operations for creating, observing, and invoking actors.",
            _ => panic!("missing Actors service docs for {name}"),
        },
        "acyclic.workers.v1" => match name {
            "WorkersService" => workers::WORKERS_SERVICE_DOC.text,
            _ => panic!("missing Workers service docs for {name}"),
        },
        "acyclic.objects.v2" => objects::OBJECTS_SERVICE_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| panic!("missing Objects service docs for {name}")),
        "acyclic.stream.v2" => match name {
            "StreamService" => "Remote operations for appending, reading, and committing streams.",
            _ => panic!("missing Stream service docs for {name}"),
        },
        "acyclic.filesystem.v2" => "Remote operations for the Filesystem v2 contract.",
        "acyclic.harness.v2" => "Remote operations for the Harness v2 contract.",
        "acyclic.machines.v1" => machines::machines_service_docs(name),
        "inference.customer.v1" => inference::inference_service_docs(name),
        "acyclic.protocol.v1" => "Remote operations for the Protocol v1 contract.",
        _ => panic!("missing service docs for package {package}: {name}"),
    }
}

macro_rules! message {
    ($type:ident, $name:literal, [$($field:expr),* $(,)?], [$($oneof:expr),* $(,)?]) => {
        pub struct $type;
        impl $type { pub const SPEC: MessageSpec = MessageSpec { name: $name, fields: &[$($field),*], oneofs: &[$($oneof),*], nested_messages: &[], is_map_entry: false, reserved_ranges: &[], reserved_names: &[] }; }
    };
}

message!(
    Binding,
    "Binding",
    [
        field("name", 1, Cardinality::Singular, FieldType::String, "name"),
        field(
            "capability",
            2,
            Cardinality::Singular,
            FieldType::String,
            "capability"
        ),
        field(
            "resource",
            3,
            Cardinality::Singular,
            FieldType::String,
            "resource"
        ),
    ],
    []
);
message!(
    ActorLimits,
    "ActorLimits",
    [
        field(
            "handler_timeout_millis",
            1,
            Cardinality::Singular,
            FieldType::Uint64,
            "handlerTimeoutMillis"
        ),
        field(
            "memory_bytes",
            2,
            Cardinality::Singular,
            FieldType::Uint64,
            "memoryBytes"
        ),
        field(
            "checkpoint_bytes",
            3,
            Cardinality::Singular,
            FieldType::Uint64,
            "checkpointBytes"
        ),
    ],
    []
);
message!(
    SubscriptionStart,
    "SubscriptionStart",
    [
        oneof_field("cursor", 1, FieldType::Uint64, "cursor", "start"),
        oneof_field("current_head", 2, FieldType::Bool, "currentHead", "start"),
    ],
    [oneof("start", false)]
);
message!(
    SubscriptionSpec,
    "SubscriptionSpec",
    [
        field(
            "subscription_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "subscriptionId"
        ),
        field(
            "stream_path",
            2,
            Cardinality::Singular,
            FieldType::String,
            "streamPath"
        ),
        field(
            "start",
            3,
            Cardinality::Singular,
            FieldType::Message("SubscriptionStart"),
            "start"
        ),
        field(
            "placement_anchor",
            4,
            Cardinality::Singular,
            FieldType::Bool,
            "placementAnchor"
        ),
    ],
    []
);
message!(
    SubscriptionObservation,
    "SubscriptionObservation",
    [
        field(
            "subscription_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "subscriptionId"
        ),
        field(
            "stream_path",
            2,
            Cardinality::Singular,
            FieldType::String,
            "streamPath"
        ),
        field(
            "state",
            3,
            Cardinality::Singular,
            FieldType::Enum("SubscriptionState"),
            "state"
        ),
        field(
            "delivered_cursor",
            4,
            Cardinality::Singular,
            FieldType::Uint64,
            "deliveredCursor"
        ),
        field(
            "completed_cursor",
            5,
            Cardinality::Singular,
            FieldType::Uint64,
            "completedCursor"
        ),
        field(
            "recoverable_cursor",
            6,
            Cardinality::Singular,
            FieldType::Uint64,
            "recoverableCursor"
        ),
        field(
            "placement_anchor",
            7,
            Cardinality::Singular,
            FieldType::Bool,
            "placementAnchor"
        ),
        field(
            "retry_count",
            8,
            Cardinality::Singular,
            FieldType::Uint32,
            "retryCount"
        ),
        field(
            "failure_code",
            9,
            Cardinality::Singular,
            FieldType::String,
            "failureCode"
        ),
        optional_field(
            "failed_cursor",
            10,
            FieldType::Uint64,
            "failedCursor",
            "_failed_cursor",
        ),
    ],
    [oneof("_failed_cursor", true)]
);
message!(
    ActorObservation,
    "ActorObservation",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "code_sha256",
            2,
            Cardinality::Singular,
            FieldType::Bytes,
            "codeSha256"
        ),
        field(
            "home_region",
            3,
            Cardinality::Singular,
            FieldType::String,
            "homeRegion"
        ),
        field(
            "state",
            4,
            Cardinality::Singular,
            FieldType::Enum("ActorState"),
            "state"
        ),
        field(
            "subscriptions",
            5,
            Cardinality::Repeated,
            FieldType::Message("SubscriptionObservation"),
            "subscriptions"
        ),
        optional_field(
            "checkpoint_unix_millis",
            6,
            FieldType::Uint64,
            "checkpointUnixMillis",
            "_checkpoint_unix_millis",
        ),
        field(
            "checkpoint_epoch",
            7,
            Cardinality::Singular,
            FieldType::Uint64,
            "checkpointEpoch"
        ),
        field(
            "configuration_revision",
            8,
            Cardinality::Singular,
            FieldType::Uint64,
            "configurationRevision"
        ),
    ],
    [oneof("_checkpoint_unix_millis", true)]
);
message!(
    CreateActorRequest,
    "CreateActorRequest",
    [
        field(
            "code_sha256",
            1,
            Cardinality::Singular,
            FieldType::Bytes,
            "codeSha256"
        ),
        field(
            "home_region",
            2,
            Cardinality::Singular,
            FieldType::String,
            "homeRegion"
        ),
        field(
            "bindings",
            3,
            Cardinality::Repeated,
            FieldType::Message("Binding"),
            "bindings"
        ),
        field(
            "limits",
            4,
            Cardinality::Singular,
            FieldType::Message("ActorLimits"),
            "limits"
        ),
        field(
            "subscriptions",
            5,
            Cardinality::Repeated,
            FieldType::Message("SubscriptionSpec"),
            "subscriptions"
        ),
        field(
            "idempotency_key",
            6,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    []
);
message!(
    CreateActorResponse,
    "CreateActorResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
    []
);
message!(
    UpdateActorRequest,
    "UpdateActorRequest",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "code_sha256",
            2,
            Cardinality::Singular,
            FieldType::Bytes,
            "codeSha256"
        ),
        field(
            "bindings",
            3,
            Cardinality::Repeated,
            FieldType::Message("Binding"),
            "bindings"
        ),
        field(
            "limits",
            4,
            Cardinality::Singular,
            FieldType::Message("ActorLimits"),
            "limits"
        ),
        field(
            "expected_configuration_revision",
            5,
            Cardinality::Singular,
            FieldType::Uint64,
            "expectedConfigurationRevision"
        ),
        field(
            "idempotency_key",
            6,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    []
);
message!(
    UpdateActorResponse,
    "UpdateActorResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
    []
);
message!(
    InspectActorRequest,
    "InspectActorRequest",
    [field(
        "actor_id",
        1,
        Cardinality::Singular,
        FieldType::String,
        "actorId"
    )],
    []
);
message!(
    InspectActorResponse,
    "InspectActorResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
    []
);
message!(
    AddSubscriptionRequest,
    "AddSubscriptionRequest",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "subscription",
            2,
            Cardinality::Singular,
            FieldType::Message("SubscriptionSpec"),
            "subscription"
        ),
        field(
            "idempotency_key",
            3,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    []
);
message!(
    AddSubscriptionResponse,
    "AddSubscriptionResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
    []
);
message!(
    RemoveSubscriptionRequest,
    "RemoveSubscriptionRequest",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "subscription_id",
            2,
            Cardinality::Singular,
            FieldType::String,
            "subscriptionId"
        ),
        field(
            "idempotency_key",
            3,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    []
);
message!(
    RemoveSubscriptionResponse,
    "RemoveSubscriptionResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
    []
);
message!(
    ResumeSubscriptionRequest,
    "ResumeSubscriptionRequest",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "subscription_id",
            2,
            Cardinality::Singular,
            FieldType::String,
            "subscriptionId"
        ),
        field(
            "idempotency_key",
            3,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    []
);
message!(
    ResumeSubscriptionResponse,
    "ResumeSubscriptionResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
    []
);
message!(
    CheckpointActorRequest,
    "CheckpointActorRequest",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "idempotency_key",
            2,
            Cardinality::Singular,
            FieldType::String,
            "idempotencyKey"
        ),
    ],
    []
);
message!(
    CheckpointActorResponse,
    "CheckpointActorResponse",
    [field(
        "actor",
        1,
        Cardinality::Singular,
        FieldType::Message("ActorObservation"),
        "actor"
    )],
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
    []
);
message!(
    InvokeActorRequest,
    "InvokeActorRequest",
    [
        field(
            "actor_id",
            1,
            Cardinality::Singular,
            FieldType::String,
            "actorId"
        ),
        field(
            "method",
            2,
            Cardinality::Singular,
            FieldType::String,
            "method"
        ),
        field("url", 3, Cardinality::Singular, FieldType::String, "url"),
        field("body", 4, Cardinality::Singular, FieldType::Bytes, "body"),
        field(
            "headers",
            5,
            Cardinality::Repeated,
            FieldType::Message("Header"),
            "headers"
        ),
    ],
    []
);
message!(
    InvokeActorResponse,
    "InvokeActorResponse",
    [
        field(
            "status",
            1,
            Cardinality::Singular,
            FieldType::Uint32,
            "status"
        ),
        field("body", 2, Cardinality::Singular, FieldType::Bytes, "body"),
        field(
            "headers",
            3,
            Cardinality::Repeated,
            FieldType::Message("Header"),
            "headers"
        ),
    ],
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
    []
);

pub const SUBSCRIPTION_STATE: EnumSpec = EnumSpec {
    name: "SubscriptionState",
    values: &[
        EnumValueSpec {
            name: "SUBSCRIPTION_STATE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "SUBSCRIPTION_STATE_ACTIVE",
            number: 1,
        },
        EnumValueSpec {
            name: "SUBSCRIPTION_STATE_PAUSED",
            number: 2,
        },
    ],
};
pub const ACTOR_STATE: EnumSpec = EnumSpec {
    name: "ActorState",
    values: &[
        EnumValueSpec {
            name: "ACTOR_STATE_UNSPECIFIED",
            number: 0,
        },
        EnumValueSpec {
            name: "ACTOR_STATE_ACTIVE",
            number: 1,
        },
        EnumValueSpec {
            name: "ACTOR_STATE_HIBERNATED",
            number: 2,
        },
        EnumValueSpec {
            name: "ACTOR_STATE_PAUSED",
            number: 3,
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
            name: "ERROR_CODE_ACTOR_NOT_FOUND",
            number: 4,
        },
        EnumValueSpec {
            name: "ERROR_CODE_SUBSCRIPTION_NOT_FOUND",
            number: 5,
        },
        EnumValueSpec {
            name: "ERROR_CODE_IDEMPOTENCY_MISMATCH",
            number: 6,
        },
        EnumValueSpec {
            name: "ERROR_CODE_CONFLICT",
            number: 7,
        },
        EnumValueSpec {
            name: "ERROR_CODE_ADMISSION_DENIED",
            number: 8,
        },
        EnumValueSpec {
            name: "ERROR_CODE_CHECKPOINT_FAILED",
            number: 9,
        },
        EnumValueSpec {
            name: "ERROR_CODE_DEPENDENCY_UNAVAILABLE",
            number: 10,
        },
    ],
};

pub const ACTORS_SERVICE: ServiceSpec = ServiceSpec {
    name: "ActorsService",
    methods: &[
        MethodSpec {
            name: "CreateActor",
            input: "CreateActorRequest",
            output: "CreateActorResponse",
            docs: "Creates an actor.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "UpdateActor",
            input: "UpdateActorRequest",
            output: "UpdateActorResponse",
            docs: "Replaces actor configuration with compare-and-swap semantics.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InspectActor",
            input: "InspectActorRequest",
            output: "InspectActorResponse",
            docs: "Returns the current actor observation.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "AddSubscription",
            input: "AddSubscriptionRequest",
            output: "AddSubscriptionResponse",
            docs: "Adds a subscription to an actor.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "RemoveSubscription",
            input: "RemoveSubscriptionRequest",
            output: "RemoveSubscriptionResponse",
            docs: "Removes a subscription from an actor.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "ResumeSubscription",
            input: "ResumeSubscriptionRequest",
            output: "ResumeSubscriptionResponse",
            docs: "Resumes a paused subscription.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "CheckpointActor",
            input: "CheckpointActorRequest",
            output: "CheckpointActorResponse",
            docs: "Requests an actor checkpoint.",
            client_streaming: false,
            server_streaming: false,
        },
        MethodSpec {
            name: "InvokeActor",
            input: "InvokeActorRequest",
            output: "InvokeActorResponse",
            docs: "Invokes an actor method.",
            client_streaming: false,
            server_streaming: false,
        },
    ],
};

/// Rust-owned HTTP projection for the Actors service.
pub const ACTORS_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        method: "POST",
        path: "/v1/actors/create",
        operation_id: "createActor",
        rpc: "acyclic.actors.v1.ActorsService/CreateActor",
        request: "CreateActorRequest",
        response: "CreateActorResponse",
        docs: "Creates an actor.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/update",
        operation_id: "updateActor",
        rpc: "acyclic.actors.v1.ActorsService/UpdateActor",
        request: "UpdateActorRequest",
        response: "UpdateActorResponse",
        docs: "Replaces actor configuration with compare-and-swap semantics.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/inspect",
        operation_id: "inspectActor",
        rpc: "acyclic.actors.v1.ActorsService/InspectActor",
        request: "InspectActorRequest",
        response: "InspectActorResponse",
        docs: "Returns the current actor observation.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/subscriptions/add",
        operation_id: "addSubscription",
        rpc: "acyclic.actors.v1.ActorsService/AddSubscription",
        request: "AddSubscriptionRequest",
        response: "AddSubscriptionResponse",
        docs: "Adds a subscription to an actor.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/subscriptions/remove",
        operation_id: "removeSubscription",
        rpc: "acyclic.actors.v1.ActorsService/RemoveSubscription",
        request: "RemoveSubscriptionRequest",
        response: "RemoveSubscriptionResponse",
        docs: "Removes a subscription from an actor.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/subscriptions/resume",
        operation_id: "resumeSubscription",
        rpc: "acyclic.actors.v1.ActorsService/ResumeSubscription",
        request: "ResumeSubscriptionRequest",
        response: "ResumeSubscriptionResponse",
        docs: "Resumes a paused subscription.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/checkpoint",
        operation_id: "checkpointActor",
        rpc: "acyclic.actors.v1.ActorsService/CheckpointActor",
        request: "CheckpointActorRequest",
        response: "CheckpointActorResponse",
        docs: "Requests an actor checkpoint.",
    },
    RouteSpec {
        method: "POST",
        path: "/v1/actors/invoke",
        operation_id: "invokeActor",
        rpc: "acyclic.actors.v1.ActorsService/InvokeActor",
        request: "InvokeActorRequest",
        response: "InvokeActorResponse",
        docs: "Invokes an actor method.",
    },
];

/// Encode the Actors contract as a descriptor set generated directly from the
/// Rust model. The bytes omit source info so they are stable across compilers.
pub fn actors_descriptor() -> Vec<u8> {
    ACTORS.descriptor_set().encode_to_vec()
}

/// Render the Actors contract as protobuf source generated from the Rust model.
pub fn actors_proto() -> String {
    ACTORS.render_proto()
}

pub(crate) fn inference_descriptor_with_options() -> Vec<u8> {
    let base = INFERENCE.descriptor_set().encode_to_vec();
    let patched = patch_inference_descriptor(&base);
    // Keep the patched FileDescriptorProto as raw bytes. Decoding it through
    // prost-types and encoding it again would discard custom validation
    // extensions, since prost-types preserves their values only as unknown
    // fields. The raw path also preserves the exact Rust-owned option wire
    // encoding used by the compatibility fixtures.
    let inference_file = wire_fields(&patched)
        .expect("Rust-owned Inference descriptor wire fields")
        .into_iter()
        .find(|field| field.tag == 1 && field.wire_type == 2)
        .map(|field| field.value.to_vec())
        .filter(|file| string_field(file, 1) == Some(INFERENCE.file_name))
        .expect("Inference file descriptor");

    // The customer file imports validation extensions, whose descriptor in
    // turn imports google/protobuf/descriptor.proto.  Keep those dependency
    // descriptors in the closure so protoc and descriptor-pool consumers can
    // load the generated schema without reaching into the source tree.  The
    // archived bytes are dependency compatibility data only; the customer
    // file above is always emitted from the Rust model and patched options.
    let archived_bytes = include_bytes!("../../inference/inference_descriptor.bin");
    let archived = prost_types::FileDescriptorSet::decode(archived_bytes.as_slice())
        .expect("archived Inference descriptor closure");
    let archived_fields = wire_fields(archived_bytes).expect("archived descriptor wire fields");
    let mut closure = Vec::with_capacity(archived_bytes.len() + inference_file.len() + 8);
    for (file, raw_file) in archived.file.into_iter().zip(
        archived_fields
            .into_iter()
            .filter(|field| field.tag == 1 && field.wire_type == 2),
    ) {
        if file.name.as_deref() == Some(INFERENCE.file_name) {
            continue;
        }
        // Source locations are compiler annotations rather than wire schema.
        // Normalize preserved dependency descriptors while retaining unknown
        // fields and extension bytes that prost-types cannot represent.
        append_length_delimited(&mut closure, 1, &strip_source_code_info(raw_file.value));
    }
    append_length_delimited(&mut closure, 1, &inference_file);
    closure
}

fn strip_source_code_info(bytes: &[u8]) -> Vec<u8> {
    let Some(fields) = wire_fields(bytes) else {
        return bytes.to_vec();
    };
    let mut output = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    for field in fields {
        if field.tag != 9 {
            output.extend_from_slice(&bytes[cursor..field.end]);
        }
        cursor = field.end;
    }
    output.extend_from_slice(&bytes[cursor..]);
    output
}

#[derive(Clone, Copy)]
struct WireField<'a> {
    tag: u32,
    wire_type: u8,
    value: &'a [u8],
    start: usize,
    end: usize,
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

fn wire_fields(bytes: &[u8]) -> Option<Vec<WireField<'_>>> {
    let mut fields = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let start = cursor;
        let key = read_varint(bytes, &mut cursor)?;
        let tag = u32::try_from(key >> 3).ok()?;
        let wire_type = u8::try_from(key & 7).ok()?;
        let value = match wire_type {
            0 => {
                let value_start = cursor;
                read_varint(bytes, &mut cursor)?;
                &bytes[value_start..cursor]
            }
            1 => {
                let value_start = cursor;
                cursor = cursor.checked_add(8)?;
                bytes.get(value_start..cursor)?
            }
            2 => {
                let length = usize::try_from(read_varint(bytes, &mut cursor)?).ok()?;
                let value_start = cursor;
                cursor = cursor.checked_add(length)?;
                bytes.get(value_start..cursor)?
            }
            5 => {
                let value_start = cursor;
                cursor = cursor.checked_add(4)?;
                bytes.get(value_start..cursor)?
            }
            _ => return None,
        };
        fields.push(WireField {
            tag,
            wire_type,
            value,
            start,
            end: cursor,
        });
    }
    Some(fields)
}

fn encode_varint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn append_length_delimited(output: &mut Vec<u8>, tag: u32, payload: &[u8]) {
    encode_varint((u64::from(tag) << 3 | 2) as usize, output);
    encode_varint(payload.len(), output);
    output.extend_from_slice(payload);
}

fn string_field(bytes: &[u8], tag: u32) -> Option<&str> {
    wire_fields(bytes)?
        .into_iter()
        .find(|field| field.tag == tag && field.wire_type == 2)
        .and_then(|field| std::str::from_utf8(field.value).ok())
}

fn rewrite_nested(bytes: &[u8], tag: u32, mut patch: impl FnMut(&[u8]) -> Vec<u8>) -> Vec<u8> {
    let Some(fields) = wire_fields(bytes) else {
        return bytes.to_vec();
    };
    let mut output = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    for field in fields {
        output.extend_from_slice(&bytes[cursor..field.start]);
        if field.tag == tag && field.wire_type == 2 {
            append_length_delimited(&mut output, field.tag, &patch(field.value));
        } else {
            output.extend_from_slice(&bytes[field.start..field.end]);
        }
        cursor = field.end;
    }
    output.extend_from_slice(&bytes[cursor..]);
    output
}

fn option_payload(subject: &str) -> Vec<u8> {
    inference::inference_raw_options(subject).encode()
}

fn append_options(bytes: &[u8], options_tag: u32, subject: &str) -> Vec<u8> {
    let options = option_payload(subject);
    if options.is_empty() {
        return bytes.to_vec();
    }
    let Some(fields) = wire_fields(bytes) else {
        return bytes.to_vec();
    };
    let mut output = Vec::with_capacity(bytes.len() + options.len() + 4);
    let mut cursor = 0;
    let mut found = false;
    for field in fields {
        output.extend_from_slice(&bytes[cursor..field.start]);
        if field.tag == options_tag && field.wire_type == 2 {
            let mut merged = field.value.to_vec();
            merged.extend_from_slice(&options);
            append_length_delimited(&mut output, field.tag, &merged);
            found = true;
        } else {
            output.extend_from_slice(&bytes[field.start..field.end]);
        }
        cursor = field.end;
    }
    output.extend_from_slice(&bytes[cursor..]);
    if !found {
        append_length_delimited(&mut output, options_tag, &options);
    }
    output
}

fn patch_inference_descriptor(bytes: &[u8]) -> Vec<u8> {
    rewrite_nested(bytes, 1, patch_inference_file)
}

fn patch_inference_file(bytes: &[u8]) -> Vec<u8> {
    let mut output = rewrite_nested(bytes, 4, patch_inference_message);
    output = rewrite_nested(&output, 5, patch_inference_enum);
    rewrite_nested(&output, 6, patch_inference_service)
}

fn patch_inference_message(bytes: &[u8]) -> Vec<u8> {
    let message = string_field(bytes, 1).unwrap_or_default();
    let mut output = rewrite_nested(bytes, 2, |field| {
        append_options(
            field,
            8,
            &format!("{message}.{}", string_field(field, 1).unwrap_or_default()),
        )
    });
    output = rewrite_nested(&output, 4, patch_inference_message);
    output = rewrite_nested(&output, 5, patch_inference_enum);
    rewrite_nested(&output, 8, |oneof| {
        append_options(
            oneof,
            2,
            &format!("{message}.{}", string_field(oneof, 1).unwrap_or_default()),
        )
    })
}

fn patch_inference_enum(bytes: &[u8]) -> Vec<u8> {
    let enum_name = string_field(bytes, 1).unwrap_or_default();
    rewrite_nested(bytes, 2, |value| {
        append_options(
            value,
            3,
            &format!("{enum_name}.{}", string_field(value, 1).unwrap_or_default()),
        )
    })
}

fn patch_inference_service(bytes: &[u8]) -> Vec<u8> {
    let service = string_field(bytes, 1).unwrap_or_default();
    rewrite_nested(bytes, 2, |method| {
        append_options(
            method,
            4,
            &format!("{service}.{}", string_field(method, 1).unwrap_or_default()),
        )
    })
}

impl ContractSpec {
    pub fn descriptor_set(&self) -> prost_types::FileDescriptorSet {
        prost_types::FileDescriptorSet {
            file: vec![self.file_descriptor()],
        }
    }

    pub fn file_descriptor(&self) -> prost_types::FileDescriptorProto {
        prost_types::FileDescriptorProto {
            name: Some(self.file_name.to_owned()),
            package: Some(self.package.to_owned()),
            dependency: self
                .dependencies
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            message_type: self
                .messages
                .iter()
                .map(|message| message_descriptor(message, self.package))
                .collect(),
            enum_type: self.enums.iter().map(enum_descriptor).collect(),
            service: self
                .services
                .iter()
                .map(|service| service_descriptor(service, self.package))
                .collect(),
            options: Some(prost_types::FileOptions {
                go_package: Some(self.options.go_package.to_owned()),
                ..Default::default()
            }),
            syntax: Some(self.syntax.to_owned()),
            ..Default::default()
        }
    }

    pub fn render_proto(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!("syntax = \"{}\";\n", self.syntax));
        output.push_str(&format!("package {};\n\n", self.package));
        for dependency in self.dependencies {
            output.push_str(&format!("import \"{}\";\n", dependency));
        }
        if !self.dependencies.is_empty() {
            output.push('\n');
        }
        output.push_str(&format!(
            "option go_package = \"{}\";\n\n",
            self.options.go_package
        ));
        for enum_ in self.enums {
            write_doc(&mut output, enum_docs(self.package, enum_.name));
            output.push_str(&format!("enum {} {{\n", enum_.name));
            for value in enum_.values {
                output.push_str(&format!(
                    "  {} = {}{};\n",
                    value.name,
                    value.number,
                    inference_option_suffix(
                        self.package,
                        &format!("{}.{}", enum_.name, value.name),
                    )
                ));
            }
            output.push_str("}\n\n");
        }
        for message in self.messages {
            write_message(&mut output, self.package, message, 0);
        }
        for service in self.services {
            write_doc(&mut output, service_docs(self.package, service.name));
            output.push_str(&format!("service {} {{\n", service.name));
            for method in service.methods {
                write_doc_indented(&mut output, method.docs, 2);
                let options = inference_option_values(
                    self.package,
                    &format!("{}.{}", service.name, method.name),
                );
                output.push_str(&format!(
                    "  rpc {}({}{}) returns ({}{})",
                    method.name,
                    if method.client_streaming {
                        "stream "
                    } else {
                        ""
                    },
                    method.input,
                    if method.server_streaming {
                        "stream "
                    } else {
                        ""
                    },
                    method.output
                ));
                if options.is_empty() {
                    output.push_str(";\n");
                } else {
                    output.push_str(" {\n");
                    for option in options {
                        output.push_str(&format!("    option {};\n", option));
                    }
                    output.push_str("  }\n");
                }
            }
            output.push_str("}\n");
        }
        output
    }
}

fn inference_option_values(package: &str, subject: &str) -> Vec<String> {
    if package != "inference.customer.v1" {
        return Vec::new();
    }
    inference::INFERENCE_OPTIONS
        .iter()
        .filter(|option| option.subject == subject)
        .map(|option| {
            let value = match option.value {
                inference::ValidationValue::Bool(value) => value.to_string(),
                inference::ValidationValue::U32(value) => value.to_string(),
                inference::ValidationValue::U64(value) => value.to_string(),
                inference::ValidationValue::Text(value) => format!("\"{value}\""),
            };
            format!("(acyclic.validation.v1.{}) = {}", option.name, value)
        })
        .collect()
}

fn inference_option_suffix(package: &str, subject: &str) -> String {
    let options = inference_option_values(package, subject);
    if options.is_empty() {
        String::new()
    } else {
        format!(" [{}]", options.join(", "))
    }
}

fn write_doc(output: &mut String, docs: &str) {
    write_doc_indented(output, docs, 0);
}

fn write_doc_indented(output: &mut String, docs: &str, indent: usize) {
    let prefix = " ".repeat(indent);
    for line in docs.lines() {
        output.push_str(&format!("{}// {}\n", prefix, line));
    }
}

fn message_docs(package: &str, name: &str) -> &'static str {
    match package {
        "acyclic.actors.v1" => actors_message_docs(name),
        "acyclic.workers.v1" => workers::WORKERS_MESSAGE_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| panic!("missing Workers message docs for {name}")),
        "acyclic.stream.v2" => stream_message_docs(name),
        "acyclic.objects.v2" => objects::OBJECTS_MESSAGE_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| panic!("missing Objects message docs for {name}")),
        "acyclic.filesystem.v2" => "A message in the Filesystem v2 wire contract.",
        "acyclic.harness.v2" => "A message in the Harness v2 wire contract.",
        "acyclic.machines.v1" => machines::machines_message_docs(name),
        "inference.customer.v1" => inference::inference_message_docs(name),
        "acyclic.protocol.v1" => "A message in the Protocol v1 wire contract.",
        _ => panic!("missing message docs for package {package}: {name}"),
    }
}

fn field_docs(package: &str, message: &str, name: &str) -> &'static str {
    if package == "acyclic.actors.v1" {
        return actors_field_docs(name);
    }
    if package == "acyclic.workers.v1" {
        return workers::WORKERS_FIELD_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| panic!("missing Workers field docs for {message}.{name}"));
    }
    match package {
        "acyclic.stream.v2" => stream_field_docs(message, name),
        "acyclic.objects.v2" => objects::OBJECTS_FIELD_DOCS
            .iter()
            .find(|doc| doc.name == name)
            .map(|doc| doc.text)
            .unwrap_or_else(|| match name {
                "mutation" => "The object mutation operation.",
                "key" => "The metadata map key.",
                "value" => "The metadata map value.",
                "total" => "The complete size of the selected range in bytes.",
                _ => "A field in the Objects v2 wire contract.",
            }),
        "acyclic.filesystem.v2" => "A field in the Filesystem v2 wire contract.",
        "acyclic.harness.v2" => "A field in the Harness v2 wire contract.",
        "acyclic.machines.v1" => machines::machines_field_docs(message, name),
        "inference.customer.v1" => inference::inference_field_docs(message, name),
        "acyclic.protocol.v1" => "A field in the Protocol v1 wire contract.",
        _ => panic!("missing field docs for package {package}: {message}.{name}"),
    }
}

fn stream_message_docs(name: &str) -> &'static str {
    match name {
        "Record" => "One immutable record in a stream.",
        "AppendRequest" => "Appends records to a stream.",
        "AppendReceipt" => "The admitted range returned by an append.",
        "TailConflict" => "The observed tail when an append precondition fails.",
        "AppendResponse" => "The result of an append operation.",
        "TailRequest" => "Reads the current tail of a stream.",
        "TailResponse" => "The current stream tail.",
        "ForkRequest" => "Forks a stream path at an admitted sequence.",
        "ForkReceipt" => "The admitted stream fork.",
        "ReadRequest" => "Reads records from a stream page by page.",
        "FollowRequest" => "Follows a stream from a starting sequence.",
        "ReadResponse" => "One streamed record response.",
        "ChildrenRequest" => "Lists child streams under a parent path.",
        "Child" => "One child stream path.",
        "ChildrenResponse" => "One streamed child stream response.",
        "ChildrenPageRequest" => "Reads one ordered page of child streams.",
        "ChildrenPageResponse" => "One ordered page of child streams.",
        "TailCondition" => "A commit condition on a stream tail.",
        "AbsentCondition" => "A commit condition requiring a stream to be absent.",
        "CommitCondition" => "One condition checked before committing mutations.",
        "AppendMutation" => "An append mutation in an atomic commit.",
        "ForkMutation" => "A fork and append mutation in an atomic commit.",
        "CommitMutation" => "One mutation in an atomic commit.",
        "CommitRequest" => "Atomically commits stream mutations after checking conditions.",
        "CommittedAppend" => "An admitted append included in a committed envelope.",
        "CommittedFork" => "An admitted fork included in a committed envelope.",
        "CommittedMutation" => "One committed stream mutation.",
        "CommittedEnvelope" => "The mutations admitted by one commit.",
        "TailCommitConflict" => "A commit conflict caused by an unexpected tail.",
        "ExistsCommitConflict" => "A commit conflict caused by an existing stream.",
        "CommitConflict" => "One conflict returned by an atomic commit.",
        "CommitConflicts" => "The conflicts returned by an atomic commit.",
        "CommitResponse" => "The result of an atomic stream commit.",
        "ReadCommitRequest" => "Reads a committed envelope by commit identifier.",
        "InspectIdempotencyRequest" => "Inspects an idempotent stream operation.",
        "IdempotencyObservation" => "The admitted outcome associated with an idempotency key.",
        "InspectIdempotencyResponse" => "The idempotency observation, when present.",
        "TokenGrant" => "A set of stream operations granted to a token.",
        "CreateTokenRequest" => "Creates a token for hosted Stream operations.",
        _ => panic!("missing Stream message docs for {name}"),
    }
}

fn stream_field_docs(_message: &str, name: &str) -> &'static str {
    match name {
        "path" => "The stream path.",
        "sequence" => "The record sequence.",
        "value" => "The record value bytes.",
        "commit_id" => "The commit identifier.",
        "committed_at_micros" => "The commit time in Unix microseconds.",
        "cursor" => "A stream cursor.",
        "records" => "Records in the stream operation.",
        "items" => "Items returned by the stream operation.",
        "limit" => "The maximum number of returned items.",
        "start" => "The inclusive starting sequence.",
        "end" => "The exclusive ending sequence.",
        "tail" => "The observed or resulting stream tail.",
        "actual_tail" => "The actual stream tail.",
        "source" => "The source stream path.",
        "destination" => "The destination stream path.",
        "forked_at" => "The sequence at which the stream was forked.",
        "from" => "The first sequence to read.",
        "parent" => "The parent stream path.",
        "after" => "The exclusive child path cursor.",
        "hierarchy_version" => "The hierarchy version used for traversal.",
        "next_after" => "The cursor for the next child page.",
        "expected" => "The expected stream tail.",
        "conditions" => "Conditions checked before mutations.",
        "condition" => "The commit condition variant.",
        "absent" => "The absence condition.",
        "exists" => "The existence conflict condition.",
        "mutations" => "Mutations admitted at one linearization point.",
        "idempotency_key" => "Client key used to make a mutation idempotent.",
        "deadline_unix_millis" => "The commit deadline in Unix milliseconds.",
        "conflicts" => "Commit conflicts returned by the service.",
        "observation" => "The admitted idempotency observation.",
        "request_digest" => "The digest of the admitted request.",
        "append" => "The append outcome or mutation.",
        "fork" => "The fork outcome or mutation.",
        "commit" => "The commit outcome.",
        "outcome" => "The operation outcome.",
        "committed" => "The committed append or envelope outcome.",
        "conflict" => "The conflict outcome.",
        "if_tail" => "The optional expected tail precondition.",
        "at_tail" => "The optional sequence at which to fork.",
        "record" => "The streamed record.",
        "next" => "The next page cursor.",
        "actual" => "The actual observed value.",
        "expires" => "The token expiration time.",
        "command" => "The stream command payload.",
        "message" => "The stream operation message.",
        "child" => "The child stream identity.",
        "children" => "Child streams in the page.",
        "expires_in" => "The token expiration interval.",
        "allow" => "The operations and paths granted to the token.",
        "subtree" => "Whether access includes descendants of the path.",
        "operations" => "Operations granted by the token.",
        _ => "A field in the Stream v2 wire contract.",
    }
}

fn write_message(output: &mut String, package: &str, message: &MessageSpec, indent: usize) {
    write_doc_indented(output, message_docs(package, message.name), indent);
    let prefix = " ".repeat(indent);
    output.push_str(&format!("{}message {} {{\n", prefix, message.name));
    for &(start, end) in message.reserved_ranges {
        if start == end {
            output.push_str(&format!("{}  reserved {};\n", prefix, start));
        } else {
            output.push_str(&format!("{}  reserved {} to {};\n", prefix, start, end));
        }
    }
    if !message.reserved_names.is_empty() {
        let names = message
            .reserved_names
            .iter()
            .map(|name| format!("\"{}\"", name))
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!("{}  reserved {};\n", prefix, names));
    }
    for field in message.fields.iter().filter(|field| {
        field.oneof.is_none()
            || message
                .oneofs
                .iter()
                .find(|oneof| Some(oneof.name) == field.oneof)
                .is_some_and(|oneof| oneof.synthetic)
    }) {
        write_field(output, package, message.name, field, indent + 2);
    }
    for oneof in message.oneofs.iter().filter(|oneof| !oneof.synthetic) {
        output.push_str(&format!("{}  oneof {} {{\n", prefix, oneof.name));
        for option in inference_option_values(package, &format!("{}.{}", message.name, oneof.name))
        {
            output.push_str(&format!("{}    option {};\n", prefix, option));
        }
        for field in message
            .fields
            .iter()
            .filter(|field| field.oneof == Some(oneof.name))
        {
            write_field(output, package, message.name, field, indent + 4);
        }
        output.push_str(&format!("{}  }}\n", prefix));
    }
    // Map-entry messages are synthesized by the protobuf map field syntax.
    // Other nested messages are emitted recursively.
    for nested in message
        .nested_messages
        .iter()
        .filter(|nested| !nested.is_map_entry)
    {
        write_message(output, package, nested, indent + 2);
    }
    output.push_str(&format!("{}}}\n\n", prefix));
}

fn write_field(
    output: &mut String,
    package: &str,
    message: &str,
    field: &FieldSpec,
    indent: usize,
) {
    if let Some(map) = field.map {
        // Protobuf source elides the synthetic map-entry declaration. Keep its
        // Rust-owned message documentation visible beside the map field so
        // source and documentation projections retain the complete contract
        // inventory without emitting a non-source map message.
        write_doc_indented(output, message_docs(package, map.entry_name), indent);
    }
    write_doc_indented(output, field_docs(package, message, field.name), indent);
    let prefix = " ".repeat(indent);
    let cardinality = match field.cardinality {
        Cardinality::Singular => "",
        Cardinality::Optional => "optional ",
        Cardinality::Repeated => "repeated ",
    };
    let type_name = field.map.map_or_else(
        || field_type_name(field.field_type).to_owned(),
        |map| {
            format!(
                "map<{}, {}>",
                field_type_name(map.key),
                field_type_name(map.value)
            )
        },
    );
    let cardinality = if field.map.is_some() { "" } else { cardinality };
    output.push_str(&format!(
        "{}{}{} {} = {}{};\n",
        prefix,
        cardinality,
        type_name,
        field.name,
        field.number,
        inference_option_suffix(package, &format!("{}.{}", message, field.name))
    ));
}

fn field_type_name(field_type: FieldType) -> &'static str {
    match field_type {
        FieldType::String => "string",
        FieldType::Bytes => "bytes",
        FieldType::Bool => "bool",
        FieldType::Int64 => "int64",
        FieldType::Sint64 => "sint64",
        FieldType::Uint32 => "uint32",
        FieldType::Uint64 => "uint64",
        FieldType::Message(name) | FieldType::ExternalMessage(name) | FieldType::Enum(name) => name,
    }
}

fn message_descriptor(message: &MessageSpec, package: &str) -> prost_types::DescriptorProto {
    prost_types::DescriptorProto {
        name: Some(message.name.to_owned()),
        field: message
            .fields
            .iter()
            .map(|field| {
                let oneof_index = field.oneof.and_then(|name| {
                    message
                        .oneofs
                        .iter()
                        .position(|oneof| oneof.name == name)
                        .map(|index| index as i32)
                });
                field_descriptor(field, message.name, package, oneof_index)
            })
            .collect(),
        oneof_decl: message
            .oneofs
            .iter()
            .map(|oneof| prost_types::OneofDescriptorProto {
                name: Some(oneof.name.to_owned()),
                ..Default::default()
            })
            .collect(),
        reserved_range: message
            .reserved_ranges
            .iter()
            .map(
                |&(start, end)| prost_types::descriptor_proto::ReservedRange {
                    start: Some(start as i32),
                    end: Some((end + 1) as i32),
                },
            )
            .collect(),
        reserved_name: message
            .reserved_names
            .iter()
            .map(|name| (*name).to_owned())
            .collect(),
        nested_type: message
            .nested_messages
            .iter()
            .map(|nested| message_descriptor(nested, package))
            .collect(),
        options: message.is_map_entry.then_some(prost_types::MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn field_descriptor(
    field: &FieldSpec,
    message_name: &str,
    package: &str,
    oneof_index: Option<i32>,
) -> prost_types::FieldDescriptorProto {
    let (field_kind, type_name) = match field.map {
        Some(map) => (
            prost_types::field_descriptor_proto::Type::Message,
            Some(format!(".{}.{}.{}", package, message_name, map.entry_name)),
        ),
        None => match field.field_type {
            FieldType::String => (prost_types::field_descriptor_proto::Type::String, None),
            FieldType::Bytes => (prost_types::field_descriptor_proto::Type::Bytes, None),
            FieldType::Bool => (prost_types::field_descriptor_proto::Type::Bool, None),
            FieldType::Int64 => (prost_types::field_descriptor_proto::Type::Int64, None),
            FieldType::Sint64 => (prost_types::field_descriptor_proto::Type::Sint64, None),
            FieldType::Uint32 => (prost_types::field_descriptor_proto::Type::Uint32, None),
            FieldType::Uint64 => (prost_types::field_descriptor_proto::Type::Uint64, None),
            FieldType::Message(name) => (
                prost_types::field_descriptor_proto::Type::Message,
                Some(format!(".{}.{}", package, name)),
            ),
            FieldType::ExternalMessage(name) => (
                prost_types::field_descriptor_proto::Type::Message,
                Some(format!(".{}", name)),
            ),
            FieldType::Enum(name) => (
                prost_types::field_descriptor_proto::Type::Enum,
                Some(format!(".{}.{}", package, name)),
            ),
        },
    };
    prost_types::FieldDescriptorProto {
        name: Some(field.name.to_owned()),
        number: Some(field.number as i32),
        label: Some(match field.map {
            Some(_) => prost_types::field_descriptor_proto::Label::Repeated as i32,
            None => match field.cardinality {
                Cardinality::Singular | Cardinality::Optional => {
                    prost_types::field_descriptor_proto::Label::Optional as i32
                }
                Cardinality::Repeated => {
                    prost_types::field_descriptor_proto::Label::Repeated as i32
                }
            },
        }),
        r#type: Some(field_kind as i32),
        type_name,
        json_name: Some(field.json_name.to_owned()),
        oneof_index,
        proto3_optional: field.proto3_optional.then_some(true),
        ..Default::default()
    }
}

fn enum_descriptor(enum_: &EnumSpec) -> prost_types::EnumDescriptorProto {
    prost_types::EnumDescriptorProto {
        name: Some(enum_.name.to_owned()),
        value: enum_
            .values
            .iter()
            .map(|value| prost_types::EnumValueDescriptorProto {
                name: Some(value.name.to_owned()),
                number: Some(value.number),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn service_descriptor(service: &ServiceSpec, package: &str) -> prost_types::ServiceDescriptorProto {
    prost_types::ServiceDescriptorProto {
        name: Some(service.name.to_owned()),
        method: service
            .methods
            .iter()
            .map(|method| prost_types::MethodDescriptorProto {
                name: Some(method.name.to_owned()),
                input_type: Some(format!(".{}.{}", package, method.input)),
                output_type: Some(format!(".{}.{}", package, method.output)),
                client_streaming: method.client_streaming.then_some(true),
                server_streaming: method.server_streaming.then_some(true),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use prost_types::{DescriptorProto, FileDescriptorSet, field_descriptor_proto};

    const GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/actors-v1.descriptor.bin"
    ));
    const STREAM_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stream-v2.descriptor.bin"
    ));
    const OBJECTS_GOLDEN_DESCRIPTOR: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/objects-v2.descriptor.bin"
    ));

    #[test]
    fn model_has_expected_public_surface() {
        assert_eq!(ACTORS.file_name, "actors/v1/actors.proto");
        assert_eq!(ACTORS.package, "acyclic.actors.v1");
        assert_eq!(ACTORS.messages.len(), 24);
        assert_eq!(ACTORS.enums.len(), 3);
        assert_eq!(ACTORS.services.len(), 1);
        assert_eq!(ACTORS.services[0].methods.len(), 8);
        let source = actors_proto();
        assert!(
            ACTORS
                .messages
                .iter()
                .all(|message| source.contains(message_docs(ACTORS.package, message.name)))
        );
        assert!(
            ACTORS
                .messages
                .iter()
                .flat_map(|message| message.fields)
                .all(|field| source.contains(field_docs(ACTORS.package, "", field.name)))
        );
        assert_eq!(ACTORS.message("SubscriptionStart").unwrap().oneofs.len(), 1);
        assert!(ACTORS.message("SubscriptionObservation").unwrap().fields[9].proto3_optional);
    }

    #[test]
    fn model_descriptor_is_exact_golden_bytes() {
        assert_eq!(actors_descriptor().as_slice(), GOLDEN_DESCRIPTOR);
        let set = FileDescriptorSet::decode(GOLDEN_DESCRIPTOR).expect("decode golden descriptor");
        let file = set.file.first().expect("file in golden descriptor");
        assert_eq!(file.source_code_info, None, "golden must omit source info");
        assert_descriptor_semantics(file);
    }

    #[test]
    fn generated_proto_has_complete_documentation() {
        let source = actors_proto();
        assert!(source.contains("// Resumes a paused subscription."));
        assert!(source.contains("// The cursor whose delivery most recently failed."));
        assert!(source.ends_with("}\n"));
    }

    #[test]
    fn stream_append_ranges_document_half_open_endings() {
        assert_eq!(
            stream_field_docs("AppendReceipt", "end"),
            "The exclusive ending sequence."
        );
        assert_eq!(
            stream_field_docs("CommittedAppend", "end"),
            "The exclusive ending sequence."
        );
        let source = stream_proto();
        assert!(source.contains("// The exclusive ending sequence."));
        assert!(!source.contains("// The inclusive ending sequence."));
    }

    #[test]
    fn removed_field_is_detected() {
        let mut changed = ACTORS.file_descriptor();
        changed.message_type[0].field.remove(0);
        let changed = FileDescriptorSet {
            file: vec![changed],
        }
        .encode_to_vec();
        assert_ne!(changed, GOLDEN_DESCRIPTOR);
    }

    #[test]
    fn changed_field_tag_is_detected() {
        let mut changed = ACTORS.file_descriptor();
        changed.message_type[0].field[0].number = Some(99);
        let changed = FileDescriptorSet {
            file: vec![changed],
        }
        .encode_to_vec();
        assert_ne!(changed, GOLDEN_DESCRIPTOR);
    }

    #[test]
    fn stream_model_descriptor_is_exact_golden_bytes() {
        assert_eq!(
            stream::stream_descriptor().as_slice(),
            STREAM_GOLDEN_DESCRIPTOR
        );
        let set = FileDescriptorSet::decode(STREAM_GOLDEN_DESCRIPTOR)
            .expect("decode Stream golden descriptor");
        let file = set.file.first().expect("Stream file in golden descriptor");
        assert_eq!(file.name.as_deref(), Some(stream::STREAM.file_name));
        assert_eq!(file.package.as_deref(), Some(stream::STREAM.package));
        assert_eq!(file.syntax.as_deref(), Some("proto3"));
        assert_eq!(file.source_code_info, None, "golden must omit source info");
        assert_eq!(file.message_type.len(), 39);
        assert_eq!(file.enum_type.len(), 1);
        assert_eq!(file.service.len(), 1);
        let tail_response = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some("TailResponse"))
            .expect("TailResponse");
        assert_eq!(tail_response.reserved_name, vec!["trim_point".to_owned()]);
        assert_eq!(tail_response.reserved_range[0].start, Some(2));
        assert_eq!(tail_response.reserved_range[0].end, Some(3));
        let commit_mutation = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some("CommitMutation"))
            .expect("CommitMutation");
        assert_eq!(
            commit_mutation.reserved_name,
            vec!["trim".to_owned(), "delete".to_owned()]
        );
        assert_eq!(commit_mutation.reserved_range.len(), 2);
        let service = &file.service[0];
        assert_eq!(service.name.as_deref(), Some("StreamService"));
        assert_eq!(service.method.len(), 10);
        assert_eq!(service.method[4].name.as_deref(), Some("Read"));
        assert_eq!(service.method[4].server_streaming, Some(true));
        assert_eq!(service.method[4].client_streaming, None);
    }

    #[test]
    fn routes_are_explicitly_linked_to_wire_methods() {
        assert_eq!(ACTORS.routes.len(), ACTORS.services[0].methods.len());
        for route in ACTORS.routes {
            let method = ACTORS.services[0]
                .methods
                .iter()
                .find(|method| {
                    format!("acyclic.actors.v1.ActorsService/{}", method.name) == route.rpc
                })
                .expect("route RPC exists in Actors service");
            assert_eq!(route.method, "POST");
            assert!(route.path.starts_with("/v1/actors/"));
            assert_eq!(route.request, method.input);
            assert_eq!(route.response, method.output);
            assert!(!route.docs.is_empty());
            assert_eq!(route.docs, method.docs);
        }
        assert_eq!(stream::STREAM.routes.len(), 10);
        let read = stream::STREAM
            .routes
            .iter()
            .find(|route| route.operation_id == "read")
            .expect("Stream read route");
        assert_eq!(read.path, "/v1/stream/read");
        assert_eq!(read.response, "ReadResponse");
        assert_eq!(
            stream::STREAM.services[0]
                .methods
                .iter()
                .find(|method| method.name == "Read")
                .map(|method| method.server_streaming),
            Some(true)
        );
    }

    #[test]
    fn objects_model_descriptor_is_exact_golden_bytes() {
        assert_eq!(
            objects::objects_descriptor().as_slice(),
            OBJECTS_GOLDEN_DESCRIPTOR
        );
        let set = FileDescriptorSet::decode(OBJECTS_GOLDEN_DESCRIPTOR)
            .expect("decode Objects v2 golden descriptor");
        let file = set.file.first().expect("Objects v2 file");
        assert_eq!(file.name.as_deref(), Some(objects::OBJECTS_V2.file_name));
        assert_eq!(file.package.as_deref(), Some("acyclic.objects.v2"));
        assert_eq!(file.dependency, vec!["google/protobuf/timestamp.proto"]);
        assert_eq!(file.message_type.len(), 36);
        assert_eq!(file.enum_type.len(), 2);
        assert_eq!(
            file.service
                .iter()
                .map(|service| service.method.len())
                .sum::<usize>(),
            13
        );
        let metadata = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some("ObjectMetadata"))
            .expect("ObjectMetadata");
        assert_eq!(metadata.nested_type.len(), 1);
        assert_eq!(metadata.nested_type[0].name.as_deref(), Some("UserEntry"));
        assert_eq!(
            metadata.nested_type[0]
                .options
                .as_ref()
                .and_then(|options| options.map_entry),
            Some(true)
        );
        let put = file
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("ObjectsService"))
            .expect("ObjectsService")
            .method
            .iter()
            .find(|method| method.name.as_deref() == Some("PutObject"))
            .expect("PutObject");
        assert_eq!(put.client_streaming, Some(true));
        assert_eq!(put.server_streaming, None);
    }

    fn assert_descriptor_semantics(file: &prost_types::FileDescriptorProto) {
        assert_eq!(file.name.as_deref(), Some(ACTORS.file_name));
        assert_eq!(file.package.as_deref(), Some(ACTORS.package));
        assert_eq!(file.syntax.as_deref(), Some(ACTORS.syntax));
        assert!(
            file.dependency
                .iter()
                .eq(ACTORS.dependencies.iter().copied())
        );
        assert_eq!(
            file.options
                .as_ref()
                .and_then(|options| options.go_package.as_deref()),
            Some(ACTORS.options.go_package)
        );
        assert_eq!(file.message_type.len(), ACTORS.messages.len());
        for expected in ACTORS.messages {
            let actual = file
                .message_type
                .iter()
                .find(|message| message.name.as_deref() == Some(expected.name))
                .unwrap_or_else(|| panic!("missing message {}", expected.name));
            assert_message(expected, actual);
        }
        assert_eq!(file.enum_type.len(), ACTORS.enums.len());
        for expected in ACTORS.enums {
            let actual = file
                .enum_type
                .iter()
                .find(|enum_| enum_.name.as_deref() == Some(expected.name))
                .unwrap_or_else(|| panic!("missing enum {}", expected.name));
            assert_eq!(
                actual.value.len(),
                expected.values.len(),
                "enum {}",
                expected.name
            );
            for value in expected.values {
                let actual_value = actual
                    .value
                    .iter()
                    .find(|candidate| candidate.name.as_deref() == Some(value.name))
                    .unwrap_or_else(|| panic!("missing enum value {}", value.name));
                assert_eq!(actual_value.number, Some(value.number));
            }
        }
        assert_eq!(file.service.len(), ACTORS.services.len());
        for expected in ACTORS.services {
            let actual = file
                .service
                .iter()
                .find(|service| service.name.as_deref() == Some(expected.name))
                .unwrap_or_else(|| panic!("missing service {}", expected.name));
            assert_eq!(actual.method.len(), expected.methods.len());
            for method in expected.methods {
                let actual_method = actual
                    .method
                    .iter()
                    .find(|candidate| candidate.name.as_deref() == Some(method.name))
                    .unwrap_or_else(|| panic!("missing method {}", method.name));
                let input = format!(".{}.{}", ACTORS.package, method.input);
                let output = format!(".{}.{}", ACTORS.package, method.output);
                assert_eq!(actual_method.input_type.as_deref(), Some(input.as_str()));
                assert_eq!(actual_method.output_type.as_deref(), Some(output.as_str()));
                assert_eq!(
                    actual_method.client_streaming.unwrap_or(false),
                    method.client_streaming
                );
                assert_eq!(
                    actual_method.server_streaming.unwrap_or(false),
                    method.server_streaming
                );
            }
        }
    }

    fn assert_message(expected: &MessageSpec, actual: &DescriptorProto) {
        assert_eq!(
            actual.field.len(),
            expected.fields.len(),
            "message {}",
            expected.name
        );
        assert_eq!(
            actual.oneof_decl.len(),
            expected.oneofs.len(),
            "oneof count {}",
            expected.name
        );
        for oneof in expected.oneofs {
            let actual_oneof = actual
                .oneof_decl
                .iter()
                .find(|candidate| candidate.name.as_deref() == Some(oneof.name))
                .unwrap_or_else(|| panic!("missing oneof {}", oneof.name));
            let index = actual
                .oneof_decl
                .iter()
                .position(|candidate| candidate.name == actual_oneof.name)
                .unwrap() as i32;
            let synthetic = actual.field.iter().any(|field| {
                field.oneof_index == Some(index) && field.proto3_optional == Some(true)
            });
            assert_eq!(synthetic, oneof.synthetic, "oneof {}", oneof.name);
        }
        for expected_field in expected.fields {
            let actual_field = actual
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some(expected_field.name))
                .unwrap_or_else(|| {
                    panic!("missing field {}.{}", expected.name, expected_field.name)
                });
            assert_eq!(actual_field.number, Some(expected_field.number as i32));
            assert_eq!(
                actual_field.json_name.as_deref(),
                Some(expected_field.json_name)
            );
            assert_eq!(
                actual_field.proto3_optional.unwrap_or(false),
                expected_field.proto3_optional
            );
            assert_eq!(
                actual_field.label,
                Some(label(expected_field.cardinality) as i32)
            );
            assert_eq!(
                actual_field.r#type,
                Some(field_type(expected_field.field_type) as i32)
            );
            let expected_type_name = match expected_field.field_type {
                FieldType::Message(name) | FieldType::Enum(name) => {
                    Some(format!(".{}.{}", ACTORS.package, name))
                }
                _ => None,
            };
            assert_eq!(actual_field.type_name, expected_type_name);
            let expected_oneof_index = expected_field.oneof.and_then(|name| {
                expected
                    .oneofs
                    .iter()
                    .position(|oneof| oneof.name == name)
                    .map(|index| index as i32)
            });
            assert_eq!(actual_field.oneof_index, expected_oneof_index);
        }
    }

    fn label(cardinality: Cardinality) -> field_descriptor_proto::Label {
        match cardinality {
            Cardinality::Singular | Cardinality::Optional => {
                field_descriptor_proto::Label::Optional
            }
            Cardinality::Repeated => field_descriptor_proto::Label::Repeated,
        }
    }
    fn field_type(field_type: FieldType) -> field_descriptor_proto::Type {
        match field_type {
            FieldType::String => field_descriptor_proto::Type::String,
            FieldType::Bytes => field_descriptor_proto::Type::Bytes,
            FieldType::Bool => field_descriptor_proto::Type::Bool,
            FieldType::Int64 => field_descriptor_proto::Type::Int64,
            FieldType::Sint64 => field_descriptor_proto::Type::Sint64,
            FieldType::Uint32 => field_descriptor_proto::Type::Uint32,
            FieldType::Uint64 => field_descriptor_proto::Type::Uint64,
            FieldType::Message(_) | FieldType::ExternalMessage(_) => {
                field_descriptor_proto::Type::Message
            }
            FieldType::Enum(_) => field_descriptor_proto::Type::Enum,
        }
    }
}
