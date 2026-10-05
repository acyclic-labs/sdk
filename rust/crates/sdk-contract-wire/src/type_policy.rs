//! Rust-owned semantic type policy for every generated SDK.
//!
//! Protobuf descriptors preserve the transport contract, but a descriptor
//! alone cannot express the strongest useful API type in each target
//! language.  This module is the one additional, Rust-authored metadata
//! boundary for that information.  Generators must consume these tables and
//! may not replace them with per-language hand-written contracts.
//!
//! The policy deliberately distinguishes three things:
//!
//! * wire storage, which must remain compatible with the archived descriptor;
//! * semantic constraints, some of which are stricter than Rust's type
//!   system (for example a fixed digest length or a non-empty value); and
//! * the idiomatic target-language representation used to expose those
//!   constraints to a compiler or static checker.
//!
//! Unknown enum numbers and unknown oneof arms are always preserved.  A
//! target may expose a closed, exhaustive convenience view only in addition
//! to the open wire representation; it must never discard an unknown value.

/// Every language target currently inventoried by the generation pipeline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TypePolicyLanguage {
    Rust,
    TypeScript,
    Python,
    Go,
    Java,
    CSharp,
    Swift,
    Cpp,
    Ruby,
    Php,
    Dart,
    Kotlin,
    Scala,
    Elixir,
    Ballerina,
    ObjectiveC,
    Erlang,
    Ocaml,
    CommonLisp,
    Ada,
    C,
    Clojure,
    Crystal,
    Elm,
    Gdscript,
    Julia,
    Nim,
    Perl,
    Powershell,
    R,
    Bash,
    Haskell,
    Lua,
}

impl TypePolicyLanguage {
    /// Stable inventory identifier used by generators and receipts.
    pub const fn id(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Python => "python",
            Self::Go => "go",
            Self::Java => "java",
            Self::CSharp => "csharp",
            Self::Swift => "swift",
            Self::Cpp => "cpp",
            Self::Ruby => "ruby",
            Self::Php => "php",
            Self::Dart => "dart",
            Self::Kotlin => "kotlin",
            Self::Scala => "scala",
            Self::Elixir => "elixir",
            Self::Ballerina => "ballerina",
            Self::ObjectiveC => "objective-c",
            Self::Erlang => "erlang",
            Self::Ocaml => "ocaml",
            Self::CommonLisp => "common-lisp",
            Self::Ada => "ada",
            Self::C => "c",
            Self::Clojure => "clojure",
            Self::Crystal => "crystal",
            Self::Elm => "elm",
            Self::Gdscript => "gdscript",
            Self::Julia => "julia",
            Self::Nim => "nim",
            Self::Perl => "perl",
            Self::Powershell => "powershell",
            Self::R => "r",
            Self::Bash => "bash",
            Self::Haskell => "haskell",
            Self::Lua => "lua",
        }
    }

    /// The complete target set, excluding the non-language documentation
    /// target.  Keep this list in Rust so the generator cannot silently omit
    /// a language when the external inventory changes.
    pub const ALL: &'static [Self] = &[
        Self::Rust, Self::TypeScript, Self::Python, Self::Go, Self::Java,
        Self::CSharp, Self::Swift, Self::Cpp, Self::Ruby, Self::Php, Self::Dart,
        Self::Kotlin, Self::Scala, Self::Elixir, Self::Ballerina, Self::ObjectiveC,
        Self::Erlang, Self::Ocaml, Self::CommonLisp, Self::Ada, Self::C,
        Self::Clojure, Self::Crystal, Self::Elm, Self::Gdscript, Self::Julia,
        Self::Nim, Self::Perl, Self::Powershell, Self::R, Self::Bash, Self::Haskell,
        Self::Lua,
    ];
}

/// Wire representation of a semantic value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireValueKind {
    String,
    Bytes,
    SignedInteger,
    UnsignedInteger,
    Boolean,
    Timestamp,
    Enum,
    Message,
    Oneof,
}

/// A semantic rule that generated APIs must expose when the target can do so.
///
/// Rules are intentionally identifiers rather than free-form generator
/// instructions.  Their meaning is stable, and each language profile below
/// states the construct used to enforce them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticRule {
    NonEmpty,
    Utf8,
    NonNegative,
    StrictlyPositive,
    FixedLength(u16),
    MaxBytes(u32),
    MaxItems(u32),
    BoundedInteger { min: i64, max: i64 },
    Sha256Digest,
    Immutable,
    Monotonic,
    CanonicalResourceName,
    ExactOneof,
    ExplicitPresence,
    PreserveUnknownEnum,
    PreserveUnknownOneof,
}

/// One Rust-owned semantic type.  `rust_name` is documentation and generator
/// provenance; the wire kind remains explicit so a projection cannot change
/// the protocol while adding a stronger local type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticType {
    pub id: &'static str,
    pub rust_name: &'static str,
    pub wire_kind: WireValueKind,
    pub rules: &'static [SemanticRule],
}

/// Explicit link from a Rust model field name to its semantic type.
///
/// The protobuf field keeps its original wire name and number; this table adds
/// the source-owned meaning that generated targets use for nominal wrappers,
/// refinement constructors, and exhaustive/open union views.  A family scope
/// prevents an incidental field name such as `name` or `key` from acquiring a
/// meaning from another contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldSemanticType {
    pub family: &'static str,
    pub field: &'static str,
    pub semantic_type: &'static str,
}

/// Exact public protobuf location for a semantic field.
///
/// The semantic field names above are stable Rust vocabulary.  This second
/// Rust-owned table records where each value is actually constructed or
/// decoded in the public SDK facade, so a generator cannot satisfy typing
/// qualification with a detached sidecar model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicFieldDirection {
    Request,
    Response,
    NestedMessage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicFieldBinding {
    pub family: &'static str,
    pub field: &'static str,
    pub semantic_type: &'static str,
    pub module: &'static str,
    pub message: &'static str,
    pub wire_field: &'static str,
    pub direction: PublicFieldDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicNestedFieldKind {
    Text,
    Message(&'static str),
}

/// Rust-owned descriptions of production requests whose nested message has a
/// refined field.  These routes keep the nested semantic wrapper in the
/// actual public client signature instead of leaving it as a detached helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicNestedRoute {
    pub family: &'static str,
    pub operation: &'static str,
    pub module: &'static str,
    pub request_message: &'static str,
    pub nested_message: &'static str,
    pub nested_field: &'static str,
    pub semantic_field: &'static str,
    pub client_attribute: &'static str,
    pub rpc: &'static str,
    pub response: &'static str,
    pub fields: &'static [(&'static str, PublicNestedFieldKind)],
}

impl PublicFieldBinding {
    /// Service client attribute used by generated remote facades.
    pub fn client_attribute(self) -> Option<&'static str> {
        match (self.module, self.message) {
            ("actors", "InvokeActorRequest") => Some("actors"),
            ("workers", "SelectDeploymentRequest")
            | ("workers", "InspectJobRequest")
            | ("workers", "InvokeVersionRequest") => Some("workers"),
            ("stream", "AppendRequest")
            | ("stream", "ForkRequest")
            | ("stream", "ReadRequest")
            | ("stream", "ReadCommitRequest") => Some("stream"),
            ("objects", "GetObjectRequest") | ("objects", "ListObjectsRequest") => Some("objects"),
            ("objects", "ListPartsRequest") => Some("multipart"),
            ("inference", "InspectRunRequest") => Some("inference.runs"),
            ("inference", "InspectContextRequest") => Some("inference.contexts"),
            ("inference", "InspectWarmRequest") => Some("inference.warm_contexts"),
            ("inference", "InspectEvaluationRequest") => Some("inference.evaluations"),
            ("machines", "CreateMachineRequest")
            | ("machines", "InspectMachineRequest")
            | ("machines", "InspectCheckpointRequest")
            | ("machines", "OperationRequest")
            | ("machines", "ListMachinesRequest") => Some("machines"),
            ("filesystem", "ReadRequest") => Some("filesystem"),
            _ => None,
        }
    }

    /// RPC method corresponding to the concrete request message.
    pub fn rpc(self) -> Option<&'static str> {
        match (self.module, self.message) {
            (_, "InvokeActorRequest") => Some("InvokeActor"),
            (_, "SelectDeploymentRequest") => Some("SelectDeployment"),
            (_, "InspectJobRequest") => Some("InspectJob"),
            (_, "InvokeVersionRequest") => Some("InvokeVersion"),
            (_, "AppendRequest") => Some("Append"),
            (_, "ForkRequest") => Some("Fork"),
            (_, "ReadRequest") => Some("Read"),
            (_, "ReadCommitRequest") => Some("ReadCommit"),
            (_, "GetObjectRequest") => Some("GetObject"),
            (_, "ListObjectsRequest") => Some("ListObjects"),
            (_, "ListPartsRequest") => Some("ListParts"),
            (_, "InspectRunRequest") => Some("Inspect"),
            (_, "InspectContextRequest") => Some("Inspect"),
            (_, "InspectWarmRequest") => Some("Inspect"),
            (_, "InspectEvaluationRequest") => Some("Inspect"),
            (_, "CreateMachineRequest") => Some("Create"),
            (_, "InspectMachineRequest") => Some("InspectMachine"),
            (_, "InspectCheckpointRequest") => Some("InspectCheckpoint"),
            (_, "OperationRequest") => Some("InspectOperation"),
            (_, "ListMachinesRequest") => Some("ListMachines"),
            _ => None,
        }
    }
}

/// A Rust-owned discriminated union projection.  The open `unknown` arm is
/// part of the wire contract, while the `known` arm gives every target a
/// statically visible payload-bearing variant for values understood by the
/// current SDK revision.  Targets may add family-specific known arms later,
/// but they must preserve this open pair when decoding newer senders.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireUnionVariant {
    pub union: &'static str,
    pub variant: &'static str,
    pub tag: &'static str,
    pub payload_wire_kind: WireValueKind,
}

const NON_EMPTY_UTF8: &[SemanticRule] = &[SemanticRule::NonEmpty, SemanticRule::Utf8];
const NON_EMPTY_BYTES: &[SemanticRule] = &[SemanticRule::NonEmpty];
const DIGEST: &[SemanticRule] = &[SemanticRule::FixedLength(32), SemanticRule::Sha256Digest];
const FIXED_32_BYTES: &[SemanticRule] = &[SemanticRule::FixedLength(32)];
const NON_NEGATIVE: &[SemanticRule] = &[SemanticRule::NonNegative];
const PRESENT_ONEOF: &[SemanticRule] = &[SemanticRule::ExactOneof, SemanticRule::PreserveUnknownOneof];
const IMMUTABLE_MESSAGE: &[SemanticRule] = &[SemanticRule::Immutable];

/// Shared semantic vocabulary used by all families.  Family models refer to
/// these names from their Rust validation metadata instead of introducing a
/// second target-language contract.
pub const SEMANTIC_TYPES: &[SemanticType] = &[
    SemanticType { id: "actor_id", rust_name: "ActorId", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "method", rust_name: "MethodName", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "path", rust_name: "ResourcePath", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "source", rust_name: "SourceName", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "destination", rust_name: "DestinationName", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "bucket_name", rust_name: "BucketName", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "object_key", rust_name: "ObjectKey", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "alias", rust_name: "VersionAlias", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "job_id", rust_name: "JobId", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "machine_id", rust_name: "MachineId", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "operation_id", rust_name: "OperationId", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "checkpoint_id", rust_name: "CheckpointId", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "idempotency_key_bytes", rust_name: "IdempotencyKeyBytes", wire_kind: WireValueKind::Bytes, rules: NON_EMPTY_BYTES },
    SemanticType { id: "idempotency_key_text", rust_name: "IdempotencyKeyText", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "idempotency_key_message", rust_name: "IdempotencyKey", wire_kind: WireValueKind::Message, rules: &[] },
    SemanticType { id: "opaque_text", rust_name: "OpaqueText", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "upload_id", rust_name: "UploadId", wire_kind: WireValueKind::String, rules: NON_EMPTY_UTF8 },
    SemanticType { id: "version_sha256", rust_name: "Sha256Digest", wire_kind: WireValueKind::Bytes, rules: DIGEST },
    SemanticType { id: "sha256_digest", rust_name: "Sha256Digest", wire_kind: WireValueKind::Bytes, rules: DIGEST },
    SemanticType { id: "revision_digest", rust_name: "RevisionDigest", wire_kind: WireValueKind::Bytes, rules: FIXED_32_BYTES },
    SemanticType { id: "immutable_image", rust_name: "Image", wire_kind: WireValueKind::Message, rules: IMMUTABLE_MESSAGE },
    SemanticType { id: "revision", rust_name: "Revision", wire_kind: WireValueKind::UnsignedInteger, rules: NON_NEGATIVE },
    SemanticType { id: "run_id", rust_name: "RunId", wire_kind: WireValueKind::Bytes, rules: &[SemanticRule::FixedLength(16)] },
    SemanticType { id: "evaluation_id", rust_name: "EvaluationId", wire_kind: WireValueKind::Bytes, rules: &[SemanticRule::FixedLength(16)] },
    SemanticType { id: "page_limit", rust_name: "PageLimit", wire_kind: WireValueKind::UnsignedInteger, rules: &[SemanticRule::StrictlyPositive, SemanticRule::MaxItems(1000)] },
    SemanticType { id: "commit_id", rust_name: "CommitId", wire_kind: WireValueKind::Bytes, rules: &[SemanticRule::NonEmpty] },
    SemanticType { id: "enum_value", rust_name: "OpenEnumValue", wire_kind: WireValueKind::Enum, rules: &[SemanticRule::PreserveUnknownEnum] },
    SemanticType { id: "oneof_arm", rust_name: "WireChoice", wire_kind: WireValueKind::Oneof, rules: PRESENT_ONEOF },
];

/// Rust-owned field mappings consumed by every target generator.
///
/// Fields absent from this table are ordinary wire values.  A generator must
/// fail closed if it sees a constrained validation rule without a mapping;
/// it may never silently project a constrained field as an unbranded scalar.
pub const FIELD_SEMANTIC_TYPES: &[FieldSemanticType] = &[
    FieldSemanticType { family: "actors", field: "actor_id", semantic_type: "actor_id" },
    FieldSemanticType { family: "actors", field: "method", semantic_type: "method" },
    FieldSemanticType { family: "workers", field: "alias", semantic_type: "alias" },
    FieldSemanticType { family: "workers", field: "version_sha256", semantic_type: "version_sha256" },
    FieldSemanticType { family: "workers", field: "idempotency_key", semantic_type: "idempotency_key_text" },
    FieldSemanticType { family: "workers", field: "job_id", semantic_type: "job_id" },
    FieldSemanticType { family: "workers", field: "method", semantic_type: "method" },
    FieldSemanticType { family: "stream", field: "idempotency_key", semantic_type: "idempotency_key_bytes" },
    FieldSemanticType { family: "stream", field: "path", semantic_type: "path" },
    FieldSemanticType { family: "stream", field: "source", semantic_type: "source" },
    FieldSemanticType { family: "stream", field: "destination", semantic_type: "destination" },
    FieldSemanticType { family: "stream", field: "limit", semantic_type: "page_limit" },
    FieldSemanticType { family: "stream", field: "commit_id", semantic_type: "commit_id" },
    FieldSemanticType { family: "objects", field: "key", semantic_type: "object_key" },
    FieldSemanticType { family: "objects", field: "etag", semantic_type: "opaque_text" },
    FieldSemanticType { family: "objects", field: "idempotency_key", semantic_type: "idempotency_key_text" },
    FieldSemanticType { family: "objects", field: "upload_id", semantic_type: "upload_id" },
    FieldSemanticType { family: "objects", field: "page_size", semantic_type: "page_limit" },
    FieldSemanticType { family: "inference", field: "run_id", semantic_type: "run_id" },
    FieldSemanticType { family: "inference", field: "revision", semantic_type: "revision_digest" },
    FieldSemanticType { family: "inference", field: "commitment", semantic_type: "sha256_digest" },
    FieldSemanticType { family: "inference", field: "evaluation_id", semantic_type: "evaluation_id" },
    FieldSemanticType { family: "inference", field: "spec_digest", semantic_type: "sha256_digest" },
    FieldSemanticType { family: "machines", field: "image", semantic_type: "immutable_image" },
    FieldSemanticType { family: "machines", field: "idempotency_key", semantic_type: "idempotency_key_message" },
    FieldSemanticType { family: "machines", field: "machine_id", semantic_type: "machine_id" },
    FieldSemanticType { family: "machines", field: "checkpoint_id", semantic_type: "checkpoint_id" },
    FieldSemanticType { family: "machines", field: "operation_id", semantic_type: "operation_id" },
    FieldSemanticType { family: "machines", field: "page_limit", semantic_type: "page_limit" },
    FieldSemanticType { family: "filesystem", field: "path", semantic_type: "path" },
    FieldSemanticType { family: "harness", field: "path", semantic_type: "path" },
];

/// Every mapped semantic field is attached to a real public request, response,
/// or nested protobuf message.  Keep this list in Rust beside the semantic
/// vocabulary; generated targets use it to emit lossless `to_wire` and
/// `from_wire` bridges.
pub const PUBLIC_FIELD_BINDINGS: &[PublicFieldBinding] = &[
    PublicFieldBinding { family: "actors", field: "actor_id", semantic_type: "actor_id", module: "actors", message: "InvokeActorRequest", wire_field: "actor_id", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "actors", field: "method", semantic_type: "method", module: "actors", message: "InvokeActorRequest", wire_field: "method", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "workers", field: "alias", semantic_type: "alias", module: "workers", message: "SelectDeploymentRequest", wire_field: "alias", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "workers", field: "version_sha256", semantic_type: "version_sha256", module: "workers", message: "SelectDeploymentRequest", wire_field: "version_sha256", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "workers", field: "idempotency_key", semantic_type: "idempotency_key_text", module: "workers", message: "SelectDeploymentRequest", wire_field: "idempotency_key", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "workers", field: "job_id", semantic_type: "job_id", module: "workers", message: "InspectJobRequest", wire_field: "job_id", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "workers", field: "method", semantic_type: "method", module: "workers", message: "InvokeVersionRequest", wire_field: "method", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "stream", field: "idempotency_key", semantic_type: "idempotency_key_bytes", module: "stream", message: "AppendRequest", wire_field: "idempotency_key", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "stream", field: "path", semantic_type: "path", module: "stream", message: "AppendRequest", wire_field: "path", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "stream", field: "source", semantic_type: "source", module: "stream", message: "ForkRequest", wire_field: "source", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "stream", field: "destination", semantic_type: "destination", module: "stream", message: "ForkRequest", wire_field: "destination", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "stream", field: "limit", semantic_type: "page_limit", module: "stream", message: "ReadRequest", wire_field: "limit", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "stream", field: "commit_id", semantic_type: "commit_id", module: "stream", message: "ReadCommitRequest", wire_field: "commit_id", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "objects", field: "key", semantic_type: "object_key", module: "objects", message: "GetObjectRequest", wire_field: "object_key", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "objects", field: "etag", semantic_type: "opaque_text", module: "objects", message: "ObjectInfo", wire_field: "etag", direction: PublicFieldDirection::Response },
    PublicFieldBinding { family: "objects", field: "idempotency_key", semantic_type: "idempotency_key_text", module: "objects", message: "MutationIdentity", wire_field: "idempotency_key", direction: PublicFieldDirection::NestedMessage },
    PublicFieldBinding { family: "objects", field: "upload_id", semantic_type: "upload_id", module: "objects", message: "ListPartsRequest", wire_field: "upload_id", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "objects", field: "page_size", semantic_type: "page_limit", module: "objects", message: "ListObjectsRequest", wire_field: "page_size", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "inference", field: "run_id", semantic_type: "run_id", module: "inference", message: "InspectRunRequest", wire_field: "run_id", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "inference", field: "revision", semantic_type: "revision_digest", module: "inference", message: "InspectContextRequest", wire_field: "revision", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "inference", field: "commitment", semantic_type: "sha256_digest", module: "inference", message: "InspectWarmRequest", wire_field: "commitment", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "inference", field: "evaluation_id", semantic_type: "evaluation_id", module: "inference", message: "InspectEvaluationRequest", wire_field: "evaluation_id", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "inference", field: "spec_digest", semantic_type: "sha256_digest", module: "inference", message: "EvaluationSpec", wire_field: "spec_digest", direction: PublicFieldDirection::NestedMessage },
    PublicFieldBinding { family: "machines", field: "image", semantic_type: "immutable_image", module: "machines", message: "CreateMachineRequest", wire_field: "image", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "machines", field: "idempotency_key", semantic_type: "idempotency_key_message", module: "machines", message: "CreateMachineRequest", wire_field: "idempotency_key", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "machines", field: "machine_id", semantic_type: "machine_id", module: "machines", message: "InspectMachineRequest", wire_field: "machine", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "machines", field: "checkpoint_id", semantic_type: "checkpoint_id", module: "machines", message: "InspectCheckpointRequest", wire_field: "checkpoint", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "machines", field: "operation_id", semantic_type: "operation_id", module: "machines", message: "OperationRequest", wire_field: "operation", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "machines", field: "page_limit", semantic_type: "page_limit", module: "machines", message: "ListMachinesRequest", wire_field: "limit", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "filesystem", field: "path", semantic_type: "path", module: "filesystem", message: "ReadRequest", wire_field: "path", direction: PublicFieldDirection::Request },
    PublicFieldBinding { family: "harness", field: "path", semantic_type: "path", module: "harness", message: "FileRef", wire_field: "normalized_path", direction: PublicFieldDirection::Response },
];

const CREATE_BUCKET_FIELDS: &[(&str, PublicNestedFieldKind)] = &[
    ("name", PublicNestedFieldKind::Text),
];
const CREATE_EVALUATION_FIELDS: &[(&str, PublicNestedFieldKind)] = &[
    ("identity", PublicNestedFieldKind::Message("RequestIdentity")),
    ("spec", PublicNestedFieldKind::Message("EvaluationSpec")),
];

pub const PUBLIC_NESTED_ROUTES: &[PublicNestedRoute] = &[
    PublicNestedRoute {
        family: "objects",
        operation: "create_bucket",
        module: "objects",
        request_message: "CreateBucketRequest",
        nested_message: "MutationIdentity",
        nested_field: "mutation",
        semantic_field: "idempotency_key",
        client_attribute: "buckets",
        rpc: "CreateBucket",
        response: "Bucket",
        fields: CREATE_BUCKET_FIELDS,
    },
    PublicNestedRoute {
        family: "inference",
        operation: "create_evaluation",
        module: "inference",
        request_message: "CreateEvaluationRequest",
        nested_message: "EvaluationSpec",
        nested_field: "spec",
        semantic_field: "spec_digest",
        client_attribute: "inference.evaluations",
        rpc: "Create",
        response: "EvaluationView",
        fields: CREATE_EVALUATION_FIELDS,
    },
];

/// Discriminants and payload kinds for every open union emitted by the type
/// policy.  This is deliberately authored beside the Rust semantic model so
/// target generators cannot silently collapse a oneof into `any`/`object`.
pub const WIRE_UNION_VARIANTS: &[WireUnionVariant] = &[
    WireUnionVariant {
        union: "wire_choice",
        variant: "KnownOneof",
        tag: "known",
        payload_wire_kind: WireValueKind::Message,
    },
    WireUnionVariant {
        union: "wire_choice",
        variant: "UnknownOneof",
        tag: "unknown",
        payload_wire_kind: WireValueKind::Bytes,
    },
];

/// How a target should expose optional fields, unions and open enums.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeProjectionProfile {
    pub language: TypePolicyLanguage,
    /// Nominal type mechanism for semantic strings, bytes and integer IDs.
    pub nominal_types: &'static str,
    /// Exhaustive/discriminated union mechanism, if the language provides one.
    pub unions: &'static str,
    /// Static refinement mechanism for `SemanticRule` values.
    pub refinements: &'static str,
    /// Presence/nullability representation; absence and explicit defaults must
    /// remain distinguishable whenever the descriptor says they are.
    pub presence: &'static str,
    /// Representation of unknown enum numbers and oneof arms.
    pub unknown_values: &'static str,
    /// Numeric representation that preserves protobuf uint64/int64 values.
    pub integers: &'static str,
    /// Static checker/compiler invoked by the qualification lane.
    pub checker: &'static str,
}

/// Rust-owned strongest practical type profile for every inventoried language.
///
/// The wording is intentionally generator-facing and testable: it names the
/// language feature to emit, not a suggestion to hand-author a facade.
pub const TYPE_PROJECTION_PROFILES: &[TypeProjectionProfile] = &[
    TypeProjectionProfile { language: TypePolicyLanguage::Rust, nominal_types: "newtype structs with private fields", unions: "enum with non_exhaustive unknown arm", refinements: "TryFrom constructors and typed errors", presence: "Option<T> plus prost presence", unknown_values: "raw enum number and UnknownOneof payload", integers: "u64/i64", checker: "cargo check + clippy" },
    TypeProjectionProfile { language: TypePolicyLanguage::TypeScript, nominal_types: "branded string/number/Uint8Array types", unions: "discriminated readonly unions with never exhaustiveness", refinements: "user-defined asserts and branded constructors", presence: "exactOptionalPropertyTypes and explicit null", unknown_values: "Unknown(raw number) enum arm and UnknownOneof object", integers: "bigint for uint64; never number", checker: "tsc --strict + eslint type rules" },
    TypeProjectionProfile { language: TypePolicyLanguage::Python, nominal_types: "NewType plus Annotated validators", unions: "Literal discriminants, TypedDict and Never", refinements: "Annotated metadata with runtime constructor", presence: "NotRequired versus Optional under strict mode", unknown_values: "IntEnum fallback carrying raw integer and UnknownOneof", integers: "int with range validators; no float coercion", checker: "pyright strict + mypy strict" },
    TypeProjectionProfile { language: TypePolicyLanguage::Go, nominal_types: "defined string/byte/integer types with constructors", unions: "tagged structs and sealed interfaces where possible", refinements: "constructor errors and validation methods", presence: "pointers/wrappers for explicit presence", unknown_values: "enum raw value plus UnknownOneof bytes", integers: "uint64/int64; never float", checker: "go vet + go test + staticcheck" },
    TypeProjectionProfile { language: TypePolicyLanguage::Java, nominal_types: "records/value wrappers and private constructors", unions: "sealed interfaces plus records", refinements: "factory methods returning typed validation errors", presence: "optional wrappers only where presence is explicit", unknown_values: "UNRECOGNIZED plus raw number and UnknownOneof", integers: "long with unsigned helper/value wrapper", checker: "javac -Xlint + Error Prone" },
    TypeProjectionProfile { language: TypePolicyLanguage::CSharp, nominal_types: "readonly record structs and required members", unions: "sealed record hierarchy with exhaustive switch", refinements: "static factories and nullable flow analysis", presence: "nullable annotations plus Optional wrapper", unknown_values: "raw enum integer and UnknownOneof record", integers: "ulong/long; checked conversions", checker: "dotnet build nullable=enable + analyzers" },
    TypeProjectionProfile { language: TypePolicyLanguage::Swift, nominal_types: "struct wrappers with failable initializers", unions: "enum associated values", refinements: "init? / throws and validation types", presence: "Optional only for explicit presence", unknown_values: "unknown(rawValue:) enum case and unknown oneof", integers: "UInt64/Int64", checker: "swiftc -warnings-as-errors" },
    TypeProjectionProfile { language: TypePolicyLanguage::Cpp, nominal_types: "strong typedef structs and explicit constructors", unions: "std::variant with Unknown alternative", refinements: "expected<T, Error> and constexpr checks", presence: "std::optional only for explicit presence", unknown_values: "raw enum value and variant UnknownOneof", integers: "std::uint64_t/std::int64_t", checker: "clang++ -std=c++20 -Werror" },
    TypeProjectionProfile { language: TypePolicyLanguage::Ruby, nominal_types: "value objects with private constructors", unions: "tagged immutable value objects", refinements: "constructor validation and typed result objects", presence: "explicit sentinel versus nil", unknown_values: "open enum raw integer wrapper and unknown oneof", integers: "Integer with no narrowing", checker: "ruby syntax + runtime contract tests" },
    TypeProjectionProfile { language: TypePolicyLanguage::Php, nominal_types: "readonly value objects and enums", unions: "sealed-style interfaces plus readonly records", refinements: "named constructors and exceptions", presence: "nullability plus explicit presence wrapper", unknown_values: "Unknown(raw enum integer) and UnknownOneof", integers: "int with string fallback for uint64", checker: "phpstan level max + PHPUnit" },
    TypeProjectionProfile { language: TypePolicyLanguage::Dart, nominal_types: "sealed class value objects", unions: "sealed classes with pattern matching", refinements: "factory constructors and Result types", presence: "nullable only when descriptor permits", unknown_values: "unknown raw enum integer and UnknownOneof subclass", integers: "int (64-bit) with checked JSON", checker: "dart analyze --fatal-infos + test" },
    TypeProjectionProfile { language: TypePolicyLanguage::Kotlin, nominal_types: "@JvmInline value classes", unions: "sealed interfaces/data classes", refinements: "require/factory returning typed failures", presence: "nullable only for explicit presence", unknown_values: "UNRECOGNIZED(raw) and UnknownOneof", integers: "ULong/Long with protobuf adapters", checker: "kotlinc + detekt" },
    TypeProjectionProfile { language: TypePolicyLanguage::Scala, nominal_types: "opaque types and case classes", unions: "sealed traits with case classes", refinements: "refined constructors and Either", presence: "Option only for explicit presence", unknown_values: "open enum Unknown case carrying raw integer", integers: "Long with UInt64 value class", checker: "scalac -Xfatal-warnings + munit" },
    TypeProjectionProfile { language: TypePolicyLanguage::Elixir, nominal_types: "typed structs and opaque typespecs", unions: "tagged tuples with @type", refinements: "guards and changesets", presence: "absent key versus nil", unknown_values: "raw enum integer and unknown tagged tuple", integers: "integer with range guards", checker: "mix format + dialyzer + ExUnit" },
    TypeProjectionProfile { language: TypePolicyLanguage::Ballerina, nominal_types: "distinct types and readonly records", unions: "typed unions with match exhaustiveness", refinements: "distinct constructors and error values", presence: "optional fields versus nil", unknown_values: "open enum raw integer and unknown union member", integers: "int/decimal with explicit uint64 codec", checker: "bal build + tests" },
    TypeProjectionProfile { language: TypePolicyLanguage::ObjectiveC, nominal_types: "class wrappers and NS_SWIFT_NAME annotations", unions: "tagged classes with Unknown subclass", refinements: "nullable designated initializers", presence: "nullable annotations plus presence bit", unknown_values: "raw enum number and unknown object", integers: "uint64_t/int64_t", checker: "clang -Weverything" },
    TypeProjectionProfile { language: TypePolicyLanguage::Erlang, nominal_types: "opaque types and dialyzer specs", unions: "tagged tuples", refinements: "guards and error tuples", presence: "absent map key versus undefined", unknown_values: "raw enum integer and unknown tuple", integers: "integer guards", checker: "rebar3 dialyzer + EUnit" },
    TypeProjectionProfile { language: TypePolicyLanguage::Ocaml, nominal_types: "private type aliases and modules", unions: "polymorphic variants or closed variants", refinements: "smart constructors returning result", presence: "option only for explicit presence", unknown_values: "open Unknown polymorphic variant carrying raw number", integers: "int64/int32 exact codecs", checker: "dune build + warnings-as-errors" },
    TypeProjectionProfile { language: TypePolicyLanguage::CommonLisp, nominal_types: "deftype plus constructor predicates", unions: "tagged structures", refinements: "type declarations and condition types", presence: "sentinel distinct from NIL", unknown_values: "raw enum integer and unknown structure", integers: "(unsigned-byte 64)/(signed-byte 64)", checker: "SBCL compile + runtime vectors" },
    TypeProjectionProfile { language: TypePolicyLanguage::Ada, nominal_types: "subtypes and private types", unions: "variant records", refinements: "range constraints and predicates", presence: "presence discriminant", unknown_values: "raw enum representation and unknown variant", integers: "Interfaces.Unsigned_64/Integer_64", checker: "gnatmake -gnatwae" },
    TypeProjectionProfile { language: TypePolicyLanguage::C, nominal_types: "opaque structs and constructor functions", unions: "tagged unions with explicit Unknown", refinements: "status-returning validators", presence: "has_field bit plus value", unknown_values: "raw enum integer and unknown union tag", integers: "uint64_t/int64_t", checker: "clang -Wall -Wextra -Werror" },
    TypeProjectionProfile { language: TypePolicyLanguage::Clojure, nominal_types: "specs and namespaced maps", unions: "tagged maps", refinements: "clojure.spec.alpha", presence: "contains? versus nil", unknown_values: "raw enum integer and unknown tag", integers: "long/bigint exact", checker: "clj-kondo + spec tests" },
    TypeProjectionProfile { language: TypePolicyLanguage::Crystal, nominal_types: "struct wrappers and aliases", unions: "union types with tagged wrappers", refinements: "constructor validation", presence: "Nil union only when explicit", unknown_values: "raw enum integer and unknown wrapper", integers: "UInt64/Int64", checker: "crystal build --error-trace" },
    TypeProjectionProfile { language: TypePolicyLanguage::Elm, nominal_types: "custom types and opaque modules", unions: "custom type constructors", refinements: "smart constructors returning Result", presence: "Maybe only for explicit presence", unknown_values: "Unknown(raw Int) constructor", integers: "Int with Bytes codec for uint64", checker: "elm make" },
    TypeProjectionProfile { language: TypePolicyLanguage::Gdscript, nominal_types: "typed classes and exported typed fields", unions: "tagged classes", refinements: "runtime validators with typed return", presence: "explicit has-field flag", unknown_values: "raw enum integer and unknown class", integers: "int with checked codec", checker: "godot --headless --check-only" },
    TypeProjectionProfile { language: TypePolicyLanguage::Julia, nominal_types: "primitive/value structs", unions: "Union types with tagged structs", refinements: "inner constructors and type assertions", presence: "Missing versus Nothing", unknown_values: "raw enum integer and unknown struct", integers: "UInt64/Int64", checker: "Julia package tests" },
    TypeProjectionProfile { language: TypePolicyLanguage::Nim, nominal_types: "distinct types", unions: "object variants", refinements: "validated constructors and concepts", presence: "Option[T] for explicit presence", unknown_values: "raw enum integer and unknown object", integers: "uint64/int64", checker: "nim check --warnings:all" },
    TypeProjectionProfile { language: TypePolicyLanguage::Perl, nominal_types: "checked constructors and immutable objects", unions: "tagged hashes/classes", refinements: "Type::Tiny constraints", presence: "exists versus undef", unknown_values: "raw enum integer and unknown tag", integers: "Math::Int64 or strings for uint64", checker: "perl -c + prove" },
    TypeProjectionProfile { language: TypePolicyLanguage::Powershell, nominal_types: "classes with typed properties", unions: "tagged classes", refinements: "ValidateScript/argument transformation", presence: "nullable property plus presence marker", unknown_values: "raw enum integer and unknown class", integers: "UInt64/Int64", checker: "PSScriptAnalyzer + Pester" },
    TypeProjectionProfile { language: TypePolicyLanguage::R, nominal_types: "S3/S4 classes with validators", unions: "S4 class unions", refinements: "validity methods", presence: "NULL versus explicit value", unknown_values: "raw enum integer and unknown S4 class", integers: "bit64::integer64", checker: "R CMD check" },
    TypeProjectionProfile { language: TypePolicyLanguage::Bash, nominal_types: "validated shell variables at process boundary", unions: "tagged JSON objects", refinements: "fail-closed validation functions", presence: "jq has() versus null", unknown_values: "raw enum integer and unknown JSON tag", integers: "decimal strings; never shell arithmetic for uint64", checker: "shellcheck + bats" },
    TypeProjectionProfile { language: TypePolicyLanguage::Haskell, nominal_types: "newtype and smart constructors", unions: "ADTs, GADTs and pattern exhaustiveness", refinements: "refined types and smart constructors", presence: "Maybe only for explicit presence", unknown_values: "Unknown(raw Int) constructor and unknown ADT arm", integers: "Word64/Int64", checker: "GHC -Wall -Werror + hlint" },
    TypeProjectionProfile { language: TypePolicyLanguage::Lua, nominal_types: "validated userdata/table constructors", unions: "tagged tables", refinements: "runtime constructors", presence: "key absence versus nil", unknown_values: "raw enum integer and unknown table tag", integers: "decimal strings for uint64", checker: "luacheck + runtime vectors" },
];

/// Find the profile for a language, allowing generators to fail closed when a
/// newly inventoried target has no type policy yet.
pub fn type_projection_profile(language: TypePolicyLanguage) -> &'static TypeProjectionProfile {
    TYPE_PROJECTION_PROFILES
        .iter()
        .find(|profile| profile.language == language)
        .expect("every inventoried language must have a Rust-owned type policy")
}

/// Find a semantic type by its stable Rust-owned identifier.
pub fn semantic_type(id: &str) -> Option<&'static SemanticType> {
    SEMANTIC_TYPES.iter().find(|item| item.id == id)
}

/// Resolve a Rust model field's semantic type for a contract family.
pub fn field_semantic_type(family: &str, field: &str) -> Option<&'static SemanticType> {
    FIELD_SEMANTIC_TYPES
        .iter()
        .find(|mapping| mapping.family == family && mapping.field == field)
        .and_then(|mapping| semantic_type(mapping.semantic_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_language_has_a_strong_projection_profile() {
        assert_eq!(TypePolicyLanguage::ALL.len(), TYPE_PROJECTION_PROFILES.len());
        for language in TypePolicyLanguage::ALL {
            let profile = type_projection_profile(*language);
            assert_eq!(profile.language, *language);
            assert!(!profile.nominal_types.is_empty());
            assert!(!profile.unions.is_empty());
            assert!(!profile.refinements.is_empty());
            assert!(!profile.presence.is_empty());
            assert!(!profile.unknown_values.is_empty());
            assert!(!profile.integers.is_empty());
        }
    }

    #[test]
    fn every_projection_preserves_unknown_wire_values() {
        for profile in TYPE_PROJECTION_PROFILES {
            assert!(profile.unknown_values.contains("raw") || profile.unknown_values.contains("Raw"), "{}: {}", profile.language.id(), profile.unknown_values);
            assert!(profile.unknown_values.contains("unknown") || profile.unknown_values.contains("Unknown"), "{}: {}", profile.language.id(), profile.unknown_values);
        }
        assert!(SEMANTIC_TYPES.iter().any(|item| item.rules.contains(&SemanticRule::PreserveUnknownEnum) || item.wire_kind == WireValueKind::Enum));
        assert!(SEMANTIC_TYPES.iter().any(|item| item.rules.contains(&SemanticRule::PreserveUnknownOneof)));
    }

    #[test]
    fn stronger_than_rust_constraints_are_in_the_rust_model() {
        assert!(semantic_type("version_sha256").expect("digest").rules.contains(&SemanticRule::FixedLength(32)));
        assert!(semantic_type("page_limit").expect("page limit").rules.contains(&SemanticRule::MaxItems(1000)));
        assert!(semantic_type("oneof_arm").expect("oneof").rules.contains(&SemanticRule::ExactOneof));
    }

    #[test]
    fn every_field_mapping_resolves_to_a_rust_semantic_type() {
        for mapping in FIELD_SEMANTIC_TYPES {
            assert!(
                semantic_type(mapping.semantic_type).is_some(),
                "{} {} maps to missing semantic type {}",
                mapping.family,
                mapping.field,
                mapping.semantic_type
            );
            assert!(field_semantic_type(mapping.family, mapping.field).is_some());
        }
    }

    #[test]
    fn every_field_mapping_has_a_concrete_public_wire_location() {
        assert_eq!(PUBLIC_FIELD_BINDINGS.len(), FIELD_SEMANTIC_TYPES.len());
        for mapping in FIELD_SEMANTIC_TYPES {
            let binding = PUBLIC_FIELD_BINDINGS
                .iter()
                .find(|candidate| candidate.family == mapping.family && candidate.field == mapping.field)
                .expect("mapped field must be attached to a public protobuf model");
            assert_eq!(binding.semantic_type, mapping.semantic_type);
            assert!(!binding.module.is_empty());
            assert!(!binding.message.is_empty());
            assert!(!binding.wire_field.is_empty());
        }
    }

    #[test]
    fn every_request_binding_has_a_generated_client_route() {
        for binding in PUBLIC_FIELD_BINDINGS {
            if binding.direction == PublicFieldDirection::Request {
                assert!(binding.client_attribute().is_some(), "missing service for {}.{}", binding.family, binding.field);
                assert!(binding.rpc().is_some(), "missing rpc for {}.{}", binding.family, binding.field);
            }
        }
    }

    #[test]
    fn nested_bindings_have_production_route_metadata() {
        assert_eq!(PUBLIC_NESTED_ROUTES.len(), 2);
        for route in PUBLIC_NESTED_ROUTES {
            assert!(!route.operation.is_empty());
            assert!(!route.client_attribute.is_empty());
            assert!(!route.rpc.is_empty());
            assert!(PUBLIC_FIELD_BINDINGS.iter().any(|binding| {
                binding.family == route.family
                    && binding.message == route.nested_message
                    && binding.field == route.semantic_field
                    && binding.direction == PublicFieldDirection::NestedMessage
            }));
        }
    }

    #[test]
    fn field_mappings_retain_each_wire_kind_before_projection() {
        assert_eq!(
            field_semantic_type("workers", "idempotency_key").unwrap().wire_kind,
            WireValueKind::String
        );
        assert_eq!(
            field_semantic_type("stream", "idempotency_key").unwrap().wire_kind,
            WireValueKind::Bytes
        );
        assert_eq!(
            field_semantic_type("machines", "idempotency_key").unwrap().wire_kind,
            WireValueKind::Message
        );
        assert_eq!(
            field_semantic_type("machines", "image").unwrap().wire_kind,
            WireValueKind::Message
        );
        assert_eq!(
            field_semantic_type("inference", "revision").unwrap().wire_kind,
            WireValueKind::Bytes
        );
    }

    #[test]
    fn open_union_projection_has_explicit_known_and_unknown_discriminants() {
        let variants = WIRE_UNION_VARIANTS
            .iter()
            .filter(|variant| variant.union == "wire_choice")
            .collect::<Vec<_>>();
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].tag, "known");
        assert_eq!(variants[1].tag, "unknown");
        assert_eq!(variants[1].payload_wire_kind, WireValueKind::Bytes);
    }
}
