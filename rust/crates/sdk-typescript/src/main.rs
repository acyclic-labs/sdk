//! Standalone Rust-owned TypeScript metadata generator prototype.
//!
//! The generated files contain route and method metadata plus deliberately
//! thin, transport-neutral facades. They are derived from each Rust family's
//! exported descriptor and `HTTP_ROUTES` table; no TypeScript source is read
//! or copied by this generator.

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
};

use prost::Message;
use prost_types::{FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DEFAULT_OUTPUT: &str = "prototype";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MethodMetadata {
    operation_id: String,
    rpc_name: String,
    rpc: String,
    docs: String,
    path: String,
    path_parameters: Vec<String>,
    http_method: String,
    request_type: String,
    response_type: String,
    client_streaming: bool,
    server_streaming: bool,
    request_encoding: String,
    response_encoding: String,
    auth: String,
    credential_policy: String,
    response_limit_policy: String,
    request_fields: Vec<FieldMetadata>,
    response_fields: Vec<FieldMetadata>,
}

/// Complete native gRPC method projection. Unlike `MethodMetadata`, this is
/// emitted for families without an HTTP projection as well, so a native
/// adapter can consume the Rust descriptor without maintaining a second RPC
/// table in TypeScript.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GrpcMethodMetadata {
    rpc_name: String,
    rpc: String,
    request_type: String,
    response_type: String,
    client_streaming: bool,
    server_streaming: bool,
    request_fields: Vec<FieldMetadata>,
    response_fields: Vec<FieldMetadata>,
}

/// Rust-owned capability, error, and validation policy for one public RPC.
/// This remains separate from `MethodMetadata` because families without an
/// HTTP projection still expose their complete operation policy here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct OperationMetadata {
    rpc: String,
    capabilities: Vec<String>,
    errors: Vec<String>,
    validations: Vec<String>,
}

/// A lossless enough field projection for target runtimes.  Keeping this in
/// the generated manifest makes JSON policy reviewable without asking a
/// TypeScript generator to infer protobuf semantics from JavaScript objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FieldMetadata {
    name: String,
    json_name: String,
    number: i32,
    wire_type: String,
    repeated: bool,
    optional: bool,
    oneof: Option<String>,
    proto3_optional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ServiceMetadata {
    family: String,
    rust_crate: String,
    source_kind: String,
    source_artifact: String,
    package: String,
    service: String,
    descriptor_sha256: String,
    source_content_sha256: String,
    /// Digest of the Rust source bytes that emitted the descriptor.
    source_model_sha256: String,
    handshake_route: String,
    handshake_version: String,
    handshake_descriptor_digest: String,
    modeled_operations: usize,
    http_projection: bool,
    remote_policy: Option<RemotePolicyMetadata>,
    operations: Vec<OperationMetadata>,
    methods: Vec<MethodMetadata>,
    grpc_methods: Vec<GrpcMethodMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemotePolicyMetadata {
    protocol: String,
    auth: String,
    request_encoding: String,
    response_encoding: String,
    credential_policy: String,
    response_limit_policy: String,
    request_timeout_millis: u64,
    behavior_binding: String,
    transport: TransportPolicyMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TransportPolicyMetadata {
    native: Vec<TransportOptionMetadata>,
    browser: Vec<TransportOptionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TransportOptionMetadata {
    kind: String,
    streaming: bool,
    bearer_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Manifest {
    generator: String,
    generator_version: String,
    source_revision: String,
    services: Vec<ServiceMetadata>,
}

#[derive(Debug)]
enum Error {
    Decode(prost::DecodeError),
    Missing(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(error) => write!(formatter, "descriptor decode failed: {error}"),
            Self::Missing(value) => write!(formatter, "missing Rust-owned contract item: {value}"),
            Self::Io(error) => write!(formatter, "I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "JSON serialization failed: {error}"),
        }
    }
}

impl std::error::Error for Error {}
impl From<prost::DecodeError> for Error {
    fn from(error: prost::DecodeError) -> Self {
        Self::Decode(error)
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

struct RustService<'a> {
    family: &'a str,
    rust_crate: &'a str,
    source_kind: &'a str,
    source_artifact: &'a str,
    source_content: Vec<u8>,
    descriptor: Vec<u8>,
    routes: Vec<(&'a str, &'a str, &'a str)>,
    operations: &'a [acyclic_sdk_contract_wire::OperationPolicy],
    transport: acyclic_sdk_contract_wire::FamilyTransportPolicy,
}

fn transport_kind(kind: acyclic_sdk_contract_wire::TransportKind) -> &'static str {
    match kind {
        acyclic_sdk_contract_wire::TransportKind::Grpc => "grpc",
        acyclic_sdk_contract_wire::TransportKind::GrpcWeb => "grpc-web",
        acyclic_sdk_contract_wire::TransportKind::HttpJson => "http",
    }
}

fn transport_options(
    policy: acyclic_sdk_contract_wire::RuntimeTransportPolicy,
) -> Vec<TransportOptionMetadata> {
    policy
        .options
        .iter()
        .map(|option| TransportOptionMetadata {
            kind: transport_kind(option.kind).to_owned(),
            streaming: option.streaming,
            bearer_auth: option.bearer_auth,
        })
        .collect()
}

fn transport_policy_metadata(
    policy: acyclic_sdk_contract_wire::FamilyTransportPolicy,
) -> TransportPolicyMetadata {
    TransportPolicyMetadata {
        native: transport_options(policy.native),
        browser: transport_options(policy.browser),
    }
}

/// The published Inference TypeScript package currently ships the canonical
/// HTTP projection only. Keep that installed-adapter qualification in the
/// Rust-generated policy so native consumers select HTTP instead of claiming
/// an unavailable gRPC adapter.
fn inference_transport_policy_metadata(
    policy: acyclic_sdk_contract_wire::FamilyTransportPolicy,
) -> TransportPolicyMetadata {
    let mut metadata = transport_policy_metadata(policy);
    metadata.native.retain(|option| option.kind == "http");
    metadata
}

fn lower_camel(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_lowercase().collect::<String>() + chars.as_str()
    })
}

fn descriptor_service<'a>(
    set: &'a FileDescriptorSet,
) -> Result<(&'a str, &'a ServiceDescriptorProto), Error> {
    let file = set
        .file
        .iter()
        .find(|file| !file.service.is_empty())
        .ok_or_else(|| Error::Missing("service descriptor".to_owned()))?;
    let service = file
        .service
        .first()
        .ok_or_else(|| Error::Missing("service descriptor".to_owned()))?;
    Ok((file.package.as_deref().unwrap_or_default(), service))
}

fn method_for<'a>(
    set: &'a FileDescriptorSet,
    operation_id: &str,
) -> Result<(&'a ServiceDescriptorProto, &'a MethodDescriptorProto), Error> {
    set.file
        .iter()
        .flat_map(|file| file.service.iter())
        .find_map(|service| {
            let service_prefix = service
                .name
                .as_deref()
                .and_then(|name| name.strip_suffix("Service"))
                .map(lower_camel);
            service
                .method
                .iter()
                .find(|method| {
                    let method_name = lower_camel(method.name.as_deref().unwrap_or_default());
                    let qualified_name = service_prefix
                        .as_deref()
                        .zip(method.name.as_deref())
                        .map(|(prefix, name)| format!("{prefix}{name}"));
                    method_name == operation_id || qualified_name.as_deref() == Some(operation_id)
                })
                .map(|method| (service, method))
        })
        .ok_or_else(|| Error::Missing(format!("RPC for route {operation_id}")))
}

fn short_type(qualified: &str) -> String {
    qualified.trim_start_matches('.').to_owned()
}

fn wire_type(kind: i32) -> &'static str {
    // prost_types::field_descriptor_proto::Type values are stable protobuf
    // numbers. Keep the mapping explicit so a new protobuf kind cannot be
    // silently represented as an arbitrary JSON scalar.
    match kind {
        1 => "double",
        2 => "float",
        3 => "int64",
        4 => "uint64",
        5 => "int32",
        6 => "fixed64",
        7 => "fixed32",
        8 => "bool",
        9 => "string",
        10 => "group",
        11 => "message",
        12 => "bytes",
        13 => "uint32",
        14 => "enum",
        15 => "sfixed32",
        16 => "sfixed64",
        17 => "sint32",
        18 => "sint64",
        _ => "unknown",
    }
}

fn fields_for(set: &FileDescriptorSet, qualified: &str) -> Result<Vec<FieldMetadata>, Error> {
    let wanted = qualified.trim_start_matches('.');
    let message = set
        .file
        .iter()
        .flat_map(|file| file.message_type.iter())
        .find(|message| {
            let name = message.name.as_deref().unwrap_or_default();
            // All Actors and Workers request/response messages are top-level;
            // retaining the suffix match also works with a descriptor that
            // omits its package in a test fixture.
            wanted.ends_with(name)
        })
        .ok_or_else(|| Error::Missing(format!("message descriptor for {qualified}")))?;
    Ok(message
        .field
        .iter()
        .map(|field| {
            let name = field.name.clone().unwrap_or_default();
            let json_name = field.json_name.clone().unwrap_or_else(|| name.clone());
            let oneof = field
                .oneof_index
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| message.oneof_decl.get(index))
                .and_then(|decl| decl.name.clone());
            let label = field.label.unwrap_or_default();
            let repeated = label == prost_types::field_descriptor_proto::Label::Repeated as i32;
            let optional = field.proto3_optional() || oneof.is_some();
            Ok(FieldMetadata {
                name,
                json_name,
                number: field.number.unwrap_or_default(),
                wire_type: wire_type(field.r#type.unwrap_or_default()).to_owned(),
                repeated,
                optional,
                oneof,
                proto3_optional: field.proto3_optional(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?)
}

fn path_parameters(path: &str) -> Vec<String> {
    path.split('{')
        .skip(1)
        .filter_map(|part| part.split('}').next())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Return the exact identity advertised by the selected Rust runtime.
///
/// Filesystem and Harness retain immutable, deployed handshake identities in
/// their family contract modules. The generic control-plane identity remains
/// the fallback for the descriptor-backed families. The family constants use
/// a `blake3:` display prefix while the wire field carries the bare digest.
fn family_handshake_identity(
    family: acyclic_sdk_contract_wire::BindingFamily,
) -> (&'static str, String) {
    let (version, digest) = match family {
        acyclic_sdk_contract_wire::BindingFamily::Filesystem => (
            acyclic_sdk_contract_wire::filesystem::HANDSHAKE_VERSION,
            acyclic_sdk_contract_wire::filesystem::ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST,
        ),
        acyclic_sdk_contract_wire::BindingFamily::Harness => (
            acyclic_sdk_contract_wire::harness::HANDSHAKE_VERSION,
            acyclic_sdk_contract_wire::harness::ARCHIVED_HANDSHAKE_DESCRIPTOR_DIGEST,
        ),
        other => (
            acyclic_sdk_contract_wire::transport_control::control_protocol_version(other),
            "",
        ),
    };
    if digest.is_empty() {
        (
            version,
            acyclic_sdk_contract_wire::transport_control::archived_descriptor_digest(family),
        )
    } else {
        (
            version,
            digest.strip_prefix("blake3:").unwrap_or(digest).to_owned(),
        )
    }
}

fn service_metadata(spec: RustService<'_>) -> Result<ServiceMetadata, Error> {
    let set = FileDescriptorSet::decode(spec.descriptor.as_slice())?;
    let (package, primary_service) = descriptor_service(&set)?;
    let modeled_operations = set
        .file
        .iter()
        .flat_map(|file| file.service.iter())
        .map(|service| service.method.len())
        .sum();
    let primary_service_name = primary_service
        .name
        .as_deref()
        .ok_or_else(|| Error::Missing("service name".to_owned()))?;
    let family = acyclic_sdk_contract_wire::BindingFamily::ALL
        .iter()
        .copied()
        .find(|family| family.name() == spec.family)
        .ok_or_else(|| Error::Missing(format!("unknown binding family {}", spec.family)))?;
    let handshake_route =
        acyclic_sdk_contract_wire::transport_control::handshake_http_route(spec.family)
            .ok_or_else(|| {
                Error::Missing(format!("missing handshake route for {}", spec.family))
            })?;
    let (handshake_version, handshake_descriptor_digest) = family_handshake_identity(family);
    let methods = spec
        .routes
        .iter()
        .map(|(operation_id, path, docs)| {
            let (service, method) = method_for(&set, operation_id)?;
            let service_name = service
                .name
                .as_deref()
                .ok_or_else(|| Error::Missing("service name".to_owned()))?;
            let rpc_name = method
                .name
                .as_deref()
                .ok_or_else(|| Error::Missing("RPC name".to_owned()))?;
            let request_type = short_type(
                method
                    .input_type
                    .as_deref()
                    .ok_or_else(|| Error::Missing(format!("input for {rpc_name}")))?,
            );
            let response_type = short_type(
                method
                    .output_type
                    .as_deref()
                    .ok_or_else(|| Error::Missing(format!("output for {rpc_name}")))?,
            );
            let request_fields = fields_for(&set, &request_type)?;
            let response_fields = fields_for(&set, &response_type)?;
            Ok(MethodMetadata {
                operation_id: (*operation_id).to_owned(),
                rpc_name: rpc_name.to_owned(),
                rpc: format!("{package}.{service_name}/{rpc_name}"),
                docs: (*docs).to_owned(),
                path: (*path).to_owned(),
                path_parameters: path_parameters(path),
                http_method: "POST".to_owned(),
                request_type,
                response_type,
                client_streaming: method.client_streaming(),
                server_streaming: method.server_streaming(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                // Both Rust HTTP clients require bearer authentication, reject
                // unbounded responses, and use canonical protobuf JSON.
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                request_fields,
                response_fields,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let mut grpc_methods = Vec::new();
    for file in &set.file {
        let package = file.package.as_deref().unwrap_or_default();
        for service in &file.service {
            let service_name = service.name.as_deref().unwrap_or_default();
            for method in &service.method {
                let rpc_name = method.name.as_deref().unwrap_or_default();
                let request_type = short_type(method.input_type.as_deref().unwrap_or_default());
                let response_type = short_type(method.output_type.as_deref().unwrap_or_default());
                grpc_methods.push(GrpcMethodMetadata {
                    rpc_name: rpc_name.to_owned(),
                    rpc: format!("{package}.{service_name}/{rpc_name}"),
                    request_type: request_type.clone(),
                    response_type: response_type.clone(),
                    client_streaming: method.client_streaming(),
                    server_streaming: method.server_streaming(),
                    request_fields: fields_for(&set, &request_type)?,
                    response_fields: fields_for(&set, &response_type)?,
                });
            }
        }
    }
    let operations = spec
        .operations
        .iter()
        .map(|policy| OperationMetadata {
            rpc: policy.rpc.to_owned(),
            capabilities: policy
                .capabilities
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            errors: policy
                .errors
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            validations: policy
                .validations
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        })
        .collect();
    Ok(ServiceMetadata {
        family: spec.family.to_owned(),
        rust_crate: spec.rust_crate.to_owned(),
        source_kind: spec.source_kind.to_owned(),
        source_artifact: spec.source_artifact.to_owned(),
        package: package.to_owned(),
        service: primary_service_name.to_owned(),
        descriptor_sha256: digest(&spec.descriptor),
        source_content_sha256: digest(&spec.source_content),
        source_model_sha256: digest(&spec.source_content),
        handshake_route,
        handshake_version: handshake_version.to_owned(),
        handshake_descriptor_digest,
        modeled_operations,
        http_projection: !spec.routes.is_empty(),
        remote_policy: match spec.family {
            "actors" | "workers" => Some(RemotePolicyMetadata {
                protocol: "https-or-loopback-http".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                request_timeout_millis: 30_000,
                behavior_binding: "generated-client".to_owned(),
                transport: transport_policy_metadata(spec.transport),
            }),
            "objects" => Some(RemotePolicyMetadata {
                protocol: "https-or-loopback-http".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                request_timeout_millis: 30_000,
                behavior_binding: "native-wasm".to_owned(),
                transport: transport_policy_metadata(spec.transport),
            }),
            "stream" => Some(RemotePolicyMetadata {
                protocol: "https-or-loopback-http".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                request_timeout_millis: 30_000,
                behavior_binding: "native-wasm".to_owned(),
                transport: transport_policy_metadata(spec.transport),
            }),
            "inference" => Some(RemotePolicyMetadata {
                protocol: "https".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                request_timeout_millis: 60_000,
                behavior_binding: "generated-client".to_owned(),
                transport: inference_transport_policy_metadata(spec.transport),
            }),
            "machines" => Some(RemotePolicyMetadata {
                protocol: "https".to_owned(),
                auth: "mtls".to_owned(),
                credential_policy: "mtls-files".to_owned(),
                request_encoding: "protobuf".to_owned(),
                response_encoding: "protobuf".to_owned(),
                response_limit_policy: "bounded-cumulative-protobuf".to_owned(),
                request_timeout_millis: 30_000,
                behavior_binding: "rust-native-grpc".to_owned(),
                transport: transport_policy_metadata(spec.transport),
            }),
            "filesystem" => Some(RemotePolicyMetadata {
                protocol: "https".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf".to_owned(),
                response_encoding: "protobuf".to_owned(),
                response_limit_policy: "bounded-cumulative-protobuf".to_owned(),
                request_timeout_millis: 30_000,
                behavior_binding: "generated-client".to_owned(),
                transport: transport_policy_metadata(spec.transport),
            }),
            _ => None,
        },
        operations,
        methods,
        grpc_methods,
    })
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn model_source_content(parts: &[&[u8]]) -> Vec<u8> {
    let mut content = Vec::new();
    for part in parts {
        content.extend_from_slice(part);
        content.push(0);
    }
    content
}

fn contract_routes<'a>(
    routes: &'a [acyclic_sdk_contract_wire::RouteSpec],
) -> Vec<(&'a str, &'a str, &'a str)> {
    routes
        .iter()
        .map(|route| {
            (
                route.operation_id,
                route.path.trim_start_matches('/'),
                route.docs,
            )
        })
        .collect()
}

fn proto_import_path(family: &str) -> String {
    match family {
        "filesystem" => {
            "../../../../typescript/packages/filesystem/generated/proto/filesystem/v2/filesystem_pb.js"
                .to_owned()
        }
        "harness" => {
            "../../../../typescript/packages/harness/generated/proto/harness/v2/harness_pb.js"
                .to_owned()
        }
        "objects" => {
            "../../../../typescript/packages/objects/generated/proto/objects/v2/objects_pb.js"
                .to_owned()
        }
        "stream" => "../../../../typescript/packages/stream/generated/proto/stream/v2/stream_pb.js"
            .to_owned(),
        _ => format!(
            "../../../../typescript/packages/{family}/generated/proto/{family}/v1/{family}_pb.js"
        ),
    }
}

fn grpc_service_prefix(method: &GrpcMethodMetadata) -> Option<String> {
    let service = method.rpc.split_once('/')?.0.rsplit('.').next()?;
    let service = service.strip_suffix("Service").unwrap_or(service);
    Some(lower_camel(service))
}

/// gRPC method names are only unique within one protobuf service.  Families
/// such as Inference expose several services, so a facade must qualify only
/// colliding names while retaining the short ergonomic names for the common
/// single-service case.
fn grpc_operation_keys(methods: &[GrpcMethodMetadata]) -> Vec<String> {
    let base = methods
        .iter()
        .map(|method| lower_camel(&method.rpc_name))
        .collect::<Vec<_>>();
    base.iter()
        .enumerate()
        .map(|(index, key)| {
            if base.iter().filter(|candidate| *candidate == key).count() == 1 {
                key.clone()
            } else {
                grpc_service_prefix(&methods[index])
                    .map(|prefix| format!("{prefix}{key}"))
                    .unwrap_or_else(|| key.clone())
            }
        })
        .collect()
}

fn package_proto_import_path(family: &str) -> String {
    match family {
        "filesystem" => "../generated/proto/filesystem/v2/filesystem_pb.js".to_owned(),
        "harness" => "../generated/proto/harness/v2/harness_pb.js".to_owned(),
        "objects" => "../generated/proto/objects/v2/objects_pb.js".to_owned(),
        "stream" => "../generated/proto/stream/v2/stream_pb.js".to_owned(),
        _ => format!("../generated/proto/{family}/v1/{family}_pb.js"),
    }
}

fn source_content_for_family(family: &str) -> Vec<u8> {
    let lib = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sdk-contract-wire/src/lib.rs"
    ));
    let registry = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sdk-contract-wire/src/family_registry.rs"
    ));
    let credential = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sdk-contract-wire/src/credential.rs"
    ));
    let transport = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../sdk-contract-wire/src/transport.rs"
    ));
    match family {
        "actors" => model_source_content(&[lib, registry, credential, transport]),
        "workers" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/workers.rs"
            )),
        ]),
        "objects" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/objects.rs"
            )),
        ]),
        "stream" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/stream.rs"
            )),
        ]),
        "inference" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/inference.rs"
            )),
        ]),
        "machines" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/machines.rs"
            )),
        ]),
        "filesystem" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/filesystem.rs"
            )),
        ]),
        "harness" => model_source_content(&[
            lib,
            registry,
            credential,
            transport,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/harness.rs"
            )),
        ]),
        other => panic!("unknown Rust-owned family {other}"),
    }
}

fn source_artifact_for_family(family: &str) -> &'static str {
    match family {
        "actors" => "acyclic_sdk_contract_wire::actors_descriptor",
        "workers" => "acyclic_sdk_contract_wire::workers::workers_descriptor",
        "objects" => "acyclic_sdk_contract_wire::objects::objects_descriptor",
        "stream" => "acyclic_sdk_contract_wire::stream::stream_descriptor",
        "inference" => "acyclic_sdk_contract_wire::inference::inference_descriptor",
        "machines" => "acyclic_sdk_contract_wire::machines::machines_descriptor",
        "filesystem" => "acyclic_sdk_contract_wire::filesystem::filesystem_descriptor",
        "harness" => "acyclic_sdk_contract_wire::harness::harness_descriptor",
        other => panic!("unknown Rust-owned family {other}"),
    }
}

fn rust_crate_for_family(family: &str) -> &'static str {
    match family {
        "actors" => "acyclic-actors",
        "workers" => "acyclic-workers",
        "objects" => "acyclic-objects",
        "stream" => "acyclic-stream",
        "inference" => "acyclic-inference",
        "machines" => "acyclic-machines",
        "filesystem" => "acyclic-filesystem",
        "harness" => "acyclic-harness",
        other => panic!("unknown Rust-owned family {other}"),
    }
}

fn rust_service(view: &'static acyclic_sdk_contract_wire::FamilyView) -> RustService<'static> {
    RustService {
        family: view.name,
        rust_crate: rust_crate_for_family(view.name),
        source_kind: "rust-model",
        source_artifact: source_artifact_for_family(view.name),
        source_content: source_content_for_family(view.name),
        descriptor: view.model.descriptor(),
        routes: contract_routes(view.routes()),
        operations: view.operation_policies,
        transport: view.transport,
    }
}

fn model() -> Result<Manifest, Error> {
    let services = acyclic_sdk_contract_wire::FAMILY_VIEWS
        .iter()
        .map(|view| service_metadata(rust_service(view)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Manifest {
        generator: "sdk-typescript".to_owned(),
        generator_version: env!("CARGO_PKG_VERSION").to_owned(),
        source_revision: env::var("SDK_SOURCE_REVISION")
            .unwrap_or_else(|_| "working-tree".to_owned()),
        services,
    })
}

fn json<T: Serialize>(value: &T) -> Result<String, Error> {
    Ok(format!("{}\n", serde_json::to_string_pretty(value)?))
}

fn typescript(service: &ServiceMetadata) -> Result<String, Error> {
    let family_path = proto_import_path(&service.family);
    let protocol_path = format!(
        "../../../../typescript/packages/{}/generated/proto/protocol/v1/protocol_pb.js",
        service.family
    );
    typescript_with_paths(service, &family_path, &protocol_path)
}

fn package_typescript(service: &ServiceMetadata) -> Result<String, Error> {
    let family_path = package_proto_import_path(&service.family);
    let protocol_path = "../generated/proto/protocol/v1/protocol_pb.js";
    typescript_with_paths(service, &family_path, protocol_path)
}

fn typescript_semantic_name(id: &str) -> String {
    let name = id
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<String>();
    format!("RustOwned{name}")
}

fn typescript_semantic_section(family: &str) -> String {
    use acyclic_sdk_contract_wire::type_policy::PUBLIC_NESTED_ROUTES;
    use acyclic_sdk_contract_wire::{
        PUBLIC_FIELD_BINDINGS, PublicFieldDirection, SemanticRule, WIRE_UNION_VARIANTS,
        WireValueKind, semantic_type,
    };
    let bindings = PUBLIC_FIELD_BINDINGS
        .iter()
        .filter(|binding| binding.family == family)
        .collect::<Vec<_>>();
    let semantic_ids = bindings
        .iter()
        .map(|binding| binding.semantic_type)
        .collect::<BTreeSet<_>>();
    let mut output = String::from(
        "// Rust-owned semantic projections. Generated from type_policy.rs; do not edit.\n\n",
    );
    output.push_str("declare const rustOwnedSemanticBrand: unique symbol;\n");
    output.push_str("export type RustOwnedSemanticString<Name extends string> = string & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticBytes<Name extends string> = Uint8Array & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticNumber<Name extends string> = number & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticMessage<Name extends string> = object & { readonly [rustOwnedSemanticBrand]: Name };\n\n");
    output.push_str("export interface RustOwnedSemanticFieldMetadata { readonly family: string; readonly field: string; readonly semanticType: string; readonly module: string; readonly message: string; readonly wireField: string; readonly direction: \"request\" | \"response\" | \"nested_message\"; readonly rules: readonly string[]; }\n\n");
    for id in semantic_ids {
        let item =
            semantic_type(id).expect("every public binding resolves to a Rust semantic type");
        let name = typescript_semantic_name(item.id);
        let base = match item.wire_kind {
            WireValueKind::String => format!("RustOwnedSemanticString<{id:?}>"),
            WireValueKind::Bytes => format!("RustOwnedSemanticBytes<{id:?}>"),
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger => {
                format!("RustOwnedSemanticNumber<{id:?}>")
            }
            WireValueKind::Boolean => "boolean".to_owned(),
            WireValueKind::Message => format!("RustOwnedSemanticMessage<{id:?}>"),
            WireValueKind::Timestamp | WireValueKind::Enum | WireValueKind::Oneof => {
                "unknown".to_owned()
            }
        };
        output.push_str(&format!("export type {name} = {base};\n"));
        let mut checks = String::new();
        for rule in item.rules {
            let check = match (item.wire_kind, rule) {
                // Message values are branded Rust-owned protobuf objects. Any
                // UUID or digest rule applies to their nested wire fields,
                // never to the object itself.
                (WireValueKind::Message, _) => String::new(),
                (WireValueKind::String | WireValueKind::Bytes, SemanticRule::NonEmpty) => {
                    "if (value.length === 0) throw new TypeError(\"value must not be empty\");"
                        .to_owned()
                }
                (
                    WireValueKind::UnsignedInteger | WireValueKind::SignedInteger,
                    SemanticRule::NonNegative,
                ) => "if (value < 0) throw new RangeError(\"value must be non-negative\");"
                    .to_owned(),
                (
                    WireValueKind::UnsignedInteger | WireValueKind::SignedInteger,
                    SemanticRule::StrictlyPositive,
                ) => "if (value <= 0) throw new RangeError(\"value must be positive\");".to_owned(),
                (WireValueKind::Bytes, SemanticRule::FixedLength(length)) => format!(
                    "if (value.byteLength !== {length}) throw new RangeError(\"value has the wrong length\");"
                ),
                (WireValueKind::Bytes, SemanticRule::MaxBytes(maximum)) => format!(
                    "if (value.byteLength > {maximum}) throw new RangeError(\"value exceeds its byte limit\");"
                ),
                (
                    WireValueKind::UnsignedInteger | WireValueKind::SignedInteger,
                    SemanticRule::MaxItems(maximum),
                ) => format!(
                    "if (value > {maximum}) throw new RangeError(\"value exceeds its item limit\");"
                ),
                (
                    _,
                    SemanticRule::Utf8
                    | SemanticRule::Sha256Digest
                    | SemanticRule::Immutable
                    | SemanticRule::Monotonic
                    | SemanticRule::CanonicalResourceName
                    | SemanticRule::ExactOneof
                    | SemanticRule::ExplicitPresence
                    | SemanticRule::PreserveUnknownEnum
                    | SemanticRule::PreserveUnknownOneof
                    | SemanticRule::BoundedInteger { .. }
                    | SemanticRule::NonEmpty
                    | SemanticRule::NonNegative
                    | SemanticRule::StrictlyPositive
                    | SemanticRule::FixedLength(_)
                    | SemanticRule::MaxBytes(_)
                    | SemanticRule::MaxItems(_),
                ) => String::new(),
            };
            checks.push_str(&check);
        }
        let parameter = match item.wire_kind {
            WireValueKind::Bytes => "value: Uint8Array",
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger => "value: number",
            WireValueKind::Boolean => "value: boolean",
            WireValueKind::Message => "value: object",
            _ => "value: string",
        };
        output.push_str(&format!("export function make{name}({parameter}): {name} {{ {checks} return value as {name}; }}\n"));
    }
    output.push('\n');
    output.push_str(&format!(
        "export const {}_PUBLIC_FIELD_BINDINGS = [\n",
        family.to_ascii_uppercase()
    ));
    for binding in &bindings {
        let direction = match binding.direction {
            PublicFieldDirection::Request => "request",
            PublicFieldDirection::Response => "response",
            PublicFieldDirection::NestedMessage => "nested_message",
        };
        let rules = semantic_type(binding.semantic_type)
            .expect("semantic binding")
            .rules
            .iter()
            .map(|rule| format!("{:?}", format!("{rule:?}")))
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!("  {{ family: {:?}, field: {:?}, semanticType: {:?}, module: {:?}, message: {:?}, wireField: {:?}, direction: {:?}, rules: [{}] }},\n", binding.family, binding.field, binding.semantic_type, binding.module, binding.message, binding.wire_field, direction, rules));
    }
    output.push_str("] as const satisfies readonly RustOwnedSemanticFieldMetadata[];\n\n");
    output.push_str(&format!(
        "export const {}_PUBLIC_NESTED_ROUTES = [\n",
        family.to_ascii_uppercase()
    ));
    for route in PUBLIC_NESTED_ROUTES
        .iter()
        .filter(|route| route.family == family)
    {
        let fields = route
            .fields
            .iter()
            .map(|(field, kind)| {
                let kind = match kind {
                    acyclic_sdk_contract_wire::type_policy::PublicNestedFieldKind::Text => "text",
                    acyclic_sdk_contract_wire::type_policy::PublicNestedFieldKind::Message(_) => {
                        "message"
                    }
                };
                format!("{{ field: {:?}, kind: {:?} }}", field, kind)
            })
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!("  {{ operation: {:?}, requestMessage: {:?}, nestedMessage: {:?}, nestedField: {:?}, semanticField: {:?}, clientAttribute: {:?}, rpc: {:?}, response: {:?}, fields: [{}] }},\n", route.operation, route.request_message, route.nested_message, route.nested_field, route.semantic_field, route.client_attribute, route.rpc, route.response, fields));
    }
    output.push_str("] as const;\n\n");
    let wire_type = |kind: WireValueKind| match kind {
        WireValueKind::String => "string",
        WireValueKind::Bytes => "Uint8Array",
        WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => "number",
        WireValueKind::Boolean => "boolean",
        WireValueKind::Message => "object",
        WireValueKind::Timestamp | WireValueKind::Enum | WireValueKind::Oneof => "unknown",
    };
    let union_variants = WIRE_UNION_VARIANTS
        .iter()
        .map(|variant| {
            format!(
                "  {{ readonly kind: {:?}; readonly value: {} }}",
                variant.tag,
                wire_type(variant.payload_wire_kind)
            )
        })
        .collect::<Vec<_>>()
        .join(" |\n");
    output.push_str(&format!(
        "export type RustOwnedWireChoice =\n{union_variants};\n\n"
    ));
    output
}

fn typescript_with_paths(
    service: &ServiceMetadata,
    family_path: &str,
    protocol_path: &str,
) -> Result<String, Error> {
    let constant = format!("{}_METHODS", service.family.to_ascii_uppercase());
    let title = format!(
        "{}{}",
        service.family[..1].to_ascii_uppercase(),
        &service.family[1..]
    );
    let mut output = String::from(
        "// Generated by sdk-typescript from Rust descriptors and HTTP_ROUTES. Do not edit.\n\n",
    );
    output
        .push_str("import { create, fromJsonString, toJsonString } from \"@bufbuild/protobuf\";\n");
    output.push_str(&format!("import {{ CapabilitySchema, CapabilitySetSchema, HandshakeRequestSchema, HandshakeResponseSchema, ProtocolIdentitySchema }} from \"{protocol_path}\";\n\n"));
    // Credential admission is emitted against the Rust WASM boundary for
    // every bearer service.  Keeping this map in the Rust generator means a
    // checked-in facade cannot silently grow a JavaScript regex fallback.
    let package_credential_validator = match service.family.as_str() {
        "actors" => Some("import { validateActorsCredential } from \"./wasm-runtime.js\";\n"),
        "workers" => Some("import { validateWorkersCredential } from \"./wasm-runtime.js\";\n"),
        "objects" => Some(
            "import { validate_objects_v2_bearer_token } from \"../generated/wasm/acyclic_objects_wasm.js\";\n",
        ),
        "stream" => Some(
            "import { validateBearerToken } from \"../generated/wasm/acyclic_stream_wasm.js\";\n",
        ),
        "inference" => Some("import { validateInferenceCredential } from \"./contract.js\";\n"),
        "filesystem" => Some("import { validateFilesystemCredential } from \"./remote-web.js\";\n"),
        _ => None,
    };
    let package_runtime_import = match service.family.as_str() {
        "objects" => Some("import \"./wasm-runtime.js\";\n"),
        "stream" => Some("import \"./contract.js\";\n"),
        _ => None,
    };
    if let Some(import) = package_runtime_import {
        output.push_str(import);
        output.push('\n');
    }
    if let Some(import) = package_credential_validator {
        output.push_str(import);
        output.push('\n');
    }
    let message_types = service
        .methods
        .iter()
        .flat_map(|method| [method.request_type.as_str(), method.response_type.as_str()])
        .chain(
            service
                .grpc_methods
                .iter()
                .flat_map(|method| [method.request_type.as_str(), method.response_type.as_str()]),
        )
        .collect::<BTreeSet<_>>();
    let family_message_imports = message_types
        .iter()
        .filter(|qualified| !qualified.starts_with("acyclic.protocol.v1."))
        .map(|qualified| local_type(qualified))
        .collect::<Vec<_>>()
        .join(", ");
    let protocol_message_imports = message_types
        .iter()
        .filter(|qualified| qualified.starts_with("acyclic.protocol.v1."))
        .map(|qualified| local_type(qualified))
        .collect::<Vec<_>>()
        .join(", ");
    if message_types.is_empty() {
        output.push_str(
            "// This family has no HTTP method projection in the current Rust model.\n\n",
        );
    } else {
        if !family_message_imports.is_empty() {
            output.push_str(&format!(
                "import type {{ {family_message_imports} }} from \"{family_path}\";\n"
            ));
        }
        if !protocol_message_imports.is_empty() {
            output.push_str(&format!(
                "import type {{ {protocol_message_imports} }} from \"{protocol_path}\";\n"
            ));
        }
        output.push('\n');
    }
    output.push_str(&typescript_semantic_section(&service.family));
    output.push_str("export interface RustOwnedFieldMetadata { readonly name: string; readonly jsonName: string; readonly number: number; readonly wireType: string; readonly repeated: boolean; readonly optional: boolean; readonly oneof?: string | undefined; readonly proto3Optional: boolean; }\n\n");
    output.push_str("export interface RustOwnedMethodMetadata {\n  readonly operationId: string;\n  readonly rpc: string;\n  readonly docs: string;\n  readonly path: string;\n  readonly pathParameters: readonly string[];\n  readonly httpMethod: \"POST\";\n  readonly requestType: string;\n  readonly responseType: string;\n  readonly clientStreaming: boolean;\n  readonly serverStreaming: boolean;\n  readonly requestEncoding: \"protobuf-json\";\n  readonly responseEncoding: \"protobuf-json\";\n  readonly auth: \"bearer\";\n  readonly credentialPolicy: \"bearer-no-crlf\";\n  readonly responseLimitPolicy: \"bounded-cumulative-utf8\";\n  readonly requestFields: readonly RustOwnedFieldMetadata[];\n  readonly responseFields: readonly RustOwnedFieldMetadata[];\n}\n\n");
    if let Some(policy) = &service.remote_policy {
        let native = policy
            .transport
            .native
            .iter()
            .map(|option| {
                format!(
                    "{{ kind: {:?}, streaming: {}, bearerAuth: {} }}",
                    option.kind, option.streaming, option.bearer_auth
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let browser = policy
            .transport
            .browser
            .iter()
            .map(|option| {
                format!(
                    "{{ kind: {:?}, streaming: {}, bearerAuth: {} }}",
                    option.kind, option.streaming, option.bearer_auth
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!(
            "export type RustOwnedTransportKind = \"grpc\" | \"grpc-web\" | \"http\";\nexport type RustOwnedRuntime = \"native\" | \"browser\";\nexport interface RustOwnedTransportOption {{ readonly kind: RustOwnedTransportKind; readonly streaming: boolean; readonly bearerAuth: boolean; }}\nexport interface RustOwnedRemotePolicy {{ readonly protocol: {:?}; readonly auth: {:?}; readonly credentialPolicy: {:?}; readonly requestEncoding: {:?}; readonly responseEncoding: {:?}; readonly responseLimitPolicy: {:?}; readonly requestTimeoutMillis: number; readonly behaviorBinding: {:?}; readonly transport: {{ readonly native: readonly RustOwnedTransportOption[]; readonly browser: readonly RustOwnedTransportOption[]; }}; }}\nexport type RustOwnedTransportAvailability = Partial<Record<RustOwnedTransportKind, boolean>>;\n\n",
            policy.protocol,
            policy.auth,
            policy.credential_policy,
            policy.request_encoding,
            policy.response_encoding,
            policy.response_limit_policy,
            policy.behavior_binding,
        ));
        output.push_str(&format!(
            "export const {}_REMOTE_POLICY = {{ protocol: {:?}, auth: {:?}, credentialPolicy: {:?}, requestEncoding: {:?}, responseEncoding: {:?}, responseLimitPolicy: {:?}, requestTimeoutMillis: {}, behaviorBinding: {:?}, transport: {{ native: [{}], browser: [{}] }} }} as const satisfies RustOwnedRemotePolicy;\n",
            service.family.to_ascii_uppercase(),
            policy.protocol,
            policy.auth,
            policy.credential_policy,
            policy.request_encoding,
            policy.response_encoding,
            policy.response_limit_policy,
            policy.request_timeout_millis,
            policy.behavior_binding,
            native,
            browser,
        ));
        output.push_str("\n/** Selects the first Rust-qualified transport that is installed for this runtime. */\nexport function selectRustOwnedTransport(policy: RustOwnedRemotePolicy, runtime: RustOwnedRuntime, requested?: RustOwnedTransportKind, availability: RustOwnedTransportAvailability = {}): RustOwnedTransportKind {\n  const options = policy.transport[runtime];\n  if (requested !== undefined) {\n    const option = options.find(candidate => candidate.kind === requested);\n    if (option === undefined || availability[requested] === false) throw new TypeError(`transport ${requested} is unavailable in the ${runtime} runtime`);\n    return option.kind;\n  }\n  const option = options.find(candidate => availability[candidate.kind] !== false);\n  if (option === undefined) throw new TypeError(`no installed transport is available in the ${runtime} runtime`);\n  return option.kind;\n}\n\n/** Identifies a missing optional adapter without swallowing endpoint or credential errors. */\nexport function isRustOwnedTransportUnavailable(error: unknown): boolean {\n  if (error === null || typeof error !== \"object\") return false;\n  const candidate = error as { readonly code?: unknown; readonly message?: unknown };\n  if (candidate.code === \"ERR_MODULE_NOT_FOUND\" || candidate.code === \"MODULE_NOT_FOUND\") return true;\n  return typeof candidate.message === \"string\" && (/Cannot find (?:module|package)/i.test(candidate.message) || /has no native companion/i.test(candidate.message));\n}\n\n");
    }
    output.push_str("export interface RustOwnedOperationMetadata { readonly rpc: string; readonly capabilities: readonly string[]; readonly errors: readonly string[]; readonly validations: readonly string[]; }\n\n");
    let operations = service
        .operations
        .iter()
        .map(|operation| {
            format!(
                "  {:?}: {{ rpc: {:?}, capabilities: {:?}, errors: {:?}, validations: {:?} }}",
                operation.rpc,
                operation.rpc,
                operation.capabilities,
                operation.errors,
                operation.validations,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    output.push_str(&format!(
        "export const {}_OPERATIONS = {{\n{operations}\n}} as const satisfies Record<string, RustOwnedOperationMetadata>;\n\n",
        service.family.to_ascii_uppercase(),
    ));
    output.push_str(&format!(
        "export const {}_SOURCE = {{ family: {:?}, rustCrate: {:?}, sourceKind: {:?}, sourceArtifact: {:?}, descriptorSha256: {:?}, sourceContentSha256: {:?}, sourceModelSha256: {:?}, handshakeRoute: {:?}, handshakeVersion: {:?}, handshakeDescriptorDigest: {:?}, modeledOperations: {}, httpProjection: {} }} as const;\n\n",
        service.family.to_ascii_uppercase(), service.family, service.rust_crate,
        service.source_kind, service.source_artifact, service.descriptor_sha256,
        service.source_content_sha256, service.source_model_sha256,
        service.handshake_route, service.handshake_version, service.handshake_descriptor_digest,
        service.modeled_operations, service.http_projection,
    ));
    output.push_str(&format!(
        "export const {}_HANDSHAKE = {{ route: {:?}, version: {:?}, descriptorDigest: {:?} }} as const;\n\n",
        service.family.to_ascii_uppercase(),
        service.handshake_route,
        service.handshake_version,
        service.handshake_descriptor_digest,
    ));
    output.push_str("export interface RustOwnedHandshakeMetadata { readonly route: string; readonly version: string; readonly descriptorDigest: string; }\n\n");
    output.push_str("/** Builds the Rust-owned control-plane request used by native gRPC adapters. */\nexport function rustOwnedGrpcHandshakeRequest(handshake: RustOwnedHandshakeMetadata, family: string) {\n  return create(HandshakeRequestSchema, { protocol: create(ProtocolIdentitySchema, { version: handshake.version, descriptorDigest: handshake.descriptorDigest }), required: create(CapabilitySetSchema, { capabilities: [create(CapabilitySchema, { name: family, version: handshake.version })] }) });\n}\n\n");
    output.push_str("/** Validates the Rust-owned control-plane response before any application RPC. */\nexport function validateRustOwnedGrpcHandshake(response: { readonly protocol?: { readonly version: string; readonly descriptorDigest: string } | undefined; readonly supported?: { readonly capabilities: readonly { readonly name: string; readonly version: string }[] } | undefined }, handshake: RustOwnedHandshakeMetadata, family: string): void {\n  const identity = response.protocol;\n  if (identity === undefined || identity.version !== handshake.version || identity.descriptorDigest !== handshake.descriptorDigest) throw new Error(\"Rust-owned gRPC handshake identity mismatch\");\n  const capabilities = response.supported?.capabilities ?? [];\n  if (!capabilities.some(capability => capability.name === family && capability.version === handshake.version)) throw new Error(\"Rust-owned gRPC handshake capability mismatch\");\n}\n\n");
    output.push_str("/** Performs the Rust-defined authenticated endpoint negotiation before application calls. */\nexport async function negotiateRustOwnedEndpoint(fetcher: typeof fetch, endpoint: URL | string, headers: HeadersInit, handshake: RustOwnedHandshakeMetadata, maximumResponseBytes = 64 * 1024, signal?: AbortSignal): Promise<void> {\n  const requestHeaders = new Headers(headers);\n  requestHeaders.set(\"content-type\", \"application/json\");\n  const request = create(HandshakeRequestSchema, { protocol: create(ProtocolIdentitySchema, { version: handshake.version, descriptorDigest: handshake.descriptorDigest }), required: create(CapabilitySetSchema) });\n  const response = await fetcher(new URL(handshake.route, endpoint), { method: \"POST\", redirect: \"error\", headers: requestHeaders, body: toJsonString(HandshakeRequestSchema, request), ...(signal === undefined ? {} : { signal }) });\n  const text = await readRustOwnedHandshakeBody(response, maximumResponseBytes);\n  if (!response.ok) throw new Error(`Rust-owned endpoint handshake failed with HTTP ${response.status}: ${text || \"empty response\"}`);\n  let parsed;\n  try { parsed = fromJsonString(HandshakeResponseSchema, text); } catch (error) { throw new Error(`Rust-owned endpoint handshake returned malformed JSON: ${error instanceof Error ? error.message : String(error)}`); }\n  const identity = parsed.protocol;\n  if (identity === undefined || identity.version !== handshake.version || identity.descriptorDigest !== handshake.descriptorDigest) throw new Error(\"Rust-owned endpoint handshake identity mismatch\");\n}\n\nasync function readRustOwnedHandshakeBody(response: Response, maximum: number): Promise<string> {\n  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError(\"maximumResponseBytes must be a positive safe integer\");\n  if (response.body === null) { const text = await response.text(); if (new TextEncoder().encode(text).byteLength > maximum) throw new Error(\"Rust-owned endpoint handshake response exceeds configured bound\"); return text; }\n  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;\n  try { for (;;) { const item = await reader.read(); if (item.done) break; size += item.value.byteLength; if (size > maximum) { await reader.cancel().catch(() => undefined); throw new Error(\"Rust-owned endpoint handshake response exceeds configured bound\"); } chunks.push(item.value); } } finally { reader.releaseLock(); }\n  const bytes = new Uint8Array(size); let offset = 0; for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }\n  try { return new TextDecoder(\"utf-8\", { fatal: true }).decode(bytes); } catch { throw new Error(\"Rust-owned endpoint handshake response is not valid UTF-8\"); }\n}\n\n");
    output.push_str(&format!("export const {constant} = "));
    let methods = service.methods.iter().map(|method| {
        let fields = |items: &[FieldMetadata]| items.iter().map(|field| {
            let oneof = field.oneof.as_deref().map_or_else(|| "undefined".to_owned(), |value| format!("{value:?}"));
            format!("{{ name: {:?}, jsonName: {:?}, number: {}, wireType: {:?}, repeated: {}, optional: {}, oneof: {}, proto3Optional: {} }}", field.name, field.json_name, field.number, field.wire_type, field.repeated, field.optional, oneof, field.proto3_optional)
        }).collect::<Vec<_>>().join(", ");
        format!("  {}: {{ operationId: {:?}, rpc: {:?}, docs: {:?}, path: {:?}, pathParameters: {:?}, httpMethod: \"POST\", requestType: {:?}, responseType: {:?}, clientStreaming: {}, serverStreaming: {}, requestEncoding: \"protobuf-json\", responseEncoding: \"protobuf-json\", auth: \"bearer\", credentialPolicy: {:?}, responseLimitPolicy: \"bounded-cumulative-utf8\", requestFields: [{}], responseFields: [{}] }}", method.operation_id, method.operation_id, method.rpc, method.docs, method.path, method.path_parameters, method.request_type, method.response_type, method.client_streaming, method.server_streaming, method.credential_policy, fields(&method.request_fields), fields(&method.response_fields))
    }).collect::<Vec<_>>().join(",\n");
    output.push_str(&format!(
        "{{\n{methods}\n}} as const satisfies Record<string, RustOwnedMethodMetadata>;\n\n"
    ));
    output.push_str("export interface RustOwnedGrpcMethodMetadata { readonly rpcName: string; readonly rpc: string; readonly requestType: string; readonly responseType: string; readonly clientStreaming: boolean; readonly serverStreaming: boolean; readonly requestFields: readonly RustOwnedFieldMetadata[]; readonly responseFields: readonly RustOwnedFieldMetadata[]; }\n\n");
    let grpc_keys = grpc_operation_keys(&service.grpc_methods);
    let grpc_methods = service.grpc_methods.iter().zip(grpc_keys.iter()).map(|(method, key)| {
        let fields = |items: &[FieldMetadata]| items.iter().map(|field| {
            let oneof = field.oneof.as_deref().map_or_else(|| "undefined".to_owned(), |value| format!("{value:?}"));
            format!("{{ name: {:?}, jsonName: {:?}, number: {}, wireType: {:?}, repeated: {}, optional: {}, oneof: {}, proto3Optional: {} }}", field.name, field.json_name, field.number, field.wire_type, field.repeated, field.optional, oneof, field.proto3_optional)
        }).collect::<Vec<_>>().join(", ");
        format!("  {}: {{ rpcName: {:?}, rpc: {:?}, requestType: {:?}, responseType: {:?}, clientStreaming: {}, serverStreaming: {}, requestFields: [{}], responseFields: [{}] }}", key, method.rpc_name, method.rpc, method.request_type, method.response_type, method.client_streaming, method.server_streaming, fields(&method.request_fields), fields(&method.response_fields))
    }).collect::<Vec<_>>().join(",\n");
    output.push_str(&format!(
        "export const {}_GRPC_METHODS = {{\n{grpc_methods}\n}} as const satisfies Record<string, RustOwnedGrpcMethodMetadata>;\n\n",
        service.family.to_ascii_uppercase(),
    ));
    if !service.grpc_methods.is_empty() {
        output.push_str("export interface RustOwnedGrpcInvoker {\n  invokeGrpc<TRequest, TResponse>(method: RustOwnedGrpcMethodMetadata, request: TRequest): Promise<TResponse>;\n  invokeGrpcStream<TRequest, TResponse>(method: RustOwnedGrpcMethodMetadata, request: TRequest): AsyncIterable<TResponse>;\n}\n\n");
        output.push_str(&format!(
            "export function create{title}GrpcClient(invoker: RustOwnedGrpcInvoker) {{\n  return {{\n"
        ));
        for (method, operation) in service.grpc_methods.iter().zip(grpc_keys.iter()) {
            let request_type = local_type(&method.request_type);
            let response_type = local_type(&method.response_type);
            if method.server_streaming {
                output.push_str(&format!(
                    "    {operation}(request: {request_type}): AsyncIterable<{response_type}> {{\n      return invoker.invokeGrpcStream<{request_type}, {response_type}>({grpc_constant}.{rpc_name}, request);\n    }},\n",
                    grpc_constant = format!("{}_GRPC_METHODS", service.family.to_ascii_uppercase()),
                    rpc_name = operation,
                ));
            } else {
                output.push_str(&format!(
                    "    {operation}(request: {request_type}): Promise<{response_type}> {{\n      return invoker.invokeGrpc<{request_type}, {response_type}>({grpc_constant}.{rpc_name}, request);\n    }},\n",
                    grpc_constant = format!("{}_GRPC_METHODS", service.family.to_ascii_uppercase()),
                    rpc_name = operation,
                ));
            }
        }
        output.push_str("  } as const;\n}\n\n");
    }
    if service.remote_policy.is_some() {
        output.push_str(&format!(
            "export const {family}_ROUTES = {constant};\n\n",
            family = service.family.to_ascii_uppercase(),
            constant = constant,
        ));
    }
    output.push_str("export function interpolateRustOwnedPath(method: RustOwnedMethodMetadata, request: unknown): string {\n  let path = method.path;\n  for (const parameter of method.pathParameters) {\n    const key = parameter === \"sha256hex\" ? \"versionSha256\" : parameter;\n    const value = (request as Record<string, unknown>)[key];\n    if (value === undefined || value === null) throw new TypeError(`missing path parameter ${key}`);\n    const rendered = value instanceof Uint8Array ? Array.from(value, byte => byte.toString(16).padStart(2, \"0\")).join(\"\") : typeof value === \"bigint\" ? value.toString() : encodeURIComponent(String(value));\n    path = path.replace(`{${parameter}}`, rendered);\n  }\n  return path;\n}\n\n");
    let credential_policy = service
        .remote_policy
        .as_ref()
        .map(|policy| policy.credential_policy.as_str())
        .unwrap_or(if service.family == "filesystem" {
            "bearer-no-crlf"
        } else {
            "none"
        });
    output.push_str(&format!(
        "export const RUST_OWNED_CREDENTIAL_POLICY = {:?} as const;\n\n",
        credential_policy
    ));
    match service.family.as_str() {
        "actors" => output.push_str("export function validateRustOwnedCredentialPolicy(token: string): void {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\") validateActorsCredential(token);\n}\n\n"),
        "workers" => output.push_str("export function validateRustOwnedCredentialPolicy(token: string): void {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\") validateWorkersCredential(token);\n}\n\n"),
        "objects" => output.push_str("export function validateRustOwnedCredentialPolicy(token: string): void {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\" && validate_objects_v2_bearer_token(token) !== \"\") throw new TypeError(\"invalid bearer credential\");\n}\n\n"),
        "stream" => output.push_str("export function validateRustOwnedCredentialPolicy(token: string): void {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\" && validateBearerToken(token) !== \"\") throw new TypeError(\"invalid bearer credential\");\n}\n\n"),
        "inference" => output.push_str("export async function validateRustOwnedCredentialPolicy(token: string): Promise<void> {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\") await validateInferenceCredential(token);\n}\n\n"),
        "filesystem" => output.push_str("export async function validateRustOwnedCredentialPolicy(token: string): Promise<void> {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\") await validateFilesystemCredential(token);\n}\n\n"),
        _ => output.push_str("export function validateRustOwnedCredentialPolicy(_token: string): void {}\n\n"),
    }
    match service.family.as_str() {
        "inference" | "filesystem" => output.push_str("export async function validateRustOwnedCredential(method: RustOwnedMethodMetadata, token: string): Promise<void> {\n  if ((method.credentialPolicy as string) === (RUST_OWNED_CREDENTIAL_POLICY as string)) await validateRustOwnedCredentialPolicy(token);\n}\n\n"),
        _ => output.push_str("export function validateRustOwnedCredential(method: RustOwnedMethodMetadata, token: string): void {\n  if ((method.credentialPolicy as string) === (RUST_OWNED_CREDENTIAL_POLICY as string)) validateRustOwnedCredentialPolicy(token);\n}\n\n"),
    }
    output.push_str(&format!(
        "export type {title}Method = keyof typeof {constant};\n\n"
    ));
    output.push_str("export interface RustOwnedInvoker {\n  invoke<TRequest, TResponse>(method: RustOwnedMethodMetadata, request: TRequest): Promise<TResponse>;\n}\n\n");
    let invoker_parameter = if service.methods.is_empty() {
        "_invoker"
    } else {
        "invoker"
    };
    output.push_str(&format!(
        "export function create{title}Client({invoker_parameter}: RustOwnedInvoker) {{\n  return {{\n"
    ));
    for method in &service.methods {
        output.push_str(&format!(
            "    {}(request: {}): Promise<{}> {{\n      return invoker.invoke<{}, {}>({constant}.{}, request);\n    }},\n",
            method.operation_id,
            local_type(&method.request_type),
            local_type(&method.response_type),
            local_type(&method.request_type),
            local_type(&method.response_type),
            method.operation_id
        ));
    }
    output.push_str("  } as const;\n}\n");
    Ok(output)
}

fn local_type(qualified: &str) -> &str {
    qualified.rsplit('.').next().unwrap_or(qualified)
}

fn legacy_loopback_test(service: &ServiceMetadata) -> Result<String, Error> {
    let title = format!(
        "{}{}",
        service.family[..1].to_ascii_uppercase(),
        &service.family[1..]
    );
    let constant = format!("{}_METHODS", service.family.to_ascii_uppercase());
    let method = service
        .methods
        .first()
        .ok_or_else(|| Error::Missing(format!("no methods for {}", service.family)))?;
    let request_type = local_type(&method.request_type);
    let request_schema = format!("{request_type}Schema");
    let family_path = proto_import_path(&service.family);
    let rendered = format!(
        "// Generated by sdk-typescript as a real loopback consumer of the generated facade.\nimport {{ create, fromJsonString, toJsonString }} from \"@bufbuild/protobuf\";\nimport {{ expect, test }} from \"bun:test\";\nimport {{ create{title}Client, {constant}, type RustOwnedInvoker }} from \"./{family}-metadata\";\nimport {{ {request_schema}, {response_schema} }} from \"../../../../typescript/packages/{family}/generated/proto/{family}/v1/{family}_pb.js\";\n\ntest(\"generated {family} facade preserves Rust-owned route, JSON encoding, and private auth\", async () => {{\n  let seenBody: unknown;\n  const server = Bun.serve({{\n    port: 0,\n    fetch: async (request) => {{\n      expect(request.method).toBe(\"POST\");\n      expect(new URL(request.url).pathname).toBe(\"/{path}\");\n      expect(request.headers.get(\"authorization\")).toBe(\"Bearer loopback-token\");\n      seenBody = await request.json();\n      return Response.json({{}});\n    }},\n  }});\n  try {{\n    const endpoint = `http://127.0.0.1:${{server.port}}/`;\n    const invoker: RustOwnedInvoker = {{\n      async invoke<TRequest, TResponse>(method: import(\"./{family}-metadata\").RustOwnedMethodMetadata, request: TRequest): Promise<TResponse> {{\n        const response = await fetch(new URL(method.path, endpoint), {{\n          method: method.httpMethod,\n          headers: {{ authorization: \"Bearer loopback-token\", \"content-type\": \"application/json\" }},\n          body: toJsonString({request_schema}, request as never),\n        }});\n        return fromJsonString({response_schema}, await response.text()) as TResponse;\n      }},\n    }};\n    const client = create{title}Client(invoker);\n    const response = await client.{operation}(create({request_schema}));\n    expect(response).toEqual(create({response_schema}));\n    expect(seenBody).toBeDefined();\n    expect({constant}.{operation}.path).toBe(\"{path}\");\n  }} finally {{\n    server.stop(true);\n  }}\n}});\n",
        family = service.family,
        path = method.path,
        operation = method.operation_id,
        request_schema = request_schema,
        response_schema = format!("{}Schema", local_type(&method.response_type)),
    );
    Ok(rendered.replace(
        &format!(
            "../../../../typescript/packages/{}/generated/proto/{}/v1/{}_pb.js",
            service.family, service.family, service.family
        ),
        &family_path,
    ))
}

fn loopback_test(service: &ServiceMetadata) -> Result<String, Error> {
    legacy_loopback_test(service)
}

fn type_contract_test(service: &ServiceMetadata) -> Result<String, Error> {
    let title = format!(
        "{}{}",
        service.family[..1].to_ascii_uppercase(),
        &service.family[1..]
    );
    let first = service
        .methods
        .first()
        .ok_or_else(|| Error::Missing(format!("no methods for {}", service.family)))?;
    let request_type = local_type(&first.request_type);
    let response_type = local_type(&first.response_type);
    let family_path = proto_import_path(&service.family);
    Ok(format!(
        "// Generated compile contract for the Rust-owned {family} facade.\nimport {{ create{title}Client, type RustOwnedInvoker }} from \"./{family}-metadata\";\nimport type {{ {request_type}, {response_type} }} from \"{family_path}\";\n\nconst invoker: RustOwnedInvoker = {{\n  invoke<TRequest, TResponse>(_method: unknown, _request: TRequest): Promise<TResponse> {{\n    throw new Error(\"compile-only\");\n  }},\n}};\nconst client = create{title}Client(invoker);\nconst request = {{}} as {request_type};\nconst typedResult: Promise<{response_type}> = client.{operation}(request);\nvoid typedResult;\n// @ts-expect-error request fields and message shape are generated and must reject arbitrary objects.\nvoid client.{operation}({{ madeUpField: true }});\n// @ts-expect-error a response cannot be assigned to a different generated message type.\nconst wrongResult: Promise<{request_type}> = client.{operation}(request);\nvoid wrongResult;\n",
        family = service.family,
        operation = first.operation_id,
    ))
}

fn generated_files(manifest: &Manifest) -> Result<Vec<(String, String)>, Error> {
    let mut files = vec![("manifest.json".to_owned(), json(manifest)?)];
    for service in &manifest.services {
        files.push((
            format!("{}-metadata.ts", service.family),
            typescript(service)?,
        ));
        if matches!(
            service.family.as_str(),
            "actors" | "workers" | "objects" | "stream"
        ) {
            files.push((
                format!("{}-loopback.test.ts", service.family),
                loopback_test(service)?,
            ));
            files.push((
                format!("{}-types.test.ts", service.family),
                type_contract_test(service)?,
            ));
        }
    }
    Ok(files)
}

fn package_generated_files(manifest: &Manifest) -> Result<Vec<(String, String)>, Error> {
    manifest
        .services
        .iter()
        .map(|service| Ok((service.family.clone(), package_typescript(service)?)))
        .collect()
}

fn write_or_check(mode: &str, output_dir: &Path) -> Result<(), Error> {
    let source_revision =
        env::var("SDK_SOURCE_REVISION").unwrap_or_else(|_| "working-tree".to_owned());
    if source_revision == "working-tree"
        && matches!(env::var("SDK_RELEASE").as_deref(), Ok("1" | "true" | "yes"))
    {
        return Err(Error::Missing(
            "release generation requires SDK_SOURCE_REVISION".to_owned(),
        ));
    }
    let files = generated_files(&model()?)?;
    let expected_names = files
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    if mode == "write" {
        fs::create_dir_all(output_dir)?;
    }
    for (name, content) in files {
        let path = output_dir.join(name);
        if mode == "check" {
            let current = fs::read_to_string(&path)?;
            if current != content {
                return Err(Error::Missing(format!(
                    "generated output drift: {}",
                    path.display()
                )));
            }
        } else {
            fs::write(&path, &content)?;
        }
    }
    if mode == "check" {
        for entry in fs::read_dir(output_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if !expected_names.contains(name.to_string_lossy().as_ref()) {
                return Err(Error::Missing(format!(
                    "stale generated output: {}",
                    entry.path().display()
                )));
            }
        }
    }
    Ok(())
}

fn package_files(
    source_root: &Path,
    generated_root_override: Option<&Path>,
    family: &str,
) -> Result<Vec<(PathBuf, PathBuf)>, Error> {
    let package_root = source_root.join("typescript").join("packages").join(family);
    if !package_root.is_dir() {
        return Err(Error::Missing(format!(
            "Rust-owned TypeScript package is missing: {}",
            package_root.display()
        )));
    }
    let mut files = Vec::new();
    // Keep the package metadata in the staged artifact so a generated facade
    // can be consumed by the normal package build and release tooling.  The
    // manifest remains input metadata; the contract and facade are still
    // emitted from the Rust model below.
    // package.json is emitted below with Rust provenance and the generated
    // facade export; copy only human-facing package documentation here.
    for name in ["README.md", "CHANGELOG.md"] {
        let source = package_root.join(name);
        if source.is_file() {
            files.push((
                source,
                PathBuf::from("typescript")
                    .join("packages")
                    .join(&family)
                    .join(name),
            ));
        }
    }
    collect_package_support_files(&package_root, &mut files, family)?;
    let package_source_root = package_root.join("src");
    if !package_source_root.is_dir() {
        return Err(Error::Missing(format!(
            "Rust-owned TypeScript package source is missing: {}",
            package_source_root.display()
        )));
    }
    collect_package_files(
        &package_source_root,
        &package_source_root,
        &mut files,
        family,
        "src",
        true,
    )?;
    let generated_root = package_root.join("generated");
    if !generated_root.is_dir() {
        return Err(Error::Missing(format!(
            "Rust-owned TypeScript generated runtime output is missing: {}",
            generated_root.display()
        )));
    }
    collect_package_files(
        &generated_root,
        &generated_root,
        &mut files,
        family,
        "generated",
        generated_root_override.is_none(),
    )?;
    // The shared handshake messages are Rust-generated once at the repository
    // boundary. Include them in every installable remote package so a facade
    // can negotiate the authenticated endpoint without importing another SDK
    // family or carrying a handwritten protocol copy.
    if generated_root_override.is_none() && !generated_root.join("proto/protocol").is_dir() {
        let shared_root = source_root.join("generated/typescript/protocol");
        if shared_root.is_dir() {
            collect_package_files(
                &shared_root,
                &shared_root,
                &mut files,
                family,
                "generated/proto/protocol",
                true,
            )?;
        }
        let control_root = source_root.join("generated/typescript/transport");
        if control_root.is_dir() {
            collect_package_files(
                &control_root,
                &control_root,
                &mut files,
                family,
                "generated/proto/transport",
                true,
            )?;
        }
    }
    if let Some(root) = generated_root_override {
        let proto_root = root.join("generated/typescript").join(family);
        if !proto_root.is_dir() {
            return Err(Error::Missing(format!(
                "Rust-owned TypeScript protobuf output is missing: {}",
                proto_root.display()
            )));
        }
        collect_package_files(
            &proto_root,
            &proto_root,
            &mut files,
            family,
            &format!("generated/proto/{family}"),
            true,
        )?;
        let protocol_root = root.join("generated/typescript/protocol");
        if protocol_root.is_dir() {
            collect_package_files(
                &protocol_root,
                &protocol_root,
                &mut files,
                family,
                "generated/proto/protocol",
                true,
            )?;
        }
        let control_root = root.join("generated/typescript/transport");
        if control_root.is_dir() {
            collect_package_files(
                &control_root,
                &control_root,
                &mut files,
                family,
                "generated/proto/transport",
                true,
            )?;
        }
        let validation_root = root.join("generated/typescript/validation");
        if validation_root.is_dir() {
            collect_package_files(
                &validation_root,
                &validation_root,
                &mut files,
                family,
                "generated/proto/validation",
                true,
            )?;
        }
    }
    Ok(files)
}

fn collect_package_support_files(
    package_root: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
    family: &str,
) -> Result<(), Error> {
    // Preserve the compiled package surface and build metadata required by
    // installation. Tests, node_modules, and generated sources are handled
    // by the dedicated collectors above.
    for entry in fs::read_dir(package_root)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if matches!(name.as_str(), "examples" | "scripts") {
                collect_package_files(&path, &path, files, family, &name, true)?;
            }
            continue;
        }
        if name == "package.json" || name == "tsconfig.tsbuildinfo" {
            continue;
        }
        if name.starts_with("tsconfig") || name == "LICENSE" {
            files.push((
                path,
                PathBuf::from("typescript")
                    .join("packages")
                    .join(family)
                    .join(name),
            ));
        }
    }
    Ok(())
}

fn generate_typescript_bindings(
    source_root: &Path,
    wire_root: &Path,
    output_root: &Path,
) -> Result<(), Error> {
    if !wire_root.is_dir() {
        return Err(Error::Missing(format!(
            "Rust wire output is missing: {}",
            wire_root.display()
        )));
    }
    let template = output_root.join(".sdk-typescript-buf.gen.yaml");
    fs::create_dir_all(output_root)?;
    fs::write(
        &template,
        "version: v2\nplugins:\n  - local: [\"bun\", \"x\", \"protoc-gen-es\"]\n    out: generated/typescript\n    strategy: all\n    opt:\n      - target=js+dts\n      - import_extension=js\n",
    )?;
    let process = Command::new("bun")
        .args([
            "x",
            "buf",
            "generate",
            &wire_root.to_string_lossy(),
            "--template",
            &template.to_string_lossy(),
            "--output",
            &output_root.to_string_lossy(),
        ])
        .current_dir(source_root)
        .status();
    let _ = fs::remove_file(&template);
    let process = process.map_err(|error| {
        Error::Missing(format!("Buf TypeScript generator could not start: {error}"))
    })?;
    if !process.success() {
        return Err(Error::Missing(format!(
            "Buf TypeScript generator failed with exit code {:?}",
            process.code(),
        )));
    }
    Ok(())
}

fn link_directory(destination: &Path, source: &Path) -> Result<(), Error> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    #[cfg(windows)]
    {
        let destination_text = destination.to_string_lossy().replace('/', "\\");
        let source_text = source.to_string_lossy().replace('/', "\\");
        let status = Command::new("cmd")
            .args(["/C", "mklink", "/J", &destination_text, &source_text])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| {
                Error::Missing(format!(
                    "could not create temporary TypeScript dependency junction: {error}"
                ))
            })?;
        if !status.success() {
            return Err(Error::Missing(format!(
                "could not create temporary TypeScript dependency junction: {}",
                destination.display()
            )));
        }
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(source, destination)?;
    #[cfg(not(any(windows, unix)))]
    return Err(Error::Missing(
        "compiled TypeScript packages require a symlink-capable platform".to_owned(),
    ));
    Ok(())
}

fn link_package_dependencies(destination: &Path, source: &Path) -> Result<(), Error> {
    if !source.is_dir() {
        return Err(Error::Missing(format!(
            "TypeScript package dependencies are missing: {}",
            source.display()
        )));
    }
    if destination.exists() || fs::symlink_metadata(destination).is_ok() {
        return Err(Error::Missing(format!(
            "temporary package dependency link already exists: {}",
            destination.display()
        )));
    }
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == ".bin" || name.starts_with('.') {
            continue;
        }
        let source_entry = entry.path();
        if !source_entry.is_dir() {
            continue;
        }
        let destination_entry = destination.join(name.as_ref());
        if name.starts_with('@') {
            fs::create_dir_all(&destination_entry)?;
            for scoped_entry in fs::read_dir(&source_entry)? {
                let scoped_entry = scoped_entry?;
                if !scoped_entry.path().is_dir() {
                    continue;
                }
                let target = fs::canonicalize(scoped_entry.path())?;
                link_directory(&destination_entry.join(scoped_entry.file_name()), &target)?;
            }
        } else {
            let target = fs::canonicalize(source_entry)?;
            link_directory(&destination_entry, &target)?;
        }
    }
    Ok(())
}

fn remove_package_dependency_link(path: &Path) -> Result<(), Error> {
    if fs::symlink_metadata(path).is_ok() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn compare_compiled_directory(expected: &Path, actual: &Path) -> Result<(), Error> {
    if !actual.is_dir() {
        return Err(Error::Missing(format!(
            "compiled TypeScript package output is missing: {}",
            actual.display()
        )));
    }
    let mut expected_files = BTreeSet::new();
    let mut actual_files = BTreeSet::new();
    collect_relative_files(expected, expected, &mut expected_files)?;
    collect_relative_files(actual, actual, &mut actual_files)?;
    if expected_files != actual_files {
        return Err(Error::Missing(format!(
            "compiled TypeScript package output drift: {}",
            actual.display()
        )));
    }
    for relative in expected_files {
        let expected_bytes = fs::read(expected.join(&relative))?;
        let actual_bytes = fs::read(actual.join(&relative))?;
        if expected_bytes != actual_bytes {
            return Err(Error::Missing(format!(
                "compiled TypeScript package file drift: {}",
                actual.join(relative).display()
            )));
        }
    }
    Ok(())
}

fn collect_relative_files(
    root: &Path,
    current: &Path,
    files: &mut BTreeSet<PathBuf>,
) -> Result<(), Error> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_relative_files(root, &path, files)?;
        } else if path.is_file() {
            files.insert(
                path.strip_prefix(root)
                    .map_err(|_| {
                        Error::Missing(format!("compiled output escaped root: {}", path.display()))
                    })?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

fn compile_package_dist(
    mode: &str,
    source_root: &Path,
    output_root: &Path,
    family: &str,
) -> Result<(), Error> {
    let source_package = source_root.join("typescript/packages").join(family);
    let output_package = output_root.join("typescript/packages").join(family);
    let output_dist = output_package.join("dist");
    let dependency_link = output_package.join("node_modules");
    let compile_root = output_root.join(".sdk-typescript-compile").join(family);
    let (target_dist, check_dist) = if mode == "write" {
        if output_dist.is_dir() {
            fs::remove_dir_all(&output_dist)?;
        }
        fs::create_dir_all(&output_dist)?;
        (output_dist.clone(), None)
    } else {
        if compile_root.exists() {
            fs::remove_dir_all(&compile_root)?;
        }
        fs::create_dir_all(&compile_root)?;
        (compile_root.clone(), Some(compile_root.clone()))
    };
    link_package_dependencies(&dependency_link, &source_package.join("node_modules"))?;
    let build_info = output_root
        .join(".sdk-typescript-compile")
        .join(format!("{family}.tsbuildinfo"));
    let compiler = source_root.join("node_modules/typescript/bin/tsc");
    let status = Command::new("node")
        .args([
            &compiler.to_string_lossy(),
            "--project",
            &output_package.join("tsconfig.json").to_string_lossy(),
            "--pretty",
            "false",
            "--outDir",
            &target_dist.to_string_lossy(),
            "--declarationMap",
            "false",
            "--tsBuildInfoFile",
            &build_info.to_string_lossy(),
        ])
        .current_dir(&output_package)
        .status()
        .map_err(|error| Error::Missing(format!("TypeScript compiler could not start: {error}")));
    let remove_link = remove_package_dependency_link(&dependency_link);
    let status = status?;
    remove_link?;
    if !status.success() {
        return Err(Error::Missing(format!(
            "TypeScript compilation failed for {family} with exit code {:?}",
            status.code()
        )));
    }
    if mode == "check" {
        compare_compiled_directory(&target_dist, &output_dist)?;
    }
    if build_info.exists() {
        fs::remove_file(build_info)?;
    }
    if let Some(check_dist) = check_dist {
        if check_dist.exists() {
            fs::remove_dir_all(check_dist)?;
        }
    }
    Ok(())
}

fn collect_package_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
    family: &str,
    output_prefix: &str,
    allow_proto: bool,
) -> Result<(), Error> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_package_files(root, &path, files, family, output_prefix, allow_proto)?;
            continue;
        }
        if !path.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| Error::Missing(format!("package file escaped root: {}", path.display())))?
            .to_path_buf();
        if output_prefix == "src" && relative == Path::new("generated-client.ts") {
            continue;
        }
        if output_prefix == "generated" && relative.starts_with("proto") && !allow_proto {
            continue;
        }
        files.push((
            path,
            PathBuf::from("typescript")
                .join("packages")
                .join(family)
                .join(output_prefix)
                .join(relative),
        ));
    }
    Ok(())
}

fn copy_or_check_package_file(mode: &str, source: &Path, destination: &Path) -> Result<(), Error> {
    // One-root developer refreshes already have these source files in place.
    // The unified pipeline uses a separate output root and still copies the
    // complete installable package tree below.
    if source == destination {
        return Ok(());
    }
    if mode == "write" {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, destination)?;
    } else {
        let current = fs::read(destination).map_err(|error| {
            Error::Missing(format!(
                "Rust-owned TypeScript package artifact is missing: {} ({error})",
                destination.display()
            ))
        })?;
        let expected = fs::read(source)?;
        if current != expected {
            return Err(Error::Missing(format!(
                "Rust-owned TypeScript package artifact drift: {}",
                destination.display()
            )));
        }
    }
    Ok(())
}

fn native_companion_dependencies(
    source_root: &Path,
    family: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, Error> {
    let companion_root = source_root
        .join("rust/crates")
        .join(format!("sdk-{family}-native"))
        .join("npm");
    if !companion_root.is_dir() {
        return Ok(serde_json::Map::new());
    }
    let mut dependencies = serde_json::Map::new();
    let mut entries = fs::read_dir(&companion_root)?.collect::<Result<Vec<_>, std::io::Error>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        if !entry.path().is_dir() {
            continue;
        }
        let manifest_path = entry.path().join("package.json");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).map_err(|error| {
                Error::Missing(format!(
                    "native companion package manifest is missing: {} ({error})",
                    manifest_path.display()
                ))
            })?)?;
        let object = value.as_object().ok_or_else(|| {
            Error::Missing(format!(
                "native companion package manifest is not an object: {}",
                manifest_path.display()
            ))
        })?;
        let name = object
            .get("name")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::Missing(format!(
                    "native companion name is missing: {}",
                    manifest_path.display()
                ))
            })?;
        let version = object
            .get("version")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::Missing(format!(
                    "native companion version is missing: {}",
                    manifest_path.display()
                ))
            })?;
        dependencies.insert(
            name.to_owned(),
            serde_json::Value::String(version.to_owned()),
        );
    }
    Ok(dependencies)
}

fn default_transport_kinds(service: &ServiceMetadata) -> serde_json::Value {
    let Some(policy) = service.remote_policy.as_ref() else {
        return serde_json::json!({});
    };
    serde_json::json!({
        "native": policy.transport.native.first().map(|option| option.kind.clone()),
        "browser": policy.transport.browser.first().map(|option| option.kind.clone()),
    })
}

fn generated_package_manifest(
    source: &Path,
    source_root: &Path,
    service: &ServiceMetadata,
    source_revision: &str,
    source_git_sha: Option<&str>,
) -> Result<String, Error> {
    let mut manifest: serde_json::Value = serde_json::from_slice(&fs::read(source)?)?;
    let object = manifest.as_object_mut().ok_or_else(|| {
        Error::Missing(format!(
            "package manifest is not an object: {}",
            source.display()
        ))
    })?;
    // Rust native companion manifests are the sole source for platform
    // package names and versions.  Removing stale input metadata here keeps
    // generated archives installable without a feature flag or hand-edited
    // JavaScript package graph.
    object.remove("optionalDependencies");
    // The generated tree already contains compiled `dist` artifacts.  A
    // source-package `prepack` hook would try to rebuild from workspace-only
    // scripts that are deliberately outside the staged package, making
    // `npm pack` depend on the source checkout instead of the Rust output.
    if let Some(scripts) = object
        .get_mut("scripts")
        .and_then(serde_json::Value::as_object_mut)
    {
        scripts.remove("prepack");
    }
    let native_companions = native_companion_dependencies(source_root, &service.family)?;
    if !native_companions.is_empty() {
        object.insert(
            "optionalDependencies".to_owned(),
            serde_json::Value::Object(native_companions.clone()),
        );
    }
    let default_transports = default_transport_kinds(service);
    let exports = object
        .entry("exports")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or_else(|| {
            Error::Missing(format!(
                "package exports is not an object: {}",
                source.display()
            ))
        })?;
    exports.insert(
        "./generated-client".to_owned(),
        serde_json::json!({
            "types": "./dist/generated-client.d.ts",
            "default": "./dist/generated-client.js"
        }),
    );
    exports.insert(
        "./provenance".to_owned(),
        serde_json::json!({
            "default": "./generated/rust-provenance.json"
        }),
    );
    if !native_companions.is_empty() {
        // Keep the generated public surface aligned with the actual native
        // adapter emitted from the package source.  This makes the default
        // factory's Rust-selected native transport addressable from an
        // installed package without a consumer-side feature switch.
        exports.insert(
            "./native".to_owned(),
            serde_json::json!({
                "types": "./dist/native.d.ts",
                "default": "./dist/native.js"
            }),
        );
    }
    object.insert(
        "acyclicGenerated".to_owned(),
        serde_json::json!({
            "generator": "sdk-typescript",
            "generatorVersion": env!("CARGO_PKG_VERSION"),
            "sourceModelRevision": source_revision,
            "sourceGitSha": source_git_sha,
            "sourceGitShaKind": source_git_sha.map(|_| "git-revision"),
            "family": service.family,
            "sourceContentSha256": service.source_content_sha256,
            "sourceModelSha256": service.source_model_sha256,
            "nativeCompanions": native_companions,
            "defaultTransports": default_transports
        }),
    );
    Ok(format!("{}\n", serde_json::to_string_pretty(&manifest)?))
}

fn generated_package_provenance(
    service: &ServiceMetadata,
    native_companions: &serde_json::Map<String, serde_json::Value>,
    source_revision: &str,
    source_git_sha: Option<&str>,
    generated_source_sha256: &str,
    generated_artifact_sha256: &str,
) -> Result<String, Error> {
    Ok(format!(
        "{}\n",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema": "acyclic.sdk.typescript.package.provenance.v1",
            "generator": "sdk-typescript",
            "generatorVersion": env!("CARGO_PKG_VERSION"),
            "sourceModelRevision": source_revision,
            "sourceGitSha": source_git_sha,
            "sourceGitShaKind": source_git_sha.map(|_| "git-revision"),
            "family": service.family,
            "rustCrate": service.rust_crate,
            "sourceArtifact": service.source_artifact,
            "descriptorSha256": service.descriptor_sha256,
            "sourceContentSha256": service.source_content_sha256,
            "sourceModelSha256": service.source_model_sha256,
            "nativeCompanions": native_companions,
            "defaultTransports": default_transport_kinds(service),
            "generatedSourceSha256": generated_source_sha256,
            "generatedClientSha256": generated_artifact_sha256
        }))?
    ))
}

fn write_or_check_generated_package_metadata(
    mode: &str,
    source_root: &Path,
    output_root: &Path,
    service: &ServiceMetadata,
    source_revision: &str,
    source_git_sha: Option<&str>,
    generated_client: &str,
) -> Result<(), Error> {
    let source_manifest = source_root
        .join("typescript/packages")
        .join(&service.family)
        .join("package.json");
    let output_root = output_root
        .join("typescript/packages")
        .join(&service.family);
    let output_manifest = output_root.join("package.json");
    let provenance = output_root.join("generated/rust-provenance.json");
    let native_companions = native_companion_dependencies(source_root, &service.family)?;
    let expected_manifest = generated_package_manifest(
        &source_manifest,
        source_root,
        service,
        source_revision,
        source_git_sha,
    )?;
    let generated_artifact = output_root.join("dist/generated-client.js");
    let generated_artifact_sha256 = digest(&fs::read(&generated_artifact).map_err(|error| {
        Error::Missing(format!(
            "generated TypeScript client artifact is missing: {} ({error})",
            generated_artifact.display()
        ))
    })?);
    let expected_provenance = generated_package_provenance(
        service,
        &native_companions,
        source_revision,
        source_git_sha,
        &digest(generated_client.as_bytes()),
        &generated_artifact_sha256,
    )?;
    if mode == "write" {
        fs::create_dir_all(provenance.parent().expect("provenance has parent"))?;
        fs::write(output_manifest, expected_manifest)?;
        fs::write(provenance, expected_provenance)?;
    } else {
        if fs::read_to_string(&output_manifest).map_err(|error| {
            Error::Missing(format!("generated package manifest is missing: {error}"))
        })? != expected_manifest
        {
            return Err(Error::Missing(format!(
                "Rust-generated package manifest drift: {}",
                output_manifest.display()
            )));
        }
        if fs::read_to_string(&provenance).map_err(|error| {
            Error::Missing(format!("generated package provenance is missing: {error}"))
        })? != expected_provenance
        {
            return Err(Error::Missing(format!(
                "Rust-generated package provenance drift: {}",
                provenance.display()
            )));
        }
    }
    Ok(())
}

fn read_wire_model_revision(wire_root: &Path) -> Result<String, Error> {
    let authority = wire_root.join("rust-authority.json");
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&authority).map_err(|error| {
            Error::Missing(format!(
                "Rust wire authority manifest is missing: {} ({error})",
                authority.display()
            ))
        })?)?;
    let revision = value
        .get("source_revision")
        .and_then(serde_json::Value::as_str)
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            Error::Missing(format!(
                "Rust wire authority model revision is invalid: {}",
                authority.display()
            ))
        })?;
    Ok(revision.to_owned())
}

fn read_wire_source_git_sha(wire_root: &Path) -> Result<Option<String>, Error> {
    let authority = wire_root.join("rust-authority.json");
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&authority).map_err(|error| {
            Error::Missing(format!(
                "Rust wire authority manifest is missing: {} ({error})",
                authority.display()
            ))
        })?)?;
    let Some(value) = value
        .get("source_git_sha")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(None);
    };
    if !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Missing(format!(
            "Rust wire authority Git revision is invalid: {}",
            authority.display()
        )));
    }
    Ok(Some(value.to_owned()))
}

fn write_or_check_packages(
    mode: &str,
    source_root: &Path,
    output_root: &Path,
    wire_root: Option<&Path>,
) -> Result<(), Error> {
    // The wire authority is the request-bound source identity for staged
    // packages.  It is a model revision, deliberately kept separate from the
    // checkout Git revision recorded by sdk-generation's outer manifest.
    let source_model_revision = wire_root
        .map(read_wire_model_revision)
        .transpose()?
        .or_else(|| env::var("SDK_SOURCE_REVISION").ok())
        .unwrap_or_else(|| "working-tree".to_owned());
    let source_git_sha = wire_root
        .map(read_wire_source_git_sha)
        .transpose()?
        .flatten()
        .or_else(|| env::var("SDK_SOURCE_GIT_SHA").ok());
    if matches!(env::var("SDK_RELEASE").as_deref(), Ok("1" | "true" | "yes"))
        && source_git_sha.is_none()
    {
        return Err(Error::Missing(
            "release package generation requires SDK_SOURCE_GIT_SHA or Rust wire authority output"
                .to_owned(),
        ));
    }
    if source_model_revision == "working-tree"
        && matches!(env::var("SDK_RELEASE").as_deref(), Ok("1" | "true" | "yes"))
    {
        return Err(Error::Missing(
            "release generation requires Rust wire authority output".to_owned(),
        ));
    }
    if let Some(wire_root) = wire_root {
        generate_typescript_bindings(source_root, wire_root, output_root)?;
    }
    let manifest = model()?;
    for (family, content) in package_generated_files(&manifest)? {
        let path = output_root
            .join("typescript")
            .join("packages")
            .join(&family)
            .join("src")
            .join("generated-client.ts");
        if mode == "write" {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
        }
        if mode == "check" {
            let current = fs::read_to_string(&path)?;
            if current != content {
                return Err(Error::Missing(format!(
                    "Rust-generated TypeScript package drift: {}",
                    path.display()
                )));
            }
        } else {
            fs::write(&path, &content)?;
        }
        let service = manifest
            .services
            .iter()
            .find(|service| service.family == family)
            .ok_or_else(|| Error::Missing(format!("missing generated service {family}")))?;
        if source_root != output_root {
            for (source, relative) in
                package_files(source_root, wire_root.map(|_| output_root), &family)?
            {
                copy_or_check_package_file(mode, &source, &output_root.join(relative))?;
            }
            // TypeScript's NodeNext resolver needs the Rust-generated package
            // manifest before compiling files that use import.meta. The
            // manifest is part of the generated artifact, so write/check it
            // before invoking the compiler and leave the same verification in
            // place for every package.
            compile_package_dist(mode, source_root, output_root, &family)?;
            write_or_check_generated_package_metadata(
                mode,
                source_root,
                output_root,
                service,
                &source_model_revision,
                source_git_sha.as_deref(),
                &content,
            )?;
        }
        if source_root == output_root && wire_root.is_none() {
            let shared_root = source_root.join("generated/typescript/protocol");
            if shared_root.is_dir() {
                let mut shared_files = Vec::new();
                collect_package_files(
                    &shared_root,
                    &shared_root,
                    &mut shared_files,
                    &family,
                    "generated/proto/protocol",
                    true,
                )?;
                for (source, relative) in shared_files {
                    copy_or_check_package_file(mode, &source, &output_root.join(relative))?;
                }
            }
            let control_root = source_root.join("generated/typescript/transport");
            if control_root.is_dir() {
                let mut control_files = Vec::new();
                collect_package_files(
                    &control_root,
                    &control_root,
                    &mut control_files,
                    &family,
                    "generated/proto/transport",
                    true,
                )?;
                for (source, relative) in control_files {
                    copy_or_check_package_file(mode, &source, &output_root.join(relative))?;
                }
            }
        }
    }
    if wire_root.is_some() {
        let generated_typescript = output_root.join("generated/typescript");
        if generated_typescript.is_dir() {
            fs::remove_dir_all(&generated_typescript)?;
        }
        let generated_root = output_root.join("generated");
        if generated_root.is_dir() && fs::read_dir(&generated_root)?.next().is_none() {
            fs::remove_dir(&generated_root)?;
        }
    }
    let compile_root = output_root.join(".sdk-typescript-compile");
    if compile_root.is_dir() && fs::read_dir(&compile_root)?.next().is_none() {
        fs::remove_dir(&compile_root)?;
    }
    Ok(())
}

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let mode = args
        .next()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| "write".to_owned());
    if !matches!(
        mode.as_str(),
        "write" | "check" | "packages-write" | "packages-check"
    ) {
        eprintln!(
            "usage: sdk-typescript [write|check] [output-directory] | [packages-write|packages-check] [repo-root]"
        );
        return ExitCode::FAILURE;
    }
    let source_root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_OUTPUT));
    // Package generation receives a source root for provenance and a distinct
    // output root for installable artifacts. The source root is deliberately
    // not written by the unified pipeline.
    let output = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| source_root.clone());
    let wire_root = args.next().map(PathBuf::from);
    let result = match mode.as_str() {
        "packages-write" => {
            write_or_check_packages("write", &source_root, &output, wire_root.as_deref())
        }
        "packages-check" => {
            write_or_check_packages("check", &source_root, &output, wire_root.as_deref())
        }
        _ => write_or_check(&mode, &output),
    };
    match result {
        Ok(()) => {
            println!(
                "{} Rust-owned TypeScript output in {}",
                if mode.ends_with("write") {
                    "wrote"
                } else {
                    "checked"
                },
                output.display()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models_all_sdk_family_routes() {
        let manifest = model().expect("Rust descriptors and routes are compatible");
        assert_eq!(manifest.services.len(), 8);
        for (service, view) in manifest
            .services
            .iter()
            .zip(acyclic_sdk_contract_wire::FAMILY_VIEWS)
        {
            assert_eq!(service.family, view.name);
            assert_eq!(service.modeled_operations, view.operation_policies.len());
            assert_eq!(service.operations.len(), view.operation_policies.len());
            for (operation, policy) in service.operations.iter().zip(view.operation_policies) {
                assert_eq!(operation.rpc, policy.rpc);
                assert!(!operation.capabilities.is_empty());
                assert!(!operation.errors.is_empty());
                assert!(!operation.validations.is_empty());
            }
            assert_eq!(service.http_projection, view.has_http_projection());
            assert!(!service.source_artifact.is_empty());
            assert!(!service.package.is_empty());
            if view.has_http_projection() {
                assert_eq!(service.methods.len(), view.routes().len());
                assert!(service.methods.iter().all(|method| {
                    !method.docs.is_empty()
                        && service
                            .operations
                            .iter()
                            .any(|operation| operation.rpc == method.rpc)
                }));
            } else {
                assert!(service.methods.is_empty());
            }
        }
        let registry_http_families = acyclic_sdk_contract_wire::explicit_http_family_views()
            .map(|view| view.name)
            .collect::<Vec<_>>();
        let manifest_http_families = manifest
            .services
            .iter()
            .filter(|service| service.http_projection)
            .map(|service| service.family.as_str())
            .collect::<Vec<_>>();
        assert_eq!(manifest_http_families, registry_http_families);
        assert_eq!(manifest.services[0].family, "actors");
        assert_eq!(manifest.services[0].methods.len(), 8);
        assert_eq!(manifest.services[1].family, "workers");
        assert_eq!(manifest.services[1].methods.len(), 7);
        assert_eq!(manifest.services[0].methods[0].operation_id, "createActor");
        assert!(!manifest.services[0].methods[0].docs.is_empty());
        assert_eq!(
            manifest.services[1].methods[5].path,
            "v1/workers/versions/{sha256hex}/invoke"
        );
        assert_eq!(
            manifest.services[1].methods[5].path_parameters,
            vec!["sha256hex"]
        );
        assert_eq!(manifest.services[2].family, "objects");
        assert_eq!(manifest.services[2].methods.len(), 13);
        let objects_policy = manifest.services[2]
            .remote_policy
            .as_ref()
            .expect("Objects policy is emitted from the Rust model");
        assert_eq!(objects_policy.protocol, "https-or-loopback-http");
        assert_eq!(objects_policy.behavior_binding, "native-wasm");
        assert_eq!(manifest.services[3].family, "stream");
        assert_eq!(manifest.services[3].methods.len(), 10);
        let stream_policy = manifest.services[3]
            .remote_policy
            .as_ref()
            .expect("Stream policy is emitted from the Rust model");
        assert_eq!(stream_policy.protocol, "https-or-loopback-http");
        assert_eq!(stream_policy.behavior_binding, "native-wasm");
        let inference = manifest
            .services
            .iter()
            .find(|service| service.family == "inference")
            .expect("Inference is exported from the Rust registry");
        assert_eq!(inference.methods.len(), 14);
        assert_eq!(inference.modeled_operations, 14);
        assert!(inference.http_projection);
        let machines = manifest
            .services
            .iter()
            .find(|service| service.family == "machines")
            .expect("Machines is exported from the Rust registry");
        assert_eq!(machines.methods.len(), 0);
        assert_eq!(machines.grpc_methods.len(), 19);
        assert_eq!(machines.modeled_operations, 19);
        assert!(!machines.http_projection);
        let machines_policy = machines
            .remote_policy
            .as_ref()
            .expect("Machines native policy is emitted from the Rust model");
        assert_eq!(machines_policy.auth, "mtls");
        assert_eq!(machines_policy.credential_policy, "mtls-files");
        assert_eq!(machines_policy.behavior_binding, "rust-native-grpc");
        assert_eq!(machines_policy.transport.native.len(), 1);
        assert_eq!(machines_policy.transport.native[0].kind, "grpc");
        for family in ["filesystem", "harness"] {
            let service = manifest
                .services
                .iter()
                .find(|service| service.family == family)
                .expect("descriptor-only family is exported from the Rust registry");
            assert!(service.modeled_operations > 0);
            assert!(service.methods.is_empty());
            assert!(!service.http_projection);
        }
        assert!(
            manifest
                .services
                .iter()
                .all(|service| { service.source_content_sha256 == service.source_model_sha256 })
        );
        let machines = generated_files(&manifest)
            .expect("generated files")
            .into_iter()
            .find(|(name, _)| name == "machines-metadata.ts")
            .expect("Machines metadata");
        assert!(machines.1.contains("MACHINES_OPERATIONS"));
        assert!(machines.1.contains("capabilities"));
        assert!(machines.1.contains("validations"));
    }

    #[test]
    fn output_is_deterministic_without_ts_input() {
        let first = generated_files(&model().expect("model")).expect("files");
        let second = generated_files(&model().expect("model")).expect("files");
        assert_eq!(first, second);
    }

    #[test]
    fn source_digest_input_includes_rust_credential_policy() {
        let bytes = source_content_for_family("actors");
        let source = String::from_utf8_lossy(&bytes);
        assert!(source.contains("BEARER_NO_CRLF"));
        assert!(source.contains("FAMILY_VIEWS"));
        assert!(source.contains("TransportKind::Grpc"));
        assert!(source.contains("ActorsService/CreateActor"));
    }

    #[test]
    fn harness_metadata_uses_the_archived_rust_handshake_identity() {
        let (version, digest) =
            family_handshake_identity(acyclic_sdk_contract_wire::BindingFamily::Harness);
        assert_eq!(version, "2");
        assert_eq!(
            digest,
            "8efc8c682b2ba1025b1221dd203685bdf999d04e87568fad3acdf0e428bd84cf"
        );
        let harness = model()
            .expect("Rust descriptors and routes are compatible")
            .services
            .into_iter()
            .find(|service| service.family == "harness")
            .expect("Harness metadata");
        assert_eq!(harness.handshake_version, version);
        assert_eq!(harness.handshake_descriptor_digest, digest);
    }
}
