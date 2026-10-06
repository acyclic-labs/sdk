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

mod filesystem_results;

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
    maximum_message_bytes: u64,
    maximum_http_request_bytes: u64,
    maximum_http_response_bytes: u64,
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

fn lower_camel(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_lowercase().collect::<String>() + chars.as_str()
    })
}

fn wire_field_name(name: &str) -> String {
    let mut output = String::new();
    let mut uppercase = false;
    for character in name.chars() {
        if character == '_' {
            uppercase = true;
        } else if uppercase {
            output.extend(character.to_uppercase());
            uppercase = false;
        } else {
            output.push(character);
        }
    }
    output
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
    let remote_limits = acyclic_sdk_contract_wire::family_remote_limits(spec.family)
        .ok_or_else(|| Error::Missing(format!("missing Rust remote size policy for {}", spec.family)))?;
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
                maximum_message_bytes: remote_limits.maximum_message_bytes,
                maximum_http_request_bytes: remote_limits.maximum_http_request_bytes,
                maximum_http_response_bytes: remote_limits.maximum_http_response_bytes,
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
                maximum_message_bytes: remote_limits.maximum_message_bytes,
                maximum_http_request_bytes: remote_limits.maximum_http_request_bytes,
                maximum_http_response_bytes: remote_limits.maximum_http_response_bytes,
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
                maximum_message_bytes: remote_limits.maximum_message_bytes,
                maximum_http_request_bytes: remote_limits.maximum_http_request_bytes,
                maximum_http_response_bytes: remote_limits.maximum_http_response_bytes,
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
                maximum_message_bytes: remote_limits.maximum_message_bytes,
                maximum_http_request_bytes: remote_limits.maximum_http_request_bytes,
                maximum_http_response_bytes: remote_limits.maximum_http_response_bytes,
                request_timeout_millis: 60_000,
                behavior_binding: "generated-client".to_owned(),
                transport: transport_policy_metadata(spec.transport),
            }),
            "machines" => Some(RemotePolicyMetadata {
                protocol: "https".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf".to_owned(),
                response_encoding: "protobuf".to_owned(),
                response_limit_policy: "bounded-cumulative-protobuf".to_owned(),
                maximum_message_bytes: remote_limits.maximum_message_bytes,
                maximum_http_request_bytes: remote_limits.maximum_http_request_bytes,
                maximum_http_response_bytes: remote_limits.maximum_http_response_bytes,
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
                maximum_message_bytes: remote_limits.maximum_message_bytes,
                maximum_http_request_bytes: remote_limits.maximum_http_request_bytes,
                maximum_http_response_bytes: remote_limits.maximum_http_response_bytes,
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
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../../proto/filesystem/v2/filesystem.proto"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../filesystem/src/facade.rs"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../filesystem/src/hosted_contract.rs"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../filesystem/src/foundation.rs"
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
    typescript_with_paths(service, &family_path, &protocol_path, &[], &[])
}

fn package_typescript(service: &ServiceMetadata, source_root: &Path) -> Result<String, Error> {
    let family_path = package_proto_import_path(&service.family);
    let protocol_path = "../generated/proto/protocol/v1/protocol_pb.js";
    let native_companion_targets = native_companion_targets(source_root, &service.family)?;
    let native_companion_names = native_companion_names(source_root, &service.family)?;
    let package_service = package_service_for_output(service, &native_companion_targets);
    typescript_with_paths(
        &package_service,
        &family_path,
        protocol_path,
        &native_companion_targets,
        &native_companion_names,
    )
}

/// Return the Rust model as it is installable for one package output.
///
/// A package may be emitted from a source tree before its native companion
/// artifacts are qualified. In that case it cannot truthfully advertise the
/// model's native gRPC preference, so the generated package surface is
/// HTTP-only until Rust-owned companion metadata is present.
fn package_service_for_output(
    service: &ServiceMetadata,
    native_companion_targets: &[String],
) -> ServiceMetadata {
    let mut package_service = service.clone();
    if package_service.family == "inference" && native_companion_targets.is_empty() {
        if let Some(policy) = package_service.remote_policy.as_mut() {
            policy.transport.native.retain(|option| option.kind == "http");
        }
    }
    package_service
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

/// Bufbuild's TypeScript projection keeps protobuf 64-bit integers lossless as
/// bigint.  These Rust semantic integers are all uint64 on the wire; bounded
/// page/count values remain number because their contract limits fit safely in
/// the JavaScript integer range.
fn typescript_semantic_integer_is_bigint(id: &str) -> bool {
    matches!(
        id,
        "revision" | "sequence" | "timestamp_millis" | "timestamp_seconds" | "uint64"
    )
}

fn typescript_semantic_section(service: &ServiceMetadata) -> String {
    use acyclic_sdk_contract_wire::type_policy::PUBLIC_NESTED_ROUTES;
    use acyclic_sdk_contract_wire::{
        PUBLIC_FIELD_BINDINGS, PublicFieldDirection, SemanticRule, WIRE_UNION_VARIANTS,
        WireValueKind, semantic_type,
    };
    let family = service.family.as_str();
    let bindings = PUBLIC_FIELD_BINDINGS
        .iter()
        .filter(|binding| binding.family == family)
        .collect::<Vec<_>>();
    let semantic_ids = bindings
        .iter()
        .map(|binding| binding.semantic_type)
        .collect::<BTreeSet<_>>();
    let mut needs_string = false;
    let mut needs_bytes = false;
    let mut needs_integer = false;
    let mut needs_uint64 = false;
    let mut needs_int64 = false;
    let mut needs_message = false;
    for id in &semantic_ids {
        let item = semantic_type(id).expect("every public binding resolves to a Rust semantic type");
        match item.wire_kind {
            WireValueKind::String => needs_string = true,
            WireValueKind::Bytes => needs_bytes = true,
            WireValueKind::UnsignedInteger => {
                if typescript_semantic_integer_is_bigint(item.id) {
                    needs_uint64 = true;
                } else {
                    needs_integer = true;
                }
            }
            WireValueKind::SignedInteger => {
                if typescript_semantic_integer_is_bigint(item.id) {
                    needs_int64 = true;
                } else {
                    needs_integer = true;
                }
            }
            WireValueKind::Message => needs_message = true,
            WireValueKind::Timestamp => needs_uint64 = true,
            WireValueKind::Enum => needs_integer = true,
            WireValueKind::Boolean | WireValueKind::Oneof => {}
        }
    }
    let needs_bigint = needs_uint64 || needs_int64;
    let mut output = String::from(
        "// Rust-owned semantic projections. Generated from type_policy.rs; do not edit.\n\n",
    );
    output.push_str("declare const rustOwnedSemanticBrand: unique symbol;\n");
    if needs_string {
        output.push_str("function assertRustOwnedString(value: unknown): asserts value is string { if (typeof value !== \"string\") throw new TypeError(\"value must be a string\"); }\n");
    }
    if needs_bytes {
        output.push_str("function assertRustOwnedUint8Array(value: unknown): asserts value is Uint8Array { if (!ArrayBuffer.isView(value) || Object.prototype.toString.call(value) !== \"[object Uint8Array]\") throw new TypeError(\"value must be a Uint8Array\"); try { Reflect.apply(Uint8Array.prototype.slice, value, [0, 0]); } catch { throw new TypeError(\"value must be a Uint8Array\"); } }\n");
    }
    if needs_integer {
        output.push_str("function assertRustOwnedInteger(value: unknown): asserts value is number { if (typeof value !== \"number\" || !Number.isFinite(value) || !Number.isSafeInteger(value)) throw new TypeError(\"value must be a finite safe integer\"); }\n");
    }
    if needs_bigint {
        output.push_str("function assertRustOwnedBigInt(value: unknown): asserts value is bigint { if (typeof value !== \"bigint\") throw new TypeError(\"value must be a bigint\"); }\n");
    }
    if needs_uint64 {
        output.push_str("function assertRustOwnedUint64(value: unknown): asserts value is bigint { assertRustOwnedBigInt(value); if (value < 0n || value > 18446744073709551615n) throw new RangeError(\"value must fit an unsigned 64-bit wire field\"); }\n");
    }
    if needs_int64 {
        output.push_str("function assertRustOwnedInt64(value: unknown): asserts value is bigint { assertRustOwnedBigInt(value); if (value < -9223372036854775808n || value > 9223372036854775807n) throw new RangeError(\"value must fit a signed 64-bit wire field\"); }\n");
    }
    if needs_message {
        output.push_str("function assertRustOwnedMessage(value: unknown): asserts value is object { if (value === null || typeof value !== \"object\") throw new TypeError(\"value must be an object\"); }\n");
    }
    output.push_str("export type RustOwnedSemanticString<Name extends string> = string & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticBytes<Name extends string> = Uint8Array & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticNumber<Name extends string> = number & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticBigInt<Name extends string> = bigint & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticMessage<Name extends string, Value extends object> = Value & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedOpenEnumValue<Name extends string> = number & { readonly [rustOwnedSemanticBrand]: Name };\n");
    output.push_str("export type RustOwnedSemanticOneof<Name extends string, Value> = Value & { readonly [rustOwnedSemanticBrand]: Name };\n\n");
    output.push_str("export interface RustOwnedSemanticFieldMetadata { readonly family: string; readonly field: string; readonly semanticType: string; readonly module: string; readonly message: string; readonly wireField: string; readonly direction: \"request\" | \"response\" | \"nested_message\" | \"embedded_only\"; readonly rules: readonly string[]; }\n\n");
    for id in semantic_ids {
        let item =
            semantic_type(id).expect("every public binding resolves to a Rust semantic type");
        let name = typescript_semantic_name(item.id);
        let base = match item.wire_kind {
            WireValueKind::String => format!("RustOwnedSemanticString<{id:?}>"),
            WireValueKind::Bytes => format!("RustOwnedSemanticBytes<{id:?}>"),
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger => {
                if typescript_semantic_integer_is_bigint(item.id) {
                    format!("RustOwnedSemanticBigInt<{id:?}>")
                } else {
                    format!("RustOwnedSemanticNumber<{id:?}>")
                }
            }
            WireValueKind::Boolean => "boolean".to_owned(),
            WireValueKind::Message => format!("RustOwnedSemanticMessage<{id:?}, Value>"),
            WireValueKind::Timestamp => format!("RustOwnedSemanticBigInt<{id:?}>"),
            WireValueKind::Enum => format!("RustOwnedOpenEnumValue<{id:?}>"),
            WireValueKind::Oneof => format!("RustOwnedSemanticOneof<{id:?}, RustOwnedWireChoice>"),
        };
        if item.wire_kind == WireValueKind::Message {
            output.push_str(&format!("export type {name}<Value extends object> = {base};\n"));
        } else {
            output.push_str(&format!("export type {name} = {base};\n"));
        }
        let mut checks = String::new();
        let bigint_integer = matches!(
            item.wire_kind,
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger
        ) && typescript_semantic_integer_is_bigint(item.id);
        let bigint_value = bigint_integer || item.wire_kind == WireValueKind::Timestamp;
        let zero = if bigint_value { "0n" } else { "0" };
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
                    WireValueKind::UnsignedInteger
                        | WireValueKind::SignedInteger
                        | WireValueKind::Timestamp,
                    SemanticRule::NonNegative,
                ) => format!(
                    "if (value < {zero}) throw new RangeError(\"value must be non-negative\");"
                ),
                (
                    WireValueKind::UnsignedInteger
                        | WireValueKind::SignedInteger
                        | WireValueKind::Timestamp,
                    SemanticRule::StrictlyPositive,
                ) => format!(
                    "if (value <= {zero}) throw new RangeError(\"value must be positive\");"
                ),
                (WireValueKind::Bytes, SemanticRule::FixedLength(length)) => format!(
                    "if (value.byteLength !== {length}) throw new RangeError(\"value has the wrong length\");"
                ),
                (WireValueKind::Bytes, SemanticRule::MaxBytes(maximum)) => format!(
                    "if (value.byteLength > {maximum}) throw new RangeError(\"value exceeds its byte limit\");"
                ),
                (
                    WireValueKind::UnsignedInteger
                        | WireValueKind::SignedInteger
                        | WireValueKind::Timestamp,
                    SemanticRule::MaxItems(maximum),
                ) => {
                    let maximum = if bigint_value {
                        format!("{maximum}n")
                    } else {
                        maximum.to_string()
                    };
                    format!(
                        "if (value > {maximum}) throw new RangeError(\"value exceeds its item limit\");"
                    )
                }
                (
                    WireValueKind::UnsignedInteger
                        | WireValueKind::SignedInteger
                        | WireValueKind::Timestamp,
                    SemanticRule::BoundedInteger { min, max },
                ) => {
                    let (min, max) = if bigint_value {
                        (format!("{min}n"), format!("{max}n"))
                    } else {
                        (min.to_string(), max.to_string())
                    };
                    format!(
                        "if (value < {min} || value > {max}) throw new RangeError(\"value is outside its bounded integer range\");"
                    )
                }
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
        let (generic, parameter, result) = match item.wire_kind {
            WireValueKind::Bytes => (String::new(), "value: Uint8Array".to_owned(), name.clone()),
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger if bigint_integer => {
                (String::new(), "value: bigint".to_owned(), name.clone())
            }
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger => {
                (String::new(), "value: number".to_owned(), name.clone())
            }
            WireValueKind::Boolean => (String::new(), "value: boolean".to_owned(), name.clone()),
            WireValueKind::Message => (
                "<Value extends object>".to_owned(),
                "value: Value".to_owned(),
                format!("{name}<Value>"),
            ),
            WireValueKind::Timestamp => (String::new(), "value: bigint".to_owned(), name.clone()),
            WireValueKind::Enum => (String::new(), "value: number".to_owned(), name.clone()),
            WireValueKind::Oneof => (String::new(), "value: RustOwnedWireChoice".to_owned(), name.clone()),
            _ => (String::new(), "value: string".to_owned(), name.clone()),
        };
        let type_check = match item.wire_kind {
            WireValueKind::String => "assertRustOwnedString(value);",
            WireValueKind::Bytes => "assertRustOwnedUint8Array(value);",
            WireValueKind::UnsignedInteger if bigint_integer => {
                "assertRustOwnedUint64(value);"
            }
            WireValueKind::SignedInteger if bigint_integer => {
                "assertRustOwnedInt64(value);"
            }
            WireValueKind::UnsignedInteger | WireValueKind::SignedInteger | WireValueKind::Enum => {
                "assertRustOwnedInteger(value);"
            }
            WireValueKind::Message => "assertRustOwnedMessage(value);",
            WireValueKind::Timestamp => "assertRustOwnedUint64(value);",
            _ => "",
        };
        checks.insert_str(0, type_check);
        output.push_str(&format!("export function make{name}{generic}({parameter}): {result} {{ {checks} return value as {result}; }}\n"));
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
            PublicFieldDirection::EmbeddedOnly => "embedded_only",
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
    // The open compatibility union is still Rust-owned, but its `known` arm
    // must consume the actual descriptor-backed messages for this service.
    // Emitting `value: object` here erases the Rust descriptor inventory at
    // the consumer boundary and lets a known payload silently become an
    // arbitrary object.  The unknown arm remains opaque bytes so newer
    // senders stay forward-compatible.
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
    let known_message_types = message_types
        .iter()
        .map(|qualified| local_type(qualified))
        .collect::<Vec<_>>();
    let known_message_payload = if known_message_types.is_empty() {
        "never".to_owned()
    } else {
        known_message_types.join(" | ")
    };
    output.push_str(&format!(
        "export type RustOwnedKnownWireMessage = {known_message_payload};\n\n"
    ));
    let union_variants = WIRE_UNION_VARIANTS
        .iter()
        .map(|variant| {
            let payload = if variant.tag == "known" {
                "RustOwnedKnownWireMessage".to_owned()
            } else {
                match variant.payload_wire_kind {
                    WireValueKind::String => "string".to_owned(),
                    WireValueKind::Bytes => "Uint8Array".to_owned(),
                    WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => {
                        "number".to_owned()
                    }
                    WireValueKind::Boolean => "boolean".to_owned(),
                    WireValueKind::Message => "never".to_owned(),
                    WireValueKind::Timestamp | WireValueKind::Enum | WireValueKind::Oneof => {
                        "unknown".to_owned()
                    }
                }
            };
            format!(
                "  {{ readonly kind: {:?}; readonly value: {payload} }}",
                variant.tag,
            )
        })
        .collect::<Vec<_>>()
        .join(" |\n");
    output.push_str(&format!(
        "export type RustOwnedWireChoice =\n{union_variants};\n\n"
    ));
    output
}

/// Emit the public facade's actual request and response types.  The protobuf
/// descriptors remain the wire contract, while this Rust-owned projection
/// recursively replaces every mapped field with its nominal semantic type.
/// Keeping the projection in generated-client.ts makes the public adapters
/// consume the same source-owned types instead of maintaining a parallel
/// TypeScript contract.
fn public_field_is_optional(service: &ServiceMetadata, message: &str, field: &str) -> bool {
    service
        .methods
        .iter()
        .flat_map(|method| {
            [
                (local_type(&method.request_type), &method.request_fields),
                (local_type(&method.response_type), &method.response_fields),
            ]
        })
        .chain(service.grpc_methods.iter().flat_map(|method| {
            [
                (local_type(&method.request_type), &method.request_fields),
                (local_type(&method.response_type), &method.response_fields),
            ]
        }))
        .find_map(|(message_name, fields)| {
            (message_name == message)
                .then(|| fields.iter().find(|candidate| candidate.name == field))
                .flatten()
                .map(|candidate| candidate.optional)
        })
        .unwrap_or(false)
}

fn typescript_public_types_section(
    service: &ServiceMetadata,
    family_path: &str,
) -> String {
    use acyclic_sdk_contract_wire::type_policy::{
        semantic_type, PublicFieldDirection, PUBLIC_FIELD_BINDINGS, PUBLIC_NESTED_ROUTES,
        WireValueKind,
    };
    let family = service.family.as_str();
    let bindings = PUBLIC_FIELD_BINDINGS
        .iter()
        .filter(|binding| binding.family == family)
        .collect::<Vec<_>>();
    let mut messages = BTreeSet::new();
    // The control handshake is emitted separately from the protocol schema
    // above. It is an adapter concern, not part of the family public facade;
    // keeping it out here also prevents a family schema from claiming the
    // transport protocol's request/response messages as its own types.
    for method in service.methods.iter().filter(|method| method.operation_id != "handshake") {
        messages.insert(local_type(&method.request_type).to_owned());
        messages.insert(local_type(&method.response_type).to_owned());
    }
    for method in service.grpc_methods.iter().filter(|method| method.rpc_name != "Handshake") {
        messages.insert(local_type(&method.request_type).to_owned());
        messages.insert(local_type(&method.response_type).to_owned());
    }
    for binding in &bindings {
        if matches!(binding.direction, PublicFieldDirection::NestedMessage) {
            messages.insert(binding.message.to_owned());
        }
    }
    for route in PUBLIC_NESTED_ROUTES.iter().filter(|route| route.family == family) {
        messages.insert(route.request_message.to_owned());
        messages.insert(route.nested_message.to_owned());
    }

    let mut output = String::new();
    output.push_str(&format!("import type * as RustWire from \"{family_path}\";\n\n"));
    output.push_str("// Rust-owned public facade types. Generated from type_policy.rs; do not edit.\n\n");
    output.push_str("export type RustOwnedPublicField<Name extends string, Value> = Value & { readonly __rustOwnedSemantic?: Name };\n\n");

    for message in messages {
        let direct = bindings
            .iter()
            .filter(|binding| binding.message == message)
            .collect::<Vec<_>>();
        let nested = PUBLIC_NESTED_ROUTES
            .iter()
            .filter(|route| route.family == family && route.request_message == message)
            .collect::<Vec<_>>();
        let mut fields = Vec::new();
        for binding in direct {
            let field = wire_field_name(binding.wire_field);
            let item = semantic_type(binding.semantic_type)
                .expect("public binding must resolve to Rust semantic type");
            let value = match item.wire_kind {
                WireValueKind::String => format!("RustOwned{}", typescript_semantic_name(item.id).trim_start_matches("RustOwned")),
                WireValueKind::Bytes => format!("RustOwned{}", typescript_semantic_name(item.id).trim_start_matches("RustOwned")),
                WireValueKind::SignedInteger | WireValueKind::UnsignedInteger => format!("RustOwned{}", typescript_semantic_name(item.id).trim_start_matches("RustOwned")),
                WireValueKind::Boolean => "boolean".to_owned(),
                WireValueKind::Message if item.rust_name == "Image" && family == "machines" => {
                    "RustOwnedPublicImage".to_owned()
                }
                WireValueKind::Message => format!(
                    "RustOwnedSemanticMessage<\"{}\", RustWire.{}>",
                    item.id.escape_default(),
                    item.rust_name
                ),
                WireValueKind::Timestamp => format!("RustOwnedSemanticBigInt<\"{}\">", item.id.escape_default()),
                WireValueKind::Enum => format!("RustOwnedOpenEnumValue<\"{}\">", item.id.escape_default()),
                WireValueKind::Oneof => format!("RustOwnedSemanticOneof<\"{}\", RustOwnedWireChoice>", item.id.escape_default()),
            };
            fields.push((
                field.clone(),
                value,
                public_field_is_optional(service, message.as_str(), binding.wire_field),
            ));
        }
        for route in nested {
            let field = wire_field_name(route.nested_field);
            fields.push((field, format!("RustOwnedPublic{}", route.nested_message), false));
        }
        fields.sort_by(|left, right| left.0.cmp(&right.0));
        fields.dedup_by(|left, right| left.0 == right.0);
        let alias = format!("RustOwnedPublic{message}");
        if family == "machines" && message == "Image" {
            output.push_str("export type RustOwnedPublicImage = Omit<RustWire.Image, \"immutableReference\"> & {\n");
            output.push_str("  readonly immutableReference: { readonly value: RustOwnedSha256Digest; readonly case: \"managedDigest\" } | { readonly value: RustOwnedSha256Digest; readonly case: \"customDigest\" } | { readonly value: Omit<RustWire.CheckpointId, \"value\"> & { readonly value: RustOwnedCheckpointId }; readonly case: \"checkpoint\" } | { readonly case: undefined; readonly value?: undefined };\n");
            output.push_str("} & { readonly [rustOwnedSemanticBrand]: \"immutable_image\" };\n");
            continue;
        }
        if fields.is_empty() {
            output.push_str(&format!("export type {alias} = RustWire.{message};\n"));
        } else {
            let names = fields
                .iter()
                .map(|(field, _, _)| format!("{field:?}"))
                .collect::<Vec<_>>()
                .join(" | ");
            output.push_str(&format!("export type {alias} = Omit<RustWire.{message}, {names}> & {{\n"));
            for (field, value, optional) in fields {
                if optional {
                    output.push_str(&format!("  readonly {field}?: {value} | undefined;\n"));
                } else {
                    output.push_str(&format!("  readonly {field}: {value};\n"));
                }
            }
            output.push_str("};\n");
        }
    }
    output.push('\n');
    output.push_str(&format!("export interface RustOwned{}PublicClient {{\n", title_case(family)));
    let mut seen = BTreeSet::<String>::new();
    let mut emit_method = |operation: &str, request_type: &str, response_type: &str, client_streaming: bool, server_streaming: bool| {
        if !seen.insert(operation.to_owned()) { return; }
        let result = if server_streaming {
            format!("AsyncIterable<RustOwnedPublic{}>", local_type(response_type))
        } else {
            format!("Promise<RustOwnedPublic{}>", local_type(response_type))
        };
        let request = if client_streaming {
            format!("AsyncIterable<RustOwnedPublic{}>", local_type(request_type))
        } else {
            format!("RustOwnedPublic{}", local_type(request_type))
        };
        output.push_str(&format!("  readonly {operation}: (request: {request}, signal?: AbortSignal) => {result};\n"));
    };
    for method in service.methods.iter().filter(|method| method.operation_id != "handshake") {
        emit_method(&method.operation_id, &method.request_type, &method.response_type, method.client_streaming, method.server_streaming);
    }
    let public_grpc_methods = service
        .grpc_methods
        .iter()
        .filter(|method| method.rpc_name != "Handshake")
        .cloned()
        .collect::<Vec<_>>();
    for (method, operation) in public_grpc_methods.iter().zip(grpc_operation_keys(&public_grpc_methods)) {
        emit_method(&operation, &method.request_type, &method.response_type, method.client_streaming, method.server_streaming);
    }
    output.push_str("}\n\n");
    output
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

fn typescript_with_paths(
    service: &ServiceMetadata,
    family_path: &str,
    protocol_path: &str,
    native_companion_targets: &[String],
    native_companion_names: &[String],
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
    output.push_str(r#"/**
 * Rust-owned await cancellation boundary for generated and thin provider clients.
 *
 * Abort stops awaiting the Rust operation. The underlying native or WASM call has
 * no JavaScript cancellation hook and may continue until its accepted lifetime
 * completes.
 */
export function invokeWithAbort<Output>(operation: () => PromiseLike<Output>, signal?: AbortSignal): Promise<Output> {
  if (signal === undefined) return Promise.resolve().then(operation);
  const reason = () => signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  if (signal.aborted) return Promise.reject(reason());
  return new Promise<Output>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => { cleanup(); reject(reason()); };
    signal.addEventListener("abort", onAbort, { once: true });
    Promise.resolve().then(operation).then(
      (value) => { cleanup(); resolve(value); },
      (error: unknown) => { cleanup(); reject(error); },
    );
  });
}

/**
 * Rust-owned cancellation boundary for an operation that has already started.
 *
 * The rejection observer is attached before checking an already-aborted signal.
 * This preserves the caller-facing abort result while observing a late native
 * rejection, which keeps a failed operation from becoming an unhandled promise.
 */
export function awaitWithAbort<Output>(operation: PromiseLike<Output>, signal?: AbortSignal): Promise<Output> {
  if (signal === undefined) return Promise.resolve(operation);
  const reason = () => signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  return new Promise<Output>((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const resolveOnce = (value: Output) => { if (settled) return; settled = true; cleanup(); resolve(value); };
    const rejectOnce = (error: unknown) => { if (settled) return; settled = true; cleanup(); reject(error); };
    const onAbort = () => rejectOnce(reason());
    // Attach this observer before the pre-abort check. The operation is already
    // in flight, so its eventual rejection must always be observed.
    Promise.resolve(operation).then(resolveOnce, rejectOnce);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}

"#);
    output.push_str("/** Identifies a native adapter load failure that is safe for the Rust-qualified fallback. */\nexport function isRustOwnedNativeLoadError(error: unknown): boolean {\n  if (error === null || typeof error !== \"object\") return false;\n  const candidate = error as { readonly code?: unknown; readonly message?: unknown };\n  if (candidate.code === \"ERR_DLOPEN_FAILED\" || candidate.code === \"DLOPEN_FAILED\") return true;\n  return typeof candidate.message === \"string\" && (/native companion did not export/i.test(candidate.message) || /failed to load native (?:companion|module)/i.test(candidate.message));\n}\n\n");
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
    output.push_str(&typescript_semantic_section(service));
    output.push_str(&typescript_public_types_section(service, family_path));
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
            "export type RustOwnedTransportKind = \"grpc\" | \"grpc-web\" | \"http\";\nexport type RustOwnedRuntime = \"native\" | \"browser\";\nexport interface RustOwnedTransportOption {{ readonly kind: RustOwnedTransportKind; readonly streaming: boolean; readonly bearerAuth: boolean; }}\nexport interface RustOwnedRemotePolicy {{ readonly protocol: {:?}; readonly auth: {:?}; readonly credentialPolicy: {:?}; readonly requestEncoding: {:?}; readonly responseEncoding: {:?}; readonly responseLimitPolicy: {:?}; readonly maximumMessageBytes: number; readonly maximumHttpRequestBytes: number; readonly maximumHttpResponseBytes: number; readonly requestTimeoutMillis: number; readonly behaviorBinding: {:?}; readonly transport: {{ readonly native: readonly RustOwnedTransportOption[]; readonly browser: readonly RustOwnedTransportOption[]; }}; }}\nexport type RustOwnedTransportAvailability = Partial<Record<RustOwnedTransportKind, boolean>>;\n\n",
            policy.protocol,
            policy.auth,
            policy.credential_policy,
            policy.request_encoding,
            policy.response_encoding,
            policy.response_limit_policy,
            policy.behavior_binding,
        ));
        output.push_str(&format!(
            "export const {}_REMOTE_POLICY = {{ protocol: {:?}, auth: {:?}, credentialPolicy: {:?}, requestEncoding: {:?}, responseEncoding: {:?}, responseLimitPolicy: {:?}, maximumMessageBytes: {}, maximumHttpRequestBytes: {}, maximumHttpResponseBytes: {}, requestTimeoutMillis: {}, behaviorBinding: {:?}, transport: {{ native: [{}], browser: [{}] }} }} as const satisfies RustOwnedRemotePolicy;\n",
            service.family.to_ascii_uppercase(),
            policy.protocol,
            policy.auth,
            policy.credential_policy,
            policy.request_encoding,
            policy.response_encoding,
            policy.response_limit_policy,
            policy.maximum_message_bytes,
            policy.maximum_http_request_bytes,
            policy.maximum_http_response_bytes,
            policy.request_timeout_millis,
            policy.behavior_binding,
            native,
            browser,
        ));
        if !native_companion_targets.is_empty() {
            output.push_str(&format!(
                "\n/** Rust-owned native companion targets present in the generated package. */\nexport const {}_NATIVE_COMPANION_TARGETS = {:?} as const;\n",
                service.family.to_ascii_uppercase(),
                native_companion_targets,
            ));
        }
        output.push_str(&format!(
            "\nconst RUST_OWNED_NATIVE_COMPANIONS = {:?} as const;\n",
            native_companion_names,
        ));
        output.push_str(r#"
/** Selects the first Rust-qualified transport that is installed for this runtime. */
export function selectRustOwnedTransport(policy: RustOwnedRemotePolicy, runtime: RustOwnedRuntime, requested?: RustOwnedTransportKind, availability: RustOwnedTransportAvailability = {}): RustOwnedTransportKind {
  const options = policy.transport[runtime];
  if (requested !== undefined) {
    const option = options.find(candidate => candidate.kind === requested);
    if (option === undefined || availability[requested] === false) throw new TypeError(`transport ${requested} is unavailable in the ${runtime} runtime`);
    return option.kind;
  }
  const option = options.find(candidate => availability[candidate.kind] !== false);
  if (option === undefined) throw new TypeError(`no installed transport is available in the ${runtime} runtime`);
  return option.kind;
}

/** Identifies a missing optional adapter without swallowing nested dependency errors. */
export function isRustOwnedTransportUnavailable(error: unknown): boolean {
  if (error === null || typeof error !== "object") return false;
  const candidate = error as { readonly code?: unknown; readonly message?: unknown };
  const message = typeof candidate.message === "string" ? candidate.message.replaceAll("\\", "/") : "";
  const missing = /Cannot find (?:module|package) ['"]([^'"]+)['"]/i.exec(message)?.[1];
  const missingNativeAdapter = missing?.endsWith("/native.js") === true || missing === "./native.js" || missing === "../native.js";
  const missingCompanion = missing !== undefined && RUST_OWNED_NATIVE_COMPANIONS.some(specifier => missing === specifier);
  if (missingNativeAdapter || missingCompanion) return true;
  return /has no native companion/i.test(message) && (candidate.code === "ERR_MODULE_NOT_FOUND" || candidate.code === "MODULE_NOT_FOUND" || candidate.code === undefined);
}

"#);
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
        "export const {}_HANDSHAKE = {{ family: {:?}, route: {:?}, version: {:?}, descriptorDigest: {:?} }} as const;\n\n",
        service.family.to_ascii_uppercase(),
        service.family,
        service.handshake_route,
        service.handshake_version,
        service.handshake_descriptor_digest,
    ));
    output.push_str("export interface RustOwnedHandshakeMetadata { readonly family: string; readonly route: string; readonly version: string; readonly descriptorDigest: string; }\n\n");
    output.push_str("/** Builds the Rust-owned control-plane request used by native gRPC adapters. */\nexport function rustOwnedGrpcHandshakeRequest(handshake: RustOwnedHandshakeMetadata, family: string) {\n  return create(HandshakeRequestSchema, { protocol: create(ProtocolIdentitySchema, { version: handshake.version, descriptorDigest: handshake.descriptorDigest }), required: create(CapabilitySetSchema, { capabilities: [create(CapabilitySchema, { name: family, version: handshake.version })] }) });\n}\n\n");
    output.push_str("/** Validates the Rust-owned control-plane response before any application RPC. */\nexport function validateRustOwnedGrpcHandshake(response: { readonly protocol?: { readonly version: string; readonly descriptorDigest: string } | undefined; readonly supported?: { readonly capabilities: readonly { readonly name: string; readonly version: string }[] } | undefined }, handshake: RustOwnedHandshakeMetadata, family: string): void {\n  const identity = response.protocol;\n  if (identity === undefined || identity.version !== handshake.version || identity.descriptorDigest !== handshake.descriptorDigest) throw new Error(\"Rust-owned gRPC handshake identity mismatch\");\n  const capabilities = response.supported?.capabilities ?? [];\n  if (!capabilities.some(capability => capability.name === family && capability.version === handshake.version)) throw new Error(\"Rust-owned gRPC handshake capability mismatch\");\n}\n\n");
    output.push_str("/** Performs the Rust-defined authenticated endpoint negotiation before application calls. */\nexport async function negotiateRustOwnedEndpoint(fetcher: typeof fetch, endpoint: URL | string, headers: HeadersInit, handshake: RustOwnedHandshakeMetadata, maximumResponseBytes = 64 * 1024, signal?: AbortSignal): Promise<void> {\n  const requestHeaders = new Headers(headers);\n  requestHeaders.set(\"content-type\", \"application/json\");\n  const request = create(HandshakeRequestSchema, { protocol: create(ProtocolIdentitySchema, { version: handshake.version, descriptorDigest: handshake.descriptorDigest }), required: create(CapabilitySetSchema, { capabilities: [create(CapabilitySchema, { name: handshake.family, version: handshake.version })] }) });\n  const response = await fetcher(new URL(handshake.route, endpoint), { method: \"POST\", redirect: \"error\", headers: requestHeaders, body: toJsonString(HandshakeRequestSchema, request), ...(signal === undefined ? {} : { signal }) });\n  const text = await readRustOwnedHandshakeBody(response, maximumResponseBytes);\n  if (!response.ok) throw new Error(`Rust-owned endpoint handshake failed with HTTP ${response.status}: ${text || \"empty response\"}`);\n  let parsed;\n  try { parsed = fromJsonString(HandshakeResponseSchema, text); } catch (error) { throw new Error(`Rust-owned endpoint handshake returned malformed JSON: ${error instanceof Error ? error.message : String(error)}`); }\n  const identity = parsed.protocol;\n  if (identity === undefined || identity.version !== handshake.version || identity.descriptorDigest !== handshake.descriptorDigest) throw new Error(\"Rust-owned endpoint handshake identity mismatch\");\n  const capabilities = parsed.supported?.capabilities ?? [];\n  if (!capabilities.some(capability => capability.name === handshake.family && capability.version === handshake.version)) throw new Error(\"Rust-owned endpoint handshake capability mismatch\");\n}\n\nasync function readRustOwnedHandshakeBody(response: Response, maximum: number): Promise<string> {\n  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError(\"maximumResponseBytes must be a positive safe integer\");\n  if (response.body === null) { const text = await response.text(); if (new TextEncoder().encode(text).byteLength > maximum) throw new Error(\"Rust-owned endpoint handshake response exceeds configured bound\"); return text; }\n  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;\n  try { for (;;) { const item = await reader.read(); if (item.done) break; size += item.value.byteLength; if (size > maximum) { await reader.cancel().catch(() => undefined); throw new Error(\"Rust-owned endpoint handshake response exceeds configured bound\"); } chunks.push(item.value); } } finally { reader.releaseLock(); }\n  const bytes = new Uint8Array(size); let offset = 0; for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }\n  try { return new TextDecoder(\"utf-8\", { fatal: true }).decode(bytes); } catch { throw new Error(\"Rust-owned endpoint handshake response is not valid UTF-8\"); }\n}\n\n");
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
        output.push_str("export interface RustOwnedGrpcInvoker {\n  invokeGrpc<TRequest, TResponse>(method: RustOwnedGrpcMethodMetadata, request: TRequest, signal?: AbortSignal): Promise<TResponse>;\n  invokeGrpcStream<TRequest, TResponse>(method: RustOwnedGrpcMethodMetadata, request: TRequest, signal?: AbortSignal): AsyncIterable<TResponse>;\n}\n\n");
        output.push_str(&format!(
            "export function create{title}GrpcClient(invoker: RustOwnedGrpcInvoker) {{\n  return {{\n"
        ));
        for (method, operation) in service.grpc_methods.iter().zip(grpc_keys.iter()) {
            let request_type = local_type(&method.request_type);
            let response_type = local_type(&method.response_type);
            if method.server_streaming {
                output.push_str(&format!(
                    "    {operation}(request: {request_type}, signal?: AbortSignal): AsyncIterable<{response_type}> {{\n      return invoker.invokeGrpcStream<{request_type}, {response_type}>({grpc_constant}.{rpc_name}, request, signal);\n    }},\n",
                    grpc_constant = format!("{}_GRPC_METHODS", service.family.to_ascii_uppercase()),
                    rpc_name = operation,
                ));
            } else {
                output.push_str(&format!(
                    "    {operation}(request: {request_type}, signal?: AbortSignal): Promise<{response_type}> {{\n      return invoker.invokeGrpc<{request_type}, {response_type}>({grpc_constant}.{rpc_name}, request, signal);\n    }},\n",
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
        _ => output.push_str("export function validateRustOwnedCredentialPolicy(token: string): void {\n  if ((RUST_OWNED_CREDENTIAL_POLICY as string) === \"bearer-no-crlf\" && (token.trim().length === 0 || /[\\r\\n]/.test(token))) throw new TypeError(\"invalid bearer credential\");\n}\n\n"),
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
    if service.family == "machines" {
        return Ok("// Generated compile contract for the Rust-owned Machines semantic facade.\nimport type { RustOwnedPublicImage, RustOwnedSha256Digest } from \"./machines-metadata\";\ndeclare const image: RustOwnedPublicImage;\nif (image.immutableReference.case === \"managedDigest\") {\n  const digest: RustOwnedSha256Digest = image.immutableReference.value;\n  void digest;\n}\nif (image.immutableReference.case === \"checkpoint\") {\n  // @ts-expect-error checkpoint payloads are not SHA-256 digests.\n  const digest: RustOwnedSha256Digest = image.immutableReference.value;\n  void digest;\n}\n".to_owned());
    }
    let first = service
        .methods
        .first()
        .ok_or_else(|| Error::Missing(format!("no methods for {}", service.family)))?;
    let request_type = local_type(&first.request_type);
    let response_type = local_type(&first.response_type);
    let family_path = proto_import_path(&service.family);
    let mut output = format!(
        "// Generated compile contract for the Rust-owned {family} facade.\nimport {{ create{title}Client, type RustOwnedInvoker }} from \"./{family}-metadata\";\nimport type {{ {request_type}, {response_type} }} from \"{family_path}\";\n\nconst invoker: RustOwnedInvoker = {{\n  invoke<TRequest, TResponse>(_method: unknown, _request: TRequest): Promise<TResponse> {{\n    throw new Error(\"compile-only\");\n  }},\n}};\nconst client = create{title}Client(invoker);\nconst request = {{}} as {request_type};\nconst typedResult: Promise<{response_type}> = client.{operation}(request);\nvoid typedResult;\n// @ts-expect-error request fields and message shape are generated and must reject arbitrary objects.\nvoid client.{operation}({{ madeUpField: true }});\n// @ts-expect-error a response cannot be assigned to a different generated message type.\nconst wrongResult: Promise<{request_type}> = client.{operation}(request);\nvoid wrongResult;\n",
        family = service.family,
        operation = first.operation_id,
    );
    if service.family == "machines" {
        output.push_str(
            "import type { RustOwnedPublicImage, RustOwnedSha256Digest } from \"./machines-metadata\";\ndeclare const image: RustOwnedPublicImage;\nif (image.immutableReference.case === \"managedDigest\") {\n  const digest: RustOwnedSha256Digest = image.immutableReference.value;\n  void digest;\n}\nif (image.immutableReference.case === \"checkpoint\") {\n  // @ts-expect-error checkpoint payloads are not SHA-256 digests.\n  const digest: RustOwnedSha256Digest = image.immutableReference.value;\n  void digest;\n}\n",
        );
    } else if service.family == "workers" {
        output.push_str(
            "import type { RustOwnedPublicInvokeResponse } from \"./workers-metadata\";\ndeclare const invoke: RustOwnedPublicInvokeResponse;\nconst revision: bigint | undefined = invoke.resolvedRevision;\nvoid revision;\n// @ts-expect-error uint64 semantic values remain bigint at the facade boundary.\nconst lossy: number = invoke.resolvedRevision;\nvoid lossy;\n",
        );
    }
    Ok(output)
}

fn await_abort_test(service: &ServiceMetadata) -> Result<String, Error> {
    Ok(format!(
        "// Generated Rust-owned cancellation regression test.\nimport {{ expect, test }} from \"bun:test\";\nimport {{ awaitWithAbort }} from \"./{}-metadata\";\n\ntest(\"awaitWithAbort observes a pre-aborted operation's late rejection\", async () => {{\n  const controller = new AbortController();\n  let rejectLate: ((reason?: unknown) => void) | undefined;\n  const operation = new Promise<never>((_resolve, reject) => {{ rejectLate = reject; }});\n  const reason = new Error(\"pre-aborted\");\n  controller.abort(reason);\n  await expect(awaitWithAbort(operation, controller.signal)).rejects.toBe(reason);\n  rejectLate!(new Error(\"late native rejection\"));\n  await Promise.resolve();\n}});\n\ntest(\"awaitWithAbort isolates two waiters when one aborts\", async () => {{\n  let resolveOperation: ((value: number) => void) | undefined;\n  const operation = new Promise<number>((resolve) => {{ resolveOperation = resolve; }});\n  const first = new AbortController();\n  const second = new AbortController();\n  const firstWaiter = awaitWithAbort(operation, first.signal);\n  const secondWaiter = awaitWithAbort(operation, second.signal);\n  const reason = new Error(\"first waiter aborted\");\n  first.abort(reason);\n  await expect(firstWaiter).rejects.toBe(reason);\n  resolveOperation!(42);\n  await expect(secondWaiter).resolves.toBe(42);\n}});\n",
        service.family
    ))
}

fn generated_files(manifest: &Manifest) -> Result<Vec<(String, String)>, Error> {
    let mut files = vec![("manifest.json".to_owned(), json(manifest)?)];
    for service in &manifest.services {
        files.push((
            format!("{}-metadata.ts", service.family),
            typescript(service)?,
        ));
        files.push((
            format!("{}-abort.test.ts", service.family),
            await_abort_test(service)?,
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
        } else if service.family == "machines" {
            files.push((
                "machines-types.test.ts".to_owned(),
                type_contract_test(service)?,
            ));
        }
    }
    Ok(files)
}

fn package_generated_files(
    manifest: &Manifest,
    source_root: &Path,
) -> Result<Vec<(String, String)>, Error> {
    manifest
        .services
        .iter()
        .map(|service| Ok((service.family.clone(), package_typescript(service, source_root)?)))
        .collect()
}

fn filesystem_root_facade() -> String {
    r#"// Generated by sdk-typescript from the Rust Filesystem model. Do not edit.
// The provider selection and lazy platform boundaries are Rust-owned output.

import type {
  BrowserFsOptions,
  FsVolumeEngine,
  HostedFsEngine,
  HostedFsOptions,
  NativeFsEngine,
  NativeFsOptions,
} from "./contracts.js";

export * from "./browser.js";

export type DefaultFsOptions = BrowserFsOptions | HostedFsOptions | NativeFsOptions;
export type DefaultFsEngine = FsVolumeEngine | HostedFsEngine | NativeFsEngine;

export async function openHostedFs(options: HostedFsOptions): Promise<HostedFsEngine> {
  const module = await import("./hosted.js");
  return module.openHostedFs(options);
}

export async function openNativeFs(options: NativeFsOptions): Promise<NativeFsEngine> {
  const module = await import("./native.js");
  return module.openNativeFs(options);
}

export async function openFs(options: DefaultFsOptions): Promise<DefaultFsEngine> {
  if ("endpoint" in options) return openHostedFs(options);
  if ("root" in options) return openNativeFs(options);
  return (await import("./browser.js")).openBrowserFs(options);
}
"#
    .to_owned()
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
        if output_prefix == "src"
            && family == "filesystem"
            && relative == Path::new("index.ts")
        {
            continue;
        }
        if output_prefix == "src"
            && family == "filesystem"
            && relative == Path::new("workspace-results.ts")
        {
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

fn native_companion_targets(source_root: &Path, family: &str) -> Result<Vec<String>, Error> {
    let companion_root = source_root
        .join("rust/crates")
        .join(format!("sdk-{family}-native"))
        .join("npm");
    if !companion_root.is_dir() {
        return Ok(Vec::new());
    }
    let mut targets = fs::read_dir(&companion_root)?
        .collect::<Result<Vec<_>, std::io::Error>>()?
        .into_iter()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    targets.sort();
    Ok(targets)
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
        let target = entry.file_name().to_string_lossy().into_owned();
        let package_family = if family == "filesystem" { "fs" } else { family };
        let expected_name = format!("@acyclic-labs/{package_family}-{target}");
        if name != expected_name {
            return Err(Error::Missing(format!(
                "native companion package name must match its Rust-owned target directory: {} ({name} != {expected_name})",
                manifest_path.display()
            )));
        }
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

fn native_companion_names(source_root: &Path, family: &str) -> Result<Vec<String>, Error> {
    let mut names = native_companion_dependencies(source_root, family)?
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    names.sort();
    Ok(names)
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

fn package_native_module_available(source_manifest: &Path) -> bool {
    let Some(package_root) = source_manifest.parent() else {
        return false;
    };
    [
        package_root.join("src/native.ts"),
        package_root.join("src/native.js"),
        package_root.join("dist/native.js"),
    ]
    .into_iter()
    .any(|path| path.is_file())
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
    let native_module_available = package_native_module_available(source);
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
    if service.family == "filesystem" {
        exports.insert(
            ".".to_owned(),
            serde_json::json!({
                "types": "./dist/index.d.ts",
                "default": "./dist/index.js"
            }),
        );
    }
    exports.insert(
        "./provenance".to_owned(),
        serde_json::json!({
            "default": "./generated/rust-provenance.json"
        }),
    );
    if native_module_available || !native_companions.is_empty() {
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
    } else {
        exports.remove("./native");
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
    generated_artifact_sha256: Option<&str>,
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
            "generatedClientSha256": generated_artifact_sha256,
            "artifactKind": if generated_artifact_sha256.is_some() { "compiled-package" } else { "source-projection" }
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
    let source_projection = source_root == output_root;
    let output_root = output_root
        .join("typescript/packages")
        .join(&service.family);
    let output_manifest = output_root.join("package.json");
    let provenance = output_root.join("generated/rust-provenance.json");
    let native_companion_targets = native_companion_targets(source_root, &service.family)?;
    let package_service = package_service_for_output(service, &native_companion_targets);
    let native_companions = native_companion_dependencies(source_root, &service.family)?;
    let expected_manifest = generated_package_manifest(
        &source_manifest,
        source_root,
        &package_service,
        source_revision,
        source_git_sha,
    )?;
    let generated_artifact = output_root.join("dist/generated-client.js");
    let generated_artifact_sha256 = if source_projection {
        None
    } else {
        Some(digest(&fs::read(&generated_artifact).map_err(|error| {
            Error::Missing(format!(
                "generated TypeScript client artifact is missing: {} ({error})",
                generated_artifact.display()
            ))
        })?))
    };
    let expected_provenance = generated_package_provenance(
        &package_service,
        &native_companions,
        source_revision,
        source_git_sha,
        &digest(generated_client.as_bytes()),
        generated_artifact_sha256.as_deref(),
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
    for (family, content) in package_generated_files(&manifest, source_root)? {
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
        if family == "filesystem" {
            let facade_path = output_root
                .join("typescript/packages/filesystem/src/index.ts");
            let facade = filesystem_root_facade();
            if mode == "check" {
                let current = fs::read_to_string(&facade_path)?;
                if current != facade {
                    return Err(Error::Missing(format!(
                        "Rust-generated Filesystem root facade drift: {}",
                        facade_path.display()
                    )));
                }
            } else {
                if let Some(parent) = facade_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&facade_path, facade)?;
            }
            let results_path = output_root
                .join("typescript/packages/filesystem/src/workspace-results.ts");
            let results = filesystem_results::emit(source_root)?;
            if mode == "check" {
                let current = fs::read_to_string(&results_path)?;
                if current != results {
                    return Err(Error::Missing(format!(
                        "Rust-generated Filesystem workspace result drift: {}",
                        results_path.display()
                    )));
                }
            } else {
                if let Some(parent) = results_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&results_path, results)?;
            }
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
            if mode == "write" {
                let source_manifest = source_root
                    .join("typescript/packages")
                    .join(&family)
                    .join("package.json");
                let output_manifest = output_root
                    .join("typescript/packages")
                    .join(&family)
                    .join("package.json");
                fs::copy(&source_manifest, &output_manifest)?;
            }
            compile_package_dist(mode, source_root, output_root, &family)?;
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
        // The checkout itself is also a supported generated output. Keep its
        // package manifest and provenance in lockstep with the Rust companion
        // manifests; otherwise a clean install of the checked-in package has
        // no optional platform adapters even though staged release artifacts
        // do. This is deliberately emitted by the Rust generator rather than
        // maintained in the TypeScript package recipe.
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
        assert_eq!(machines_policy.auth, "bearer");
        assert_eq!(machines_policy.credential_policy, "bearer-no-crlf");
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
    fn semantic_constructors_guard_runtime_inputs() {
        let manifest = model().expect("model");
        let machines = manifest
            .services
            .iter()
            .find(|service| service.family == "machines")
            .expect("Machines metadata");
        let generated = typescript_semantic_section(machines);
        assert!(generated.contains("ArrayBuffer.isView(value)"));
        assert!(generated.contains("Object.prototype.toString.call(value) !== \"[object Uint8Array]\""));
        assert!(generated.contains("Reflect.apply(Uint8Array.prototype.slice, value, [0, 0])"));
        assert!(generated.contains("Number.isFinite(value)"));
        assert!(generated.contains("Number.isSafeInteger(value)"));
        assert!(generated.contains("RustOwnedSemanticBigInt"));
        assert!(generated.contains("RustOwnedOpenEnumValue"));
        assert!(generated.contains("RustOwnedSemanticOneof"));
        assert!(generated.contains("assertRustOwnedBigInt(value);"));
        assert!(generated.contains("value must fit an unsigned 64-bit wire field"));
        assert!(generated.contains("18446744073709551615n"));
        assert!(generated.contains("value must be positive"));
        assert!(generated.contains("value exceeds its item limit"));
        assert!(generated.contains("typeof value !== \"object\""));
        assert!(generated.contains("assertRustOwnedUint8Array(value);"));
        assert!(generated.contains("assertRustOwnedInteger(value);"));
        assert!(generated.contains(
            "export function makeRustOwnedIdempotencyKeyMessage<Value extends object>(value: Value): RustOwnedIdempotencyKeyMessage<Value>"
        ));
        assert!(!generated.contains("makeRustOwnedIdempotencyKeyMessage(<Value extends object>"));
        assert!(!generated.contains("assertRustOwnedMessage(value);assertRustOwnedMessage(value);"));

        let filesystem = manifest
            .services
            .iter()
            .find(|service| service.family == "filesystem")
            .expect("Filesystem metadata");
        let filesystem_generated = typescript_semantic_section(filesystem);
        assert!(filesystem_generated.contains("typeof value !== \"string\""));
        assert!(filesystem_generated.contains("assertRustOwnedString(value);"));
        assert!(!filesystem_generated.contains("function assertRustOwnedInt64"));
        assert!(!filesystem_generated.contains("function assertRustOwnedMessage"));
    }

    #[test]
    fn credential_policy_emits_machine_bearer_guard() {
        let service = model()
            .expect("model")
            .services
            .into_iter()
            .find(|service| service.family == "machines")
            .expect("Machines metadata");
        let generated = typescript(&service).expect("generated Machines metadata");
        assert!(generated.contains("token.trim().length === 0"));
        assert!(generated.contains("/[\\r\\n]/.test(token)"));
        assert!(!generated.contains("validateRustOwnedCredentialPolicy(_token"));
    }

    #[test]
    fn generated_abort_helpers_observe_started_promises_before_preabort() {
        let service = model()
            .expect("model")
            .services
            .into_iter()
            .find(|service| service.family == "actors")
            .expect("Actors metadata");
        let generated = typescript(&service).expect("generated Actors metadata");
        let observer = generated
            .find("Promise.resolve(operation).then(resolveOnce, rejectOnce);")
            .expect("started-operation rejection observer");
        let preabort = generated
            .find("if (signal.aborted) onAbort();")
            .expect("started-operation pre-abort check");
        assert!(observer < preabort);
        assert!(generated.contains(
            "if (signal === undefined) return Promise.resolve().then(operation);"
        ));

        let runtime_test = await_abort_test(&service).expect("generated abort regression test");
        assert!(runtime_test.contains("pre-aborted operation's late rejection"));
        assert!(runtime_test.contains("rejectLate!(new Error(\"late native rejection\"))"));
        assert!(runtime_test.contains("isolates two waiters when one aborts"));
        assert!(runtime_test.contains("resolveOperation!(42)"));
    }

    #[test]
    fn public_types_preserve_wire64_presence_and_machine_image_oneof() {
        let manifest = model().expect("model");
        let machines = manifest
            .services
            .iter()
            .find(|service| service.family == "machines")
            .expect("Machines metadata");
        let generated = typescript_public_types_section(
            machines,
            "../generated/proto/machines/v1/machines_pb.js",
        );
        assert!(generated.contains("RustOwnedPublicImage = Omit<RustWire.Image, \"immutableReference\">"));
        assert!(generated.contains("readonly immutableReference:"));
        assert!(generated.contains("case: \"managedDigest\""));
        assert!(generated.contains("case: \"customDigest\""));
        assert!(generated.contains("case: \"checkpoint\""));
        assert!(generated.contains("Omit<RustWire.CheckpointId, \"value\"> & { readonly value: RustOwnedCheckpointId }"));
        assert!(!generated.contains("readonly value: RustWire.CheckpointId; readonly case: \"checkpoint\""));
        assert!(!generated.contains("readonly customDigest: RustOwnedSha256Digest"));
        assert!(!generated.contains("readonly managedDigest: RustOwnedSha256Digest"));

        let workers = manifest
            .services
            .iter()
            .find(|service| service.family == "workers")
            .expect("Workers metadata");
        let workers_generated = typescript_public_types_section(
            workers,
            "../generated/proto/workers/v1/workers_pb.js",
        );
        assert!(workers_generated.contains("readonly resolvedRevision?: RustOwnedRevision | undefined;"));
    }

    #[test]
    fn package_model_removes_inference_grpc_without_a_companion() {
        let inference = model()
            .expect("model")
            .services
            .into_iter()
            .find(|service| service.family == "inference")
            .expect("Inference metadata");
        let adjusted = package_service_for_output(&inference, &[]);
        let native = adjusted
            .remote_policy
            .expect("Inference policy")
            .transport
            .native;
        assert_eq!(native.iter().map(|option| option.kind.as_str()).collect::<Vec<_>>(), ["http"]);

        let with_companion = package_service_for_output(
            &inference,
            &["linux-x64-gnu".to_owned()],
        );
        let native = with_companion
            .remote_policy
            .expect("Inference policy")
            .transport
            .native;
        assert!(native.iter().any(|option| option.kind == "grpc"));
    }

    #[test]
    fn generated_missing_module_guard_names_only_rust_adapters() {
        let stream = model()
            .expect("model")
            .services
            .into_iter()
            .find(|service| service.family == "stream")
            .expect("Stream metadata");
        let generated = typescript_with_paths(
            &stream,
            "stream.proto.js",
            "protocol.proto.js",
            &["linux-x64-gnu".to_owned()],
            &["@acyclic-labs/stream-linux-x64-gnu".to_owned()],
        )
        .expect("generated TypeScript");
        assert!(generated.contains("@acyclic-labs/stream-linux-x64-gnu"));
        assert!(generated.contains("Cannot find (?:module|package)"));
        assert!(generated.contains("missingCompanion"));
        assert!(generated.contains("missingNativeAdapter"));
        assert!(!generated.contains(
            "if (candidate.code === \"ERR_MODULE_NOT_FOUND\" || candidate.code === \"MODULE_NOT_FOUND\") return true;"
        ));
    }

    #[test]
    fn filesystem_root_facade_is_rust_emitted() {
        let generated = filesystem_root_facade();
        assert!(generated.starts_with(
            "// Generated by sdk-typescript from the Rust Filesystem model. Do not edit."
        ));
        assert!(generated.contains("export async function openFs"));
        assert!(generated.contains("if (\"endpoint\" in options)"));
        assert!(generated.contains("if (\"root\" in options)"));
        assert!(generated.contains("./browser.js"));
        assert!(generated.contains("./native.js"));
    }

    #[test]
    fn filesystem_workspace_results_are_rust_emitted() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let generated = filesystem_results::emit(&repository)
            .expect("generated Filesystem results");
        assert!(generated.starts_with(
            "// Generated by sdk-typescript from the Rust Filesystem result model. Do not edit."
        ));
        assert!(generated.contains("const JOIN_STATUSES"));
        assert!(generated.contains("const GENERATION_LIMIT_FIELDS"));
        assert!(generated.contains("export function parseWorkspaceCommit"));
    }

    #[test]
    fn native_export_tracks_an_actual_package_module() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        assert!(package_native_module_available(
            &repository.join("typescript/packages/filesystem/package.json")
        ));
        assert!(!package_native_module_available(
            &repository.join("typescript/packages/actors/package.json")
        ));
    }

    #[test]
    fn package_optional_companions_are_read_from_rust_owned_manifests() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let stream = native_companion_dependencies(&repository, "stream")
            .expect("Stream native package metadata");
        let machines = native_companion_dependencies(&repository, "machines")
            .expect("Machines native package metadata");
        let filesystem = native_companion_dependencies(&repository, "filesystem")
            .expect("Filesystem native package metadata");
        assert!(filesystem.contains_key("@acyclic-labs/fs-darwin-arm64"));

        assert_eq!(stream.len(), 8);
        assert_eq!(machines.len(), 8);
        assert!(stream.contains_key("@acyclic-labs/stream-linux-x64-musl"));
        assert!(stream.contains_key("@acyclic-labs/stream-darwin-arm64"));
        assert!(machines.contains_key("@acyclic-labs/machines-linux-x64-gnu"));
        assert!(machines.contains_key("@acyclic-labs/machines-win32-arm64"));
        assert!(stream
            .values()
            .all(|version| version.as_str() == Some("0.2.0")));
        assert!(machines
            .values()
            .all(|version| version.as_str() == Some("0.2.0")));

        for (family, expected) in [("stream", 8usize), ("machines", 8usize), ("filesystem", filesystem.len())] {
            let service = model()
                .expect("Rust model")
                .services
                .into_iter()
                .find(|service| service.family == family)
                .expect("native family metadata");
            let source_manifest = repository
                .join("typescript/packages")
                .join(family)
                .join("package.json");
            let generated = generated_package_manifest(
                &source_manifest,
                &repository,
                &service,
                "working-tree",
                None,
            )
            .expect("Rust-generated package manifest");
            let manifest: serde_json::Value =
                serde_json::from_str(&generated).expect("generated package JSON");
            assert_eq!(
                manifest
                    .get("optionalDependencies")
                    .and_then(serde_json::Value::as_object)
                    .map(|value| value.len()),
                Some(expected),
                "{family} optional dependencies must be Rust-owned companion packages",
            );
        }
    }

    #[test]
    fn inference_native_companions_drive_package_metadata_and_exports() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let targets = native_companion_targets(&repository, "inference")
            .expect("Inference native target metadata");
        let companions = native_companion_dependencies(&repository, "inference")
            .expect("Inference native package metadata");
        assert_eq!(targets.len(), 8);
        assert!(targets.iter().any(|target| target == "linux-x64-gnu"));
        assert!(targets.iter().any(|target| target == "linux-x64-musl"));
        assert_eq!(companions.len(), targets.len());
        assert!(companions.contains_key("@acyclic-labs/inference-darwin-arm64"));
        assert!(companions
            .values()
            .all(|version| version.as_str() == Some("0.2.0")));

        let service = model()
            .expect("Rust model")
            .services
            .into_iter()
            .find(|service| service.family == "inference")
            .expect("Inference metadata");
        let adjusted = package_service_for_output(&service, &targets);
        assert!(adjusted
            .remote_policy
            .expect("Inference policy")
            .transport
            .native
            .iter()
            .any(|option| option.kind == "grpc"));

        let generated = package_typescript(&service, &repository)
            .expect("Rust-generated Inference package facade");
        assert!(generated.contains("INFERENCE_NATIVE_COMPANION_TARGETS"));
        assert!(generated.contains("@acyclic-labs/inference-linux-x64-gnu"));
        assert!(generated.contains("@acyclic-labs/inference-win32-x64"));

        let manifest = generated_package_manifest(
            &repository.join("typescript/packages/inference/package.json"),
            &repository,
            &service,
            "working-tree",
            None,
        )
        .expect("Rust-generated Inference package manifest");
        let manifest: serde_json::Value =
            serde_json::from_str(&manifest).expect("generated package JSON");
        assert_eq!(
            manifest
                .get("optionalDependencies")
                .and_then(serde_json::Value::as_object)
                .map(|value| value.len()),
            Some(8)
        );
        assert_eq!(
            manifest
                .get("exports")
                .and_then(serde_json::Value::as_object)
                .and_then(|exports| exports.get("./native"))
                .and_then(serde_json::Value::as_object)
                .and_then(|native| native.get("default"))
                .and_then(serde_json::Value::as_str),
            Some("./dist/native.js")
        );
    }

    #[test]
    fn source_projection_provenance_ignores_stale_dist_artifact() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let service = model()
            .expect("Rust model")
            .services
            .into_iter()
            .find(|service| service.family == "actors")
            .expect("Actors metadata");
        let root = env::temp_dir().join(format!(
            "sdk-typescript-source-projection-{}",
            std::process::id()
        ));
        let package = root.join("typescript/packages/actors");
        fs::create_dir_all(package.join("dist")).expect("temporary package");
        fs::copy(
            repository.join("typescript/packages/actors/package.json"),
            package.join("package.json"),
        )
        .expect("source package manifest");
        fs::write(package.join("dist/generated-client.js"), "stale").expect("stale artifact");

        write_or_check_generated_package_metadata(
            "write",
            &root,
            &root,
            &service,
            "working-tree",
            None,
            "generated source",
        )
        .expect("source projection metadata");
        let provenance: serde_json::Value = serde_json::from_slice(
            &fs::read(package.join("generated/rust-provenance.json")).expect("provenance"),
        )
        .expect("provenance JSON");
        assert!(provenance["generatedClientSha256"].is_null());
        assert_eq!(provenance["artifactKind"], "source-projection");
        assert!(package.join("dist/generated-client.js").is_file());
        fs::remove_dir_all(root).expect("temporary package cleanup");
    }

    #[test]
    fn compiled_package_provenance_requires_external_dist_artifact_hash() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let service = model()
            .expect("Rust model")
            .services
            .into_iter()
            .find(|service| service.family == "actors")
            .expect("Actors metadata");
        let root = env::temp_dir().join(format!(
            "sdk-typescript-compiled-package-{}",
            std::process::id()
        ));
        let source_package = root.join("source/typescript/packages/actors");
        let output_package = root.join("output/typescript/packages/actors");
        fs::create_dir_all(&source_package).expect("source package");
        fs::create_dir_all(&output_package).expect("output package");
        fs::copy(
            repository.join("typescript/packages/actors/package.json"),
            source_package.join("package.json"),
        )
        .expect("source package manifest");

        let missing = write_or_check_generated_package_metadata(
            "write",
            &root.join("source"),
            &root.join("output"),
            &service,
            "working-tree",
            None,
            "generated source",
        )
        .expect_err("external package must require its compiled artifact");
        assert!(matches!(missing, Error::Missing(message) if message.contains("generated TypeScript client artifact is missing")));

        let artifact = output_package.join("dist/generated-client.js");
        fs::create_dir_all(artifact.parent().expect("artifact parent")).expect("artifact directory");
        let artifact_bytes = b"compiled artifact";
        fs::write(&artifact, artifact_bytes).expect("compiled artifact");
        write_or_check_generated_package_metadata(
            "write",
            &root.join("source"),
            &root.join("output"),
            &service,
            "working-tree",
            None,
            "generated source",
        )
        .expect("compiled package metadata");
        let provenance: serde_json::Value = serde_json::from_slice(
            &fs::read(output_package.join("generated/rust-provenance.json")).expect("provenance"),
        )
        .expect("provenance JSON");
        assert_eq!(provenance["generatedClientSha256"], digest(artifact_bytes));
        assert_eq!(provenance["artifactKind"], "compiled-package");
        fs::remove_dir_all(root).expect("temporary package cleanup");
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
