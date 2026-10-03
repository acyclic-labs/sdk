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
    process::ExitCode,
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
    modeled_operations: usize,
    http_projection: bool,
    remote_policy: Option<RemotePolicyMetadata>,
    methods: Vec<MethodMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RemotePolicyMetadata {
    protocol: String,
    auth: String,
    request_encoding: String,
    response_encoding: String,
    credential_policy: String,
    response_limit_policy: String,
    behavior_binding: String,
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
        modeled_operations,
        http_projection: !spec.routes.is_empty(),
        remote_policy: match spec.family {
            "objects" => Some(RemotePolicyMetadata {
                protocol: "https-or-loopback-http".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                behavior_binding: "native-wasm".to_owned(),
            }),
            "stream" => Some(RemotePolicyMetadata {
                protocol: "https-or-loopback-http".to_owned(),
                auth: "bearer".to_owned(),
                credential_policy: "bearer-no-crlf".to_owned(),
                request_encoding: "protobuf-json".to_owned(),
                response_encoding: "protobuf-json".to_owned(),
                response_limit_policy: "bounded-cumulative-utf8".to_owned(),
                behavior_binding: "native-wasm".to_owned(),
            }),
            _ => None,
        },
        methods,
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

fn model() -> Result<Manifest, Error> {
    let services = vec![
        service_metadata(RustService {
            family: "actors",
            rust_crate: "acyclic-actors",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::actors_descriptor",
            source_content: model_source_content(&[include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sdk-contract-wire/src/lib.rs"
            ))]),
            descriptor: acyclic_sdk_contract_wire::actors_descriptor(),
            routes: contract_routes(acyclic_sdk_contract_wire::ACTORS.routes),
        })?,
        service_metadata(RustService {
            family: "workers",
            rust_crate: "acyclic-workers",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::workers::workers_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/workers.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::workers::workers_descriptor(),
            routes: contract_routes(acyclic_sdk_contract_wire::workers::WORKERS.routes),
        })?,
        service_metadata(RustService {
            family: "objects",
            rust_crate: "acyclic-objects",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::objects::objects_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/objects.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::objects_descriptor(),
            routes: contract_routes(acyclic_sdk_contract_wire::OBJECTS_V2.routes),
        })?,
        service_metadata(RustService {
            family: "stream",
            rust_crate: "acyclic-stream",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::stream::stream_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/stream.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::stream_descriptor(),
            routes: contract_routes(acyclic_sdk_contract_wire::STREAM.routes),
        })?,
        service_metadata(RustService {
            family: "inference",
            rust_crate: "acyclic-inference",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::inference::inference_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/inference.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::inference::inference_descriptor(),
            routes: contract_routes(acyclic_sdk_contract_wire::INFERENCE.routes),
        })?,
        service_metadata(RustService {
            family: "machines",
            rust_crate: "acyclic-machines",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::machines::machines_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/machines.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::machines::machines_descriptor(),
            routes: Vec::new(),
        })?,
        service_metadata(RustService {
            family: "filesystem",
            rust_crate: "acyclic-filesystem",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::filesystem::filesystem_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/filesystem.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::filesystem::filesystem_descriptor(),
            routes: Vec::new(),
        })?,
        service_metadata(RustService {
            family: "harness",
            rust_crate: "acyclic-harness",
            source_kind: "rust-model",
            source_artifact: "acyclic_sdk_contract_wire::harness::harness_descriptor",
            source_content: model_source_content(&[
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/lib.rs"
                )),
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../sdk-contract-wire/src/harness.rs"
                )),
            ]),
            descriptor: acyclic_sdk_contract_wire::harness::harness_descriptor(),
            routes: Vec::new(),
        })?,
    ];
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
    let constant = format!("{}_METHODS", service.family.to_ascii_uppercase());
    let title = format!(
        "{}{}",
        service.family[..1].to_ascii_uppercase(),
        &service.family[1..]
    );
    let mut output = String::from(
        "// Generated by sdk-typescript from Rust descriptors and HTTP_ROUTES. Do not edit.\n\n",
    );
    let message_types = service
        .methods
        .iter()
        .flat_map(|method| {
            [
                local_type(&method.request_type),
                local_type(&method.response_type),
            ]
        })
        .collect::<BTreeSet<_>>();
    let message_imports = message_types.iter().copied().collect::<Vec<_>>().join(", ");
    let family_path = proto_import_path(&service.family);
    if message_imports.is_empty() {
        output.push_str(
            "// This family has no HTTP method projection in the current Rust model.\n\n",
        );
    } else {
        output.push_str(&format!(
            "import type {{ {message_imports} }} from \"{family_path}\";\n\n"
        ));
    }
    output.push_str("export interface RustOwnedFieldMetadata { readonly name: string; readonly jsonName: string; readonly number: number; readonly wireType: string; readonly repeated: boolean; readonly optional: boolean; readonly oneof?: string | undefined; readonly proto3Optional: boolean; }\n\n");
    output.push_str("export interface RustOwnedMethodMetadata {\n  readonly operationId: string;\n  readonly rpc: string;\n  readonly docs: string;\n  readonly path: string;\n  readonly pathParameters: readonly string[];\n  readonly httpMethod: \"POST\";\n  readonly requestType: string;\n  readonly responseType: string;\n  readonly clientStreaming: boolean;\n  readonly serverStreaming: boolean;\n  readonly requestEncoding: \"protobuf-json\";\n  readonly responseEncoding: \"protobuf-json\";\n  readonly auth: \"bearer\";\n  readonly credentialPolicy: \"bearer-no-crlf\";\n  readonly responseLimitPolicy: \"bounded-cumulative-utf8\";\n  readonly requestFields: readonly RustOwnedFieldMetadata[];\n  readonly responseFields: readonly RustOwnedFieldMetadata[];\n}\n\n");
    if let Some(policy) = &service.remote_policy {
        output.push_str(&format!(
            "export interface RustOwnedRemotePolicy {{ readonly protocol: {:?}; readonly auth: {:?}; readonly credentialPolicy: {:?}; readonly requestEncoding: {:?}; readonly responseEncoding: {:?}; readonly responseLimitPolicy: {:?}; readonly behaviorBinding: {:?}; }}\n\n",
            policy.protocol,
            policy.auth,
            policy.credential_policy,
            policy.request_encoding,
            policy.response_encoding,
            policy.response_limit_policy,
            policy.behavior_binding,
        ));
        output.push_str(&format!(
            "export const {}_REMOTE_POLICY: RustOwnedRemotePolicy = {{ protocol: {:?}, auth: {:?}, credentialPolicy: {:?}, requestEncoding: {:?}, responseEncoding: {:?}, responseLimitPolicy: {:?}, behaviorBinding: {:?} }};\n",
            service.family.to_ascii_uppercase(),
            policy.protocol,
            policy.auth,
            policy.credential_policy,
            policy.request_encoding,
            policy.response_encoding,
            policy.response_limit_policy,
            policy.behavior_binding,
        ));
    }
    output.push_str(&format!(
        "export const {}_SOURCE = {{ family: {:?}, rustCrate: {:?}, sourceKind: {:?}, sourceArtifact: {:?}, descriptorSha256: {:?}, sourceContentSha256: {:?}, sourceModelSha256: {:?}, modeledOperations: {}, httpProjection: {} }} as const;\n\n",
        service.family.to_ascii_uppercase(), service.family, service.rust_crate,
        service.source_kind, service.source_artifact, service.descriptor_sha256,
        service.source_content_sha256, service.source_model_sha256,
        service.modeled_operations, service.http_projection,
    ));
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
    if service.remote_policy.is_some() {
        output.push_str(&format!(
            "export const {family}_ROUTES = {constant};\n\n",
            family = service.family.to_ascii_uppercase(),
            constant = constant,
        ));
    }
    output.push_str("export function interpolateRustOwnedPath(method: RustOwnedMethodMetadata, request: unknown): string {\n  let path = method.path;\n  for (const parameter of method.pathParameters) {\n    const key = parameter === \"sha256hex\" ? \"versionSha256\" : parameter;\n    const value = (request as Record<string, unknown>)[key];\n    if (value === undefined || value === null) throw new TypeError(`missing path parameter ${key}`);\n    const rendered = value instanceof Uint8Array ? Array.from(value, byte => byte.toString(16).padStart(2, \"0\")).join(\"\") : typeof value === \"bigint\" ? value.toString() : encodeURIComponent(String(value));\n    path = path.replace(`{${parameter}}`, rendered);\n  }\n  return path;\n}\n\n");
    output.push_str("export function validateRustOwnedCredential(method: RustOwnedMethodMetadata, token: string): void {\n  if (method.credentialPolicy === \"bearer-no-crlf\" && (!token.trim() || /[\\r\\n]/.test(token))) throw new TypeError(\"invalid bearer credential\");\n}\n\n");
    output.push_str(&format!(
        "export type {title}Method = keyof typeof {constant};\n\n"
    ));
    output.push_str("export interface RustOwnedInvoker {\n  invoke<TRequest, TResponse>(method: RustOwnedMethodMetadata, request: TRequest): Promise<TResponse>;\n}\n\n");
    output.push_str(&format!(
        "export function create{title}Client(invoker: RustOwnedInvoker) {{\n  return {{\n"
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
            fs::write(&path, content)?;
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

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let mode = args
        .next()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| "write".to_owned());
    if mode != "write" && mode != "check" {
        eprintln!("usage: sdk-typescript [write|check] [output-directory]");
        return ExitCode::FAILURE;
    }
    let output = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_OUTPUT));
    match write_or_check(&mode, &output) {
        Ok(()) => {
            println!(
                "{} Rust-owned TypeScript prototype in {}",
                if mode == "write" { "wrote" } else { "checked" },
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
        assert_eq!(machines.modeled_operations, 19);
        assert!(!machines.http_projection);
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
    }

    #[test]
    fn output_is_deterministic_without_ts_input() {
        let first = generated_files(&model().expect("model")).expect("files");
        let second = generated_files(&model().expect("model")).expect("files");
        assert_eq!(first, second);
    }
}
