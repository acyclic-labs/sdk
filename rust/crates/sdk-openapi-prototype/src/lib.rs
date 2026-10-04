//! Rust-owned OpenAPI projection prototype.
//!
//! The generator consumes `acyclic_sdk_contract_wire::ACTORS`, a Rust-authored
//! contract model including its hosted route projection. It does not read
//! generated Rust bindings or accept a hand-authored OpenAPI document.

use std::collections::{BTreeMap, BTreeSet};

use acyclic_sdk_contract_wire::objects::OBJECTS_HTTP_JSON_FRAME_BYTES;
use acyclic_sdk_contract_wire::workers::{
    WORKERS_ENUM_DOCS, WORKERS_FIELD_DOCS, WORKERS_MESSAGE_DOCS, WORKERS_SERVICE_DOC,
};
use acyclic_sdk_contract_wire::{
    Cardinality, ContractSpec, FieldSpec, FieldType, RouteSpec, explicit_http_family_views,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

/// Errors reported while projecting the canonical contract model.
#[derive(Debug)]
pub enum Error {
    /// A required Actors service or method is absent from the model.
    MissingContract(String),
    /// The model contains a type that this small prototype does not map.
    UnsupportedType(String),
    /// Generated JSON could not be serialized.
    Json(serde_json::Error),
    /// A generated artifact could not be read.
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingContract(name) => write!(f, "missing canonical contract: {name}"),
            Self::UnsupportedType(name) => write!(f, "unsupported contract type: {name}"),
            Self::Json(error) => write!(f, "OpenAPI JSON serialization failed: {error}"),
            Self::Io(error) => write!(f, "OpenAPI artifact could not be read: {error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn schema_key(contract: &ContractSpec, name: &str) -> String {
    format!("{}_{}", contract.package.replace('.', "_"), name)
}

fn package_title(package: &str) -> String {
    let segment = package.split('.').nth(1).unwrap_or(package);
    let mut chars = segment.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_uppercase().collect::<String>() + chars.as_str()
    })
}

#[derive(Debug, Default)]
struct ContractDocs {
    messages: BTreeMap<String, String>,
    fields: BTreeMap<(String, String), String>,
    enums: BTreeMap<String, String>,
    methods: BTreeMap<String, String>,
    services: BTreeMap<String, String>,
}

impl ContractDocs {
    fn parse(contract: &ContractSpec) -> Self {
        if contract.package == "inference.customer.v1" {
            return Self::from_inference_model(contract);
        }
        let mut docs = Self::default();
        let mut pending = Vec::new();
        let mut message = None::<String>;
        let mut service = None::<String>;
        let mut enum_name = None::<String>;
        for raw_line in contract.render_proto().lines() {
            let line = raw_line.trim_end();
            let trimmed = line.trim();
            if let Some(comment) = trimmed.strip_prefix("// ") {
                pending.push(comment.to_owned());
                continue;
            }
            if trimmed.is_empty() {
                continue;
            }
            if let Some(name) = declaration_name(trimmed, "message ") {
                let name = name.to_owned();
                docs.messages.insert(name.clone(), pending.join("\n"));
                pending.clear();
                message = Some(name);
                enum_name = None;
                continue;
            }
            if let Some(name) = declaration_name(trimmed, "enum ") {
                let name = name.to_owned();
                docs.enums.insert(name.clone(), pending.join("\n"));
                pending.clear();
                enum_name = Some(name);
                message = None;
                continue;
            }
            if let Some(name) = declaration_name(trimmed, "service ") {
                let name = name.to_owned();
                docs.services.insert(name.clone(), pending.join("\n"));
                pending.clear();
                service = Some(name);
                message = None;
                enum_name = None;
                continue;
            }
            if let Some(name) = rpc_name(trimmed) {
                if service.is_some() {
                    docs.methods.insert(name.to_owned(), pending.join("\n"));
                }
                pending.clear();
                continue;
            }
            if message.is_some() && enum_name.is_none() {
                if let Some(name) = field_name(trimmed) {
                    docs.fields.insert(
                        (message.clone().unwrap_or_default(), name.to_owned()),
                        pending.join("\n"),
                    );
                    pending.clear();
                    continue;
                }
            }
            if trimmed == "}" {
                pending.clear();
                if message.is_some() {
                    message = None;
                } else if enum_name.is_some() {
                    enum_name = None;
                } else if service.is_some() {
                    service = None;
                }
                continue;
            }
            pending.clear();
        }
        if contract.package == "acyclic.workers.v1" {
            docs.messages = WORKERS_MESSAGE_DOCS
                .iter()
                .map(|entry| (entry.name.to_owned(), entry.text.to_owned()))
                .collect();
            docs.enums = WORKERS_ENUM_DOCS
                .iter()
                .map(|entry| (entry.name.to_owned(), entry.text.to_owned()))
                .collect();
            docs.services.insert(
                "WorkersService".to_owned(),
                WORKERS_SERVICE_DOC.text.to_owned(),
            );
            for method in contract.services.iter().flat_map(|service| service.methods) {
                docs.methods
                    .insert(method.name.to_owned(), method.docs.to_owned());
            }
            for message in contract.messages {
                for field in message.fields {
                    if let Some(entry) = WORKERS_FIELD_DOCS
                        .iter()
                        .find(|entry| entry.name == field.name)
                    {
                        docs.fields.insert(
                            (message.name.to_owned(), field.name.to_owned()),
                            entry.text.to_owned(),
                        );
                    }
                }
            }
        }
        docs
    }

    /// Inference routes and fields are already Rust-owned, but the current
    /// wire renderer has not yet published its comment tables. Keep the
    /// projection usable without inventing protocol prose; these structural
    /// labels are explicitly marked as such in the generated source block.
    fn from_inference_model(contract: &ContractSpec) -> Self {
        let mut docs = Self::default();
        for message in contract.messages {
            docs.messages.insert(
                message.name.to_owned(),
                format!("Rust-owned Inference message {}.", message.name),
            );
            for field in message.fields {
                docs.fields.insert(
                    (message.name.to_owned(), field.name.to_owned()),
                    format!(
                        "Rust-owned Inference field {}.{}.",
                        message.name, field.name
                    ),
                );
            }
        }
        for enum_ in contract.enums {
            docs.enums.insert(
                enum_.name.to_owned(),
                format!("Rust-owned Inference enum {}.", enum_.name),
            );
        }
        for service in contract.services {
            docs.services.insert(
                service.name.to_owned(),
                format!("Rust-owned Inference service {}.", service.name),
            );
            for method in service.methods {
                docs.methods
                    .insert(method.name.to_owned(), method.docs.to_owned());
            }
        }
        docs
    }

    fn required<'a>(map: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str, Error> {
        map.get(key)
            .map(String::as_str)
            .filter(|docs| !docs.is_empty())
            .ok_or_else(|| Error::MissingContract(format!("missing Rust documentation: {key}")))
    }

    fn required_field(&self, message: &str, field: &str) -> Result<&str, Error> {
        self.fields
            .get(&(message.to_owned(), field.to_owned()))
            .map(String::as_str)
            .filter(|docs| !docs.is_empty())
            .ok_or_else(|| {
                Error::MissingContract(format!(
                    "missing Rust field documentation: {message}.{field}"
                ))
            })
    }
}

fn declaration_name<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    line.strip_prefix(prefix)
        .and_then(|rest| rest.split_whitespace().next())
}

fn rpc_name(line: &str) -> Option<&str> {
    line.strip_prefix("rpc ")
        .and_then(|rest| rest.split('(').next())
        .filter(|name| !name.is_empty())
}

fn field_name(line: &str) -> Option<&str> {
    let line = line.strip_suffix(';')?;
    let before_number = line.split('=').next()?.trim();
    before_number.split_whitespace().last()
}

fn field_schema(contract: &ContractSpec, field: &FieldSpec) -> Result<Value, Error> {
    let mut schema = match field.field_type {
        FieldType::String => json!({"type": "string"}),
        FieldType::Bytes => {
            json!({"type": "string", "format": "byte", "x-protobuf-json": "base64"})
        }
        FieldType::Bool => json!({"type": "boolean"}),
        FieldType::Int64 => {
            json!({"type": "string", "pattern": "^-?[0-9]+$", "x-protobuf-json": "decimal-string", "x-protobuf-range": "-9223372036854775808..9223372036854775807"})
        }
        FieldType::Sint64 => {
            json!({"type": "string", "pattern": "^-?[0-9]+$", "x-protobuf-json": "decimal-string", "x-protobuf-range": "-9223372036854775808..9223372036854775807", "x-protobuf-encoding": "zigzag"})
        }
        FieldType::Uint32 => json!({"type": "integer", "minimum": 0, "maximum": 4294967295u64}),
        // Protobuf JSON requires decimal strings for uint64 values.
        FieldType::Uint64 => {
            json!({"type": "string", "pattern": "^[0-9]+$", "x-protobuf-json": "decimal-string", "x-protobuf-range": "0..18446744073709551615"})
        }
        FieldType::Message(name) => {
            if contract.message(name).is_none() && field.map.is_none() {
                return Err(Error::MissingContract(name.into()));
            }
            json!({"$ref": format!("#/components/schemas/{}", schema_key(contract, name))})
        }
        FieldType::ExternalMessage(name) if name == "google.protobuf.Timestamp" => json!({
            "type": "string",
            "format": "date-time",
            "x-protobuf-message": name,
            "x-protobuf-json": "RFC3339 timestamp string"
        }),
        FieldType::ExternalMessage(name) => {
            return Err(Error::UnsupportedType(format!(
                "external message {name} requires imported component projection"
            )));
        }
        FieldType::Enum(name) => {
            if contract.enum_(name).is_none() {
                return Err(Error::MissingContract(name.into()));
            }
            json!({"$ref": format!("#/components/schemas/{}", schema_key(contract, name))})
        }
    };
    if field.proto3_optional {
        schema["nullable"] = json!(true);
        schema["x-protobuf-presence"] = json!("explicit");
    }
    if let Some(oneof) = field.oneof {
        schema["x-protobuf-oneof"] = json!(oneof);
    }
    if let Some(map) = field.map {
        let map_value = FieldSpec {
            cardinality: Cardinality::Singular,
            field_type: map.value,
            oneof: None,
            proto3_optional: false,
            map: None,
            ..*field
        };
        schema = json!({
            "type": "object",
            "additionalProperties": field_schema(contract, &map_value)?,
            "x-protobuf-map-entry": map.entry_name,
        });
    } else if field.cardinality == Cardinality::Repeated {
        schema = json!({"type": "array", "items": schema});
    }
    Ok(schema)
}

fn message_schema(
    contract: &ContractSpec,
    docs: &ContractDocs,
    name: &str,
) -> Result<Value, Error> {
    let descriptor = contract
        .message(name)
        .ok_or_else(|| Error::MissingContract(name.into()))?;
    let mut properties = Map::new();
    for field in descriptor.fields {
        let mut schema = field_schema(contract, field)?;
        schema["description"] = json!(docs.required_field(name, field.name)?);
        properties.insert(field.json_name.to_owned(), schema);
    }
    let mut schema = Map::new();
    schema.insert("type".into(), json!("object"));
    schema.insert(
        "description".into(),
        json!(ContractDocs::required(&docs.messages, name)?),
    );
    schema.insert("properties".into(), Value::Object(properties));
    if !descriptor.oneofs.is_empty() {
        schema.insert(
            "x-protobuf-oneofs".into(),
            json!(
                descriptor
                    .oneofs
                    .iter()
                    .map(|oneof| oneof.name)
                    .collect::<Vec<_>>()
            ),
        );
    }
    Ok(Value::Object(schema))
}

fn enum_schema(contract: &ContractSpec, docs: &ContractDocs, name: &str) -> Result<Value, Error> {
    let descriptor = contract
        .enum_(name)
        .ok_or_else(|| Error::MissingContract(name.into()))?;
    Ok(json!({
        "description": ContractDocs::required(&docs.enums, name)?,
        "oneOf": [
            {"type": "string", "enum": descriptor.values.iter().map(|value| value.name).collect::<Vec<_>>()},
            {"type": "integer", "format": "int32"}
        ],
        "x-protobuf-enum": name,
        "x-protobuf-enum-unknown": "preserve-numeric"
    }))
}

fn contract_digest(contract: &ContractSpec) -> String {
    let docs = ContractDocs::parse(contract);
    let mut canonical = String::new();
    canonical.push_str(contract.file_name);
    canonical.push('|');
    canonical.push_str(contract.syntax);
    canonical.push('|');
    canonical.push_str(contract.package);
    canonical.push('|');
    canonical.push_str(contract.options.go_package);
    for dependency in contract.dependencies {
        canonical.push('|');
        canonical.push_str(dependency);
    }
    for message in contract.messages {
        canonical.push('|');
        canonical.push_str(message.name);
        canonical.push('|');
        canonical.push_str(
            docs.messages
                .get(message.name)
                .map(String::as_str)
                .unwrap_or_default(),
        );
        for field in message.fields {
            canonical.push('|');
            canonical.push_str(field.name);
            canonical.push(':');
            canonical.push_str(field.json_name);
            canonical.push(':');
            canonical.push_str(&field.number.to_string());
            canonical.push(':');
            canonical.push_str(&format!("{:?}", field.cardinality));
            canonical.push(':');
            canonical.push_str(&format!("{:?}", field.field_type));
            canonical.push(':');
            canonical.push_str(field.oneof.unwrap_or_default());
            canonical.push(':');
            canonical.push_str(
                docs.fields
                    .get(&(message.name.to_owned(), field.name.to_owned()))
                    .map(String::as_str)
                    .unwrap_or_default(),
            );
            canonical.push(':');
            canonical.push_str(if field.proto3_optional {
                "optional"
            } else {
                "implicit"
            });
        }
        for oneof in message.oneofs {
            canonical.push('|');
            canonical.push_str(oneof.name);
            canonical.push(':');
            canonical.push_str(if oneof.synthetic {
                "synthetic"
            } else {
                "explicit"
            });
        }
    }
    for enum_ in contract.enums {
        canonical.push('|');
        canonical.push_str(enum_.name);
        canonical.push('|');
        canonical.push_str(
            docs.enums
                .get(enum_.name)
                .map(String::as_str)
                .unwrap_or_default(),
        );
        for value in enum_.values {
            canonical.push('|');
            canonical.push_str(value.name);
            canonical.push(':');
            canonical.push_str(&value.number.to_string());
        }
    }
    for service in contract.services {
        canonical.push('|');
        canonical.push_str(service.name);
        canonical.push('|');
        canonical.push_str(
            docs.services
                .get(service.name)
                .map(String::as_str)
                .unwrap_or_default(),
        );
        for method in service.methods {
            canonical.push('|');
            canonical.push_str(method.name);
            canonical.push(':');
            canonical.push_str(method.input);
            canonical.push(':');
            canonical.push_str(method.output);
            canonical.push(':');
            canonical.push_str(method.docs);
        }
    }
    for route in contract.routes {
        canonical.push('|');
        canonical.push_str(route.method);
        canonical.push(':');
        canonical.push_str(route.path);
        canonical.push(':');
        canonical.push_str(route.operation_id);
        canonical.push(':');
        canonical.push_str(route.rpc);
        canonical.push(':');
        canonical.push_str(route.request);
        canonical.push(':');
        canonical.push_str(route.response);
        canonical.push(':');
        canonical.push_str(route.docs);
    }
    format_digest(canonical.as_bytes())
}

fn inference_validation_options(contract: &ContractSpec) -> Option<Value> {
    if contract.package != "inference.customer.v1" {
        return None;
    }
    Some(json!({
        "source": "acyclic_sdk_contract_wire::inference::INFERENCE_OPTIONS",
        "items": acyclic_sdk_contract_wire::inference::INFERENCE_OPTIONS
            .iter()
            .map(|option| {
                let value = match option.value {
                    acyclic_sdk_contract_wire::inference::ValidationValue::Bool(value) => {
                        json!(value)
                    }
                    acyclic_sdk_contract_wire::inference::ValidationValue::U32(value) => {
                        json!(value)
                    }
                    acyclic_sdk_contract_wire::inference::ValidationValue::U64(value) => {
                        json!(value.to_string())
                    }
                    acyclic_sdk_contract_wire::inference::ValidationValue::Text(value) => {
                        json!(value)
                    }
                };
                json!({"subject": option.subject, "name": option.name, "value": value})
            })
            .collect::<Vec<_>>(),
    }))
}

fn service_for_route<'a>(
    contract: &'a ContractSpec,
    route: &acyclic_sdk_contract_wire::RouteSpec,
) -> Result<
    (
        &'a acyclic_sdk_contract_wire::ServiceSpec,
        &'a acyclic_sdk_contract_wire::MethodSpec,
    ),
    Error,
> {
    let identity = route
        .rpc
        .strip_prefix(contract.package)
        .and_then(|rest| rest.strip_prefix('.'))
        .ok_or_else(|| {
            Error::MissingContract(format!("RPC {} is outside {}", route.rpc, contract.package))
        })?;
    let (service_name, method_name) = identity
        .split_once('/')
        .ok_or_else(|| Error::MissingContract(format!("malformed RPC identity {}", route.rpc)))?;
    let service = contract
        .services
        .iter()
        .find(|service| service.name == service_name)
        .ok_or_else(|| Error::MissingContract(format!("service for {}", route.rpc)))?;
    let method = service
        .methods
        .iter()
        .find(|method| method.name == method_name)
        .ok_or_else(|| Error::MissingContract(format!("method for {}", route.rpc)))?;
    Ok((service, method))
}

fn format_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn path_parameters(path: &str) -> Vec<Value> {
    let mut parameters = Vec::new();
    let mut remaining = path;
    while let Some(start) = remaining.find('{') {
        let after_start = &remaining[start + 1..];
        let Some(end) = after_start.find('}') else {
            break;
        };
        let name = &after_start[..end];
        if !name.is_empty() {
            parameters.push(json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }));
        }
        remaining = &after_start[end + 1..];
    }
    parameters
}

fn object_limit(contract: &ContractSpec, name: &str) -> Option<u64> {
    contract
        .enum_("ObjectsLimit")?
        .values
        .iter()
        .find(|value| value.name == name)
        .and_then(|value| u64::try_from(value.number).ok())
}

fn frame_fields(contract: &ContractSpec, message: &str, fields: &[&str]) -> bool {
    contract.message(message).is_some_and(|message| {
        fields.iter().all(|name| {
            message
                .fields
                .iter()
                .any(|field| field.json_name == *name && field.oneof == Some("frame"))
        })
    })
}

fn objects_streaming_metadata(contract: &ContractSpec, route: &RouteSpec) -> Option<Value> {
    if contract.package != "acyclic.objects.v2" {
        return None;
    }
    let body_frame_bytes = object_limit(contract, "OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES")?;
    let json_record_bytes = OBJECTS_HTTP_JSON_FRAME_BYTES;
    if route.rpc.starts_with("acyclic.objects.v2.ObjectsService/") {
        return match (route.request, route.response) {
            ("PutObjectRequest", "ObjectInfo")
                if frame_fields(contract, route.request, &["header", "body", "complete"]) =>
            {
                Some(json!({
                "request_record_type": route.request,
                "response_record_type": route.response,
                "record_encoding": "canonical protobuf JSON record followed by LF",
                "accepts_crlf": true,
                "frame_limits": {"json_record_bytes": json_record_bytes, "body_bytes": body_frame_bytes},
                "request_sequence": ["header", "body*", "complete"],
                "completion": {"field": "complete", "value": true, "must_be_last": true},
                "error": {"pre_stream": {"media_type": "application/json", "record_type": "ErrorDetail"}},
                "trailer": {"supported": false},
                "idempotency": {"field": "header.mutation.idempotencyKey", "body_digest": "complete decoded body bytes and logical header fields; retry key excluded"},
                "cancellation": {"incomplete_stream": "no publication", "complete_frame_required": true}
                }))
            }
            ("GetObjectRequest", "GetObjectResponse")
                if frame_fields(contract, route.response, &["header", "body", "error"]) =>
            {
                Some(json!({
                "request_record_type": route.request,
                "response_record_type": route.response,
                "record_encoding": "canonical protobuf JSON record followed by LF",
                "accepts_crlf": true,
                "frame_limits": {"json_record_bytes": json_record_bytes, "body_bytes": body_frame_bytes},
                "response_sequence": ["header", "body*", "error?"],
                "error": {
                    "pre_stream": {"media_type": "application/json", "record_type": "ErrorDetail"},
                    "in_stream": {"field": format!("{}.error", route.response), "terminal": true, "no_subsequent_records": true}
                },
                "trailer": {"record_type": route.response, "field": "error", "terminal": true},
                "idempotency": {"supported": false},
                "cancellation": {"caller_abort": "read stream terminates without accepting another record"}
                }))
            }
            _ => None,
        };
    }
    if route
        .rpc
        .starts_with("acyclic.objects.v2.MultipartService/")
        && route.request == "UploadPartRequest"
        && route.response == "UploadedPart"
        && frame_fields(contract, route.request, &["header", "body", "complete"])
    {
        return Some(json!({
            "request_record_type": route.request,
            "response_record_type": route.response,
            "record_encoding": "canonical protobuf JSON record followed by LF",
            "accepts_crlf": true,
            "frame_limits": {"json_record_bytes": json_record_bytes, "body_bytes": body_frame_bytes},
            "request_sequence": ["header", "body*", "complete"],
            "completion": {"field": "complete", "value": true, "must_be_last": true},
            "error": {"pre_stream": {"media_type": "application/json", "record_type": "ErrorDetail"}},
            "trailer": {"supported": false},
            "idempotency": {"field": "header.mutation.idempotencyKey", "body_digest": "complete decoded body bytes and logical header fields; retry key excluded"},
            "cancellation": {"incomplete_stream": "no publication", "complete_frame_required": true}
        }));
    }
    None
}

/// Generate an OpenAPI projection from a Rust-owned contract model.
pub fn document() -> Result<Value, Error> {
    document_from_contract(&acyclic_sdk_contract_wire::ACTORS)
}

/// The HTTP projection mode for a Rust-owned contract.
///
/// Protobuf server streams are represented as repeated HTTP polling requests
/// only when the caller opts into `Polling`. This keeps the generated
/// OpenAPI document honest: the projection never claims that a gRPC stream is
/// SSE, NDJSON, or another event framing protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProjection {
    /// Emit only unary RPC routes.
    Unary,
    /// Emit unary routes and server-streaming routes as one-message polling
    /// operations, retaining the modeled gRPC streaming direction in an
    /// extension.
    Polling,
}

/// Generate an OpenAPI projection from an explicitly supplied Rust model.
/// This is public so drift tests can prove source-model changes affect output.
pub fn document_from_contract(contract: &ContractSpec) -> Result<Value, Error> {
    document_from_contract_with_projection(contract, HttpProjection::Polling)
}

/// Generate a projection with an explicit HTTP transport policy.
pub fn document_from_contract_with_projection(
    contract: &ContractSpec,
    projection: HttpProjection,
) -> Result<Value, Error> {
    let docs = ContractDocs::parse(contract);
    if contract.services.is_empty() {
        return Err(Error::MissingContract("service".into()));
    }
    let title_name = if contract.services.len() == 1 {
        contract.services[0]
            .name
            .strip_suffix("Service")
            .unwrap_or(contract.services[0].name)
            .to_owned()
    } else {
        package_title(contract.package)
    };
    let mut schemas = Map::new();
    for message in contract.messages {
        schemas.insert(
            schema_key(contract, message.name),
            message_schema(contract, &docs, message.name)?,
        );
    }
    for enum_ in contract.enums {
        schemas.insert(
            schema_key(contract, enum_.name),
            enum_schema(contract, &docs, enum_.name)?,
        );
    }
    let mut paths = Map::new();
    let mut seen_routes = BTreeSet::new();
    for route in contract.routes {
        if route.method != "POST" {
            return Err(Error::UnsupportedType(format!(
                "HTTP method {} for {}",
                route.method, route.operation_id
            )));
        }
        if !seen_routes.insert(route.path) {
            return Err(Error::MissingContract(format!(
                "duplicate route {}",
                route.path
            )));
        }
        let (service, method) = service_for_route(contract, route)
            .map_err(|error| Error::MissingContract(format!("{}: {error}", route.operation_id)))?;
        let expected_rpc = format!("{}.{}/{}", contract.package, service.name, method.name);
        if route.rpc != expected_rpc
            || route.request != method.input
            || route.response != method.output
        {
            return Err(Error::MissingContract(format!(
                "route {} does not match modeled RPC {}",
                route.operation_id, method.name
            )));
        }
        if route.docs != method.docs {
            return Err(Error::MissingContract(format!(
                "route {} documentation does not match modeled RPC {}",
                route.operation_id, method.name
            )));
        }
        if (method.client_streaming || method.server_streaming)
            && projection == HttpProjection::Unary
        {
            return Err(Error::UnsupportedType(format!(
                "streaming RPC {} requires the explicit streaming-aware HTTP projection",
                method.name
            )));
        }
        // Objects' HTTP projection uses NDJSON for the streaming direction,
        // while retaining ordinary protobuf JSON on the unary side of an
        // upload or download. Other families keep the generic JSON/polling
        // projection below until they declare a different HTTP framing.
        let objects_ndjson = contract.package == "acyclic.objects.v2"
            && (method.client_streaming || method.server_streaming);
        let request_media_type = if objects_ndjson && method.client_streaming {
            "application/x-ndjson"
        } else {
            "application/json"
        };
        let response_media_type = if objects_ndjson && method.server_streaming {
            "application/x-ndjson"
        } else {
            "application/json"
        };
        let mut responses = json!({
            "200": {"description": "Successful response", "content": {response_media_type: {"schema": {"$ref": format!("#/components/schemas/{}", schema_key(contract, method.output))}}}}
        });
        if contract.message("Error").is_some() {
            responses["default"] = json!({"description": "Canonical service error", "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", schema_key(contract, "Error"))}}}});
        } else if contract.package == "acyclic.objects.v2"
            && contract.message("ErrorDetail").is_some()
        {
            responses["default"] = json!({"description": "Canonical service error", "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", schema_key(contract, "ErrorDetail"))}}}});
        } else {
            responses["default"] = json!({"description": "Canonical service error"});
        }
        let mut operation = json!({
            "operationId": route.operation_id,
            "description": route.docs,
            "x-protobuf-rpc": route.rpc,
            "x-protobuf-streaming": {"client": method.client_streaming, "server": method.server_streaming},
            "x-acyclic-route-source": "acyclic_sdk_contract_wire::ContractSpec.routes",
            "security": [{"bearerAuth": []}],
            "requestBody": {"required": true, "content": {request_media_type: {"schema": {"$ref": format!("#/components/schemas/{}", schema_key(contract, method.input))}}}},
            "responses": responses
        });
        let parameters = path_parameters(route.path);
        if !parameters.is_empty() {
            operation["parameters"] = json!(parameters);
        }
        if method.server_streaming && contract.package == "acyclic.stream.v2" {
            operation["x-acyclic-http-polling"] = json!({
                "mode": "one-message-per-request",
                "grpc_streaming": "server",
                "framing": "application/json",
                "sse": false,
                "ndjson": false,
                "continuation": "caller repeats the modeled request with its cursor"
            });
        } else if method.client_streaming || method.server_streaming {
            operation["x-acyclic-http-streaming"] = json!({
                "grpc_client_streaming": method.client_streaming,
                "grpc_server_streaming": method.server_streaming,
                "wire_projection": "Rust-model request/response schemas",
                "sse": false,
                "ndjson": objects_ndjson
            });
        }
        if let Some(metadata) = objects_streaming_metadata(contract, route) {
            operation["x-acyclic-objects-streaming"] = metadata;
        }
        paths.insert(
            route.path.to_owned(),
            json!({route.method.to_ascii_lowercase(): operation}),
        );
    }
    let mut output = json!({
        "openapi": "3.0.3",
        "info": {"title": format!("Acyclic {title_name} API"), "version": "v1", "description": format!("{} OpenAPI is a generated HTTP/JSON projection.", contract.services.iter().map(|service| ContractDocs::required(&docs.services, service.name)).collect::<Result<Vec<_>, _>>()?.join(" "))},
        "x-acyclic-source": {"contract_digest": contract_digest(contract), "contract": contract.package, "routes": "acyclic_sdk_contract_wire::ContractSpec.routes", "wire_authority": "acyclic_sdk_contract_wire::ContractSpec", "documentation": if contract.package == "inference.customer.v1" { "ContractSpec structural labels; wire comment table pending" } else { "ContractSpec::render_proto Rust comments" }, "http_projection": match projection { HttpProjection::Unary => "unary", HttpProjection::Polling => "polling" }},
        "security": [{"bearerAuth": []}],
        "paths": paths,
        "components": {"securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}, "schemas": schemas}
    });
    if let Some(options) = inference_validation_options(contract) {
        output["x-acyclic-validation-options"] = options;
    }
    Ok(output)
}

/// Generate the explicit Stream HTTP polling projection.
pub fn document_stream_polling() -> Result<Value, Error> {
    document_from_contract_with_projection(
        &acyclic_sdk_contract_wire::stream::STREAM,
        HttpProjection::Polling,
    )
}

/// Serialize the Stream HTTP polling projection with stable key ordering.
pub fn json_document_stream_polling() -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(&document_stream_polling()?)? + "\n")
}

/// Generate the Inference HTTP projection, including Rust-owned validation options.
pub fn document_inference() -> Result<Value, Error> {
    document_from_contract_with_projection(
        &acyclic_sdk_contract_wire::inference::INFERENCE,
        HttpProjection::Polling,
    )
}

/// Serialize the Inference HTTP projection with stable key ordering.
pub fn json_document_inference() -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(&document_inference()?)? + "\n")
}

/// Serialize the generated projection with stable key ordering.
pub fn json_document() -> Result<String, Error> {
    json_document_from_contract(&acyclic_sdk_contract_wire::ACTORS)
}

/// Serialize any exported Rust contract's OpenAPI projection with stable key ordering.
pub fn json_document_from_contract(contract: &ContractSpec) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(&document_from_contract(contract)?)? + "\n")
}

const POWERSHELL_WORKERS_ADAPTATION_TEMPLATE: &str = r#"[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$request = Join-Path $GeneratedRoot 'src/AcyclicWorkersHttp/Model/__REQUEST_MODEL__.ps1'
$response = Join-Path $GeneratedRoot 'src/AcyclicWorkersHttp/Model/__RESPONSE_MODEL__.ps1'
$apiClient = Join-Path $GeneratedRoot 'src/AcyclicWorkersHttp/Private/ApiClient.ps1'
# Rust-owned uint64 JSON remains a quoted decimal string: '__RESPONSE_UINT64_JSON__'.
foreach ($path in @($request, $response, $apiClient)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Rust-owned Workers PowerShell target is missing generated anchor: $path"
    }
}

$requestText = Get-Content -LiteralPath $request -Raw
$requestPattern = '(?s)        \$PSBoundParameters \| Out-DebugParameter \| Write-Debug\r?\n\s*\$PSO = \[PSCustomObject\]@\{'
$requestReplacement = @"
        `$PSBoundParameters | Out-DebugParameter | Write-Debug

        # Rust-owned protobuf bytes are represented as RFC 4648 base64 in JSON.
        if (`$null -ne `${__REQUEST_BODY_VARIABLE__} -and `${__REQUEST_BODY_VARIABLE__} -is [byte[]]) {
            `${__REQUEST_BODY_VARIABLE__} = [Convert]::ToBase64String(`${__REQUEST_BODY_VARIABLE__})
        } elseif (`$null -ne `${__REQUEST_BODY_VARIABLE__} -and `${__REQUEST_BODY_VARIABLE__} -isnot [string]) {
            throw "'__REQUEST_BODY_JSON__' must be a byte[] or an RFC 4648 base64 string."
        }

        `$PSO = [PSCustomObject]@{
"@
if (-not [regex]::IsMatch($requestText, $requestPattern)) {
    throw 'Rust-owned Workers PowerShell request anchor changed; refusing an unreviewed adaptation.'
}
$requestText = [regex]::Replace($requestText, $requestPattern, [System.Text.RegularExpressions.MatchEvaluator]{ param($match) $requestReplacement }, 1)
Set-Content -LiteralPath $request -Value $requestText -NoNewline

$responseText = Get-Content -LiteralPath $response -Raw
$bodyAnchor = "            `$__RESPONSE_BODY_VARIABLE__ = `$JsonParameters.PSobject.Properties['__RESPONSE_BODY_JSON__'].value"
$bodyReplacement = @"
            `$__RESPONSE_BODY_VARIABLE__ = `$JsonParameters.PSobject.Properties['__RESPONSE_BODY_JSON__'].value
            if (`$null -ne `$__RESPONSE_BODY_VARIABLE__ -and `$__RESPONSE_BODY_VARIABLE__ -is [string]) { `$__RESPONSE_BODY_VARIABLE__ = [Convert]::FromBase64String(`$__RESPONSE_BODY_VARIABLE__) }
"@
$shaAnchor = "            `$__RESPONSE_SHA_VARIABLE__ = `$JsonParameters.PSobject.Properties['__RESPONSE_SHA_JSON__'].value"
$shaReplacement = @"
            `$__RESPONSE_SHA_VARIABLE__ = `$JsonParameters.PSobject.Properties['__RESPONSE_SHA_JSON__'].value
            if (`$null -ne `$__RESPONSE_SHA_VARIABLE__ -and `$__RESPONSE_SHA_VARIABLE__ -is [string]) { `$__RESPONSE_SHA_VARIABLE__ = [Convert]::FromBase64String(`$__RESPONSE_SHA_VARIABLE__) }
"@
if (-not $responseText.Contains($bodyAnchor) -or -not $responseText.Contains($shaAnchor)) {
    throw 'Rust-owned Workers PowerShell response anchors changed; refusing an unreviewed adaptation.'
}
$responseText = $responseText.Replace($bodyAnchor, $bodyReplacement.TrimEnd("`r", "`n"))
$responseText = $responseText.Replace($shaAnchor, $shaReplacement.TrimEnd("`r", "`n"))
Set-Content -LiteralPath $response -Value $responseText -NoNewline

$apiText = Get-Content -LiteralPath $apiClient -Raw
$apiAnchor = '                return ConvertFrom-Json \$Response'
$apiReplacement = @"
                if (`$ReturnType -eq '__RESPONSE_MODEL__') {
                    return ConvertFrom-JsonTo__RESPONSE_MODEL__ -Json `$Response
                }
                return ConvertFrom-Json `$Response
"@
if (-not [regex]::IsMatch($apiText, $apiAnchor)) {
    throw 'Rust-owned Workers PowerShell API client anchor changed; refusing an unreviewed adaptation.'
}
$apiText = [regex]::Replace($apiText, $apiAnchor, [System.Text.RegularExpressions.MatchEvaluator]{ param($match) $apiReplacement.TrimEnd("`r", "`n") }, 1)
Set-Content -LiteralPath $apiClient -Value $apiText -NoNewline

Write-Output "Applied Rust-owned Workers protobuf-bytes adaptation to $GeneratedRoot"
"#;

fn powershell_pascal_name(json_name: &str) -> String {
    let mut result = String::new();
    let mut uppercase = true;
    for character in json_name.chars() {
        if character == '_' || character == '-' {
            uppercase = true;
        } else if uppercase {
            result.extend(character.to_uppercase());
            uppercase = false;
        } else {
            result.push(character);
        }
    }
    result
}

fn required_workers_field<'a>(
    contract: &'a ContractSpec,
    message: &str,
    json_name: &str,
    field_type: FieldType,
) -> Result<&'a FieldSpec, Error> {
    let field = contract
        .message(message)
        .and_then(|message| {
            message
                .fields
                .iter()
                .find(|field| field.json_name == json_name)
        })
        .ok_or_else(|| Error::MissingContract(format!("Workers field {message}.{json_name}")))?;
    if field.field_type != field_type {
        return Err(Error::UnsupportedType(format!(
            "Workers field {message}.{json_name} has unexpected type {:?}",
            field.field_type
        )));
    }
    Ok(field)
}

/// Render the narrow PowerShell bytes adapter from the Rust-owned Workers model.
///
/// OpenAPI Generator owns the package scaffolding; this renderer owns the
/// target-specific protobuf JSON boundary and derives every field anchor from
/// `ContractSpec`. It deliberately emits no routes or shared validation logic.
pub fn powershell_workers_adaptation_script(contract: &ContractSpec) -> Result<String, Error> {
    if contract.package != "acyclic.workers.v1" {
        return Err(Error::MissingContract(format!(
            "PowerShell Workers adapter requires acyclic.workers.v1, got {}",
            contract.package
        )));
    }
    let request_body = required_workers_field(
        contract,
        "InvokeDeploymentRequest",
        "body",
        FieldType::Bytes,
    )?;
    let response_body =
        required_workers_field(contract, "InvokeResponse", "body", FieldType::Bytes)?;
    let response_sha = required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedSha256",
        FieldType::Bytes,
    )?;
    let response_revision = required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedRevision",
        FieldType::Uint64,
    )?;
    let _ = response_revision;

    let request_model = "AcyclicWorkersV1InvokeDeploymentRequest";
    let response_model = "AcyclicWorkersV1InvokeResponse";
    let request_body_variable = powershell_pascal_name(request_body.json_name);
    let response_body_variable = powershell_pascal_name(response_body.json_name);
    let response_sha_variable = powershell_pascal_name(response_sha.json_name);
    let mut script = POWERSHELL_WORKERS_ADAPTATION_TEMPLATE.to_owned();
    for (placeholder, value) in [
        ("__REQUEST_MODEL__", request_model),
        ("__RESPONSE_MODEL__", response_model),
        ("__REQUEST_BODY_VARIABLE__", request_body_variable.as_str()),
        ("__REQUEST_BODY_JSON__", request_body.json_name),
        (
            "__RESPONSE_BODY_VARIABLE__",
            response_body_variable.as_str(),
        ),
        ("__RESPONSE_BODY_JSON__", response_body.json_name),
        ("__RESPONSE_SHA_VARIABLE__", response_sha_variable.as_str()),
        ("__RESPONSE_SHA_JSON__", response_sha.json_name),
        ("__RESPONSE_UINT64_JSON__", response_revision.json_name),
    ] {
        script = script.replace(placeholder, value);
    }
    Ok(script)
}

const C_WORKERS_ADAPTATION_SCRIPT: &str = r#"[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string] $GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$path = Join-Path $GeneratedRoot 'model/acyclic_workers_v1_job_observation.c'
if (-not (Test-Path -LiteralPath $path)) {
  throw "Rust-owned Workers C target is missing generated anchor: $path"
}
$text = Get-Content -LiteralPath $path -Raw
$old = @'
    acyclic_workers_v1_job_observation_t *result = acyclic_workers_v1_job_observation_create_internal (
        attempt_copy,
        cancellation_requested_copy,
        failure_code,
        job_id,
        resolved_sha256,
        result,
        state
        );
    if (!result) {
        free(attempt_copy);
        free(cancellation_requested_copy);
    }
    return result;
'@
$new = @'
    acyclic_workers_v1_job_observation_t *result_local_var = acyclic_workers_v1_job_observation_create_internal (
        attempt_copy,
        cancellation_requested_copy,
        failure_code,
        job_id,
        resolved_sha256,
        result,
        state
        );
    if (!result_local_var) {
        free(attempt_copy);
        free(cancellation_requested_copy);
    }
    return result_local_var;
'@
if (-not $text.Contains($old)) {
  throw "Refusing C adaptation: expected OAG result-name collision anchor was not found."
}
$updated = $text.Replace($old, $new)
Set-Content -LiteralPath $path -Value $updated -Encoding utf8
Write-Output "Applied Rust-owned Workers C compile adaptation to $GeneratedRoot"
"#;

/// Render the narrow C compile adaptation from the Rust-owned Workers model.
///
/// OpenAPI Generator owns the package scaffolding; this renderer owns only the
/// target-specific source repair required by the selected compiler. It emits
/// no routes, transport behavior, or shared model logic.
pub fn c_workers_adaptation_script(contract: &ContractSpec) -> Result<String, Error> {
    if contract.package != "acyclic.workers.v1" {
        return Err(Error::MissingContract(format!(
            "C Workers adapter requires acyclic.workers.v1, got {}",
            contract.package
        )));
    }
    required_workers_field(contract, "InvokeResponse", "body", FieldType::Bytes)?;
    required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedSha256",
        FieldType::Bytes,
    )?;
    required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedRevision",
        FieldType::Uint64,
    )?;
    Ok(C_WORKERS_ADAPTATION_SCRIPT.to_owned())
}

const DART_WORKERS_ADAPTATION_SCRIPT: &str = r#"[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$modelDir = Join-Path $GeneratedRoot 'lib/model'
$models = @(
    'acyclic_workers_v1_error_code.dart',
    'acyclic_workers_v1_job_state.dart'
)
foreach ($name in $models) {
    $path = Join-Path $modelDir $name
    if (-not (Test-Path -LiteralPath $path)) { throw "Generated Dart model is missing: $path" }
    $text = Get-Content -LiteralPath $path -Raw
    $class = if ($name -like '*error_code*') { 'AcyclicWorkersV1ErrorCode' } else { 'AcyclicWorkersV1JobState' }
    $constructorAnchor = "$class({`r`n  });"
    $constructorReplacement = "$class();"
    if (-not $text.Contains($constructorAnchor)) {
        $constructorAnchor = $constructorAnchor.Replace("`r`n", "`n")
    }
    if (-not $text.Contains($constructorAnchor)) { throw "Dart empty-constructor anchor changed: $path" }
    $text = $text.Replace($constructorAnchor, $constructorReplacement)
    $anchor = "  bool operator ==(Object other) => identical(this, other) || other is $class &&`r`n`r`n  @override`r`n  int get hashCode =>`r`n    // ignore: unnecessary_parenthesis`r`n"
    $replacement = "  bool operator ==(Object other) => identical(this, other) || other is $class;`r`n`r`n  @override`r`n  int get hashCode => runtimeType.hashCode;`r`n"
    if (-not $text.Contains($anchor)) {
        $anchor = $anchor.Replace("`r`n", "`n")
        $replacement = $replacement.Replace("`r`n", "`n")
    }
    if (-not $text.Contains($anchor)) { throw "Dart empty-enum anchor changed: $path" }
    Set-Content -LiteralPath $path -Value $text.Replace($anchor, $replacement) -NoNewline
}

$pubspec = Join-Path $GeneratedRoot 'pubspec.yaml'
if (-not (Test-Path -LiteralPath $pubspec)) { throw "Generated Dart pubspec is missing: $pubspec" }
$pubspecText = Get-Content -LiteralPath $pubspec -Raw
$licenseAnchor = "homepage: 'homepage'"
if (-not $pubspecText.Contains($licenseAnchor)) { throw 'Dart pubspec license anchor changed.' }
if (-not $pubspecText.Contains("license: 'Apache-2.0'")) {
    $pubspecText = $pubspecText.Replace($licenseAnchor, "$licenseAnchor`r`nlicense: 'Apache-2.0'")
}
$testAnchor = "  test: '>=1.21.6 <1.22.0'"
$testReplacement = "  test: '>=1.31.0 <2.0.0'"
if (-not $pubspecText.Contains($testAnchor)) { throw 'Dart test dependency anchor changed; refusing an unreviewed adaptation.' }
$pubspecText = $pubspecText.Replace($testAnchor, $testReplacement)
Set-Content -LiteralPath $pubspec -Value $pubspecText -NoNewline
$smoke = Join-Path $GeneratedRoot 'tool/rust_owned_package_smoke.dart'
if (-not (Test-Path -LiteralPath (Split-Path -Parent $smoke))) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $smoke) | Out-Null
}
@'
import 'package:openapi/api.dart';

void main() {
  final request = AcyclicWorkersV1InvokeDeploymentRequest(body: 'AQID');
  if (request.toJson()['body'] != 'AQID') {
    throw StateError('generated request bytes did not retain base64 JSON');
  }
  final response = AcyclicWorkersV1InvokeResponse(
    body: 'b2s=',
    resolvedRevision: '18446744073709551615',
    resolvedSha256: 'AQID',
    status: 200,
  );
  final decoded = AcyclicWorkersV1InvokeResponse.fromJson(response.toJson());
  if (decoded?.body != 'b2s=' ||
      decoded?.resolvedRevision != '18446744073709551615' ||
      decoded?.resolvedSha256 != 'AQID' ||
      decoded?.status != 200) {
    throw StateError('generated response JSON round trip changed bytes or uint64');
  }
  if (AcyclicWorkersV1ErrorCode().toJson().isNotEmpty ||
      AcyclicWorkersV1JobState().toJson().isNotEmpty) {
    throw StateError('generated empty enum models were not adapted');
  }
}
'@ | Set-Content -LiteralPath $smoke -NoNewline
Write-Output "Applied Rust-owned Workers Dart runtime/test adaptation to $GeneratedRoot"
"#;

/// Render the narrow Dart runtime and test-tool adaptation from the Workers model.
///
/// The generated package remains the source of routes and models. This adapter
/// only repairs two empty enum constructors, records the package license scope,
/// pins the current test runner line, and emits a package-owned AOT smoke test.
pub fn dart_workers_adaptation_script(contract: &ContractSpec) -> Result<String, Error> {
    if contract.package != "acyclic.workers.v1" {
        return Err(Error::MissingContract(format!(
            "Dart Workers adapter requires acyclic.workers.v1, got {}",
            contract.package
        )));
    }
    required_workers_field(contract, "InvokeResponse", "body", FieldType::Bytes)?;
    required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedRevision",
        FieldType::Uint64,
    )?;
    Ok(DART_WORKERS_ADAPTATION_SCRIPT.to_owned())
}

const JULIA_WORKERS_ADAPTATION_SOURCE: &str = r#"module AcyclicWorkers

using Base64
using Downloads

export InvokeDeploymentRequest, InvokeResponse, ApiError, CancellationToken,
       cancel!, invoke_deployment

struct InvokeDeploymentRequest
    body::Vector{UInt8}
end

struct InvokeResponse
    body::Vector{UInt8}
    resolved_sha256::Vector{UInt8}
    resolved_revision::UInt64
    status::Int
end

struct ApiError <: Exception
    status::Int
    body::String
end

struct CancellationToken
    cancelled::Base.RefValue{Bool}
end

CancellationToken() = CancellationToken(Ref(false))
cancel!(token::CancellationToken) = (token.cancelled[] = true)

function _json_string(payload::AbstractString, name::AbstractString)
    match_result = match(Regex("\\\"" * name * "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\""), payload)
    match_result === nothing && error("missing JSON field: " * name)
    return match_result.captures[1]
end

function _invoke_url(base_url::AbstractString, alias::AbstractString)
    return rstrip(base_url, '/') * "__INVOKE_PATH__" |> url -> replace(url, "{alias}" => alias)
end

function invoke_deployment(base_url::AbstractString, alias::AbstractString,
                           request::InvokeDeploymentRequest;
                           token::CancellationToken=CancellationToken())
    token.cancelled[] && throw(InterruptException())
    payload = "{\"body\":\"" * base64encode(request.body) * "\"}"
    response_output = IOBuffer()
    response = Downloads.request(
        _invoke_url(base_url, alias);
        method="POST",
        headers=["content-type" => "application/json"],
        input=IOBuffer(payload),
        output=response_output,
        throw=false,
    )
    token.cancelled[] && throw(InterruptException())
    response_body = String(take!(response_output))
    if response.status >= 400
        throw(ApiError(response.status, response_body))
    end
    return InvokeResponse(
        base64decode(_json_string(response_body, "body")),
        base64decode(_json_string(response_body, "resolvedSha256")),
        parse(UInt64, _json_string(response_body, "resolvedRevision")),
        response.status,
    )
end

Base.showerror(io::IO, error::ApiError) = print(io, "HTTP ", error.status, ": ", error.body)

end
"#;

/// Render the Rust-owned Julia HTTP package source for the Workers model.
///
/// This emits only the target transport/model boundary. The route and field
/// anchors are checked against `ContractSpec`; no handwritten contract table
/// or independent shared schema is accepted.
pub fn julia_workers_adaptation_source(contract: &ContractSpec) -> Result<String, Error> {
    if contract.package != "acyclic.workers.v1" {
        return Err(Error::MissingContract(format!(
            "Julia Workers adapter requires acyclic.workers.v1, got {}",
            contract.package
        )));
    }
    required_workers_field(
        contract,
        "InvokeDeploymentRequest",
        "body",
        FieldType::Bytes,
    )?;
    required_workers_field(contract, "InvokeResponse", "body", FieldType::Bytes)?;
    required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedSha256",
        FieldType::Bytes,
    )?;
    required_workers_field(
        contract,
        "InvokeResponse",
        "resolvedRevision",
        FieldType::Uint64,
    )?;
    let route = contract
        .routes
        .iter()
        .find(|route| route.operation_id == "invokeDeployment")
        .ok_or_else(|| Error::MissingContract("Workers invokeDeployment route".into()))?;
    let mut source = JULIA_WORKERS_ADAPTATION_SOURCE.to_owned();
    source = source.replace("__INVOKE_PATH__", route.path);
    Ok(source)
}

fn julia_string(value: &str) -> Result<String, Error> {
    Ok(serde_json::to_string(value)?)
}

fn julia_string_array(values: &[&str]) -> Result<String, Error> {
    values
        .iter()
        .map(|value| julia_string(value))
        .collect::<Result<Vec<_>, _>>()
        .map(|values| format!("[{}]", values.join(", ")))
}

/// Render the Rust registry-backed Julia route/policy projection for one
/// explicit HTTP family. This is an inventory module, not a second transport
/// implementation: route, RPC, capability, error, and validation metadata all
/// come from `sdk-contract-wire`'s unified family registry.
pub fn julia_family_projection_source(family: &str) -> Result<String, Error> {
    let view = explicit_http_family_views()
        .find(|view| view.name == family)
        .ok_or_else(|| Error::MissingContract(format!("explicit HTTP family {family}")))?;
    let contract = view
        .model
        .as_contract_spec()
        .ok_or_else(|| Error::MissingContract(format!("structured HTTP family {family}")))?;
    let mut routes = String::new();
    for route in view.routes() {
        let method_name = route.rpc.rsplit('/').next().unwrap_or_default();
        let method = contract
            .services
            .iter()
            .flat_map(|service| service.methods)
            .find(|method| method.name == method_name)
            .ok_or_else(|| Error::MissingContract(format!("route method {method_name}")))?;
        routes.push_str(&format!(
            "    RouteSpec({}, {}, {}, {}, {}, {}, {}, {}, {}),\n",
            julia_string(route.method)?,
            julia_string(route.path)?,
            julia_string(route.operation_id)?,
            julia_string(route.rpc)?,
            julia_string(route.request)?,
            julia_string(route.response)?,
            julia_string(route.docs)?,
            method.client_streaming,
            method.server_streaming,
        ));
    }
    let mut policies = String::new();
    for policy in view.operation_policies {
        policies.push_str(&format!(
            "    OperationPolicy({}, {}, {}, {}),\n",
            julia_string(policy.rpc)?,
            julia_string_array(policy.capabilities)?,
            julia_string_array(policy.errors)?,
            julia_string_array(policy.validations)?,
        ));
    }
    Ok(format!(
        concat!(
            "module AcyclicHttpProjection\n\n",
            "using Downloads\n\n",
            "export RouteSpec, OperationPolicy, ApiError, CancellationToken, cancel!, FAMILY, PACKAGE, HTTP_PROJECTION, GRPC_PROJECTION, DEFAULT_TRANSPORT, SUPPORTED_TRANSPORTS, ROUTES, POLICIES, select_transport, request_json, request_with_recovery, request_stream\n\n",
            "struct RouteSpec\n",
            "    method::String\n",
            "    path::String\n",
            "    operation_id::String\n",
            "    rpc::String\n",
            "    request::String\n",
            "    response::String\n",
            "    docs::String\n",
            "    client_streaming::Bool\n",
            "    server_streaming::Bool\n",
            "end\n\n",
            "struct OperationPolicy\n",
            "    rpc::String\n",
            "    capabilities::Vector{{String}}\n",
            "    errors::Vector{{String}}\n",
            "    validations::Vector{{String}}\n",
            "end\n\n",
            "struct ApiError <: Exception\n",
            "    status::Int\n",
            "    body::String\n",
            "end\n\n",
            "struct CancellationToken\n",
            "    cancelled::Base.RefValue{{Bool}}\n",
            "end\n\n",
            "CancellationToken() = CancellationToken(Ref(false))\n",
            "cancel!(token::CancellationToken) = (token.cancelled[] = true)\n\n",
            "const FAMILY = {}\n",
            "const PACKAGE = {}\n",
            "const HTTP_PROJECTION = true\n",
            "const GRPC_PROJECTION = false\n",
            "const DEFAULT_TRANSPORT = :http_json\n",
            "const SUPPORTED_TRANSPORTS = [:http_json]\n",
            "const ROUTES = RouteSpec[\n{}]\n",
            "const POLICIES = [\n{}]\n\n",
            "function select_transport(override=nothing)\n",
            "    override === nothing && return DEFAULT_TRANSPORT\n",
            "    override in SUPPORTED_TRANSPORTS || throw(ArgumentError(\"unsupported Julia transport override\"))\n",
            "    return override\n",
            "end\n\n",
            "function _policy(route::RouteSpec)\n",
            "    policy = findfirst(item -> item.rpc == route.rpc, POLICIES)\n",
            "    policy === nothing && throw(ArgumentError(\"missing Rust operation policy\"))\n",
            "    return POLICIES[policy]\n",
            "end\n\n",
            "function _url(base_url::AbstractString, route::RouteSpec)\n",
            "    return rstrip(base_url, '/') * route.path\n",
            "end\n\n",
            "function request_json(base_url::AbstractString, route_index::Integer; payload=\"{{}}\", bearer_token=nothing, token=CancellationToken())\n",
            "    token.cancelled[] && throw(InterruptException())\n",
            "    route = ROUTES[route_index]\n",
            "    headers = [\"content-type\" => \"application/json\"]\n",
            "    bearer_token === nothing || push!(headers, \"authorization\" => \"Bearer \" * bearer_token)\n",
            "    output = IOBuffer()\n",
            "    response = Downloads.request(_url(base_url, route); method=route.method, headers=headers, input=IOBuffer(payload), output=output, throw=false)\n",
            "    token.cancelled[] && throw(InterruptException())\n",
            "    body = String(take!(output))\n",
            "    response.status >= 400 && throw(ApiError(response.status, body))\n",
            "    return (status=response.status, body=body, route=route, policy=_policy(route))\n",
            "end\n\n",
            "function request_with_recovery(base_url::AbstractString, route_index::Integer; payload=\"{{}}\", bearer_token=nothing, token=CancellationToken(), attempts=2)\n",
            "    route = ROUTES[route_index]\n",
            "    policy = _policy(route)\n",
            "    recoverable = any(endswith(capability, \".read\") for capability in policy.capabilities) || any(occursin(\"idempotent_mutation\", capability) for capability in policy.capabilities)\n",
            "    attempts > 1 && !recoverable && throw(ArgumentError(\"refusing non-idempotent replay\"))\n",
            "    last_error = nothing\n",
            "    for _ in 1:max(attempts, 1)\n",
            "        try\n",
            "            return request_json(base_url, route_index; payload, bearer_token, token)\n",
            "        catch error\n",
            "            last_error = error\n",
            "            error isa ApiError && error.status in (408, 429, 500, 502, 503, 504) || rethrow()\n",
            "        end\n",
            "    end\n",
            "    throw(last_error)\n",
            "end\n\n",
            "function request_stream(base_url::AbstractString, route_index::Integer; payload=\"{{}}\", bearer_token=nothing, token=CancellationToken())\n",
            "    route = ROUTES[route_index]\n",
            "    (route.client_streaming || route.server_streaming) || throw(ArgumentError(\"route is not streaming\"))\n",
            "    result = request_json(base_url, route_index; payload, bearer_token, token)\n",
            "    return filter(!isempty, split(result.body, '\\n'))\n",
            "end\n\n",
            "end\n"
        ),
        julia_string(view.name)?,
        julia_string(contract.package)?,
        routes,
        policies,
    ))
}

/// Return whether an existing generated artifact matches the current model.
/// This is the stale-output guard used by CI and local generation checks.
pub fn check_json_file(path: impl AsRef<std::path::Path>) -> Result<bool, Error> {
    Ok(std::fs::read_to_string(path)? == json_document()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_sdk_contract_wire::inference::INFERENCE;
    use acyclic_sdk_contract_wire::objects::OBJECTS_V2;
    use acyclic_sdk_contract_wire::stream::STREAM;
    use acyclic_sdk_contract_wire::workers::WORKERS;
    use acyclic_sdk_contract_wire::{ACTORS, ContractSpec, FieldSpec};
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn derives_all_rust_owned_actor_routes() {
        let doc = document().expect("Actors model projects");
        let paths = doc.get("paths").and_then(Value::as_object).expect("paths");
        assert_eq!(paths.len(), ACTORS.routes.len());
        for route in ACTORS.routes {
            assert!(
                paths.contains_key(route.path),
                "route path was rewritten: {}",
                route.path
            );
            assert_eq!(paths[route.path]["post"]["operationId"], route.operation_id);
            assert_eq!(paths[route.path]["post"]["x-protobuf-rpc"], route.rpc);
        }
        assert_eq!(
            paths["/v1/actors/create"]["post"]["x-protobuf-rpc"],
            "acyclic.actors.v1.ActorsService/CreateActor"
        );
        assert!(
            doc.get("servers").is_none(),
            "deployment endpoint must be configured by consumers"
        );
        assert!(
            doc["components"]["schemas"]["acyclic_actors_v1_CreateActorRequest"]
                .get("required")
                .is_none()
        );
    }

    #[test]
    fn workers_export_has_golden_routes_descriptions_and_path_parameters() {
        let doc = document_from_contract(&WORKERS).expect("Workers model projects");
        let paths = doc["paths"].as_object().expect("Workers paths");
        assert_eq!(paths.len(), 7);
        assert_eq!(
            paths["/v1/workers/versions/{sha256hex}/invoke"]["post"]["operationId"],
            "invokeVersion"
        );
        assert_eq!(
            paths["/v1/workers/versions/{sha256hex}/invoke"]["post"]["parameters"][0]["name"],
            "sha256hex"
        );
        assert_eq!(
            paths["/v1/workers/deployments/{alias}/invoke"]["post"]["parameters"][0]["name"],
            "alias"
        );
        assert_eq!(doc["info"]["title"], "Acyclic Workers API");
        for message in WORKERS.messages {
            let schema = &doc["components"]["schemas"][&schema_key(&WORKERS, message.name)];
            assert!(
                schema["description"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty()),
                "missing Workers message description: {}",
                message.name
            );
            for field in message.fields {
                assert!(
                    schema["properties"][field.json_name]["description"]
                        .as_str()
                        .is_some_and(|text| !text.is_empty()),
                    "missing Workers field description: {}.{}",
                    message.name,
                    field.name
                );
            }
        }
    }

    #[test]
    fn powershell_adapter_is_derived_from_workers_bytes_and_uint64_fields() {
        let script = powershell_workers_adaptation_script(&WORKERS)
            .expect("Workers PowerShell adapter renders");
        assert!(script.contains("AcyclicWorkersV1InvokeDeploymentRequest.ps1"));
        assert!(script.contains("AcyclicWorkersV1InvokeResponse.ps1"));
        assert!(script.contains("ToBase64String"));
        assert!(script.contains("FromBase64String"));
        assert!(script.contains("'resolvedRevision'"));
        assert!(script.contains("refusing an unreviewed adaptation"));
    }

    #[test]
    fn powershell_adapter_rejects_a_non_workers_contract_before_emission() {
        let error = powershell_workers_adaptation_script(&ACTORS)
            .expect_err("Actors must not receive Workers byte adaptation");
        assert!(error.to_string().contains("requires acyclic.workers.v1"));
    }

    #[test]
    fn c_adapter_is_rust_owned_and_anchor_checked() {
        let script = c_workers_adaptation_script(&WORKERS).expect("Workers C adapter renders");
        assert!(script.contains("result_local_var"));
        assert!(script.contains("expected OAG result-name collision anchor"));
        assert!(script.contains("acyclic_workers_v1_job_observation.c"));
        assert!(script.contains("Applied Rust-owned Workers C compile adaptation"));
    }

    #[test]
    fn c_adapter_rejects_a_non_workers_contract_before_emission() {
        let error = c_workers_adaptation_script(&ACTORS)
            .expect_err("Actors must not receive Workers C adaptation");
        assert!(error.to_string().contains("requires acyclic.workers.v1"));
    }

    #[test]
    fn dart_adapter_is_rust_owned_and_pins_compatible_test_runner() {
        let script =
            dart_workers_adaptation_script(&WORKERS).expect("Workers Dart adapter renders");
        assert!(script.contains("acyclic_workers_v1_error_code.dart"));
        assert!(script.contains("runtimeType.hashCode"));
        assert!(script.contains("license: 'Apache-2.0'"));
        assert!(script.contains("test: '>=1.31.0 <2.0.0'"));
        assert!(script.contains("rust_owned_package_smoke.dart"));
        assert!(script.contains("Dart test dependency anchor changed"));
    }

    #[test]
    fn dart_adapter_rejects_a_non_workers_contract_before_emission() {
        let error = dart_workers_adaptation_script(&ACTORS)
            .expect_err("Actors must not receive Workers Dart adaptation");
        assert!(error.to_string().contains("requires acyclic.workers.v1"));
    }

    #[test]
    fn julia_adapter_is_rust_owned_and_anchors_transport_semantics() {
        let source =
            julia_workers_adaptation_source(&WORKERS).expect("Workers Julia adapter renders");
        assert!(source.contains("base64encode"));
        assert!(source.contains("parse(UInt64"));
        assert!(source.contains("CancellationToken"));
        assert!(source.contains("/v1/workers/deployments/{alias}/invoke"));
        assert!(!source.contains("__INVOKE_PATH__"));
    }

    #[test]
    fn julia_adapter_rejects_a_non_workers_contract_before_emission() {
        let error = julia_workers_adaptation_source(&ACTORS)
            .expect_err("Actors must not receive Workers Julia adaptation");
        assert!(error.to_string().contains("requires acyclic.workers.v1"));
    }

    #[test]
    fn julia_family_projection_uses_registry_routes_and_policies() {
        for family in ["actors", "workers", "stream", "objects", "inference"] {
            let source = julia_family_projection_source(family)
                .expect("explicit HTTP family projection renders");
            assert!(source.contains("const HTTP_PROJECTION = true"));
            assert!(source.contains("const GRPC_PROJECTION = false"));
            assert!(source.contains("const ROUTES = RouteSpec["));
            assert!(source.contains("const POLICIES = ["));
        }
    }

    #[test]
    fn julia_family_projection_rejects_non_http_registry_families() {
        let error = julia_family_projection_source("machines")
            .expect_err("machines has no explicit HTTP projection");
        assert!(error.to_string().contains("explicit HTTP family machines"));
    }

    #[test]
    fn stream_http_projection_is_explicit_polling_without_sse_equivalence() {
        let doc = document_stream_polling().expect("Stream polling projection");
        let paths = doc["paths"].as_object().expect("Stream paths");
        assert_eq!(paths.len(), STREAM.routes.len());
        let read = &paths["/v1/stream/read"]["post"];
        assert_eq!(read["x-protobuf-streaming"]["server"], true);
        assert_eq!(
            read["x-acyclic-http-polling"]["mode"],
            "one-message-per-request"
        );
        assert_eq!(read["x-acyclic-http-polling"]["sse"], false);
        assert_eq!(read["x-acyclic-http-polling"]["ndjson"], false);
        assert!(
            serde_json::to_string(&doc)
                .expect("Stream document JSON")
                .find("text/event-stream")
                .is_none()
        );
        assert_eq!(doc["x-acyclic-source"]["http_projection"], "polling");
    }

    #[test]
    fn stream_polling_export_preserves_rust_descriptions_for_every_schema() {
        let doc = document_stream_polling().expect("Stream model projects");
        let schemas = doc["components"]["schemas"].as_object().expect("schemas");
        for message in STREAM.messages {
            let schema = &schemas[&schema_key(&STREAM, message.name)];
            assert!(
                schema["description"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty()),
                "missing Stream message description: {}",
                message.name
            );
            for field in message.fields {
                assert!(
                    schema["properties"][field.json_name]["description"]
                        .as_str()
                        .is_some_and(|text| !text.is_empty()),
                    "missing Stream field description: {}.{}",
                    message.name,
                    field.name
                );
            }
        }
    }

    #[test]
    fn objects_export_preserves_all_routes_external_timestamp_and_stream_direction() {
        let doc = document_from_contract(&OBJECTS_V2).expect("Objects model projects");
        let paths = doc["paths"].as_object().expect("Objects paths");
        assert_eq!(paths.len(), OBJECTS_V2.routes.len());
        assert_eq!(doc["info"]["title"], "Acyclic Objects API");
        assert_eq!(
            paths["/v2/objects/objects/put"]["post"]["x-protobuf-streaming"]["client"],
            true
        );
        assert_eq!(
            paths["/v2/objects/objects/get"]["post"]["x-protobuf-streaming"]["server"],
            true
        );
        let put = &paths["/v2/objects/objects/put"]["post"];
        assert!(put["requestBody"]["content"]["application/x-ndjson"].is_object());
        assert!(put["requestBody"]["content"]["application/json"].is_null());
        assert!(put["responses"]["200"]["content"]["application/json"].is_object());
        assert_eq!(put["x-acyclic-http-streaming"]["ndjson"], true);
        assert_eq!(
            put["x-acyclic-objects-streaming"]["request_record_type"],
            "PutObjectRequest"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["response_record_type"],
            "ObjectInfo"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["record_encoding"],
            "canonical protobuf JSON record followed by LF"
        );
        assert_eq!(put["x-acyclic-objects-streaming"]["accepts_crlf"], true);
        assert_eq!(
            put["x-acyclic-objects-streaming"]["frame_limits"]["json_record_bytes"],
            131072
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["frame_limits"]["body_bytes"],
            65536
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["request_sequence"][0],
            "header"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["request_sequence"][1],
            "body*"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["request_sequence"][2],
            "complete"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["completion"]["must_be_last"],
            true
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["error"]["pre_stream"]["record_type"],
            "ErrorDetail"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["trailer"]["supported"],
            false
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["idempotency"]["field"],
            "header.mutation.idempotencyKey"
        );
        assert_eq!(
            put["x-acyclic-objects-streaming"]["cancellation"]["complete_frame_required"],
            true
        );
        assert_eq!(
            put["responses"]["default"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/acyclic_objects_v2_ErrorDetail"
        );
        let get = &paths["/v2/objects/objects/get"]["post"];
        assert!(get["requestBody"]["content"]["application/json"].is_object());
        assert!(get["responses"]["200"]["content"]["application/x-ndjson"].is_object());
        assert!(get["responses"]["200"]["content"]["application/json"].is_null());
        assert_eq!(get["x-acyclic-http-streaming"]["ndjson"], true);
        assert_eq!(
            get["x-acyclic-objects-streaming"]["response_record_type"],
            "GetObjectResponse"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["record_encoding"],
            "canonical protobuf JSON record followed by LF"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["frame_limits"]["json_record_bytes"],
            131072
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["frame_limits"]["body_bytes"],
            65536
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["response_sequence"][0],
            "header"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["response_sequence"][1],
            "body*"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["response_sequence"][2],
            "error?"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["error"]["in_stream"]["field"],
            "GetObjectResponse.error"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["error"]["in_stream"]["terminal"],
            true
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["trailer"]["field"],
            "error"
        );
        assert_eq!(
            get["x-acyclic-objects-streaming"]["idempotency"]["supported"],
            false
        );
        assert_eq!(
            get["responses"]["default"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/acyclic_objects_v2_ErrorDetail"
        );
        let upload_part = &paths["/v2/objects/multipart/upload-part"]["post"];
        assert!(upload_part["requestBody"]["content"]["application/x-ndjson"].is_object());
        assert!(upload_part["requestBody"]["content"]["application/json"].is_null());
        assert!(upload_part["responses"]["200"]["content"]["application/json"].is_object());
        assert_eq!(upload_part["x-acyclic-http-streaming"]["ndjson"], true);
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["request_record_type"],
            "UploadPartRequest"
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["response_record_type"],
            "UploadedPart"
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["record_encoding"],
            "canonical protobuf JSON record followed by LF"
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["frame_limits"]["json_record_bytes"],
            131072
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["frame_limits"]["body_bytes"],
            65536
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["completion"]["field"],
            "complete"
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["trailer"]["supported"],
            false
        );
        assert_eq!(
            upload_part["x-acyclic-objects-streaming"]["idempotency"]["field"],
            "header.mutation.idempotencyKey"
        );
        assert_eq!(
            upload_part["responses"]["default"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/acyclic_objects_v2_ErrorDetail"
        );
        let create_bucket = &paths["/v2/objects/buckets/create"]["post"];
        assert!(create_bucket["requestBody"]["content"]["application/json"].is_object());
        assert!(create_bucket["responses"]["200"]["content"]["application/json"].is_object());
        assert_eq!(
            doc["components"]["schemas"]["acyclic_objects_v2_ObjectInfo"]["properties"]["lastModified"]
                ["format"],
            "date-time"
        );
        assert_eq!(
            doc["components"]["schemas"]["acyclic_objects_v2_ObjectInfo"]["properties"]["size"]["x-protobuf-json"],
            "decimal-string"
        );
        assert_eq!(
            doc["components"]["schemas"]["acyclic_objects_v2_GetObjectResponse"]["properties"]["body"]
                ["x-protobuf-json"],
            "base64"
        );
        assert_eq!(
            doc["components"]["schemas"]["acyclic_objects_v2_GetObjectResponse"]["properties"]["error"]
                ["x-protobuf-oneof"],
            "frame"
        );
        assert_eq!(
            doc["components"]["schemas"]["acyclic_objects_v2_PutObjectRequest"]["properties"]["body"]
                ["x-protobuf-oneof"],
            "frame"
        );
        for message in OBJECTS_V2.messages {
            let schema = &doc["components"]["schemas"][&schema_key(&OBJECTS_V2, message.name)];
            assert!(
                schema["description"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty()),
                "missing Objects message description: {}",
                message.name
            );
        }
    }

    #[test]
    fn objects_streaming_metadata_is_bound_to_rust_frame_fields_and_limits() {
        let message = OBJECTS_V2
            .message("PutObjectRequest")
            .expect("PutObjectRequest");
        let mut fields = message.fields.to_vec();
        fields[0] = FieldSpec {
            json_name: "headerChanged",
            ..fields[0]
        };
        let changed_fields: &'static [FieldSpec] = Box::leak(fields.into_boxed_slice());
        let changed_message = acyclic_sdk_contract_wire::MessageSpec {
            fields: changed_fields,
            ..*message
        };
        let mut messages = OBJECTS_V2.messages.to_vec();
        let message_index = messages
            .iter()
            .position(|item| item.name == "PutObjectRequest")
            .expect("PutObjectRequest index");
        messages[message_index] = changed_message;
        let changed_messages: &'static [_] = Box::leak(messages.into_boxed_slice());

        let limits = OBJECTS_V2.enum_("ObjectsLimit").expect("ObjectsLimit");
        let mut values = limits.values.to_vec();
        let body_limit_index = values
            .iter()
            .position(|value| value.name == "OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES")
            .expect("body frame limit");
        values[body_limit_index] = acyclic_sdk_contract_wire::EnumValueSpec {
            number: 32768,
            ..values[body_limit_index]
        };
        let changed_values: &'static [_] = Box::leak(values.into_boxed_slice());
        let changed_limit = acyclic_sdk_contract_wire::EnumSpec {
            values: changed_values,
            ..*limits
        };
        let mut enums = OBJECTS_V2.enums.to_vec();
        let enum_index = enums
            .iter()
            .position(|item| item.name == "ObjectsLimit")
            .expect("ObjectsLimit index");
        enums[enum_index] = changed_limit;
        let changed_enums: &'static [_] = Box::leak(enums.into_boxed_slice());

        let altered_fields = ContractSpec {
            messages: changed_messages,
            ..OBJECTS_V2
        };
        let doc = document_from_contract(&altered_fields).expect("mutated Objects fields");
        let put = &doc["paths"]["/v2/objects/objects/put"]["post"];
        assert!(put["x-acyclic-objects-streaming"].is_null());
        let original = document_from_contract(&OBJECTS_V2).expect("original Objects model");
        assert_eq!(
            original["paths"]["/v2/objects/objects/put"]["post"]["x-acyclic-objects-streaming"]["frame_limits"]
                ["body_bytes"],
            65536
        );
        let altered_limits = ContractSpec {
            enums: changed_enums,
            ..OBJECTS_V2
        };
        let limit_doc = document_from_contract(&altered_limits).expect("mutated Objects limit");
        assert_eq!(
            limit_doc["paths"]["/v2/objects/objects/put"]["post"]["x-acyclic-objects-streaming"]["frame_limits"]
                ["body_bytes"],
            32768
        );
    }

    #[test]
    fn inference_export_preserves_routes_options_and_watch_polling_metadata() {
        let doc = document_inference().expect("Inference model projects");
        let paths = doc["paths"].as_object().expect("Inference paths");
        assert_eq!(paths.len(), INFERENCE.routes.len());
        assert_eq!(doc["info"]["title"], "Acyclic Customer API");
        let operation_ids = paths
            .values()
            .map(|path| path["post"]["operationId"].as_str().expect("operation id"))
            .collect::<BTreeSet<_>>();
        assert_eq!(operation_ids.len(), INFERENCE.routes.len());
        assert_eq!(
            paths["/v1/inference/runs/inspect"]["post"]["operationId"],
            "runsInspect"
        );
        assert_eq!(
            paths["/v1/inference/runs/inspect"]["post"]["x-protobuf-rpc"],
            "inference.customer.v1.RunsService/Inspect"
        );
        assert_eq!(
            paths["/v1/inference/runs/watch"]["post"]["x-acyclic-http-streaming"]["sse"],
            false
        );
        assert_eq!(
            paths["/v1/inference/runs/watch"]["post"]["x-acyclic-http-streaming"]["ndjson"],
            false
        );
        let options = doc["x-acyclic-validation-options"]["items"]
            .as_array()
            .expect("Inference validation options");
        assert!(options.iter().any(|option| {
            option["subject"] == "EvaluationSpec.maximum_case_results"
                && option["name"] == "max_uint64"
                && option["value"] == "65536"
        }));
        assert_eq!(
            doc["components"]["schemas"]["inference_customer_v1_ExactRational"]["properties"]["numerator"]
                ["x-protobuf-encoding"],
            "zigzag"
        );
    }

    #[test]
    fn preserves_protobuf_json_presence_oneof_and_unknown_enum_values() {
        let doc = document().expect("Actors model projects");
        let schemas = &doc["components"]["schemas"];
        assert_eq!(
            schemas["acyclic_actors_v1_ActorObservation"]["properties"]["codeSha256"]["format"],
            "byte"
        );
        assert_eq!(
            schemas["acyclic_actors_v1_ActorObservation"]["properties"]["checkpointUnixMillis"]["x-protobuf-presence"],
            "explicit"
        );
        assert_eq!(
            schemas["acyclic_actors_v1_ActorLimits"]["properties"]["memoryBytes"]["x-protobuf-json"],
            "decimal-string"
        );
        assert_eq!(
            schemas["acyclic_actors_v1_SubscriptionStart"]["properties"]["cursor"]["x-protobuf-oneof"],
            "start"
        );
        assert_eq!(
            schemas["acyclic_actors_v1_ActorState"]["x-protobuf-enum-unknown"],
            "preserve-numeric"
        );
        assert!(
            schemas["acyclic_actors_v1_ActorObservation"]
                .get("additionalProperties")
                .is_none(),
            "protobuf JSON objects keep unknown fields forward-compatible"
        );
        let enum_forms = schemas["acyclic_actors_v1_ActorState"]["oneOf"]
            .as_array()
            .expect("enum forms");
        assert!(enum_forms.iter().any(|item| item["type"] == "integer"));
        assert!(enum_forms.iter().any(|item| {
            item["enum"]
                .as_array()
                .is_some_and(|values| values.iter().any(|value| value == "ACTOR_STATE_ACTIVE"))
        }));
    }

    #[test]
    fn route_path_mutation_changes_generated_paths_and_preserves_parameter_encoding() {
        let original = document().expect("original");
        let route = ACTORS.routes[0];
        let mut routes = ACTORS.routes.to_vec();
        routes[0] = acyclic_sdk_contract_wire::RouteSpec {
            path: "/v1/actors/create/{tenant_id}",
            ..route
        };
        let altered_routes: &'static [_] = Box::leak(routes.into_boxed_slice());
        let altered = ContractSpec {
            routes: altered_routes,
            ..ACTORS
        };
        let mutated = document_from_contract(&altered).expect("mutated route");
        assert!(original["paths"].get("/v1/actors/create").is_some());
        assert!(mutated["paths"].get("/v1/actors/create").is_none());
        assert_eq!(
            mutated["paths"]["/v1/actors/create/{tenant_id}"]["post"]["parameters"][0]["name"],
            "tenant_id"
        );
        assert_eq!(
            path_parameters("/v1/objects/{object_path}/parts/{part_id}")
                .iter()
                .map(|parameter| parameter["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["object_path", "part_id"]
        );
        assert_ne!(
            original["x-acyclic-source"]["contract_digest"],
            mutated["x-acyclic-source"]["contract_digest"]
        );
    }

    #[test]
    fn rust_comment_descriptions_cover_every_projected_model_node() {
        let doc = document().expect("Actors model projects");
        let schemas = doc["components"]["schemas"].as_object().expect("schemas");
        let service = ACTORS
            .services
            .iter()
            .find(|service| service.name == "ActorsService")
            .expect("Actors service");
        for message in ACTORS.messages {
            let schema = &schemas[&schema_key(&ACTORS, message.name)];
            assert!(
                schema["description"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "missing message description: {}",
                message.name
            );
            for field in message.fields {
                let description = &schema["properties"][field.json_name]["description"];
                assert!(
                    description.as_str().is_some_and(|value| !value.is_empty()),
                    "missing field description: {}.{}",
                    message.name,
                    field.name
                );
            }
        }
        for enum_ in ACTORS.enums {
            assert!(
                schemas[&schema_key(&ACTORS, enum_.name)]["description"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty()),
                "missing enum description: {}",
                enum_.name
            );
        }
        for method in service.methods {
            let operation = doc["paths"]
                .as_object()
                .expect("paths")
                .values()
                .find_map(|path| {
                    let operation = &path["post"];
                    (operation["x-protobuf-rpc"]
                        == format!("{}.{} /{}", ACTORS.package, "ActorsService", method.name)
                            .replace(" ", ""))
                    .then_some(operation)
                })
                .expect("operation");
            assert_eq!(operation["description"], method.docs);
        }
        assert!(
            doc["info"]["description"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[test]
    fn source_model_mutation_changes_generated_schema() {
        let original = document().expect("original");
        let message = ACTORS.message("ActorLimits").expect("ActorLimits");
        let mut fields = message.fields.to_vec();
        let changed = fields[1];
        fields[1] = FieldSpec {
            json_name: "memoryBytesChanged",
            ..changed
        };
        let leaked_fields: &'static [FieldSpec] = Box::leak(fields.into_boxed_slice());
        let changed_message = acyclic_sdk_contract_wire::MessageSpec {
            fields: leaked_fields,
            ..*message
        };
        let mut messages = ACTORS.messages.to_vec();
        let index = messages
            .iter()
            .position(|item| item.name == "ActorLimits")
            .expect("ActorLimits index");
        messages[index] = changed_message;
        let altered: &'static [acyclic_sdk_contract_wire::MessageSpec] =
            Box::leak(messages.into_boxed_slice());
        let mutated = ContractSpec {
            messages: altered,
            ..ACTORS
        };
        let generated = document_from_contract(&mutated).expect("mutated");
        assert!(
            original["components"]["schemas"]["acyclic_actors_v1_ActorLimits"]["properties"]
                .get("memoryBytes")
                .is_some()
        );
        assert!(
            generated["components"]["schemas"]["acyclic_actors_v1_ActorLimits"]["properties"]
                .get("memoryBytesChanged")
                .is_some()
        );
        assert_ne!(
            original["x-acyclic-source"]["contract_digest"],
            generated["x-acyclic-source"]["contract_digest"]
        );
    }

    #[test]
    fn deleted_source_method_fails_generation() {
        let service = ACTORS.services[0];
        let methods: &'static [_] = Box::leak(
            service.methods[..service.methods.len() - 1]
                .to_vec()
                .into_boxed_slice(),
        );
        let changed_service = acyclic_sdk_contract_wire::ServiceSpec { methods, ..service };
        let services: &'static [_] = Box::leak(vec![changed_service].into_boxed_slice());
        let altered = ContractSpec { services, ..ACTORS };
        let error = document_from_contract(&altered).expect_err("deleted source method must fail");
        assert!(error.to_string().contains("invokeActor"));
    }

    #[test]
    fn output_is_reproducible() {
        assert_eq!(
            json_document().expect("JSON"),
            json_document().expect("JSON")
        );
    }

    #[test]
    fn stale_artifact_check_detects_and_accepts_current_output() {
        let path =
            std::env::temp_dir().join(format!("acyclic-sdk-openapi-{}.json", std::process::id()));
        std::fs::write(&path, "{}\n").expect("stale fixture");
        assert!(!check_json_file(&path).expect("stale check"));
        std::fs::write(&path, json_document().expect("current fixture")).expect("current fixture");
        assert!(check_json_file(&path).expect("fresh check"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn loopback_fixture_round_trips_uint64_oneof_and_unknown_enum_json() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("loopback listener");
        let address = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("fixture request");
            let mut request = Vec::new();
            stream.read_to_end(&mut request).expect("fixture read");
            let request = String::from_utf8(request).expect("utf8 request");
            assert!(request.contains("\"memoryBytes\":\"18446744073709551615\""));
            assert!(request.contains("\"codeSha256\":\"AQID\""));
            assert!(request.contains("\"currentHead\":true"));
            assert!(request.contains("\"futureField\":\"kept\""));
            let response = r#"{"code":12345,"message":"forward-compatible enum"}"#;
            write!(stream, "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).expect("fixture response");
        });

        let payload = r#"{"limits":{"memoryBytes":"18446744073709551615","codeSha256":"AQID"},"subscriptions":[{"start":{"currentHead":true}}],"futureField":"kept"}"#;
        let mut client = TcpStream::connect(address).expect("fixture connect");
        write!(client, "POST /v1/actors/create HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", payload.len(), payload).expect("fixture request");
        client
            .shutdown(Shutdown::Write)
            .expect("fixture request shutdown");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .expect("fixture response read");
        server.join().expect("fixture server");
        let response = String::from_utf8(response).expect("utf8 response");
        assert!(response.starts_with("HTTP/1.1 409 Conflict"));
        let body = response.split("\r\n\r\n").nth(1).expect("response body");
        let value: Value = serde_json::from_str(body).expect("protobuf JSON response");
        assert_eq!(value["code"], 12345);
        assert_eq!(value["message"], "forward-compatible enum");
    }

    #[test]
    fn loopback_fixture_uses_worker_and_stream_http_routes() {
        let workers = document_from_contract(&WORKERS).expect("Workers model");
        let stream = document_stream_polling().expect("Stream model");
        assert!(
            workers["paths"]
                .get("/v1/workers/deployments/{alias}/invoke")
                .is_some()
        );
        assert!(stream["paths"].get("/v1/stream/read").is_some());

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("loopback listener");
        let address = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("fixture request");
            let mut request = Vec::new();
            stream.read_to_end(&mut request).expect("fixture read");
            let request = String::from_utf8(request).expect("utf8 request");
            assert!(request.starts_with("POST /v1/workers/deployments/prod/invoke HTTP/1.1"));
            let response = r#"{"jobId":"job-1"}"#;
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).expect("fixture response");
        });
        let payload = r#"{"method":"main","input":"AQID"}"#;
        let mut client = TcpStream::connect(address).expect("fixture connect");
        write!(client, "POST /v1/workers/deployments/prod/invoke HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", payload.len(), payload).expect("fixture request");
        client
            .shutdown(Shutdown::Write)
            .expect("fixture request shutdown");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .expect("fixture response read");
        server.join().expect("fixture server");
        assert!(
            String::from_utf8(response)
                .expect("utf8 response")
                .starts_with("HTTP/1.1 200 OK")
        );

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("stream listener");
        let address = listener.local_addr().expect("stream listener address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("stream request");
            let mut request = Vec::new();
            stream.read_to_end(&mut request).expect("stream read");
            let request = String::from_utf8(request).expect("stream utf8");
            assert!(request.starts_with("POST /v1/stream/read HTTP/1.1"));
            let response = r#"{"items":[],"nextCursor":"cursor-2"}"#;
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).expect("stream response");
        });
        let payload = r#"{"path":"/objects/a","limit":1}"#;
        let mut client = TcpStream::connect(address).expect("stream connect");
        write!(client, "POST /v1/stream/read HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", payload.len(), payload).expect("stream request");
        client
            .shutdown(Shutdown::Write)
            .expect("stream request shutdown");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .expect("stream response read");
        server.join().expect("stream server");
        let response = String::from_utf8(response).expect("stream response utf8");
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("cursor-2"));
    }
}
