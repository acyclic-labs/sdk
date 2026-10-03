//! Rust-owned OpenAPI projection prototype.
//!
//! This crate deliberately consumes the Actors crate's generated descriptor and
//! `HTTP_ROUTES` table. It does not accept or parse a hand-authored OpenAPI
//! document. OpenAPI is an HTTP/JSON projection; the descriptor remains the
//! authority for field numbers, protobuf JSON spelling, enum values, bytes,
//! 64-bit integer representation, oneof membership and optional presence.

use std::collections::{BTreeMap, BTreeSet};

use acyclic_actors::FILE_DESCRIPTOR_SET;
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet, MethodDescriptorProto};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

const PACKAGE: &str = "acyclic.actors.v1";
const SERVICE: &str = "ActorsService";

/// Errors reported while projecting the canonical descriptor.
#[derive(Debug)]
pub enum Error {
    /// The checked-in descriptor is not a valid protobuf descriptor set.
    DescriptorDecode(prost::DecodeError),
    /// A required Actors service or method is absent from the descriptor.
    MissingContract(String),
    /// The descriptor contains a type that this small prototype does not map.
    UnsupportedType(String),
    /// Generated JSON could not be serialized.
    Json(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DescriptorDecode(error) => write!(f, "descriptor decode failed: {error}"),
            Self::MissingContract(name) => write!(f, "missing canonical Actors contract: {name}"),
            Self::UnsupportedType(name) => write!(f, "unsupported protobuf type: {name}"),
            Self::Json(error) => write!(f, "OpenAPI JSON serialization failed: {error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<prost::DecodeError> for Error {
    fn from(error: prost::DecodeError) -> Self { Self::DescriptorDecode(error) }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self { Self::Json(error) }
}

#[derive(Clone)]
struct Model {
    messages: BTreeMap<String, DescriptorProto>,
    enums: BTreeMap<String, prost_types::EnumDescriptorProto>,
}

impl Model {
    fn from_descriptor(bytes: &[u8]) -> Result<Self, Error> {
        let files = FileDescriptorSet::decode(bytes)?;
        let mut model = Self { messages: BTreeMap::new(), enums: BTreeMap::new() };
        for file in files.file {
            let package = file.package.as_deref().unwrap_or_default();
            for item in file.message_type {
                collect_message(&mut model.messages, &mut model.enums, package, &item);
            }
            for item in file.enum_type {
                let name = qualified(package, item.name.as_deref().unwrap_or_default());
                model.enums.insert(name, item);
            }
        }
        Ok(model)
    }

    fn message_schema(&self, name: &str) -> Result<Value, Error> {
        let descriptor = self.messages.get(name).ok_or_else(|| Error::MissingContract(name.into()))?;
        let mut properties = Map::new();
        for field in &descriptor.field {
            let field_name = field.json_name.as_deref().or(field.name.as_deref())
                .ok_or_else(|| Error::MissingContract(format!("field in {name}")))?;
            let mut schema = self.field_schema(field)?;
            if field.proto3_optional.unwrap_or(false) {
                schema["x-protobuf-presence"] = json!("explicit");
            }
            if let Some(oneof_index) = field.oneof_index {
                schema["x-protobuf-oneof"] = json!(descriptor.oneof_decl
                    .get(oneof_index as usize)
                    .and_then(|item| item.name.as_deref())
                    .unwrap_or("unknown"));
            }
            properties.insert(field_name.to_owned(), schema);
        }
        let mut out = Map::new();
        out.insert("type".into(), json!("object"));
        out.insert("properties".into(), Value::Object(properties));
        out.insert("additionalProperties".into(), json!(false));
        // Proto3 fields are absent-by-default; requiredness is represented by
        // validation metadata and Rust validation, never invented here.
        if !descriptor.oneof_decl.is_empty() {
            out.insert("x-protobuf-oneofs".into(), json!(descriptor.oneof_decl.iter()
                .filter_map(|oneof| oneof.name.clone()).collect::<Vec<_>>()));
        }
        Ok(Value::Object(out))
    }

    fn field_schema(&self, field: &FieldDescriptorProto) -> Result<Value, Error> {
        let kind = field.r#type();
        if field.label() == prost_types::field_descriptor_proto::Label::Repeated {
            if let Some(type_name) = field.type_name.as_deref() {
                if let Some(entry) = self.messages.get(type_name.trim_start_matches('.')) {
                    if entry.options.as_ref().is_some_and(|options| options.map_entry.unwrap_or(false)) {
                        return Ok(json!({
                            "type": "object",
                            "additionalProperties": {"x-protobuf-map-entry": type_name},
                            "x-protobuf-map-entry": true,
                        }));
                    }
                }
            }
            return Ok(json!({"type": "array", "items": self.scalar_schema(field, kind)?}));
        }
        self.scalar_schema(field, kind)
    }

    fn scalar_schema(&self, field: &FieldDescriptorProto, kind: prost_types::field_descriptor_proto::Type) -> Result<Value, Error> {
        use prost_types::field_descriptor_proto::Type;
        let mut schema = match kind {
            Type::Double | Type::Float => json!({"type": "number"}),
            Type::Int32 | Type::Sint32 | Type::Sfixed32 => json!({"type": "integer", "format": "int32"}),
            Type::Uint32 | Type::Fixed32 => json!({"type": "integer", "format": "int64", "minimum": 0}),
            // Protobuf JSON mandates decimal strings for all 64-bit integers.
            Type::Int64 | Type::Sint64 | Type::Sfixed64 => json!({"type": "string", "format": "int64", "x-protobuf-json": "decimal-string"}),
            Type::Uint64 | Type::Fixed64 => json!({"type": "string", "format": "int64", "pattern": "^[0-9]+$", "x-protobuf-json": "decimal-string"}),
            Type::Bool => json!({"type": "boolean"}),
            Type::String => json!({"type": "string"}),
            Type::Bytes => json!({"type": "string", "format": "byte", "x-protobuf-json": "base64"}),
            Type::Message | Type::Group => {
                let name = field.type_name.as_deref().unwrap_or_default().trim_start_matches('.');
                json!({"$ref": format!("#/components/schemas/{}", schema_key(name))})
            }
            Type::Enum => {
                let name = field.type_name.as_deref().unwrap_or_default().trim_start_matches('.');
                let enum_descriptor = self.enums.get(name).ok_or_else(|| Error::MissingContract(name.into()))?;
                let values: Vec<Value> = enum_descriptor.value.iter()
                    .filter_map(|value| value.name.clone()).map(Value::String).collect();
                json!({"type": "string", "enum": values, "x-protobuf-enum": name})
            }
        };
        if field.proto3_optional.unwrap_or(false) {
            schema["nullable"] = json!(true);
        }
        Ok(schema)
    }
}

fn collect_message(messages: &mut BTreeMap<String, DescriptorProto>, enums: &mut BTreeMap<String, prost_types::EnumDescriptorProto>, package: &str, item: &DescriptorProto) {
    let name = qualified(package, item.name.as_deref().unwrap_or_default());
    for nested in &item.nested_type {
        collect_message(messages, enums, &name, nested);
    }
    for nested in &item.enum_type {
        enums.insert(qualified(&name, nested.name.as_deref().unwrap_or_default()), nested.clone());
    }
    messages.insert(name, item.clone());
}

fn qualified(parent: &str, name: &str) -> String {
    if parent.is_empty() { name.to_owned() } else { format!("{parent}.{name}") }
}

fn schema_key(name: &str) -> String { name.replace('.', "_") }

fn method_name(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map_or_else(String::new, |first| first.to_lowercase().collect::<String>() + chars.as_str())
}

fn method_by_name<'a>(service: &'a prost_types::ServiceDescriptorProto, name: &str) -> Option<&'a MethodDescriptorProto> {
    service.method.iter().find(|method| method.name.as_deref() == Some(name))
}

/// Generate the complete Actors OpenAPI projection as a JSON value.
pub fn document() -> Result<Value, Error> {
    let model = Model::from_descriptor(FILE_DESCRIPTOR_SET)?;
    let files = FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)?;
    let service = files.file.iter().flat_map(|file| file.service.iter())
        .find(|candidate| candidate.name.as_deref() == Some(SERVICE))
        .ok_or_else(|| Error::MissingContract(SERVICE.into()))?;
    let source_sha256 = hex_digest(FILE_DESCRIPTOR_SET);
    let mut schemas = Map::new();
    for name in model.messages.keys().filter(|name| name.starts_with(PACKAGE)) {
        schemas.insert(schema_key(name), model.message_schema(name)?);
    }
    for (name, descriptor) in model.enums.iter().filter(|(name, _)| name.starts_with(PACKAGE)) {
        schemas.insert(schema_key(name), json!({
            "type": "string",
            "enum": descriptor.value.iter().filter_map(|value| value.name.clone()).collect::<Vec<_>>(),
            "x-protobuf-enum": name,
        }));
    }

    let mut paths = Map::new();
    let mut seen_routes = BTreeSet::new();
    for (route_name, path) in acyclic_actors::HTTP_ROUTES {
        if !seen_routes.insert(*path) { return Err(Error::MissingContract(format!("duplicate route {path}"))); }
        let rpc_name = service.method.iter().find(|method| method_name(method.name.as_deref().unwrap_or_default()) == *route_name)
            .and_then(|method| method.name.as_deref())
            .ok_or_else(|| Error::MissingContract((*route_name).into()))?;
        let method = method_by_name(service, rpc_name).ok_or_else(|| Error::MissingContract(rpc_name.into()))?;
        if method.client_streaming() || method.server_streaming() { return Err(Error::UnsupportedType(format!("streaming RPC {rpc_name}"))); }
        let input = method.input_type.as_deref().unwrap_or_default().trim_start_matches('.');
        let output = method.output_type.as_deref().unwrap_or_default().trim_start_matches('.');
        let operation = json!({
            "operationId": route_name,
            "x-protobuf-rpc": format!("{PACKAGE}.{SERVICE}/{rpc_name}"),
            "x-acyclic-route-source": "acyclic_actors::HTTP_ROUTES",
            "security": [{"bearerAuth": []}],
            "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", schema_key(input))}}}},
            "responses": {"200": {"description": "Successful response", "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", schema_key(output))}}}}, "default": {"description": "Canonical Actors error", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/acyclic_actors_v1_Error"}}}}}
        });
        paths.insert(format!("/{path}"), json!({"post": operation}));
    }
    Ok(json!({
        "openapi": "3.0.3",
        "info": {"title": "Acyclic Actors API", "version": "v1", "description": "Derived from the Rust-owned protobuf descriptor and route table. OpenAPI is a generated HTTP/JSON projection."},
        "servers": [{"url": "https://api.acyclic.dev"}],
        "x-acyclic-source": {"descriptor_sha256": source_sha256, "contract": "acyclic.actors.v1", "routes": "acyclic_actors::HTTP_ROUTES", "wire_authority": "protobuf"},
        "security": [{"bearerAuth": []}],
        "paths": paths,
        "components": {"securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}, "schemas": schemas}
    }))
}

/// Serialize the generated projection with stable, human-readable ordering.
pub fn json_document() -> Result<String, Error> { Ok(serde_json::to_string_pretty(&document()?)? + "\n") }

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_all_rust_owned_actor_routes() {
        let doc = document().expect("Actors descriptor projects");
        let paths = doc.get("paths").and_then(Value::as_object).expect("paths");
        assert_eq!(paths.len(), acyclic_actors::HTTP_ROUTES.len());
        assert!(paths.contains_key("/v1/actors/create"));
        assert_eq!(paths["/v1/actors/create"]["post"]["x-protobuf-rpc"], "acyclic.actors.v1.ActorsService/CreateActor");
    }

    #[test]
    fn preserves_protobuf_json_and_presence_metadata() {
        let doc = document().expect("Actors descriptor projects");
        let schemas = &doc["components"]["schemas"];
        assert_eq!(schemas["acyclic_actors_v1_ActorObservation"]["properties"]["codeSha256"]["format"], "byte");
        assert_eq!(schemas["acyclic_actors_v1_ActorObservation"]["properties"]["checkpointUnixMillis"]["x-protobuf-presence"], "explicit");
        assert_eq!(schemas["acyclic_actors_v1_ActorLimits"]["properties"]["memoryBytes"]["x-protobuf-json"], "decimal-string");
        assert_eq!(schemas["acyclic_actors_v1_SubscriptionStart"]["properties"]["cursor"]["x-protobuf-oneof"], "start");
    }

    #[test]
    fn output_is_reproducible() {
        assert_eq!(json_document().expect("JSON"), json_document().expect("JSON"));
    }
}
