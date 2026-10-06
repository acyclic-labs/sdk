//! Small, runnable comparison of Rust-authored metadata emitters.
//!
//! This intentionally keeps protocol metadata in a separate value.  The output is
//! useful evidence for the research report: each OSS crate emits a useful view of
//! Rust types, but none is a replacement for a descriptor-backed protocol model.

use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use specta::{Type, Types};
use specta_typescript::Typescript;
use std::collections::HashMap;
use typeshare_core::{
    context::{ParseContext, ParseFileContext},
    language::{CrateTypes, Language, TypeScript},
    parser::parse,
};
use utoipa::OpenApi;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, Type)]
struct ActorRequest {
    actor_id: String,
    revision: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema, Type)]
struct ActorResponse {
    actor_id: String,
    accepted: bool,
}

#[utoipa::path(
    post,
    path = "/v1/actors",
    request_body = ActorRequest,
    responses((status = 200, description = "Actor accepted", body = ActorResponse))
)]
#[allow(dead_code)]
fn create_actor() -> ActorResponse {
    ActorResponse {
        actor_id: "example".to_owned(),
        accepted: true,
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(create_actor),
    components(schemas(ActorRequest, ActorResponse)),
    info(title = "Actors prototype", version = "0.1")
)]
struct ActorsApi;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Type)]
struct StreamRecord {
    sequence: u32,
    payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Type)]
struct FollowRequest {
    cursor: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Type)]
struct FollowFrame {
    record: Option<StreamRecord>,
    complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Type)]
#[allow(dead_code)]
struct FilesystemRequest {
    path: String,
    recursive: bool,
    include_hidden: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct FilesystemBehavior {
    operation: &'static str,
    idempotent: bool,
    cancellation: &'static str,
    consistency: &'static str,
    output: &'static str,
}

fn openapi_output() -> String {
    ActorsApi::openapi()
        .to_pretty_json()
        .expect("utoipa should serialize its generated OpenAPI document")
}

fn specta_output() -> String {
    let types = Types::default()
        .register::<StreamRecord>()
        .register::<FollowRequest>()
        .register::<FollowFrame>();
    Typescript::default()
        .export(&types, specta_serde::Format)
        .expect("specta should export the stream types")
}

fn typeshare_output() -> String {
    let source = r#"
        #[typeshare]
        pub struct FilesystemRequest {
            pub path: String,
            pub recursive: bool,
            pub include_hidden: Option<bool>,
        }

        #[typeshare]
        pub enum FilesystemResult {
            Found,
            Missing,
        }
    "#;
    let parse_context = ParseContext::default();
    let file = ParseFileContext {
        source_code: source.to_owned(),
        crate_name: "metadata_fixture".into(),
        file_name: "filesystem.rs".to_owned(),
        file_path: "filesystem.rs".into(),
    };
    let mut data = parse(&parse_context, file)
        .expect("typeshare should parse the fixture")
        .expect("typeshare fixture contains #[typeshare] items");
    data.multi_file = false;
    let mut all_types: CrateTypes = HashMap::new();
    all_types.insert(data.crate_name.clone(), data.type_names.clone());
    let mut output = Vec::new();
    TypeScript::default()
        .generate_types(&mut output, &all_types, data)
        .expect("typeshare should generate TypeScript");
    String::from_utf8(output).expect("typeshare output should be UTF-8")
}

fn marker_report(document: &str) -> Value {
    let lower = document.to_ascii_lowercase();
    json!({
        "protobuf_field_tags": lower.contains("protobuf_tag") || lower.contains("field_number"),
        "field_presence_rules": lower.contains("presence"),
        "custom_options": lower.contains("custom_option") || lower.contains("custom option"),
        "rpc_identity": lower.contains("\"rpc\"") || lower.contains("\"service\""),
        "streaming_semantics": lower.contains("\"streaming\"")
            || lower.contains("server_stream")
            || lower.contains("client_stream"),
        "behavior_policy": lower.contains("idempotent") || lower.contains("cancellation"),
    })
}

fn main() {
    let openapi = openapi_output();
    let stream_schema = serde_json::to_string_pretty(&schema_for!(StreamRecord))
        .expect("schemars should serialize the stream schema");
    let specta = specta_output();
    let typeshare = typeshare_output();
    let behavior = serde_json::to_string_pretty(&FilesystemBehavior {
        operation: "list",
        idempotent: true,
        cancellation: "safe-before-commit",
        consistency: "snapshot-at-start",
        output: "FilesystemResult",
    })
    .expect("behavior metadata should serialize");

    let mut summary = serde_json::Map::new();
    summary.insert("utoipa_openapi".into(), Value::String(openapi.clone()));
    summary.insert(
        "schemars_stream_schema".into(),
        Value::String(stream_schema.clone()),
    );
    summary.insert("specta_typescript".into(), Value::String(specta.clone()));
    summary.insert(
        "typeshare_typescript".into(),
        Value::String(typeshare.clone()),
    );
    summary.insert(
        "filesystem_behavior_metadata".into(),
        Value::String(behavior.clone()),
    );
    summary.insert("utoipa_gaps".into(), marker_report(&openapi));
    summary.insert("schemars_gaps".into(), marker_report(&stream_schema));
    summary.insert("specta_gaps".into(), marker_report(&specta));
    summary.insert("typeshare_gaps".into(), marker_report(&typeshare));
    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Object(summary)).unwrap()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_emitters_produce_nonempty_contracts() {
        assert!(openapi_output().contains("/v1/actors"));
        assert!(serde_json::to_string(&schema_for!(StreamRecord))
            .unwrap()
            .contains("StreamRecord"));
        assert!(specta_output().contains("StreamRecord"));
        assert!(typeshare_output().contains("FilesystemRequest"));
    }

    #[test]
    fn generated_views_do_not_claim_protocol_identity() {
        assert_eq!(
            marker_report(&openapi_output())["protobuf_field_tags"],
            false
        );
        assert_eq!(marker_report(&specta_output())["rpc_identity"], false);
        assert_eq!(marker_report(&typeshare_output())["behavior_policy"], false);
    }
}
