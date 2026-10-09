//! Canonical model exchange fixtures emitted by the actual Rust serializers.
#![allow(missing_docs, reason = "contract example binary, not public API")]
use acyclic_harness::{
    Error, Result,
    conversation::Limits,
    model::{
        Model, ModelContent, ModelContentPart, ModelMessage, ModelPrefix, ModelRequest, ModelRole,
        PreparedModelRequest, ToolResultContent,
    },
    tool::ToolDefinition,
};
use serde_json::json;
fn text(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|error| Error::Invalid(error.to_string()))
}
fn main() -> Result<()> {
    let request = ModelRequest {
        model: Model::new("mock", "exact", "pinned", json!({}))?,
        messages: vec![
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("é\0🦀\r\n".into()),
            },
            ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(ModelContentPart::ToolCall {
                    call_id: "call-1".into(),
                    name: "echo".into(),
                    arguments: json!("é"),
                }),
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: ModelContent::Part(ModelContentPart::ToolResult {
                    call_id: "call-1".into(),
                    name: "echo".into(),
                    content: ToolResultContent::Json { value: json!("é") },
                }),
            },
        ],
        tools: vec![ToolDefinition {
            name: "echo".into(),
            revision: "schema-2".into(),
            description: "Echo".into(),
            input_schema: json!({"type":"string"}),
            output_schema: json!({"type":"string"}),
            projection_schema: acyclic_harness::tool::json_projection_schema(
                json!({"type":"string"}),
            ),
        }],
        max_output_tokens: Some(32),
    };
    let prepared = PreparedModelRequest::prepare(request, Limits::default())?;
    println!(
        "{}",
        json!({
            "request": text(prepared.bytes().to_vec())?,
            "prefix": text(ModelPrefix::select(&prepared, None)?.canonical_bytes()?)?,
        })
    );
    Ok(())
}
