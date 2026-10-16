//! Admission of reference-only immutable requests before offline persistence.
use crate::{
    Error, Result,
    conversation::{FileRef, ReferencedAttachments},
    core::Authority,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    operation_id: String,
    authority: Authority,
    kind: String,
    payload: Payload,
    offline_safe: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    #[serde(default, deserialize_with = "present")]
    content: Option<FileRef>,
    #[serde(default, deserialize_with = "present")]
    attachments: Option<ReferencedAttachments>,
    #[serde(default)]
    artifacts: Vec<FileRef>,
    #[serde(default)]
    references: Vec<FileRef>,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
}

fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(decoder).map(Some)
}

fn unsafe_request() -> Error {
    Error::Invalid("request is not safe for offline replay".into())
}

fn forbidden(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "token",
        "authorization",
        "credential",
        "secret",
        "password",
        "bearer",
        "api_key",
        "api-key",
        "apikey",
    ]
    .iter()
    .any(|word| key.contains(word))
        || key.split(['_', '-']).any(|word| {
            matches!(
                word,
                "scope" | "proof" | "body" | "text" | "bytes" | "base64" | "data"
            )
        })
}

/// Validates the persisted command's reference-only shape. Approvals and other
/// explicitly online-only actions never enter an offline volume. Credentials,
/// inline bodies, unknown fields and free-form metadata are rejected, including
/// malformed references. This grants no execution authority; the target still
/// authenticates the request when it is delivered.
pub fn validate_offline_request(bytes: &[u8]) -> Result<()> {
    let command: Request = serde_json::from_slice(bytes).map_err(|_| unsafe_request())?;
    if !command.offline_safe || command.kind == "interaction.resolve.approval" {
        return Err(unsafe_request());
    }
    command
        .authority
        .stream_path(&mut None)
        .map_err(|_| unsafe_request())?;
    crate::contract::validate_component_label(&command.operation_id, "operation")
        .map_err(|_| unsafe_request())?;
    crate::contract::validate_component_label(&command.kind, "command kind")
        .map_err(|_| unsafe_request())?;
    let payload = command.payload;
    for file in payload
        .content
        .iter()
        .chain(&payload.artifacts)
        .chain(&payload.references)
    {
        file.validate(&mut None).map_err(|_| unsafe_request())?;
    }
    if let Some(attachments) = payload.attachments {
        attachments
            .validate(&mut None)
            .map_err(|_| unsafe_request())?;
    }
    for (key, value) in payload.metadata {
        if forbidden(&key) || !matches!(value, Value::Null | Value::Bool(_) | Value::Number(_)) {
            return Err(unsafe_request());
        }
    }
    Ok(())
}
