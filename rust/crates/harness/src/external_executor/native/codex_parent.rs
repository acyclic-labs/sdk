//! Native transcripts retain ancestry that Codex omits from nested hook inputs.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufReader, Read};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct Record {
    #[serde(rename = "type")]
    kind: Kind,
    payload: Metadata,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    SessionMeta,
}

#[derive(Deserialize, Serialize)]
struct Metadata {
    session_id: Uuid,
    id: Uuid,
    parent_thread_id: Option<Uuid>,
}

fn invalid() -> Error {
    Error::Invalid("Codex transcript ancestry differs from its hook".into())
}

fn decode(reader: impl Read) -> Result<Metadata> {
    // Deserialize only the first record. Ignored native fields need no Value
    // tree, and neither a long transcript nor later events are replayed here.
    Record::deserialize(&mut serde_json::Deserializer::from_reader(reader))
        .map(|record| record.payload)
        .map_err(|_| invalid())
}

fn bind(input: &mut Value, metadata: Metadata, child_transcript: bool) -> Result<()> {
    let session = input["session_id"].as_str().ok_or_else(invalid)?;
    let child = input["agent_id"].as_str().ok_or_else(invalid)?;
    let session = Uuid::parse_str(session).map_err(|_| invalid())?;
    let child = Uuid::parse_str(child).map_err(|_| invalid())?;
    if metadata.session_id != session || child == session {
        return Err(invalid());
    }
    let parent = if child_transcript {
        if metadata.id != child {
            return Err(invalid());
        }
        metadata.parent_thread_id.ok_or_else(invalid)?
    } else {
        // SubagentStop also supplies the parent's transcript; the child's
        // transcript can be absent when the native host has already retired it.
        metadata.id
    };
    if parent == child {
        return Err(invalid());
    }
    let caller = (parent != session).then(|| parent.to_string());
    if input.get("parent_agent_id").is_some() && input.get("parentAgentId").is_some() {
        return Err(invalid());
    }
    for field in ["parent_agent_id", "parentAgentId"] {
        if let Some(value) = input.get(field)
            && value != &serde_json::json!(caller)
        {
            return Err(invalid());
        }
    }
    input["parent_agent_id"] = serde_json::json!(caller);
    // The normalizer rejects duplicate aliases; retain just the canonical one.
    input
        .as_object_mut()
        .ok_or_else(invalid)?
        .remove("parentAgentId");
    Ok(())
}

pub(super) fn prepare(event: &str, input: &mut Value) -> Result<()> {
    let (field, child_transcript) = match event {
        "SubagentStart" => ("transcript_path", true),
        "SubagentStop" if input["agent_transcript_path"].is_string() => {
            ("agent_transcript_path", true)
        }
        "SubagentStop" => ("transcript_path", false),
        _ => return Ok(()),
    };
    let Some(path) = input[field].as_str() else {
        return Ok(());
    };
    let file = std::fs::File::open(path).map_err(super::storage)?;
    bind(input, decode(BufReader::new(file))?, child_transcript)
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn fuzz(bytes: &[u8]) {
    // Exercise the actual reader on raw bytes too: converting through Value
    // would hide duplicate fields and malformed encodings from this decoder.
    let _ = decode(bytes);
    if let Ok((mut input, record, child_transcript)) =
        serde_json::from_slice::<(Value, Value, bool)>(bytes)
    {
        let bytes = serde_json::to_vec(&record).unwrap();
        if let Ok(metadata) = decode(bytes.as_slice()) {
            let encoded = serde_json::to_vec(&Record {
                kind: Kind::SessionMeta,
                payload: Metadata {
                    session_id: metadata.session_id,
                    id: metadata.id,
                    parent_thread_id: metadata.parent_thread_id,
                },
            })
            .unwrap();
            let again = decode(encoded.as_slice()).unwrap();
            let mut replay = input.clone();
            let first = bind(&mut input, metadata, child_transcript);
            let second = bind(&mut replay, again, child_transcript);
            assert_eq!(first.is_ok(), second.is_ok());
            assert_eq!(input, replay);
        }
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
