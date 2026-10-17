#![no_main]
//! The JSONL wire spells every byte string in one tagged form: `{"text":..}`
//! exactly when the bytes are UTF-8, else padded standard `{"base64":..}`.
//! Arbitrary input is used both as a wire line and, after a frame selector
//! byte, as file/head/digest bytes.
use acyclic_harness::{
    client::jsonl,
    task_volume::{Change, Cursor, FileKey, FileValue, Generation, HeadFile, HeadSnapshot, Query},
};
use acyclic_stream::OwnedBytes;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

const ID: &str = "00000000-0000-0000-0000-000000000001";

/// Independent reference for the byte form, the same one the unit properties
/// use: the exact text for UTF-8, otherwise the `base64` crate's padded
/// standard encoding, compared exactly.
fn assert_form(value: &Value, bytes: &[u8]) {
    let expected = match std::str::from_utf8(bytes) {
        Ok(text) => json!({ "text": text }),
        Err(_) => json!({ "base64": STANDARD.encode(bytes) }),
    };
    assert_eq!(value, &expected);
}

fn integer_array(value: &Value) -> bool {
    match value {
        Value::Array(items) => {
            (!items.is_empty() && items.iter().all(Value::is_number))
                || items.iter().any(integer_array)
        }
        Value::Object(fields) => fields.values().any(integer_array),
        _ => false,
    }
}

fn digest(data: &[u8], offset: usize) -> [u8; 32] {
    std::array::from_fn(|index| data.get(offset + index).copied().unwrap_or(0xff))
}

/// Each run encodes one frame: the first byte selects the request, change or
/// snapshot frame and the rest are its bytes. Every frame's bytes must equal
/// the independent reference form, so the snapshot head and the live file are
/// spelled identically for the same bytes without encoding both per run.
/// Decoded values are compared structurally; encoding is deterministic, so an
/// equal value re-encodes to the same line.
fn frame(selector: u8, data: &[u8]) {
    let key: FileKey =
        serde_json::from_value(json!({"Session":{"session":ID,"file":{"Request":ID}}})).unwrap();
    let file = FileValue::new(key.clone(), data.to_vec());
    let (volume, commit) = (digest(data, 0), digest(data, 32));
    let cursor: Cursor = serde_json::from_value(json!({
        "volume": volume, "epoch": ID, "schema": acyclic_harness::task_volume::SCHEMA,
        "query": digest(data, 64), "position": data.len(), "accepted": data.len(),
        "durable": data.len(), "commit": commit,
    }))
    .unwrap();
    match selector % 3 {
        0 => {
            let line = jsonl::request(&file).unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_form(&request["bytes"], data);
            assert!(!integer_array(&request));
            assert_eq!(jsonl::parse_request(&line).unwrap(), file);
        }
        1 => {
            let change = Change {
                file: Some(file),
                files: vec![],
                removed: vec![],
                cursor: cursor.clone(),
            };
            let encoded = jsonl::change(&change).unwrap();
            let decoded = jsonl::parse_change(&encoded).unwrap();
            assert_eq!(decoded.file, change.file);
            assert!(decoded.files.is_empty() && decoded.removed.is_empty());
            assert_eq!(decoded.cursor, cursor);
            let encoded: Value = serde_json::from_str(&encoded).unwrap();
            assert!(!integer_array(&encoded));
            assert_form(&encoded["file"]["bytes"], data);
            assert_form(&encoded["cursor"]["volume"], &volume);
            assert_form(&encoded["cursor"]["commit"], &commit);
        }
        _ => {
            // A head split into segments travels as the one value a live
            // frame spells, and decodes as that single segment.
            let at = usize::from(data.first().copied().unwrap_or(0)).min(data.len());
            let generation = Generation {
                volume,
                epoch: cursor.epoch,
                position: cursor.position,
                commit,
            };
            let head = |segments: Vec<OwnedBytes>| HeadSnapshot {
                files: vec![HeadFile {
                    key: key.clone(),
                    channel: None,
                    publication: None,
                    segments,
                    length: data.len() as u64,
                    through_sequence: Some(data.len() as u64),
                }],
                generation,
                cursor: cursor.clone(),
            };
            let query = Query::Session(serde_json::from_value(json!(ID)).unwrap());
            let snapshot = jsonl::snapshot(
                &query,
                &head(vec![data[..at].to_vec().into(), data[at..].to_vec().into()]),
            )
            .unwrap();
            assert_eq!(
                jsonl::parse_snapshot(&snapshot).unwrap(),
                (query, head(vec![data.to_vec().into()]))
            );
            let snapshot: Value = serde_json::from_str(&snapshot).unwrap();
            assert!(!integer_array(&snapshot));
            assert_form(&snapshot[1]["files"][0]["bytes"], data);
            assert_form(&snapshot[1]["generation"]["volume"], &volume);
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    if let Some((&selector, bytes)) = data.split_first() {
        frame(selector, bytes);
    }

    // Any accepted line re-encodes canonically, without integer arrays.
    if let Ok(text) = std::str::from_utf8(data) {
        if let Ok(file) = jsonl::parse_request(text) {
            let line = jsonl::request(&file).unwrap();
            assert!(!integer_array(&serde_json::from_str(&line).unwrap()));
            assert_eq!(jsonl::parse_request(&line).unwrap(), file);
        }
        if let Ok(change) = jsonl::parse_change(text) {
            let line = jsonl::change(&change).unwrap();
            assert!(!integer_array(&serde_json::from_str(&line).unwrap()));
            assert_eq!(
                jsonl::change(&jsonl::parse_change(&line).unwrap()).unwrap(),
                line
            );
        }
        if let Ok((query, head)) = jsonl::parse_snapshot(text) {
            let line = jsonl::snapshot(&query, &head).unwrap();
            assert!(!integer_array(&serde_json::from_str(&line).unwrap()));
            let (query, head) = jsonl::parse_snapshot(&line).unwrap();
            assert_eq!(jsonl::snapshot(&query, &head).unwrap(), line);
        }
    }
});
