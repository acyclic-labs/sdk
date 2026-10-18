#![no_main]
//! The JSONL wire spells every byte string in one tagged form: `{"text":..}`
//! exactly when the bytes are UTF-8, else padded standard `{"base64":..}`.
//! Arbitrary input is used both as a wire line and, after a frame selector
//! byte, as file/head/digest bytes.
pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::{
    client::jsonl,
    task_volume::{Change, Cursor, FileKey, FileValue, Generation, HeadFile, HeadSnapshot, Query},
};
use acyclic_stream::OwnedBytes;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

const ID: &str = "00000000-0000-0000-0000-000000000001";

/// Independent reference for the byte form, the same one the unit properties
/// use: the exact text for UTF-8, otherwise the `base64` crate's padded
/// standard encoding, compared exactly.
fn assert_form(value: &Value, bytes: &[u8]) {
    let text = bytes.utf8_chunks().next().map_or("", |chunk| chunk.valid());
    let expected = if text.len() == bytes.len() {
        json!({ "text": text })
    } else {
        json!({ "base64": STANDARD.encode(bytes) })
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
    let key: FileKey = fuzz_support::must_decode(serde_json::from_value(
        json!({"Session":{"session":ID,"file":{"Request":ID}}}),
    ));
    let file = FileValue::new(key.clone(), data.to_vec());
    let (volume, commit) = (digest(data, 0), digest(data, 32));
    let cursor: Cursor = fuzz_support::must_decode(serde_json::from_value(json!({
        "volume": volume, "epoch": ID, "schema": acyclic_harness::task_volume::SCHEMA,
        "query": digest(data, 64), "position": data.len(), "accepted": data.len(),
        "durable": data.len(), "commit": commit,
    })));
    match selector % 3 {
        0 => {
            let line = jsonl::request(&file).unwrap();
            let request: Value = fuzz_support::must_decode(serde_json::from_str(&line));
            assert_form(&request["bytes"], data);
            assert!(!integer_array(&request));
            let _ = fuzz_support::round_trip(&file, jsonl::parse_request(&line));
        }
        1 => {
            let change = Change {
                file: Some(file),
                files: vec![],
                removed: vec![],
                cursor: cursor.clone(),
            };
            let encoded = jsonl::change(&change).unwrap();
            let decoded =
                fuzz_support::round_trip_by(&encoded, jsonl::parse_change(&encoded), jsonl::change);
            assert_eq!(decoded.file, change.file);
            assert!(decoded.files.is_empty() && decoded.removed.is_empty());
            assert_eq!(decoded.cursor, cursor);
            let encoded: Value = fuzz_support::must_decode(serde_json::from_str(&encoded));
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
            let query =
                Query::Session(fuzz_support::must_decode(serde_json::from_value(json!(ID))));
            let snapshot = jsonl::snapshot(
                &query,
                &head(vec![data[..at].to_vec().into(), data[at..].to_vec().into()]),
            )
            .unwrap();
            let _ = fuzz_support::round_trip(
                &(query, head(vec![data.to_vec().into()])),
                jsonl::parse_snapshot(&snapshot),
            );
            let snapshot: Value = fuzz_support::must_decode(serde_json::from_str(&snapshot));
            assert!(!integer_array(&snapshot));
            assert_form(&snapshot[1]["files"][0]["bytes"], data);
            assert_form(&snapshot[1]["generation"]["volume"], &volume);
        }
    }
}

fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    if data.len() > 4096 {
        return;
    }
    if let Some((&selector, bytes)) = data.split_first() {
        frame(selector, bytes);
    }
    if let Some(file) = fuzz_support::FuzzInput::parse_text(fuzz_input, jsonl::parse_request) {
        let line = jsonl::request(&file).unwrap();
        let value = fuzz_support::must_decode(serde_json::from_str(&line));
        assert!(!integer_array(&value));
        let _ = fuzz_support::round_trip(&file, jsonl::parse_request(&line));
    }
    if let Some(change) = fuzz_support::FuzzInput::parse_text(fuzz_input, jsonl::parse_change) {
        let line = jsonl::change(&change).unwrap();
        let value = fuzz_support::must_decode(serde_json::from_str(&line));
        assert!(!integer_array(&value));
        let _ = fuzz_support::round_trip_by(&line, jsonl::parse_change(&line), jsonl::change);
    }
    if let Some((query, head)) =
        fuzz_support::FuzzInput::parse_text(fuzz_input, jsonl::parse_snapshot)
    {
        let line = jsonl::snapshot(&query, &head).unwrap();
        let value = fuzz_support::must_decode(serde_json::from_str(&line));
        assert!(!integer_array(&value));
        let _ =
            fuzz_support::round_trip_by(&line, jsonl::parse_snapshot(&line), |(query, head)| {
                jsonl::snapshot(query, head)
            });
    }
});
