#![no_main]
//! The JSONL wire spells every byte string in one tagged form: `{"text":..}`
//! exactly when the bytes are UTF-8, else padded standard `{"base64":..}`.
//! Arbitrary input is used both as file/head/digest bytes and as a wire line.
use acyclic_harness::{
    client::jsonl,
    task_volume::{Change, Cursor, FileKey, FileValue, Generation, HeadFile, HeadSnapshot, Query},
};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

const ID: &str = "00000000-0000-0000-0000-000000000001";

/// Independent reference for the byte form, without the codec's base64 engine:
/// the exact text for UTF-8, otherwise canonical-length padded base64.
fn assert_form(value: &Value, bytes: &[u8]) {
    let form = value.as_object().expect("bytes are a tagged object");
    assert_eq!(form.len(), 1, "exactly one tag");
    match std::str::from_utf8(bytes) {
        Ok(text) => assert_eq!(form.get("text"), Some(&json!(text))),
        Err(_) => {
            let encoded = form["base64"].as_str().expect("binary bytes are base64");
            assert_eq!(encoded.len(), bytes.len().div_ceil(3) * 4);
            let body = encoded.trim_end_matches('=');
            assert!(encoded.len() - body.len() <= 2);
            assert!(
                body.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/')
            );
        }
    }
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

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    let key: FileKey =
        serde_json::from_value(json!({"Session":{"session":ID,"file":{"Request":ID}}})).unwrap();
    let file = FileValue::new(key.clone(), data.to_vec());
    let line = jsonl::request(&file).unwrap();
    let request: Value = serde_json::from_str(&line).unwrap();
    assert_form(&request["bytes"], data);
    assert!(!integer_array(&request));
    assert_eq!(jsonl::parse_request(&line).unwrap(), file);

    let (volume, commit) = (digest(data, 0), digest(data, 32));
    let cursor: Cursor = serde_json::from_value(json!({
        "volume": volume, "epoch": ID, "schema": acyclic_harness::task_volume::SCHEMA,
        "query": digest(data, 64), "position": data.len(), "accepted": data.len(),
        "durable": data.len(), "commit": commit,
    }))
    .unwrap();
    let change = Change {
        file: Some(file),
        files: vec![],
        removed: vec![],
        cursor: cursor.clone(),
    };
    let encoded = jsonl::change(&change).unwrap();
    let decoded = jsonl::parse_change(&encoded).unwrap();
    assert_eq!(decoded.file, change.file);
    assert_eq!(decoded.cursor, cursor);
    assert_eq!(jsonl::change(&decoded).unwrap(), encoded);
    let encoded: Value = serde_json::from_str(&encoded).unwrap();
    assert!(!integer_array(&encoded));
    assert_form(&encoded["cursor"]["volume"], &volume);
    assert_form(&encoded["cursor"]["commit"], &commit);

    // A head split into segments travels as the one value a live frame spells.
    let at = usize::from(data.first().copied().unwrap_or(0)).min(data.len());
    let head = HeadSnapshot {
        files: vec![HeadFile {
            key,
            channel: None,
            publication: None,
            segments: vec![data[..at].to_vec().into(), data[at..].to_vec().into()],
            length: data.len() as u64,
            through_sequence: Some(data.len() as u64),
        }],
        generation: Generation {
            volume,
            epoch: cursor.epoch,
            position: cursor.position,
            commit,
        },
        cursor,
    };
    let query = Query::Session(serde_json::from_value(json!(ID)).unwrap());
    let snapshot = jsonl::snapshot(&query, &head).unwrap();
    let (query, materialized) = jsonl::parse_snapshot(&snapshot).unwrap();
    let joined: Vec<u8> = materialized.files[0]
        .segments
        .iter()
        .flat_map(|segment| segment.iter().copied())
        .collect();
    assert_eq!(joined, data);
    assert_eq!(materialized.files[0].length, data.len() as u64);
    assert_eq!(jsonl::snapshot(&query, &materialized).unwrap(), snapshot);
    let snapshot: Value = serde_json::from_str(&snapshot).unwrap();
    assert!(!integer_array(&snapshot));
    assert_eq!(snapshot[1]["files"][0]["bytes"], encoded["file"]["bytes"]);

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
