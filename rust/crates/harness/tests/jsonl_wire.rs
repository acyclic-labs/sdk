//! Canonical JSONL wire contracts.
use acyclic_harness::client::jsonl;
use acyclic_harness::{
    OperationId,
    task_volume::{
        Change, Cursor, FileKey, FileValue, Generation, HeadFile, HeadSnapshot, Query, SCHEMA,
        TaskFile,
    },
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use proptest::prelude::*;
use serde_json::{Value, json};

fn request_key(operation: OperationId) -> FileKey {
    FileKey::Session {
        session: acyclic_harness::task_volume::Component::new(operation.to_string()).unwrap(),
        file: TaskFile::Request(
            acyclic_harness::task_volume::Component::new(operation.to_string()).unwrap(),
        ),
    }
}

/// Independent reference for the one byte form: UTF-8 is text, anything else
/// is padded standard base64, and the two never overlap.
fn byte_form(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => json!({ "text": text }),
        Err(_) => json!({ "base64": STANDARD.encode(bytes) }),
    }
}

/// No JSON array of numbers anywhere on the wire.
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

fn cursor(id: [u8; 16], digest: [u8; 32], position: u64) -> Cursor {
    Cursor {
        volume: digest,
        epoch: uuid::Uuid::from_bytes(id),
        schema: SCHEMA,
        query: [b'q'; 32],
        position,
        accepted: position,
        durable: position,
        commit: [0xff; 32],
    }
}

fn joined(file: &HeadFile) -> Vec<u8> {
    file.segments
        .iter()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}

fn bytes_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        ".{0,32}".prop_map(String::into_bytes),
        prop::collection::vec(any::<u8>(), 0..64),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // One canonical request-file codec preserves exact bytes and identities,
    // and spells the bytes in the single tagged form.
    #[test]
    fn canonical_request_files_round_trip(
        bytes in bytes_strategy(), id in any::<[u8;16]>(),
        requester in prop::option::of(any::<[u8;16]>()),
    ) {
        let operation = OperationId::from_bytes(id);
        let file = FileValue::new(request_key(operation), bytes.clone());
        let mut stored = serde_json::to_value(&file).unwrap();
        let requester = requester.map(acyclic_harness::TaskId::from_bytes);
        stored["requester"] = json!(requester);
        let file: FileValue = serde_json::from_value(stored).unwrap();
        let line = jsonl::request(&file).unwrap();
        prop_assert!(!line.contains('\n'));
        let value: Value = serde_json::from_str(&line).unwrap();
        prop_assert_eq!(&value["bytes"], &byte_form(&bytes));
        prop_assert_eq!(&value["requester"], &json!(requester));
        prop_assert!(!integer_array(&value));
        prop_assert_eq!(jsonl::parse_request(&line).unwrap(), file);
    }
}

#[test]
fn request_codec_rejects_the_removed_wire_dialect_and_trailing_values() {
    for line in [
        r#"{"command":{"operation":{"operationId":"op"}}}"#,
        "{} {}",
        "null",
    ] {
        assert!(jsonl::parse_request(line).is_err());
    }
    assert!(jsonl::parse_request(&"x".repeat(jsonl::MAXIMUM_REQUEST_BYTES + 1)).is_err());
}

// Negative controls: every spelling other than the canonical one is refused,
// including the integer arrays the serde representation of `FileValue` writes.
#[test]
fn byte_codec_rejects_every_non_canonical_spelling() {
    let file = FileValue::new(
        request_key(OperationId::from_bytes([7; 16])),
        b"hi".to_vec(),
    );
    let canonical: Value = serde_json::from_str(&jsonl::request(&file).unwrap()).unwrap();
    assert_eq!(canonical["bytes"], json!({ "text": "hi" }));
    assert_eq!(jsonl::parse_request(&canonical.to_string()).unwrap(), file);
    let removed = serde_json::to_string(&file).unwrap();
    assert!(removed.contains(r#""bytes":[104,105]"#));
    assert!(jsonl::parse_request(&removed).is_err());
    for bytes in [
        json!([104, 105]),
        json!("hi"),
        json!({ "base64": "aGk=" }),
        json!({ "base64": "/w" }),
        json!({ "base64": "/x==" }),
        json!({ "base64": "/w== " }),
        json!({ "text": "hi", "base64": "/w==" }),
        json!({ "hex": "6869" }),
        json!({ "text": [104, 105] }),
        json!({}),
    ] {
        let mut line = canonical.clone();
        line["bytes"] = bytes.clone();
        assert!(
            jsonl::parse_request(&line.to_string()).is_err(),
            "accepted {bytes}"
        );
    }
    let mut binary = canonical;
    binary["bytes"] = json!({ "base64": "/w==" });
    assert_eq!(
        jsonl::parse_request(&binary.to_string())
            .unwrap()
            .bytes
            .as_slice(),
        &[0xff][..]
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Snapshot and every cursor-follow publication preserve the same file bytes,
    // identity and accepted/durable cut as the request codec, spell a file's
    // bytes identically in both frames, and carry digests in the same form.
    #[test]
    fn snapshots_and_follows_round_trip(
        bytes in bytes_strategy(), split in any::<prop::sample::Index>(),
        id in any::<[u8;16]>(), digest in any::<[u8;32]>(), position in 0u64..1000,
    ) {
        let operation = OperationId::from_bytes(id);
        let key = request_key(operation);
        let cursor = cursor(id, digest, position);
        let at = split.index(bytes.len() + 1);
        let head = HeadSnapshot {
            files: vec![HeadFile {
                key: key.clone(),
                channel: None,
                publication: None,
                segments: vec![bytes[..at].to_vec().into(), bytes[at..].to_vec().into()],
                length: bytes.len() as u64,
                through_sequence: None,
            }],
            cursor: cursor.clone(),
            generation: Generation { volume: cursor.volume, epoch: cursor.epoch, position, commit: cursor.commit },
        };
        let query = Query::Session(acyclic_harness::task_volume::Component::new(operation.to_string()).unwrap());
        let snapshot = jsonl::snapshot(&query, &head).unwrap();
        let (decoded_query, decoded) = jsonl::parse_snapshot(&snapshot).unwrap();
        prop_assert_eq!(&decoded_query, &query);
        prop_assert_eq!(&joined(&decoded.files[0]), &bytes);
        prop_assert_eq!(decoded.files[0].length, bytes.len() as u64);
        prop_assert_eq!(&decoded.cursor, &head.cursor);
        prop_assert_eq!(&decoded.generation, &head.generation);
        prop_assert_eq!(jsonl::snapshot(&decoded_query, &decoded).unwrap(), snapshot.clone());

        let change = Change { file: Some(FileValue::new(key, bytes.clone())), files: vec![], removed: vec![], cursor };
        let encoded = jsonl::change(&change).unwrap();
        let decoded = jsonl::parse_change(&encoded).unwrap();
        prop_assert_eq!(&decoded.file, &change.file);
        prop_assert_eq!(&decoded.cursor, &change.cursor);
        prop_assert_eq!(jsonl::change(&decoded).unwrap(), encoded.clone());
        let snapshot: Value = serde_json::from_str(&snapshot).unwrap();
        let encoded: Value = serde_json::from_str(&encoded).unwrap();
        prop_assert!(!integer_array(&snapshot) && !integer_array(&encoded));
        prop_assert_eq!(&snapshot[1]["files"][0]["bytes"], &encoded["file"]["bytes"]);
        prop_assert_eq!(&encoded["file"]["bytes"], &byte_form(&bytes));
        prop_assert_eq!(&encoded["cursor"]["volume"], &byte_form(&digest));
        prop_assert_eq!(&snapshot[1]["generation"]["commit"], &byte_form(&[0xff; 32]));
    }
}

// Negative controls for the frames: the removed segmented head spelling, a
// wrong-length digest, and integer-array digests are refused.
#[test]
fn frames_reject_segments_and_malformed_digests() {
    let id = [3; 16];
    let operation = OperationId::from_bytes(id);
    let change = Change {
        file: Some(FileValue::new(request_key(operation), b"x".to_vec())),
        files: vec![],
        removed: vec![],
        cursor: cursor(id, [1; 32], 4),
    };
    let canonical: Value = serde_json::from_str(&jsonl::change(&change).unwrap()).unwrap();
    for (field, digest) in [
        ("volume", json!([1u8; 32].to_vec())),
        ("volume", json!({ "text": "\u{1}" })),
        ("commit", json!({ "base64": STANDARD.encode([0xff; 31]) })),
    ] {
        let mut line = canonical.clone();
        line["cursor"][field] = digest;
        assert!(jsonl::parse_change(&line.to_string()).is_err());
    }
    let head = HeadSnapshot {
        files: vec![HeadFile {
            key: request_key(operation),
            channel: None,
            publication: None,
            segments: vec![b"x".to_vec().into()],
            length: 1,
            through_sequence: None,
        }],
        cursor: change.cursor.clone(),
        generation: Generation {
            volume: [1; 32],
            epoch: change.cursor.epoch,
            position: 4,
            commit: [0xff; 32],
        },
    };
    let query = Query::Session(
        acyclic_harness::task_volume::Component::new(operation.to_string()).unwrap(),
    );
    let mut snapshot: Value =
        serde_json::from_str(&jsonl::snapshot(&query, &head).unwrap()).unwrap();
    assert_eq!(snapshot[1]["files"][0]["bytes"], json!({ "text": "x" }));
    let file = snapshot[1]["files"][0].as_object_mut().unwrap();
    file.remove("bytes");
    file.insert("segments".into(), json!([[120]]));
    assert!(jsonl::parse_snapshot(&snapshot.to_string()).is_err());
}

// The checked-in fuzz corpus: canonical frames decode and re-encode
// byte-identically; the removed integer-array, base64-of-text and segmented
// spellings are refused.
#[test]
fn checked_in_wire_corpus() {
    macro_rules! seed {
        ($name:literal) => {
            std::str::from_utf8(include_bytes!(concat!(
                "fixtures/wire-corpus/",
                $name
            )))
            .unwrap()
        };
    }
    for line in [seed!("request-text"), seed!("request-base64"), seed!("request-requester")] {
        assert_eq!(
            jsonl::request(&jsonl::parse_request(line).unwrap()).unwrap(),
            line
        );
    }
    let change = seed!("change-text");
    assert_eq!(
        jsonl::change(&jsonl::parse_change(change).unwrap()).unwrap(),
        change
    );
    let snapshot = seed!("snapshot-head");
    let (query, head) = jsonl::parse_snapshot(snapshot).unwrap();
    assert_eq!(jsonl::snapshot(&query, &head).unwrap(), snapshot);
    for line in [
        seed!("request-integer-array"),
        seed!("request-base64-of-text"),
    ] {
        assert!(jsonl::parse_request(line).is_err());
    }
    assert!(jsonl::parse_snapshot(seed!("snapshot-segments")).is_err());
}
