use super::*;
use acyclic_harness::{
    OperationId,
    task_volume::{Component, FileKey, FileValue, TaskFile},
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // One canonical request-file codec preserves exact bytes and identities.
    #[test]
    fn canonical_request_files_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..128), id in any::<[u8;16]>()) {
        let operation = OperationId::from_bytes(id);
        let file = FileValue::new(FileKey::Session {
            session: Component::new(operation.to_string()).unwrap(),
            file: TaskFile::Request(Component::new(operation.to_string()).unwrap()),
        }, bytes);
        let line = jsonl::request(&file).unwrap();
        prop_assert!(!line.contains('\n'));
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Snapshot and every cursor-follow publication preserve the same file bytes,
    // identity and accepted/durable cut as the request codec.
    #[test]
    fn snapshots_and_follows_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..64), id in any::<[u8;16]>(), position in 0u64..1000) {
        use acyclic_harness::task_volume::{Change, Cursor, Generation, HeadFile, HeadSnapshot, Query, SCHEMA};
        let operation = OperationId::from_bytes(id);
        let key = FileKey::Session { session: Component::new(operation.to_string()).unwrap(), file: TaskFile::Request(Component::new(operation.to_string()).unwrap()) };
        let cursor = Cursor { volume:[1;32], epoch:uuid::Uuid::from_bytes(id), schema:SCHEMA, query:[2;32], position, accepted:position, durable:position, commit:[3;32] };
        let head = HeadSnapshot { files:vec![HeadFile { key:key.clone(), channel:None, publication:None, segments:vec![bytes.clone().into()], length:bytes.len() as u64, through_sequence:None }], cursor:cursor.clone(), generation:Generation { volume:cursor.volume, epoch:cursor.epoch, position, commit:cursor.commit } };
        let query = Query::Session(Component::new(operation.to_string()).unwrap());
        prop_assert_eq!(jsonl::parse_snapshot(&jsonl::snapshot(&query, &head).unwrap()).unwrap(), (query, head));
        let change = Change { file:Some(FileValue::new(key, bytes)), files:vec![], removed:vec![], cursor };
        let encoded = jsonl::change(&change).unwrap();
        let decoded = jsonl::parse_change(&encoded).unwrap();
        prop_assert_eq!(jsonl::change(&decoded).unwrap(), encoded);
    }
}
