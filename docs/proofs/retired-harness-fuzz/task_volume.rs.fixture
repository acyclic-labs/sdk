#![no_main]
use acyclic_harness::task_volume::{Cursor, FileKey, FileValue, Generation, Query};
use acyclic_stream::{CommitId, Record, wire, wire_codec};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(path) = std::str::from_utf8(bytes) {
        let _ = Query::folder(path);
    }
    if let Ok(path) = std::str::from_utf8(bytes)
        && let Ok(key) = FileKey::parse(path)
    {
        assert_eq!(key.path(), path);
    }
    if let Ok(request) = serde_json::from_slice::<acyclic_harness::interaction::Interaction>(bytes)
    {
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            serde_json::from_slice::<acyclic_harness::interaction::Interaction>(&encoded).unwrap(),
            request
        );
    }
    let _ = acyclic_harness::interaction::ApprovalDecisionFile::decode(bytes, bytes.len() as u64);
    let _ = serde_json::from_slice::<FileKey>(bytes);
    if let Ok(query) = serde_json::from_slice::<Query>(bytes) {
        assert_eq!(
            serde_json::from_slice::<Query>(&serde_json::to_vec(&query).unwrap()).unwrap(),
            query
        );
        let _ = query.ranges();
    }
    let _ = serde_json::from_slice::<Cursor>(bytes);
    let _ = serde_json::from_slice::<Generation>(bytes);
    for sequence in [0, 1] {
        let _ = FileValue::from_record(&Record {
            sequence,
            value: bytes.to_vec().into(),
            commit_id: CommitId::default(),
            committed_at_micros: 0,
            headers: Default::default(),
        });
    }
    if let Ok(request) = wire::CommitRequest::decode(bytes)
        && let Ok(request) = wire_codec::commit_from_wire(request)
    {
        assert_eq!(
            wire_codec::commit_from_wire(wire_codec::commit_to_wire(&request)),
            Ok(request)
        );
    }
    if let Ok(observation) = acyclic_stream::persistence::decode_observation(bytes, 4096) {
        assert_eq!(
            acyclic_stream::persistence::encode_observation(&observation, 4096).as_deref(),
            Ok(bytes)
        );
    }
});
