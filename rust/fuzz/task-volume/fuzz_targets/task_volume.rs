#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::task_volume::{Cursor, FileKey, FileValue, Generation, Query};
use acyclic_stream::{CommitId, Record, wire, wire_codec};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(path) = fuzz_input.text() {
        let _ = Query::folder(path);
    }
    if let Some(path) = fuzz_input.text()
        && let Some(key) = fuzz_support::FuzzInput::parse_text(fuzz_input, FileKey::parse)
    {
        assert_eq!(key.path(), path);
    }
    let _ = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<FileKey>);
    let _ = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<Query>);
    let _ = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<Cursor>);
    let _ = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<Generation>);
    for sequence in [0, 1] {
        let _ = FileValue::from_record(&Record {
            sequence,
            value: bytes.to_vec().into(),
            commit_id: CommitId::default(),
            committed_at_micros: 0,
        });
    }
    if let Some(request) = fuzz_support::protobuf_input!(fuzz_input, wire::CommitRequest)
        && let Some(request) = request.admit(wire_codec::commit_from_wire)
    {
        let _ = fuzz_support::round_trip(
            &request,
            wire_codec::commit_from_wire(wire_codec::commit_to_wire(&request)),
        );
    }
    if let Some(observation) = fuzz_support::FuzzInput::parse(fuzz_input, |raw_input| {
        acyclic_stream::persistence::decode_observation(raw_input, 4096)
    }) {
        assert_eq!(
            acyclic_stream::persistence::encode_observation(&observation, 4096).as_deref(),
            Ok(bytes)
        );
    }
});
