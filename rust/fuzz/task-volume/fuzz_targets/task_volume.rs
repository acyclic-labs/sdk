#![no_main]
//! Exercise production interaction, approval and stream persistence admission.
use acyclic_stream::{wire, wire_codec};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(request) = serde_json::from_slice::<acyclic_harness::interaction::Interaction>(bytes)
    {
        let decoded = serde_json::to_vec(&request)
            .map_err(|error| ("serialize", error))
            .and_then(|encoded| {
                serde_json::from_slice::<acyclic_harness::interaction::Interaction>(&encoded)
                    .map_err(|error| ("decode", error))
            });
        assert!(
            decoded.as_ref().is_ok_and(|decoded| decoded == &request),
            "interaction round trip failed: original={request:?}; decoded={decoded:?}"
        );
    }
    let _ = acyclic_harness::interaction::ApprovalDecisionFile::decode(bytes, bytes.len() as u64);
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
