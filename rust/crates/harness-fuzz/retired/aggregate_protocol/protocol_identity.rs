//! Fuzz command protocol validation and protobuf round-trip stability.
#![no_main]

use acyclic_harness::{core::Action, wire, wire_api, wire_codec};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fn canonical_action_matches_reference(bytes: &[u8]) {
    let Ok(action) = serde_json::from_slice::<Action>(bytes) else {
        return;
    };
    let Ok(mut value) = serde_json::to_value(&action) else {
        return;
    };
    value.sort_all_objects();
    let Ok(encoded) = serde_json::to_vec(&value) else {
        panic!("invalid canonical Value");
    };
    let Ok(mut probe) = wire::CommandEnvelope::decode(
        include_bytes!("../corpus/protocol_identity/command-valid").as_slice(),
    ) else {
        panic!("invalid command seed");
    };
    probe.protocol = Some(wire_api::current_protocol());
    probe.action_type = value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| unreachable!("serialized Action has a kind tag"))
        .into();
    probe.canonical_action_json = bytes.to_vec();
    let result = wire_codec::decode_command_envelope(probe);
    assert_eq!(result.is_ok(), encoded == bytes);
}

fuzz_target!(|data: &[u8]| {
    let Ok(command) = wire::CommandEnvelope::decode(data) else {
        return;
    };
    canonical_action_matches_reference(&command.canonical_action_json);
    let expected = match command.protocol.as_ref() {
        None => Err(wire::ErrorCode::Invalid),
        Some(protocol) if protocol != &wire_api::current_protocol() => {
            Err(wire::ErrorCode::Unsupported)
        }
        Some(_) => Ok(()),
    };
    assert_eq!(
        wire_api::validate_command_protocol(&command).map_err(|error| error.code()),
        expected
    );
    let decoded = wire_codec::decode_command_envelope(command.clone())
        .map_err(|error| (error.code(), error.to_string()));
    if data == include_bytes!("../corpus/protocol_identity/command-valid")
        || data == include_bytes!("../corpus/protocol_identity/command-nested")
    {
        assert!(decoded.is_ok(), "valid seed must reach action decoding");
    }
    if let Err(code) = expected {
        assert!(matches!(&decoded, Err((actual, _)) if *actual == code));
    }
    let restored = wire::CommandEnvelope::decode(command.encode_to_vec().as_slice());
    assert!(matches!(&restored, Ok(restored) if restored == &command));
    if let Ok(restored) = restored {
        assert_eq!(
            wire_api::validate_command_protocol(&restored).map_err(|error| error.code()),
            expected
        );
        assert_eq!(
            wire_codec::decode_command_envelope(restored)
                .map_err(|error| (error.code(), error.to_string())),
            decoded
        );
    }
    let resume = wire::ResumeRequest {
        protocol: command.protocol.clone(),
        ..Default::default()
    };
    assert_eq!(
        wire_api::validate_resume_protocol(&resume).map_err(|error| error.code()),
        expected
    );
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "owner".into(),
    };
    let status = wire::OperationStatus {
        protocol: command.protocol,
        owner: Some(owner.clone()),
        operation: Some(wire::OperationIdentity {
            operation_id: "operation".into(),
            idempotency_key: String::new(),
        }),
        state: wire::CompletionState::Running as i32,
        ..Default::default()
    };
    let expected = expected.map_err(|code| {
        if code == wire::ErrorCode::Invalid {
            wire::ErrorCode::Conflict
        } else {
            code
        }
    });
    assert_eq!(
        wire_api::validate_operation_status("operation", Some(&owner), &status)
            .map_err(|error| error.code()),
        expected
    );
});
