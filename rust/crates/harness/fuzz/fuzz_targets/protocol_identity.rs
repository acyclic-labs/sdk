//! Fuzz protocol validation and protobuf round-trip stability.
#![no_main]

use acyclic_harness::{wire, wire_api};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fuzz_target!(|data: &[u8]| {
    let Ok(command) = wire::CommandEnvelope::decode(data) else {
        return;
    };
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
    let restored = wire::CommandEnvelope::decode(command.encode_to_vec().as_slice());
    assert!(matches!(&restored, Ok(restored) if restored == &command));
    if let Ok(restored) = restored {
        assert_eq!(
            wire_api::validate_command_protocol(&restored).map_err(|error| error.code()),
            expected
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
