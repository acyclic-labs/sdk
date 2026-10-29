//! Fuzz resume/status protocol validation and protobuf round-trip stability.
#![no_main]

use acyclic_harness::{wire, wire_api};
use libfuzzer_sys::fuzz_target;
use prost::Message as _;

fuzz_target!(|data: &[u8]| {
    let Ok(resume) = wire::ResumeRequest::decode(data) else {
        return;
    };
    // Keep acceptance reachable when the compiled descriptor changes, while
    // independently checking the exact protocol supplied by the corpus.
    let current = wire::ResumeRequest {
        protocol: Some(wire_api::current_protocol()),
        ..resume.clone()
    };
    for resume in [resume, current] {
        let expected = match resume.protocol.as_ref() {
            None => Err(wire::ErrorCode::Invalid),
            Some(protocol) if protocol != &wire_api::current_protocol() => {
                Err(wire::ErrorCode::Unsupported)
            }
            Some(_) => Ok(()),
        };
        assert_eq!(
            wire_api::validate_resume_protocol(&resume).map_err(|error| error.code()),
            expected
        );
        let restored = wire::ResumeRequest::decode(resume.encode_to_vec().as_slice());
        assert!(matches!(&restored, Ok(restored) if restored == &resume));
        if let Ok(restored) = restored {
            assert_eq!(
                wire_api::validate_resume_protocol(&restored).map_err(|error| error.code()),
                expected
            );
        }
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "owner".into(),
        };
        let status = wire::OperationStatus {
            protocol: resume.protocol,
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
        let restored = wire::OperationStatus::decode(status.encode_to_vec().as_slice());
        assert!(matches!(&restored, Ok(restored) if restored == &status));
        assert_eq!(
            wire_api::validate_operation_status("operation", Some(&owner), &status)
                .map_err(|error| error.code()),
            expected
        );
        if let Ok(restored) = restored {
            assert_eq!(
                wire_api::validate_operation_status("operation", Some(&owner), &restored)
                    .map_err(|error| error.code()),
                expected
            );
        }
    }
});
