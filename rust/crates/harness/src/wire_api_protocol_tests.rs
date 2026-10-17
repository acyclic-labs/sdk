use super::*;
use proptest::prelude::*;
use prost::Message as _;

fn protocol() -> impl Strategy<Value = Option<wire::ProtocolIdentity>> {
    prop_oneof![
        Just(None),
        Just(Some(current_protocol())),
        ("(?s).{0,16}", "(?s).{0,96}").prop_map(|(version, descriptor_digest)| {
            Some(wire::ProtocolIdentity {
                version,
                descriptor_digest,
            })
        }),
    ]
}

fn status(
    protocol: Option<wire::ProtocolIdentity>,
    owner: wire::Authority,
) -> wire::OperationStatus {
    wire::OperationStatus {
        protocol,
        owner: Some(owner),
        operation: Some(wire::OperationIdentity {
            operation_id: "operation".into(),
            idempotency_key: String::new(),
        }),
        state: wire::CompletionState::Running as i32,
        ..Default::default()
    }
}

proptest! {
    #[test]
    fn protocol_decisions_survive_wire_round_trips(
        protocol in protocol(),
        payload in proptest::collection::vec(any::<u8>(), 0..1024),
        invalid_status in any::<bool>(),
    ) {
        let expected = match &protocol {
            None => Err(wire::ErrorCode::Invalid),
            Some(protocol) if protocol != &current_protocol() => Err(wire::ErrorCode::Unsupported),
            Some(_) => Ok(()),
        };
        let command = wire::CommandEnvelope {
            protocol: protocol.clone(), canonical_action_json: payload, ..Default::default()
        };
        let decoded = wire::CommandEnvelope::decode(command.encode_to_vec().as_slice())
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&decoded, &command);
        prop_assert_eq!(validate_command_protocol(&command).map_err(|error| error.code()), expected);
        prop_assert_eq!(validate_command_protocol(&decoded).map_err(|error| error.code()), expected);
        let resume = wire::ResumeRequest { protocol: protocol.clone(), ..Default::default() };
        prop_assert_eq!(validate_resume_protocol(&resume).map_err(|error| error.code()), expected);
        let handshake = wire::HandshakeRequest { protocol: protocol.clone(), required: None };
        prop_assert_eq!(negotiate(&handshake, &wire::CapabilitySet::default()).map(|_| ()).map_err(|error| error.code()), expected);
        let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "owner".into() };
        let status = status(protocol, owner.clone());
        let decoded = wire::OperationStatus::decode(status.encode_to_vec().as_slice())
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&decoded, &status);
        let expected = match expected {
            Err(wire::ErrorCode::Invalid) => Err(wire::ErrorCode::Conflict),
            Ok(()) if invalid_status => Err(wire::ErrorCode::Conflict),
            other => other,
        };
        for status in [&status, &decoded] {
            let actual = validate_operation_status("operation", (!invalid_status).then_some(&owner), status)
                .map_err(|error| error.code());
            prop_assert_eq!(actual, expected);
        }
    }
}

#[test]
fn protocol_validation_needs_no_owned_identity() -> Result<()> {
    let command = wire::CommandEnvelope {
        protocol: Some(current_protocol()),
        ..Default::default()
    };
    let resume = wire::ResumeRequest {
        protocol: Some(current_protocol()),
        ..Default::default()
    };
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "owner".into(),
    };
    let status = status(Some(current_protocol()), owner.clone());
    let mut totals = Vec::new();
    for observing in [false, true] {
        let dispatch = if observing {
            tracing::Dispatch::new(tracing_subscriber::registry())
        } else {
            tracing::Dispatch::none()
        };
        let _guard = tracing::dispatcher::set_default(&dispatch);
        for operation in 0..3 {
            let mut samples = Vec::with_capacity(64);
            let mut counts = None;
            for _ in 0..64 {
                let mut error = None;
                let started = std::time::Instant::now();
                let allocation = acyclic_test_support::measure(|| {
                    for _ in 0..128 {
                        let result = match operation {
                            0 => validate_command_protocol(std::hint::black_box(&command)),
                            1 => validate_resume_protocol(std::hint::black_box(&resume)),
                            _ => validate_operation_status(
                                "operation",
                                Some(std::hint::black_box(&owner)),
                                std::hint::black_box(&status),
                            ),
                        };
                        if let Err(failure) = result {
                            error = Some(failure);
                            break;
                        }
                    }
                });
                samples.push(started.elapsed() / 128);
                if let Some(error) = error {
                    return Err(error);
                }
                let actual = (
                    allocation.count_total,
                    allocation.bytes_total,
                    allocation.bytes_max,
                );
                assert_eq!(*counts.get_or_insert(actual), actual);
            }
            samples.sort_unstable();
            eprintln!(
                "protocol gate tracing={observing} operation={operation}: allocation_per_128={counts:?}; median={:?}",
                samples.get(samples.len() / 2)
            );
            totals.push(counts);
        }
    }
    assert!(
        totals.iter().all(|counts| *counts == Some((0, 0, 0))),
        "protocol gate allocates: {totals:?}"
    );
    Ok(())
}
