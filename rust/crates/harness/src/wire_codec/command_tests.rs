use super::*;
use proptest::prelude::*;
use prost::Message as _;

proptest! {
    // Property: protobuf round trips preserve command decoding and the old
    // runtime-descriptor protocol decision, including error precedence.
    #[test]
    fn command_round_trip_preserves_identity_and_payload(
        reason in prop::option::of(".{0,256}"), revision in any::<u64>(),
        protocol in 0u8..4, noncanonical in any::<bool>(),
        digest in 0u8..3, damaged_scope in any::<bool>(),
    ) {
        let action = Action::TransitionLifecycle {
            to: crate::core::LifecycleState::Active, reason,
        };
        let mut envelope = tests::command_fixture(&action)?;
        envelope.expected_revision = revision;
        if digest != 0 {
            let (_, command) = decode_command_envelope(envelope.clone())?;
            envelope.intent_digest = crate::executor::json::canonical_json_digest(
                &command, crate::store::default_projection_read_limits().maximum_bytes, &mut None,
            )?.to_vec();
            if digest == 2 {
                let first = envelope.intent_digest.first_mut()
                    .ok_or_else(|| TestCaseError::fail("empty digest"))?;
                *first ^= 1;
            }
        }
        match protocol {
            0 => envelope.protocol = None,
            1 => envelope.protocol.as_mut().ok_or_else(|| TestCaseError::fail("missing protocol"))?.version.push('x'),
            2 => envelope.protocol.as_mut().ok_or_else(|| TestCaseError::fail("missing protocol"))?.descriptor_digest.push('x'),
            _ => (),
        }
        if noncanonical { envelope.canonical_action_json.push(b' '); }
        if damaged_scope {
            let _ = envelope.scope.as_mut().ok_or_else(|| TestCaseError::fail("missing scope"))?.proof.pop();
        }
        let digest = blake3::hash(crate::FILE_DESCRIPTOR_SET).to_hex();
        let expected = match envelope.protocol.as_ref() {
            None => Err(wire::ErrorCode::Invalid),
            Some(value) if (value.version.as_str(), value.descriptor_digest.as_str())
                != (EVENT_WIRE_VERSION, digest.as_str()) => Err(wire::ErrorCode::Unsupported),
            _ if noncanonical || damaged_scope => Err(wire::ErrorCode::Invalid),
            _ if envelope.intent_digest.is_empty() => Ok(()),
            _ => {
                // Recompute from the accepted command; comparing actual digest
                // bytes avoids assuming that any particular digest is invalid.
                let mut clean = envelope.clone();
                clean.intent_digest.clear();
                let (_, command) = decode_command_envelope(clean)?;
                let expected = crate::executor::json::canonical_json_digest(
                    &command, crate::store::default_projection_read_limits().maximum_bytes, &mut None,
                )?;
                if envelope.intent_digest == expected { Ok(()) } else { Err(wire::ErrorCode::Conflict) }
            },
        };
        let restored = wire::CommandEnvelope::decode(envelope.encode_to_vec().as_slice())
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&envelope, &restored);
        let actual = decode_command_envelope(envelope).map_err(|error| (error.code(), error.to_string()));
        let round_trip = decode_command_envelope(restored).map_err(|error| (error.code(), error.to_string()));
        prop_assert_eq!(&actual, &round_trip);
        prop_assert_eq!(actual.as_ref().map(|_| ()).map_err(|error| error.0), expected);
    }
}

#[test]
fn command_decode_without_subscriber() -> Result<()> {
    command_decode_cost(false)
}

#[test]
fn command_decode_with_tracing() -> Result<()> {
    command_decode_cost(true)
}

fn command_decode_cost(observing: bool) -> Result<()> {
    let dispatch = if observing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let action = Action::TransitionLifecycle {
        to: crate::core::LifecycleState::Active,
        reason: None,
    };
    let valid = tests::command_fixture(&action)?;
    for (label, envelope) in [
        ("missing_floor", wire::CommandEnvelope::default()),
        ("missing", wire::CommandEnvelope::default()),
        ("clone_only", valid.clone()),
        ("valid", valid),
    ] {
        let mut counts = None;
        let mut samples = Vec::with_capacity(32);
        for _ in 0..32 {
            let mut failure = None;
            let started = std::time::Instant::now();
            let allocation = acyclic_test_support::measure(|| {
                for _ in 0..128 {
                    if label == "clone_only" {
                        std::hint::black_box(envelope.clone());
                        continue;
                    }
                    let input = std::hint::black_box(envelope.clone());
                    let result = if label == "missing_floor" {
                        let result = Err(Error::Invalid("protocol identity is missing".into()));
                        drop(input);
                        result
                    } else {
                        decode_command_envelope(input)
                    };
                    let expected = if label == "valid" {
                        Ok(())
                    } else {
                        Err(wire::ErrorCode::Invalid)
                    };
                    if result.as_ref().map(|_| ()).map_err(|error| error.code()) != expected {
                        failure = Some(result);
                        break;
                    }
                    let _ = std::hint::black_box(result);
                }
            });
            samples.push(started.elapsed() / 128);
            assert!(failure.is_none(), "unexpected command decode: {failure:?}");
            let actual = (
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max,
            );
            assert_eq!(*counts.get_or_insert(actual), actual);
        }
        samples.sort_unstable();
        eprintln!(
            "command decode tracing={observing} case={label}: allocation_per_128_including_input_clone={counts:?}; median={:?}",
            samples.get(samples.len() / 2)
        );
    }
    Ok(())
}
