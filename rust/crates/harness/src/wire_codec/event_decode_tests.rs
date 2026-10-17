use super::*;
use crate::store::HistoryWork;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use proptest::prelude::*;
use prost::Message as _;

fn budget(hashed: u64) -> WorkBudget {
    WorkBudget {
        source_bytes_read: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: hashed,
        items_examined: 1_000_000,
        allocation_operations: 10_000,
        peak_allocation_bytes: 16_777_216,
        ..WorkBudget::default()
    }
}

proptest! {
    // Property: wire round trips preserve the original runtime-descriptor
    // decision, every later validation error, and protocol-error precedence.
    #[test]
    fn event_round_trip_preserves_identity_and_error_precedence(
        protocol in 0u8..4, damage in 0u8..6, revision in any::<u64>(),
    ) {
        let mut envelope = wire::EventEnvelope::decode(tests::fixture().as_slice())
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        envelope.revision = revision;
        match protocol {
            0 => envelope.protocol = None,
            1 => envelope.protocol.as_mut().ok_or_else(|| TestCaseError::fail("missing protocol"))?.version.push('x'),
            2 => envelope.protocol.as_mut().ok_or_else(|| TestCaseError::fail("missing protocol"))?.descriptor_digest.push('x'),
            _ => (),
        }
        match damage {
            0 => envelope.authority = None,
            1 => envelope.operation_id = "invalid".into(),
            2 => envelope.scope = None,
            3 => envelope.canonical_payload_json.push(b' '),
            4 => envelope.attestation.clear(),
            _ => (),
        }
        let digest = blake3::hash(crate::FILE_DESCRIPTOR_SET).to_hex();
        let expected = match envelope.protocol.as_ref() {
            None => Err(wire::ErrorCode::Storage),
            Some(value) if (value.version.as_str(), value.descriptor_digest.as_str())
                != (EVENT_WIRE_VERSION, digest.as_str()) => Err(wire::ErrorCode::Unsupported),
            _ if damage == 1 => Err(wire::ErrorCode::Invalid),
            _ if damage < 5 => Err(wire::ErrorCode::Storage),
            _ => Ok(()),
        };
        let bytes = envelope.encode_to_vec();
        let restored = wire::EventEnvelope::decode(bytes.as_slice())
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&restored, &envelope);
        let actual = decode_event(&bytes, &mut None).map_err(|error| (error.code(), error.to_string()));
        let round_trip = decode_event(&restored.encode_to_vec(), &mut None)
            .map_err(|error| (error.code(), error.to_string()));
        prop_assert_eq!(&actual, &round_trip);
        prop_assert_eq!(actual.as_ref().map(|_| ()).map_err(|error| error.0), expected);
        if let Ok(((authority, event), _)) = actual {
            prop_assert_eq!(event.revision, revision);
            prop_assert_eq!(encode_event(&authority, &event, &mut None)?, bytes);
        }
    }
}

#[test]
fn event_decode_retains_both_descriptor_charges_and_exact_budget_boundary() -> Result<()> {
    let bytes = tests::fixture();
    let descriptor = crate::FILE_DESCRIPTOR_SET.len() as u64;
    for allowance in [
        0,
        descriptor - 1,
        descriptor,
        2 * descriptor - 1,
        2 * descriptor,
        2 * descriptor + 1,
    ] {
        let token = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(allowance), &token)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let before = native.live_allocation_bytes();
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let result = decode_event(&bytes, &mut Some(&mut io));
        assert_eq!(result.is_ok(), allowance >= 2 * descriptor);
        if result.is_ok() {
            assert_eq!(io.receipt().bytes_hashed, 2 * descriptor);
        }
        drop(result);
        drop(io.into_custody()?);
        assert_eq!(native.live_allocation_bytes(), before);
    }
    Ok(())
}

#[test]
fn event_decode_without_subscriber() -> Result<()> {
    event_decode_cost(false, false)
}

#[test]
fn event_decode_with_tracing() -> Result<()> {
    event_decode_cost(true, false)
}

#[test]
fn event_decode_with_accounting() -> Result<()> {
    event_decode_cost(false, true)
}

fn event_decode_cost(tracing: bool, accounted: bool) -> Result<()> {
    let bytes = tests::fixture();
    let reference = decode_event(&bytes, &mut None)?;
    let dispatch = if tracing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mut times = Vec::with_capacity(64);
    let mut counts = None;
    let mut charges = None;
    for _ in 0..64 {
        let token = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(16_777_216), &token)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let mut work: Option<&mut dyn HistoryIo> = accounted.then_some(&mut io);
        let mut result = None;
        let started = std::time::Instant::now();
        let allocation = acyclic_test_support::measure(|| {
            result = Some(decode_event(std::hint::black_box(&bytes), &mut work));
        });
        times.push(started.elapsed());
        let decoded = result.ok_or_else(|| Error::Invalid("decoder did not run".into()))??;
        assert_eq!(decoded.0, reference.0);
        let actual = (
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_max,
        );
        assert_eq!(*counts.get_or_insert(actual), actual);
        if accounted {
            let actual = io.receipt();
            assert_eq!(
                actual.bytes_hashed,
                2 * crate::FILE_DESCRIPTOR_SET.len() as u64
            );
            assert_eq!(*charges.get_or_insert(actual), actual);
        }
        drop(decoded);
        drop(io.into_custody()?);
    }
    times.sort_unstable();
    eprintln!(
        "event decode tracing={tracing} accounting={accounted}: input_bytes={}; descriptor_bytes={}; allocations={counts:?}; median={:?}; charges={charges:?}",
        bytes.len(),
        crate::FILE_DESCRIPTOR_SET.len(),
        times.get(times.len() / 2)
    );
    Ok(())
}
