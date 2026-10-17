use super::*;

proptest::proptest! {
    #[test]
    fn compiled_digest_matches_every_descriptor_partition(
        chunk_size in 1usize..=crate::FILE_DESCRIPTOR_SET.len() + 1,
    ) {
        let mut hasher = blake3::Hasher::new();
        for chunk in crate::FILE_DESCRIPTOR_SET.chunks(chunk_size) {
            hasher.update(chunk);
        }
        let protocol = protocol_identity();
        let digest = hasher.finalize().to_hex();
        proptest::prop_assert_eq!(protocol.version, EVENT_WIRE_VERSION);
        proptest::prop_assert_eq!(protocol.descriptor_digest, digest.as_str());
    }
}

#[test]
fn protocol_identity_cost() {
    for observing in [false, true] {
        let dispatch = if observing {
            tracing::Dispatch::new(tracing_subscriber::registry())
        } else {
            tracing::Dispatch::none()
        };
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let mut samples = Vec::with_capacity(64);
        let mut expected = None;
        for _ in 0..64 {
            let started = std::time::Instant::now();
            let allocation = acyclic_test_support::measure(|| {
                for _ in 0..128 {
                    std::hint::black_box(protocol_identity());
                }
            });
            samples.push(started.elapsed() / 128);
            let counts = (
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max,
            );
            assert_eq!(counts, (256, 128 * 65, 65));
            assert_eq!(*expected.get_or_insert(counts), counts);
        }
        samples.sort_unstable();
        eprintln!(
            "protocol tracing={observing}: allocation={expected:?}; median={:?}; descriptor_bytes={}",
            samples.get(samples.len() / 2),
            crate::FILE_DESCRIPTOR_SET.len()
        );
    }
}

#[cfg(feature = "filesystem")]
#[test]
fn event_identity_without_observer() -> Result<()> {
    event_identity_cost(false, false)
}

#[cfg(feature = "filesystem")]
#[test]
fn event_identity_with_tracing() -> Result<()> {
    event_identity_cost(true, false)
}

#[cfg(feature = "filesystem")]
#[test]
fn event_identity_with_accounting() -> Result<()> {
    event_identity_cost(false, true)
}

#[cfg(feature = "filesystem")]
fn event_identity_cost(observing: bool, accounted: bool) -> Result<()> {
    use crate::store::HistoryWork;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    let ((authority, event), _) = decode_event(&tests::fixture(), &mut None)?;
    let reference = encode_event_envelope(&authority, &event, &mut None)?;
    let dispatch = if observing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mut samples = Vec::with_capacity(64);
    let mut counts = Vec::with_capacity(64);
    let mut charges = None;
    for _ in 0..64 {
        let token = CancellationToken::new();
        let native = NativeMaterializationWork::new(
            WorkBudget {
                bytes_copied: 16_777_216,
                bytes_encoded: 16_777_216,
                bytes_hashed: 16_777_216,
                items_examined: 1_000_000,
                allocation_operations: 10_000,
                peak_allocation_bytes: 16_777_216,
                ..WorkBudget::default()
            },
            &token,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let mut work: Option<&mut dyn HistoryIo> = accounted.then_some(&mut io);
        let mut result = None;
        let started = std::time::Instant::now();
        let allocation = acyclic_test_support::measure(|| {
            result = Some(encode_event_envelope(&authority, &event, &mut work));
        });
        samples.push(started.elapsed());
        counts.push((
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_max,
        ));
        let envelope = result.ok_or_else(|| Error::Invalid("encoder did not run".into()))??;
        assert_eq!(envelope, reference);
        if accounted {
            let actual = io.receipt();
            assert_eq!(actual.bytes_hashed, crate::FILE_DESCRIPTOR_SET.len() as u64);
            assert_eq!(*charges.get_or_insert(actual), actual);
        }
        drop(envelope);
        drop(io.into_custody()?);
    }
    samples.sort_unstable();
    eprintln!(
        "event tracing={observing} accounting={accounted}: min={:?}; max={:?}; median={:?}; charges={charges:?}",
        counts.iter().min(),
        counts.iter().max(),
        samples.get(samples.len() / 2)
    );
    Ok(())
}
