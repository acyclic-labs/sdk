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
        proptest::prop_assert_eq!(protocol.version, PROTOCOL_VERSION);
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
