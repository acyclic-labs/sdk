use super::*;
use proptest::prelude::*;

fn bounds() -> JsonBounds {
    JsonBounds {
        maximum_bytes: u64::MAX,
        maximum_items: u64::MAX,
        maximum_depth: 256,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn every_prefix_matches_the_complete_canonical_encoder(
        messages in prop::collection::vec(".{0,64}", 0..24),
    ) {
        let messages: Vec<_> = messages.iter().enumerate()
            .map(|(index, text)| serde_json::json!({"z": [{"index":index, "text":text}], "a": [null, true, 1.5]}))
            .collect();
        for count in 0..=messages.len()+1 {
            let (full, prefix) = canonical_sequence_digests(&messages, Some(count), bounds(), None, &mut None).unwrap();
            prop_assert_eq!(full, identity::canonical_json_digest(&messages, u64::MAX, &mut None).unwrap());
            let expected = messages.get(..count).map(|prefix|
                identity::canonical_json_digest(prefix, u64::MAX, &mut None).unwrap());
            prop_assert_eq!(prefix, expected);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn hashing_scratch_stays_flat_from_one_to_a_thousand_messages() -> Result<()> {
    let item = serde_json::json!({"z": [0, 1, "escaped \"\\\né".repeat(32)], "a": {"x": true}});
    let mut maximum = 0;
    for count in [1, 10, 100, 1000] {
        let values = vec![item.clone(); count];
        let mut result = Ok(([0; 32], None));
        let start = std::time::Instant::now();
        let fixed = acyclic_test_support::measure(|| {
            result =
                canonical_sequence_digests(&values, Some(count / 2), bounds(), None, &mut None);
        });
        let (full, prefix) = result?;
        let elapsed = start.elapsed();
        let mut old = Ok([0; 32]);
        let start = std::time::Instant::now();
        let growing = acyclic_test_support::measure(|| {
            old = identity::canonical_json_digest(&values, u64::MAX, &mut None);
        });
        let old_elapsed = start.elapsed();
        assert_eq!(full, old?);
        assert_eq!(
            prefix,
            Some(identity::canonical_json_digest(
                &values[..count / 2],
                u64::MAX,
                &mut None
            )?)
        );
        if count == 1 {
            maximum = fixed.bytes_max;
        }
        assert_eq!(fixed.bytes_max, maximum, "count={count}");
        assert_eq!(fixed.bytes_current, 0);
        assert_eq!(fixed.count_current, 0);
        if count > 1 {
            assert!(
                growing.bytes_max > maximum,
                "old whole-array conversion must fail the same bound"
            );
        }
        eprintln!(
            "sequence_digest count={count} scratch={} old_scratch={} time_us={} old_time_us={}",
            fixed.bytes_max,
            growing.bytes_max,
            elapsed.as_micros(),
            old_elapsed.as_micros()
        );
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn original_ledgers_enforce_hash_cuts_and_release_sequence_scratch() -> Result<()> {
    let values = vec![serde_json::json!({"z": [1, true], "a": "\0é\\"}); 4];
    let full = crate::contract::canonical_json_bytes(&values)?;
    let prefix = crate::contract::canonical_json_bytes(&values[..2])?;
    for native in [false, true] {
        for hash in [0, 1, full.len() as u64, full.len() as u64 + 1] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
            budget.bytes_hashed = hash;
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                crate::executor::PreparationIo::from_native_work(&work)?
            } else {
                crate::executor::PreparationIo::from_budget(budget, &cancellation)?
            };
            let baseline = io.owned_bytes()?;
            let result = io.canonical_sequence_digests(&values, Some(2));
            if hash < full.len() as u64 + 1 {
                assert!(result.is_err(), "native={native} hash={hash}");
                drop(result);
            } else {
                assert_eq!(
                    result?,
                    (
                        *blake3::hash(&full).as_bytes(),
                        Some(*blake3::hash(&prefix).as_bytes())
                    )
                );
                assert_eq!(io.receipt().bytes_hashed, hash);
            }
            // A native failure may carry its diagnostic custody until dropped.
            assert_eq!(io.owned_bytes()?, baseline, "native={native} hash={hash}");
        }
    }
    Ok(())
}

#[test]
fn shared_prefix_is_serialized_once_per_digest_pair() -> Result<()> {
    struct Counted<'a>(&'a std::cell::Cell<usize>);
    impl Serialize for Counted<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.0.set(self.0.get() + 1);
            serde_json::json!({"z": 1, "a": [true]}).serialize(serializer)
        }
    }
    let count = std::cell::Cell::new(0);
    let values: Vec<_> = (0..1000).map(|_| Counted(&count)).collect();
    canonical_sequence_digests(&values, Some(999), bounds(), None, &mut None)?;
    assert_eq!(count.get(), 1000);
    count.set(0);
    identity::canonical_json_digest(&values[..999], u64::MAX, &mut None)?;
    identity::canonical_json_digest(&values, u64::MAX, &mut None)?;
    assert_eq!(
        count.get(),
        1999,
        "the previous path must fail the one-pass bound"
    );
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn original_ledger_peak_is_independent_of_message_count() -> Result<()> {
    let value = serde_json::json!({"z": [1, true], "a": "\0é\\"});
    for native in [false, true] {
        let mut maximum = None;
        for count in [1, 10, 100, 1000] {
            let values = vec![value.clone(); count];
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(8 * 1024 * 1024, 8 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = if native {
                crate::executor::PreparationIo::from_native_work(&work)?
            } else {
                crate::executor::PreparationIo::from_budget(budget, &cancellation)?
            };
            let baseline = io.owned_bytes()?;
            let (full, prefix) = io.canonical_sequence_digests(&values, Some(count / 2))?;
            assert_eq!(
                full,
                identity::canonical_json_digest(&values, u64::MAX, &mut None)?
            );
            assert_eq!(
                prefix,
                Some(identity::canonical_json_digest(
                    &values[..count / 2],
                    u64::MAX,
                    &mut None
                )?)
            );
            let peak = io.receipt().peak_allocation_bytes;
            let first = *maximum.get_or_insert(peak);
            assert_eq!(peak, first, "native={native} count={count}");
            assert_eq!(io.owned_bytes()?, baseline);
            eprintln!("sequence_digest_ledger native={native} count={count} peak={peak}");
        }
    }
    Ok(())
}

#[test]
fn large_unicode_messages_cross_every_hash_buffer_boundary() -> Result<()> {
    for bytes in [4095, 4096, 4097, 8191, 8192, 8193, 65536] {
        let values = [
            serde_json::json!({"z": "\\\"é🦀".repeat(bytes), "a": [null, false]}),
            serde_json::json!({"z": [1, "tail"], "a": {"nested": true}}),
        ];
        for prefix in 0..=values.len() {
            let (full, cut) =
                canonical_sequence_digests(&values, Some(prefix), bounds(), None, &mut None)?;
            assert_eq!(
                full,
                identity::canonical_json_digest(&values, u64::MAX, &mut None)?,
                "bytes={bytes}"
            );
            assert_eq!(
                cut,
                Some(identity::canonical_json_digest(
                    &values[..prefix],
                    u64::MAX,
                    &mut None
                )?),
                "bytes={bytes} prefix={prefix}"
            );
        }
    }
    Ok(())
}
