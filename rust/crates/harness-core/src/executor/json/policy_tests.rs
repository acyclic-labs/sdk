use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn selected_byte_policies_apply_with_or_without_an_observer(
        text in "[a-zé🦀\\x00]{0,128}",
    ) {
        let encoded = crate::contract::canonical_json_bytes(&text).unwrap();
        let maximum = encoded.len() as u64;
        let expected = *blake3::hash(&encoded).as_bytes();
        for policy in [None, Some(maximum), Some(maximum - 1)] {
            let admitted = policy.is_none_or(|maximum| maximum >= encoded.len() as u64);
            for digest in [
                canonical_json_digest(&text, policy, &mut None),
                borrowed_json_digest(&text, policy, &mut None),
            ] {
                match digest {
                    Ok(digest) => {
                        prop_assert!(admitted);
                        prop_assert_eq!(digest, expected);
                    }
                    Err(_) => prop_assert!(!admitted),
                }
            }
            for maximum_depth in [0, u32::MAX] {
                let counted = json_byte_length_at_depth(&text, policy, maximum_depth, &mut None);
                match counted {
                    Ok(bytes) => {
                        prop_assert!(admitted);
                        prop_assert_eq!(bytes, maximum);
                    }
                    Err(_) => prop_assert!(!admitted),
                }
            }
            let bytes = crate::store::canonical_bytes_with_work(&text, policy, &mut None);
            match bytes {
                Ok(bytes) => {
                    prop_assert!(admitted);
                    prop_assert_eq!(&bytes, &encoded);
                }
                Err(_) => prop_assert!(!admitted),
            }
        }
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn unlimited_identity_and_byte_count_use_the_original_native_ledger() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkError};

    let text = "x".repeat(40 * 1024 * 1024);
    let length = text.len() as u64 + 2;
    let mut expected = blake3::Hasher::new();
    expected.update(b"\"");
    expected.update(text.as_bytes());
    expected.update(b"\"");

    for count_only in [false, true] {
        for admitted in [true, false] {
            let mut budget = acyclic_fs::performance::work_budget(64, 64 * 1024 * 1024);
            budget.bytes_copied = 0;
            budget.bytes_encoded = length - u64::from(!admitted);
            let original = NativeMaterializationWork::new(budget, &CancellationToken::new())
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = original.live_allocation_bytes();
            let mut history = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&original)?,
            );
            let result = if count_only {
                json_byte_length(&text, None, &mut Some(&mut history)).map(|count| {
                    assert_eq!(count, length);
                    None
                })
            } else {
                borrowed_json_digest(&text, None, &mut Some(&mut history)).map(Some)
            };
            if admitted {
                assert_eq!(
                    result?,
                    (!count_only).then(|| *expected.finalize().as_bytes())
                );
                assert_eq!(original.receipt().bytes_encoded, length);
                assert_eq!(
                    original.receipt().bytes_hashed,
                    if count_only { 0 } else { length }
                );
            } else {
                assert!(matches!(
                    result,
                    Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            WorkError::BudgetExceeded {
                                counter: "bytes_encoded",
                                ..
                            }
                        ),
                        ..
                    })
                ));
                assert!(original.receipt().bytes_hashed < length);
            }
            assert_eq!(original.receipt().bytes_copied, 0);
            drop(history);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}
