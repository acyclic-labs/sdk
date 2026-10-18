//! Exact aggregate item boundaries on the canonical serializer's native path.
thread_local! {
    pub(super) static RESTORE_BYTE_CHARGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use crate::executor::json::canonical_json_digest;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters, WorkError};
    use proptest::prelude::*;

    const LIMIT: u64 = 65_536;
    // One array and one string, examined by serialization and index construction.
    const ITEMS: u64 = 2 * 2;

    fn check(prior: u64, restore: bool) -> std::result::Result<(), String> {
        let old = RESTORE_BYTE_CHARGE.replace(restore);
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                RESTORE_BYTE_CHARGE.set(self.0);
            }
        }
        let _reset = Reset(old);
        let native = NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(LIMIT, 4 * 1024 * 1024),
            &CancellationToken::new(),
        )
        .unwrap();
        native
            .charge(WorkCounters {
                items_examined: prior,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
        let baseline = native.live_allocation_bytes();
        let value = serde_json::json!(["x".repeat(4096)]);
        let expected = serde_json::to_vec(&value).unwrap();
        let mut history = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native).unwrap(),
        );
        let result = canonical_json_digest(&value, LIMIT, &mut Some(&mut history));
        if prior + ITEMS <= LIMIT {
            let digest = result
                .map_err(|error| format!("admitted total {} rejected: {error:?}", prior + ITEMS))?;
            if digest != *blake3::hash(&expected).as_bytes()
                || native.receipt().items_examined != prior + ITEMS
                || native.receipt().bytes_hashed != expected.len() as u64
            {
                return Err("canonical bytes or exact native work differ".into());
            }
        } else {
            match result {
                Err(crate::Error::NativeWork {
                    cause:
                        acyclic_fs::NativeTreeMaterializationError::Work(WorkError::BudgetExceeded {
                            counter: "items_examined",
                            observed,
                            maximum: LIMIT,
                        }),
                    ..
                }) if observed > LIMIT && observed <= prior + ITEMS => {}
                other => {
                    return Err(format!(
                        "exhausted item budget did not retain its cause: {other:?}"
                    ));
                }
            }
            // Each admission is atomic: no rejected work is charged.
            if native.receipt().items_examined > LIMIT {
                return Err("denied work changed the original ledger".into());
            }
            if prior + ITEMS == LIMIT + 1 && native.receipt().items_examined != prior + 2 {
                return Err("denied index scan changed the admitted serialization prefix".into());
            }
        }
        drop(history);
        if native.live_allocation_bytes() != baseline {
            return Err("canonical digest leaked native custody".into());
        }
        Ok(())
    }

    #[test]
    fn canonical_array_item_budget_accepts_65536_and_rejects_65537() {
        check(LIMIT - ITEMS, false).unwrap();
        check(LIMIT + 1 - ITEMS, false).unwrap();
    }

    #[test]
    fn restored_byte_charge_fails_the_same_native_item_boundary_oracle() {
        // A roomy budget reaches the exact-work equality; the tight budget
        // independently proves that byte overcharging rejects an admitted call.
        let error = check(0, true).unwrap_err();
        assert_eq!(error, "canonical bytes or exact native work differ");
        let error = check(LIMIT - ITEMS, true).unwrap_err();
        assert!(error.contains("admitted total 65536 rejected"), "{error}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn every_native_item_prefix_preserves_exact_work_and_denial(prior in 0u64..=LIMIT) {
            check(prior, false).map_err(TestCaseError::fail)?;
        }
    }
}
