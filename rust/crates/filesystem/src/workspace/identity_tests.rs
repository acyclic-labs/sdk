use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;

// Frozen check/effect/completion sequence, initialized on a separate ledger.
fn old(
    namespace: [u8; 16],
    name: &WorkspaceName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<WorkspaceId, WorkspaceError> {
    accounting.check()?;
    let performed = crate::WorkCounters {
        bytes_hashed: (b"acyclic-fs-workspace-id-v1\0".len() + 16 + 8 + name.as_str().len()) as u64,
        bytes_copied: 16,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(performed)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-workspace-id-v1\0");
    hasher.update(&namespace);
    hasher.update(&(name.as_str().len() as u64).to_le_bytes());
    hasher.update(name.as_str().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    accounting.record_completed(performed)?;
    Ok(WorkspaceId::from_bytes(bytes))
}

fn outcome(result: Result<WorkspaceId, WorkspaceError>) -> Result<[u8; 16], String> {
    result
        .map(WorkspaceId::into_bytes)
        .map_err(|error| format!("{error:?}"))
}

fn measured<T>(operation: impl FnOnce() -> T) -> (T, AllocationInfo) {
    let mut output = None;
    let heap = measure(|| output = Some(operation()));
    (output.unwrap(), heap)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Full bytes/errors/receipts match the old single-thread behavior under
    // arbitrary limits, prior work, cancellation, poison and nested policies.
    #[test]
    fn identity_effect_reservations_preserve_output_errors_receipts_and_zero_heap(
        namespace in any::<[u8; 16]>(),
        raw in prop::collection::vec(any::<char>(), 1..17),
        copy_limit in 0_u64..49, hash_limit in 0_u64..513,
        prior_copy in 0_u64..33, prior_hash in 0_u64..129,
        mode in 0_u8..5, recheck in any::<bool>(), failure in 0_u8..6,
    ) {
        let name = WorkspaceName::new(raw.into_iter().collect::<String>(),
            &crate::WorkBudget::UNBOUNDED, &CancellationToken::new());
        prop_assume!(name.is_ok());
        let name = name.unwrap().value;
        // Independent preimage construction and one-shot hash oracle.
        let mut input = b"acyclic-fs-workspace-id-v1\0".to_vec();
        input.extend_from_slice(&namespace);
        input.extend_from_slice(&(name.as_str().len() as u64).to_le_bytes());
        input.extend_from_slice(name.as_str().as_bytes());
        let expected: [u8; 16] = blake3::hash(&input).as_bytes()[..16].try_into().unwrap();
        let limit = crate::WorkBudget { bytes_copied: copy_limit, bytes_hashed: hash_limit, ..crate::WorkBudget::UNBOUNDED };
        let roots_cancel = [CancellationToken::new(), CancellationToken::new()];
        let callers = [CancellationToken::new(), CancellationToken::new()];
        let roots = [0, 1].map(|i| crate::NativeMaterializationWork::new(
            if mode <= 1 { limit } else { crate::WorkBudget::UNBOUNDED }, &roots_cancel[i],
        ).unwrap());
        let work = roots.each_ref().map(|root| {
            let mut work = root.clone();
            for _ in 0..mode.saturating_sub(1).min(2) { work = work.native_volume_work(limit).unwrap(); }
            if mode == 4 { work = work.invocation_view().unwrap().0; }
            if recheck { work = work.recheck_admitted(); }
            work
        });
        let mut accounting = [0, 1].map(|i| if mode == 0 {
            KernelAccounting::standalone(limit, &callers[i])
        } else { KernelAccounting::native(&work[i], &callers[i]) });
        let prior = crate::WorkCounters { bytes_copied: prior_copy, bytes_hashed: prior_hash, ..crate::WorkCounters::UNCHARGED };
        for i in [0, 1] {
            let _ = accounting[i].record_completed(prior);
            match failure {
                1 => callers[i].cancel(),
                2 if mode != 0 => roots_cancel[i].cancel(),
                3 if mode != 0 => work[i].state(|state| state.poison_native_original_work()),
                4 if mode != 0 => { roots_cancel[i].cancel(); callers[i].cancel(); },
                5 if mode != 0 => { work[i].state(|state| state.poison_native_original_work()); callers[i].cancel(); },
                _ => (),
            }
        }
        let (before, old_heap) = measured(|| old(namespace, &name, &mut accounting[0]));
        let (after, new_heap) = measured(|| WorkspaceId::with_accounting(namespace, &name, &mut accounting[1]));
        prop_assert_eq!(old_heap, AllocationInfo::default());
        prop_assert_eq!(new_heap, AllocationInfo::default());
        let before = outcome(before);
        let after = outcome(after);
        if let Ok(output) = &after { prop_assert_eq!(*output, expected); }
        prop_assert_eq!(after, before);
        prop_assert_eq!(accounting[1].spent(), accounting[0].spent());
        prop_assert_eq!(roots[1].live_allocation_bytes(), roots[0].live_allocation_bytes());
        let credit = work[1].state(|state| (state.budget.bytes_copied, state.budget.bytes_hashed, state.original_budget()));
        prop_assert_eq!(credit.0, credit.2.bytes_copied);
        prop_assert_eq!(credit.1, credit.2.bytes_hashed);
    }
}

#[test]
fn exact_identity_copy_and_hash_allowances_succeed_one_byte_less_has_no_effect() {
    let cancellation = CancellationToken::new();
    let name = WorkspaceName::new("repo", &crate::WorkBudget::UNBOUNDED, &cancellation)
        .unwrap()
        .value;
    let hashed = (b"acyclic-fs-workspace-id-v1\0".len() + 16 + 8 + 4) as u64;
    for (copies, hashes, denied) in [
        (16, hashed, false),
        (15, hashed, true),
        (16, hashed - 1, true),
    ] {
        for depth in 0..3 {
            let limit = crate::WorkBudget {
                bytes_copied: copies,
                bytes_hashed: hashes,
                ..crate::WorkBudget::UNBOUNDED
            };
            let root = crate::NativeMaterializationWork::new(limit, &cancellation).unwrap();
            let mut work = root.clone();
            for _ in 0..depth {
                work = work.native_volume_work(limit).unwrap();
            }
            let before = root.receipt();
            let result = WorkspaceId::derive([1; 16], &name, &&work, &cancellation);
            assert_eq!(result.is_err(), denied);
            let expected = before
                .checked_add(crate::WorkCounters {
                    bytes_copied: if denied { 0 } else { 16 },
                    bytes_hashed: if denied { 0 } else { hashed },
                    ..crate::WorkCounters::UNCHARGED
                })
                .unwrap();
            assert_eq!(root.receipt(), expected);
        }
    }
}
