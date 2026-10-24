use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use std::cell::Cell;

fn dependency(index: u8) -> Dependency {
    Dependency {
        region: DependencyRegion::DirectoryName {
            directory_id: FileId::from_bytes([1; 16]),
            name: LogicalName::new(
                super::super::super::NameEncoding::Utf8,
                vec![index + 1; 8],
                64,
            )
            .expect("fixed nonempty name"),
        },
        expected: DependencyState::Absent,
    }
}

fn initial(count: u8) -> CheckoutDependencies {
    CheckoutDependencies::new((0..count).map(dependency), [], 128).expect("bounded proof")
}

fn semantics(
    proof: &CheckoutDependencies,
) -> Vec<(DependencyRegion, DependencyState, DependencyUse)> {
    proof
        .captured
        .entries
        .iter()
        .map(|(region, state)| (region.clone(), state.expected, state.usage))
        .collect()
}

// Independent old semantics: copy the complete prior map, then merge the input.
// This reference never calls the production prepare, commit, or custody code.
fn reference(count: u8, input: &[u8]) -> Vec<(DependencyRegion, DependencyState, DependencyUse)> {
    let before: BTreeMap<_, _> = (0..count)
        .map(|index| {
            let value = dependency(index);
            (value.region, (value.expected, DependencyUse::Observation))
        })
        .collect();
    let mut after = before.clone();
    for index in input {
        let value = dependency(*index);
        after
            .entry(value.region)
            .and_modify(|state| {
                state.1 = state.1.with(DependencyUse::Mutation);
            })
            .or_insert((value.expected, DependencyUse::Mutation));
    }
    after
        .into_iter()
        .map(|(region, (expected, usage))| (region, expected, usage))
        .collect()
}

fn run_cut(count: u8, input: &[u8], shared: bool, weak: bool, cut: usize) -> usize {
    let mut proof = initial(count);
    let before = semantics(&proof);
    let address = Arc::as_ptr(&proof.captured);
    let names = (
        proof.name_data_bytes,
        proof.cloned_name_data_bytes,
        proof.data_generation,
    );
    let held = shared.then(|| proof.clone());
    let weak = weak.then(|| Arc::downgrade(&proof.captured));
    let owner = Arc::new(ProofDataCustody::default());
    let calls = Cell::new(0);
    let step = || -> Result<(), crate::FsError> {
        calls.set(calls.get() + 1);
        if calls.get() == cut {
            Err(WorkError::Overflow.into())
        } else {
            Ok(())
        }
    };
    let extension = proof
        .prepare_mutations(input.iter().copied().map(dependency).collect(), 128)
        .expect("consistent bounded input");
    let result = proof.commit_with_data_claims::<true, _, _>(
        extension,
        |_, _, _, _| step(),
        |bytes| {
            step()?;
            Ok(ProofDataClaim::pending_control(&owner, bytes))
        },
        |bytes, _| {
            step()?;
            Ok(ProofDataClaim::new(Some(&owner), bytes))
        },
        step,
    );
    if result.is_err() {
        assert_eq!(semantics(&proof), before);
        assert_eq!(Arc::as_ptr(&proof.captured), address);
        assert_eq!(
            (
                proof.name_data_bytes,
                proof.cloned_name_data_bytes,
                proof.data_generation
            ),
            names
        );
        assert_eq!(
            owner.retained_payload_bytes(),
            0,
            "rejected claims retire together"
        );
    } else {
        assert_eq!(semantics(&proof), reference(count, input));
    }
    if let Some(held) = held {
        assert_eq!(semantics(&held), before);
    }
    drop(weak);
    drop(proof);
    assert_eq!(owner.retained_payload_bytes(), 0);
    calls.get()
}

#[test]
fn every_admission_custody_and_finish_cut_is_atomic() {
    for shared in [false, true] {
        for weak in [false, true] {
            let input = [0, 7, 8, 8, 9];
            let calls = run_cut(8, &input, shared, weak, 0);
            for cut in 1..=calls {
                run_cut(8, &input, shared, weak, cut);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn generated_delta_matches_independent_map_or_retires_every_claim(
        count in 1u8..25,
        input in prop::collection::vec(0u8..32, 0..12),
        shared in any::<bool>(), weak in any::<bool>(), cut in 0usize..100,
    ) {
        run_cut(count, &input, shared, weak, cut);
    }
}

fn measured_upgrade(count: u8, old_clone: bool) -> (AllocationInfo, WorkCounters) {
    let mut proof = initial(count);
    let extension = proof
        .prepare_mutations(vec![dependency(count - 1)], 128)
        .expect("existing key");
    let mut work = WorkCounters::UNCHARGED;
    let allocation = measure(|| {
        // Restore the facade's former unconditional snapshot. It forces the
        // production COW path, including actual map nodes and name buffers.
        let held = old_clone.then(|| proof.clone());
        proof
            .commit_with_data_custody::<crate::FsError>(
                extension,
                None,
                |_, operations, copied, items| {
                    work = work.checked_add(WorkCounters {
                        allocation_operations: operations,
                        bytes_copied: copied,
                        items_examined: items,
                        ..WorkCounters::UNCHARGED
                    })?;
                    Ok(())
                },
            )
            .expect("unbounded admission");
        drop(held);
    });
    assert_eq!(semantics(&proof), reference(count, &[count - 1]));
    (allocation, work)
}

fn staged_delta_retirement(count: u8) -> AllocationInfo {
    // Independently prepare the same one-key input. Commit consumes this delta,
    // so its already allocated map node and name must retire inside the scope.
    let proof = initial(count);
    let extension = proof
        .prepare_mutations(vec![dependency(count - 1)], 128)
        .expect("existing key");
    measure(|| drop(extension))
}

fn assert_upgrade_pin(count: u8, value: (AllocationInfo, WorkCounters)) {
    assert_eq!(
        value.0,
        staged_delta_retirement(count),
        "only the independently prepared input delta retires; existing keys are never copied"
    );
    assert_eq!(
        value.1,
        WorkCounters {
            items_examined: 2,
            ..WorkCounters::UNCHARGED
        }
    );
}

#[test]
fn existing_dependency_upgrade_has_exact_constant_work_and_no_allocations() {
    let mut first = None;
    for count in [8, 24] {
        let current = measured_upgrade(count, false);
        assert_upgrade_pin(count, current);
        if let Some(first) = first {
            assert_eq!(current, first, "retained history cannot affect delta work");
        }
        first = Some(current);
        let old = measured_upgrade(count, true);
        assert!(
            std::panic::catch_unwind(|| assert_upgrade_pin(count, old)).is_err(),
            "the old snapshot must fail the same pin"
        );
        eprintln!("DEPENDENCY_DELTA count={count} current={current:?} old={old:?}");
    }
}

#[test]
fn install_before_fallible_finish_is_detected() {
    let mut proof = initial(8);
    let before = semantics(&proof);
    let extension = proof
        .prepare_mutations(vec![dependency(8)], 128)
        .expect("new key");
    // Mutate the ordering by using a successful finish, then reject afterward.
    proof
        .commit_with_data_custody::<crate::FsError>(extension, None, |_, _, _, _| Ok(()))
        .expect("unbounded admission");
    let rejected: Result<(), WorkError> = Err(WorkError::Overflow);
    assert!(rejected.is_err());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| assert_eq!(
            semantics(&proof),
            before
        )))
        .is_err(),
        "installing before the last fallible step must fail the atomicity assertion"
    );
}

#[cfg(not(target_arch = "wasm32"))]
fn original_owner(
    cancel: &CancellationToken,
) -> (crate::NativeMaterializationWork, Arc<ProofDataCustody>) {
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, cancel)
        .expect("original work");
    let mut header = original
        .claim_root_metadata(std::mem::size_of::<ProofDataCustody>() as u64)
        .expect("header");
    let owner = Arc::new(ProofDataCustody::default());
    original
        .register_native_checkout_control(&owner, &mut header)
        .expect("registered producer");
    (original, owner)
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_native_upgrade_has_no_copy_or_allocation_and_rejects_snapshot_control() {
    let mut first = None;
    for count in [8, 24] {
        for old_clone in [false, true] {
            let cancel = CancellationToken::new();
            let (original, owner) = original_owner(&cancel);
            let mut proof = initial(count);
            let extension = proof
                .prepare_mutations(vec![dependency(count - 1)], 128)
                .expect("existing key");
            let mut staged = original
                .claim(0, 0, WorkCounters::UNCHARGED)
                .expect("no new names");
            let before = original.receipt();
            let allocation = measure(|| {
                let held = old_clone.then(|| proof.clone());
                proof
                    .commit_with_original_data_custody_then(
                        extension,
                        &owner,
                        &mut staged,
                        &original,
                        &cancel,
                        |_| Ok(()),
                    )
                    .expect("original admission");
                drop(held);
            });
            let expected = before
                .checked_add(WorkCounters {
                    items_examined: 2,
                    ..WorkCounters::UNCHARGED
                })
                .expect("bounded work");
            if old_clone {
                assert_ne!(allocation, staged_delta_retirement(count));
                assert_ne!(original.receipt(), expected);
            } else {
                assert_eq!(allocation, staged_delta_retirement(count));
                assert_eq!(original.receipt(), expected);
                if let Some(first) = first {
                    assert_eq!(
                        allocation, first,
                        "retained history cannot affect delta work"
                    );
                }
                first = Some(allocation);
            }
            assert_eq!(semantics(&proof), reference(count, &[count - 1]));
            eprintln!(
                "NATIVE_DEPENDENCY_DELTA count={count} old={old_clone} allocation={allocation:?}"
            );
        }
    }
}

#[test]
fn stale_extensions_revalidate_current_count_and_expected_state_before_finish() {
    for contradictory in [false, true] {
        let mut proof = initial(0);
        let first = dependency(0);
        let stale = proof
            .prepare_mutations(vec![first.clone()], 1)
            .expect("initial admission");
        let mut observed = if contradictory { first } else { dependency(1) };
        if contradictory {
            observed.expected = DependencyState::Present(Digest::from_bytes([3; 32]));
        }
        let observation = proof
            .prepare_observations(vec![observed], 1)
            .expect("observation");
        proof
            .commit_with_data_custody::<crate::FsError>(observation, None, |_, _, _, _| Ok(()))
            .expect("first region fits");
        let before = semantics(&proof);
        let owner = Arc::new(ProofDataCustody::default());
        let mut finished = false;
        let result = proof.commit_with_data_custody_then::<crate::FsError, _>(
            stale,
            Some(&owner),
            |_, _, _, _| Ok(()),
            || {
                finished = true;
                Ok(())
            },
        );
        assert!(result.is_err());
        assert!(
            !finished,
            "rejected stale admission cannot transfer the root"
        );
        assert_eq!(semantics(&proof), before);
    }
}

#[test]
fn omitted_revalidation_control_breaks_the_same_dependency_bound() {
    let mut proof = initial(0);
    let stale = proof
        .prepare_mutations(vec![dependency(0)], 1)
        .expect("initial admission");
    let observation = proof
        .prepare_observations(vec![dependency(1)], 1)
        .expect("observation");
    proof
        .commit_with_data_custody::<crate::FsError>(observation, None, |_, _, _, _| Ok(()))
        .expect("first region fits");
    proof
        .commit_with_data_claims::<false, crate::FsError, _>(
            stale,
            |_, _, _, _| Ok(()),
            |_| Ok(ProofDataClaim::default()),
            |_, _| Ok(ProofDataClaim::default()),
            || Ok(()),
        )
        .expect("omitted validation wrongly accepts the stale extension");
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| assert!(proof.len() <= 1)))
            .is_err()
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_stale_admission_retires_new_names_before_any_copy_or_root_transfer() {
    let cancel = CancellationToken::new();
    let (original, owner) = original_owner(&cancel);
    let mut proof = initial(0);
    let stale = proof
        .prepare_mutations(vec![dependency(0)], 1)
        .expect("initial admission");
    let observation = proof
        .prepare_observations(vec![dependency(1)], 1)
        .expect("observation");
    proof
        .commit_with_data_custody::<crate::FsError>(observation, None, |_, _, _, _| Ok(()))
        .expect("first region fits");
    let held = proof.clone();
    let mut staged = original
        .claim(8, 1, WorkCounters::UNCHARGED)
        .expect("incoming name");
    let before = original.receipt();
    let mut finished = false;
    assert!(
        proof
            .commit_with_original_data_custody_then(
                stale,
                &owner,
                &mut staged,
                &original,
                &cancel,
                |_| {
                    finished = true;
                    Ok(())
                },
            )
            .is_err()
    );
    assert!(!finished);
    assert_eq!(proof, held);
    assert!(Arc::ptr_eq(&proof.captured, &held.captured));
    assert_eq!(owner.retained_payload_bytes(), 0);
    assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
    assert_eq!(
        original.receipt().allocation_operations,
        before.allocation_operations
    );
}
