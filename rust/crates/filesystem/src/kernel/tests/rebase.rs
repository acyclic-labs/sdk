use super::*;
use crate::async_storage::poll_ready;
use crate::kernel::NameEncoding;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::convert::Infallible;

struct Probe {
    calls: Cell<u64>,
    states: BTreeMap<(GenerationId, DependencyRegion), DependencyState>,
}

impl RebaseProbe for Probe {
    type Error = Infallible;

    fn probe(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
    ) -> Result<ProbeReceipt, OperationFailure<Self::Error>> {
        self.calls.set(self.calls.get() + 1);
        let work = WorkCounters {
            page_reads: 1,
            ..WorkCounters::default()
        };
        assert!(work.verify(budget).is_ok());
        Ok(ProbeReceipt {
            value: self
                .states
                .get(&(generation, region.clone()))
                .copied()
                .unwrap_or(DependencyState::Absent),
            work,
        })
    }
}

impl AsyncRebaseProbe for Probe {
    type Error = Infallible;

    async fn probe_async(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<ProbeReceipt, OperationFailure<Self::Error>> {
        let _ = cancellation;
        RebaseProbe::probe(self, generation, region, budget)
    }
}

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn name(value: &[u8]) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(NameEncoding::Utf8, value.to_vec(), 255)?)
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_data_is_already_original_standing_but_retires_with_its_actual_name_buffer()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = crate::Fs::memory();
    let workspace = fs.create_workspace("native-name-buffer-custody").await?;
    let checkout = workspace.checkout_measured(
        crate::model::GenerationSelector::Head, crate::model::CheckoutMode::read_only_pinned(),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let original_work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let candidate = checkout.private_candidate_with_native_work(&original_work, &cancellation)?;
    let owner = candidate.private_control_custody();
    drop(candidate);
    let original = owner.completed_data_work.lock().native_original.as_ref()
        .and_then(Weak::upgrade).expect("real native constructor registered its producer");
    let initial = owner.native_control_events();
    let witness = original_work.checkout_control_sink(&owner)?;
    let mut allocation = original_work.claim_allocation(3, 1)?;
    let mut name = Vec::new();
    name.try_reserve_exact(3)?;
    allocation.reconcile_capacity(name.capacity() as u64)?;
    original_work.charge(WorkCounters { bytes_copied: 3, ..WorkCounters::UNCHARGED })?;
    name.extend_from_slice(b"abc");
    let bytes = name.capacity() as u64;
    cancellation.cancel();
    let name = (name, allocation.transfer_checkout_data_bytes(allocation.bytes(), &owner, &witness)?);
    drop(allocation);
    drop(witness);
    let original_weak = Arc::downgrade(&original);
    assert_eq!(owner.retained_bytes(), bytes);
    assert_eq!(owner.physical_retained_bytes(), 0, "native name bytes stay DATA");
    assert_eq!(owner.live_payload_counts(), (bytes, bytes));
    assert_eq!(owner.non_native_retained_payload_bytes(), 0,
        "the original observer cannot re-adopt its already admitted native DATA");
    assert_eq!(owner.native_control_events(), (initial.0 + u128::from(bytes), initial.1));
    drop(name.1.clone());
    drop(original_work);
    drop(original);
    assert!(original_weak.upgrade().is_some());
    drop(name);
    assert_eq!(owner.native_control_events(),
        (initial.0 + u128::from(bytes), initial.1 + u128::from(bytes)));
    assert_eq!(owner.live_payload_counts(), (0, 0));
    assert_eq!(owner.non_native_retained_payload_bytes(), 0);
    assert!(original_weak.upgrade().is_none());
    Ok(())
}

#[test]
fn equal_root_rebase_is_constant_work_and_makes_no_probe() -> Result<(), Box<dyn std::error::Error>>
{
    let dependencies = CheckoutDependencies::new(
        [Dependency {
            region: DependencyRegion::FileRecord(FileId::from_bytes([1; 16])),
            expected: DependencyState::Present(Digest::from_bytes([2; 32])),
        }],
        [],
        10,
    )?;
    let probe = Probe {
        calls: Cell::new(0),
        states: BTreeMap::new(),
    };
    let receipt = classify_rebase(
        &probe,
        generation(3),
        generation(3),
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(probe.calls.get(), 0);
    assert_eq!(receipt.work, WorkCounters::default());
    assert!(matches!(receipt.decision, RebaseDecision::Safe { .. }));
    Ok(())
}

#[test]
fn changed_negative_lookup_is_a_region_specific_conflict() -> Result<(), Box<dyn std::error::Error>>
{
    let region = DependencyRegion::DirectoryName {
        directory_id: FileId::from_bytes([4; 16]),
        name: name(b"new-file")?,
    };
    let dependencies = CheckoutDependencies::new(
        [Dependency {
            region: region.clone(),
            expected: DependencyState::Absent,
        }],
        [Dependency {
            region: region.clone(),
            expected: DependencyState::Absent,
        }],
        10,
    )?;
    let target = generation(5);
    let actual = DependencyState::Present(Digest::from_bytes([6; 32]));
    let probe = Probe {
        calls: Cell::new(0),
        states: BTreeMap::from([((target, region.clone()), actual)]),
    };
    let receipt = classify_rebase(
        &probe,
        generation(7),
        target,
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(probe.calls.get(), 1);
    assert_eq!(receipt.work.items_examined, 1);
    assert_eq!(receipt.work.page_reads, 1);
    assert_eq!(
        receipt.decision,
        RebaseDecision::Conflicted {
            conflicts: vec![RebaseConflict {
                region,
                usage: DependencyUse::ObservationAndMutation,
                expected: DependencyState::Absent,
                actual,
            }],
            truncated: false,
        }
    );
    Ok(())
}

#[test]
fn only_distinct_captured_regions_are_probed() -> Result<(), Box<dyn std::error::Error>> {
    let first = Dependency {
        region: DependencyRegion::Metadata(FileId::from_bytes([8; 16])),
        expected: DependencyState::Present(Digest::from_bytes([9; 32])),
    };
    let second = Dependency {
        region: DependencyRegion::ContentRange {
            file_id: FileId::from_bytes([10; 16]),
            offset: 1024,
            length: 16,
        },
        expected: DependencyState::Absent,
    };
    let dependencies =
        CheckoutDependencies::new([first.clone(), first.clone(), second.clone()], [], 2)?;
    assert_eq!(dependencies.len(), 2);
    let target = generation(11);
    let probe = Probe {
        calls: Cell::new(0),
        states: BTreeMap::from([
            ((target, first.region), first.expected),
            ((target, second.region), second.expected),
        ]),
    };
    let receipt = classify_rebase(
        &probe,
        generation(12),
        target,
        &dependencies,
        2,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(probe.calls.get(), 2);
    assert_eq!(receipt.work.items_examined, 2);
    assert!(matches!(receipt.decision, RebaseDecision::Safe { .. }));
    Ok(())
}

#[test]
fn malformed_and_contradictory_dependencies_fail_at_capture() {
    let file_id = FileId::from_bytes([13; 16]);
    assert!(matches!(
        CheckoutDependencies::new(
            [Dependency {
                region: DependencyRegion::ContentRange {
                    file_id,
                    offset: 0,
                    length: 0,
                },
                expected: DependencyState::Absent,
            }],
            [],
            1,
        ),
        Err(DependencyError::EmptyContentRange)
    ));
    let region = DependencyRegion::FileRecord(file_id);
    assert!(matches!(
        CheckoutDependencies::new(
            [Dependency {
                region: region.clone(),
                expected: DependencyState::Absent,
            }],
            [Dependency {
                region,
                expected: DependencyState::Present(Digest::from_bytes([14; 32])),
            }],
            1,
        ),
        Err(DependencyError::ContradictoryState)
    ));
}

#[test]
fn clearing_reverted_mutations_preserves_observations() -> Result<(), DependencyError> {
    let observed = Dependency {
        region: DependencyRegion::FileRecord(FileId::from_bytes([15; 16])),
        expected: DependencyState::Absent,
    };
    let mutated = Dependency {
        region: DependencyRegion::FileLength(FileId::from_bytes([16; 16])),
        expected: DependencyState::Absent,
    };
    let both = Dependency {
        region: DependencyRegion::Metadata(FileId::from_bytes([17; 16])),
        expected: DependencyState::Absent,
    };
    let mut dependencies =
        CheckoutDependencies::new([observed.clone(), both.clone()], [mutated, both], 3)?;
    dependencies.clear_mutations();
    assert_eq!(dependencies.len(), 2);
    assert!(dependencies.captured.contains_key(&observed.region));
    assert!(
        dependencies
            .captured
            .values()
            .all(|state| { matches!(state.usage, DependencyUse::Observation) })
    );
    Ok(())
}

#[test]
fn merged_proofs_hold_both_histories_or_refuse_a_contradiction() -> Result<(), DependencyError> {
    let dependency = |byte: u8, expected: DependencyState| Dependency {
        region: DependencyRegion::FileRecord(FileId::from_bytes([byte; 16])),
        expected,
    };
    let shared = dependency(20, DependencyState::Absent);
    let observed = dependency(21, DependencyState::Absent);
    let mutated = dependency(22, DependencyState::Absent);
    let base = CheckoutDependencies::new([shared.clone()], [], 8)?;
    let mut readers = base.clone();
    readers.extend_observations(vec![observed.clone()], 8)?;
    let mut writer = base.clone();
    writer.commit(writer.prepare_mutations(vec![mutated.clone(), shared.clone()], 8)?);

    let merged = writer
        .merged(&readers)
        .ok_or(DependencyError::ContradictoryState)?;
    assert_eq!(merged.len(), 3);
    assert_eq!(
        merged.captured.get(&shared.region).map(|state| state.usage),
        Some(DependencyUse::ObservationAndMutation)
    );
    assert_eq!(
        merged
            .captured
            .get(&observed.region)
            .map(|state| state.usage),
        Some(DependencyUse::Observation)
    );
    assert_eq!(
        merged
            .captured
            .get(&mutated.region)
            .map(|state| state.usage),
        Some(DependencyUse::Mutation)
    );
    assert_eq!(base.merged(&base), Some(base.clone()));

    let contradicting = CheckoutDependencies::new(
        [dependency(
            21,
            DependencyState::Present(Digest::from_bytes([1; 32])),
        )],
        [],
        8,
    )?;
    assert_eq!(readers.merged(&contradicting), None);
    Ok(())
}

#[test]
fn measured_proof_merge_cancellation_preserves_completed_name_copy_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory_id = FileId::from_bytes([203; 16]);
    let mut padded = Vec::with_capacity(64);
    padded.extend_from_slice(b"first");
    let first = CheckoutDependencies::new([Dependency {
        region: DependencyRegion::DirectoryName {
            directory_id, name: LogicalName::new(NameEncoding::Utf8, padded, 64)?,
        },
        expected: DependencyState::Absent,
    }], [], 8)?;
    let second = CheckoutDependencies::new([Dependency {
        region: DependencyRegion::DirectoryName {
            directory_id, name: name(b"second")?,
        },
        expected: DependencyState::Absent,
    }], [], 8)?;
    let original = first.logical_data();
    let cancellation = CancellationToken::new();
    let owner = Arc::new(ProofDataCustody::default());
    let mut allocations = super::super::allocation::AllocationLedger::default();
    let mut work = WorkCounters::default();
    let result = first.merged_with_data_admission(
        &second, Some(&owner), |bytes, operations, copied, items| -> Result<(), crate::FsError> {
            cancellation.check()?;
            work.charge_items(items, &WorkBudget::UNBOUNDED)?;
            let mut next = work;
            crate::collection::charge_handle_copy(copied, &mut next, WorkBudget::UNBOUNDED)?;
            allocations.claim_bytes(bytes, operations, &mut next, WorkBudget::UNBOUNDED)
                .map_err(crate::collection::handle_allocation_error)?;
            work = next;
            if copied != 0 {
                // Cancellation is seen at the next step, after this admitted
                // first name was actually cloned by the production engine.
                cancellation.cancel();
            }
            Ok(())
        },
    );
    assert!(matches!(result, Err(crate::FsError::Cancelled(_))));
    assert_eq!(work.items_examined, 1);
    assert_eq!(work.bytes_copied, 5, "copy uses length, not the borrowed capacity");
    assert_eq!(work.allocation_operations, 1);
    assert_eq!(work.peak_allocation_bytes, 5);
    assert_eq!(owner.retained_payload_bytes(), 0, "cancelled merge dropped its actual cloned key");
    assert_eq!(first.logical_data(), original, "failed preparation never mutates authority");
    assert_eq!(second.logical_data().2, 6);
    let merged = first.merged(&second).ok_or("compatible proof rejected")?;
    assert_eq!(merged.logical_data().2, 11);
    assert_eq!(merged.len(), 2);
    Ok(())
}

#[test]
fn dependency_bounds_and_extensions_are_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let first = Dependency {
        region: DependencyRegion::FileRecord(FileId::from_bytes([21; 16])),
        expected: DependencyState::Absent,
    };
    let second = Dependency {
        region: DependencyRegion::Metadata(FileId::from_bytes([22; 16])),
        expected: DependencyState::Absent,
    };
    assert!(matches!(
        CheckoutDependencies::new([], [], 0),
        Err(DependencyError::ZeroLimit)
    ));
    assert!(matches!(
        CheckoutDependencies::new([first.clone(), second.clone()], [], 1),
        Err(DependencyError::TooManyDependencies { maximum: 1 })
    ));

    let mut dependencies = CheckoutDependencies::new([first.clone()], [], 2)?;
    let original = dependencies.clone();
    assert!(matches!(
        dependencies.extend_observations(Vec::new(), 0),
        Err(DependencyError::ZeroLimit)
    ));
    let contradictory = Dependency {
        region: first.region.clone(),
        expected: DependencyState::Present(Digest::from_bytes([23; 32])),
    };
    assert!(matches!(
        dependencies.prepare_mutations(vec![contradictory], 2),
        Err(DependencyError::ContradictoryState)
    ));
    assert_eq!(dependencies, original);

    let staged_conflict = Dependency {
        region: second.region.clone(),
        expected: DependencyState::Present(Digest::from_bytes([24; 32])),
    };
    assert!(matches!(
        dependencies.extend_observations(vec![second.clone(), staged_conflict], 2),
        Err(DependencyError::ContradictoryState)
    ));
    assert_eq!(dependencies, original);
    assert!(matches!(
        dependencies.extend_observations(vec![second.clone()], 1),
        Err(DependencyError::TooManyDependencies { maximum: 1 })
    ));
    assert_eq!(dependencies, original);

    let extension = dependencies.prepare_mutations(vec![first.clone()], 2)?;
    assert_eq!(dependencies, original, "preparing never changes the proof");
    dependencies.commit(extension);
    assert!(matches!(
        dependencies
            .captured
            .get(&first.region)
            .map(|state| state.usage),
        Some(DependencyUse::ObservationAndMutation)
    ));
    dependencies.extend_observations(vec![first], 2)?;
    dependencies.extend_observations(vec![second], 2)?;
    assert_eq!(dependencies.len(), 2);
    Ok(())
}

#[test]
fn dependency_data_budget_denies_cow_before_copy_and_eq_ignores_caches()
-> Result<(), Box<dyn std::error::Error>> {
    let directory_id = FileId::from_bytes([71; 16]);
    let mut padded = Vec::with_capacity(64);
    padded.extend_from_slice(b"key");
    let dependency = |name| Dependency {
        region: DependencyRegion::DirectoryName { directory_id, name },
        expected: DependencyState::Absent,
    };
    let mut proof = CheckoutDependencies::new([
        dependency(LogicalName::new(NameEncoding::Utf8, padded, 64)?),
    ], [], 8)?;
    let exact = CheckoutDependencies::new([
        dependency(LogicalName::new(NameEncoding::Utf8, b"key".to_vec(), 64)?),
    ], [], 8)?;
    assert_eq!(proof, exact, "capacity and allocation generation are not semantic proof");
    assert_ne!(proof.logical_data().2, exact.logical_data().2);
    let held = proof.clone();
    let before = proof.logical_data();
    let extension = proof.prepare_observations(vec![Dependency {
        region: DependencyRegion::Metadata(FileId::from_bytes([72; 16])),
        expected: DependencyState::Absent,
    }], 8)?;
    let owner = Arc::new(ProofDataCustody::default());
    let mut allocations = super::super::allocation::AllocationLedger::default();
    let mut work = WorkCounters::default();
    let budget = WorkBudget { peak_allocation_bytes: 3, ..WorkBudget::UNBOUNDED };
    let denied = proof.commit_with_data_custody::<WorkError>(
        extension, Some(&owner), |bytes, operations, copied, _| {
            let mut next = work.checked_add(WorkCounters {
                bytes_copied: copied, ..WorkCounters::default()
            })?;
            allocations.claim_bytes(bytes, operations, &mut next, budget)
                .map_err(|error| error.into_work_or(WorkError::Overflow))?;
            work = next;
            Ok(())
        },
    );
    assert!(denied.is_err());
    assert_eq!(proof.logical_data(), before);
    assert!(Arc::ptr_eq(&proof.captured, &held.captured));
    assert_eq!(owner.retained_bytes(), 0);
    assert_eq!(work.allocation_operations, 0);
    assert_eq!(work.bytes_copied, 0, "name fits but the actual map Arc must be admitted too");
    assert_eq!(owner.physical_retained_bytes(), 0);
    assert_eq!(proof, held);
    Ok(())
}


fn conflicting_dependencies() -> Result<(CheckoutDependencies, Probe), DependencyError> {
    let first = Dependency {
        region: DependencyRegion::FileRecord(FileId::from_bytes([31; 16])),
        expected: DependencyState::Absent,
    };
    let second = Dependency {
        region: DependencyRegion::Metadata(FileId::from_bytes([32; 16])),
        expected: DependencyState::Absent,
    };
    let target = generation(33);
    Ok((
        CheckoutDependencies::new([first.clone(), second.clone()], [], 2)?,
        Probe {
            calls: Cell::new(0),
            states: BTreeMap::from([
                (
                    (target, first.region),
                    DependencyState::Present(Digest::from_bytes([34; 32])),
                ),
                (
                    (target, second.region),
                    DependencyState::Present(Digest::from_bytes([35; 32])),
                ),
            ]),
        },
    ))
}

#[test]
fn sync_rebase_conflict_limit_and_truncation_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let (dependencies, probe) = conflicting_dependencies()?;
    let zero = classify_rebase(
        &probe,
        generation(30),
        generation(33),
        &dependencies,
        0,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("zero conflict limit was accepted")?;
    assert!(matches!(zero.error, RebaseError::ZeroConflictLimit));
    assert_eq!(*zero.work, WorkCounters::default());
    assert_eq!(probe.calls.get(), 0);

    let receipt = classify_rebase(
        &probe,
        generation(30),
        generation(33),
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        receipt.decision,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: true
        } if conflicts.len() == 1
    ));
    assert_eq!(probe.calls.get(), 2);
    Ok(())
}

#[test]
fn async_rebase_conflict_limit_and_truncation_are_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let (dependencies, probe) = conflicting_dependencies()?;
    let cancellation = CancellationToken::new();
    let zero = poll_ready(classify_rebase_async(
        &probe,
        generation(30),
        generation(33),
        &dependencies,
        0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("async zero-limit classification blocked")?
    .err()
    .ok_or("async zero conflict limit was accepted")?;
    assert!(matches!(zero.error, RebaseError::ZeroConflictLimit));
    assert_eq!(*zero.work, WorkCounters::default());

    let receipt = poll_ready(classify_rebase_async(
        &probe,
        generation(30),
        generation(33),
        &dependencies,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("async classification blocked")??;
    assert!(matches!(
        receipt.decision,
        RebaseDecision::Conflicted {
            ref conflicts,
            truncated: true
        } if conflicts.len() == 1
    ));
    assert_eq!(probe.calls.get(), 2);
    Ok(())
}
