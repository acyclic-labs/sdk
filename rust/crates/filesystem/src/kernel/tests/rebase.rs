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
        dependencies.extend_mutations(vec![contradictory], 2),
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

    dependencies.extend_mutations(vec![first.clone()], 2)?;
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
