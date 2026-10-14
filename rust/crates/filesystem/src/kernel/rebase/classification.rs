use super::*;

/// Compares only captured regions against a candidate generation.
pub fn classify_rebase<P: RebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
) -> Result<RebaseReceipt, OperationFailure<RebaseError<P::Error>>> {
    if base == candidate {
        return Ok(RebaseReceipt {
            decision: RebaseDecision::Safe {
                generation: candidate,
            },
            work: WorkCounters::default(),
        });
    }
    if maximum_conflicts == 0 {
        return Err(OperationFailure::before_work(
            RebaseError::ZeroConflictLimit,
        ));
    }
    let mut work = WorkCounters::default();
    let mut conflicts = Vec::new();
    let mut truncated = false;
    for (region, captured) in dependencies.captured.entries.iter() {
        let semantic = WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        };
        work = work
            .checked_add(semantic)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let actual = probe
            .probe(candidate, region, remaining)
            .map_err(|failure| failure.map_with_prior_work(work, RebaseError::Probe))?;
        work = work
            .checked_add(actual.work)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if actual.value != captured.expected {
            if u32::try_from(conflicts.len()).unwrap_or(u32::MAX) < maximum_conflicts {
                conflicts.push(RebaseConflict {
                    region: region.clone(),
                    usage: captured.usage,
                    expected: captured.expected,
                    actual: actual.value,
                });
            } else {
                truncated = true;
            }
        }
    }
    let decision = if conflicts.is_empty() {
        RebaseDecision::Safe {
            generation: candidate,
        }
    } else {
        RebaseDecision::Conflicted {
            conflicts,
            truncated,
        }
    };
    Ok(RebaseReceipt { decision, work })
}

/// Nonblocking safe-rebase classification over the same exact dependencies as [`classify_rebase`].
pub async fn classify_rebase_async<P: AsyncRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<RebaseReceipt, OperationFailure<RebaseError<P::Error>>> {
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    let adapter = StandaloneClassificationProbe(probe);
    let classified = classify_rebase_with_accounting(
        &adapter,
        base,
        candidate,
        dependencies,
        maximum_conflicts,
        &mut accounting,
        cancellation,
    )
    .await;
    let work = accounting.spent();
    match classified {
        Ok((decision, _allocation)) => {
            #[cfg(not(target_arch = "wasm32"))]
            drop(_allocation);
            Ok(RebaseReceipt { decision, work })
        }
        Err(error) => Err(OperationFailure::new(
            match error {
                ClassifyRebaseFailure::Probe(error) => RebaseError::Probe(error),
                ClassifyRebaseFailure::ZeroConflictLimit => RebaseError::ZeroConflictLimit,
                ClassifyRebaseFailure::Accounting(crate::FsError::Work(error)) => {
                    RebaseError::Work(error)
                }
                ClassifyRebaseFailure::Accounting(crate::FsError::Cancelled(_)) => {
                    RebaseError::Cancelled
                }
                ClassifyRebaseFailure::Accounting(_) => RebaseError::AllocationFailed,
            },
            work,
        )),
    }
}

pub(crate) trait AccountingRebaseProbe {
    type Error;
    fn probe_with_accounting(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<DependencyState, ClassifyRebaseFailure<Self::Error>>>;
}

pub(crate) enum ClassifyRebaseFailure<E> {
    Probe(E),
    Accounting(crate::FsError),
    ZeroConflictLimit,
}

struct StandaloneClassificationProbe<'a, P>(&'a P);

impl<P: AsyncRebaseProbe> AccountingRebaseProbe for StandaloneClassificationProbe<'_, P> {
    type Error = P::Error;
    async fn probe_with_accounting(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<DependencyState, ClassifyRebaseFailure<Self::Error>> {
        // This adapter is constructed only by the standalone public API.
        let budget = accounting
            .remaining()
            .map_err(ClassifyRebaseFailure::Accounting)?;
        match self
            .0
            .probe_async(generation, region, budget, cancellation)
            .await
        {
            Ok(receipt) => {
                retain_standalone_probe_work(accounting, receipt.work)
                    .map_err(ClassifyRebaseFailure::Accounting)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                // Probe failures retain the work that actually happened,
                // including an over-budget measured backend receipt.
                retain_standalone_probe_work(accounting, *failure.work)
                    .map_err(ClassifyRebaseFailure::Accounting)?;
                Err(ClassifyRebaseFailure::Probe(failure.error))
            }
        }
    }
}

fn retain_standalone_probe_work(
    accounting: &mut super::allocation::KernelAccounting<'_>,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    match accounting {
        super::allocation::KernelAccounting::Standalone { work, budget, .. } => {
            *work = work.checked_add(delta)?;
            work.verify(*budget)?;
        }
        super::allocation::KernelAccounting::BorrowedStandalone { work, budget, .. } => {
            **work = work.checked_add(delta)?;
            work.verify(*budget)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        super::allocation::KernelAccounting::NativeOriginal { .. } => {
            unreachable!("the standalone adapter cannot wrap an original-native probe")
        }
    }
    Ok(())
}

pub(crate) async fn classify_rebase_with_accounting<P: AccountingRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<
    (RebaseDecision, super::allocation::KernelOwnedAllocation),
    ClassifyRebaseFailure<P::Error>,
> {
    use super::persistent_point::Owned;
    accounting
        .check()
        .map_err(ClassifyRebaseFailure::Accounting)?;
    if base == candidate {
        let allocation = accounting
            .claim_owned(0, 0)
            .map_err(ClassifyRebaseFailure::Accounting)?;
        return Ok((
            RebaseDecision::Safe {
                generation: candidate,
            },
            allocation,
        ));
    }
    if maximum_conflicts == 0 {
        return Err(ClassifyRebaseFailure::ZeroConflictLimit);
    }
    let mut conflicts = Owned::<Vec<RebaseConflict>>::allocate(0, accounting)
        .map_err(ClassifyRebaseFailure::Accounting)?;
    let result = async {
        let mut truncated = false;
        for (region, captured) in dependencies.captured.entries.iter() {
            cancellation
                .check()
                .map_err(|error| ClassifyRebaseFailure::Accounting(error.into()))?;
            accounting
                .charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(ClassifyRebaseFailure::Accounting)?;
            let actual = probe
                .probe_with_accounting(candidate, region, accounting, cancellation)
                .await?;
            if actual == captured.expected {
                continue;
            }
            if conflicts.value.len() >= maximum_conflicts as usize {
                truncated = true;
                continue;
            }
            conflicts
                .ensure_for_push(maximum_conflicts as usize, accounting)
                .map_err(ClassifyRebaseFailure::Accounting)?;
            let (region, mut allocation, bytes) = clone_conflict_region(region, accounting)
                .map_err(ClassifyRebaseFailure::Accounting)?;
            if let Err(error) = conflicts.allocation.absorb(&mut allocation) {
                drop(region);
                return Err(ClassifyRebaseFailure::Accounting(error));
            }
            conflicts.bytes = conflicts
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| ClassifyRebaseFailure::Accounting(WorkError::Overflow.into()))?;
            conflicts.value.push(RebaseConflict {
                region,
                usage: captured.usage,
                expected: captured.expected,
                actual,
            });
        }
        Ok(truncated)
    }
    .await;
    match result {
        Ok(truncated) => {
            let decision = if conflicts.value.is_empty() {
                RebaseDecision::Safe {
                    generation: candidate,
                }
            } else {
                RebaseDecision::Conflicted {
                    conflicts: conflicts.value,
                    truncated,
                }
            };
            Ok((decision, conflicts.allocation))
        }
        Err(error) => {
            conflicts
                .retire(accounting)
                .map_err(ClassifyRebaseFailure::Accounting)?;
            Err(error)
        }
    }
}

fn clone_conflict_region(
    region: &DependencyRegion,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<
    (
        DependencyRegion,
        super::allocation::KernelOwnedAllocation,
        u64,
    ),
    crate::FsError,
> {
    use super::persistent_btree::Format;
    use super::tree_mutation::TreeFormat;
    match region {
        DependencyRegion::DirectoryName { directory_id, name } => {
            let (name, allocation) =
                super::persistent_io::clone_key::<TreeFormat>(name, u32::MAX, accounting, None, 0)?;
            let bytes = TreeFormat::key_owned_bytes(&name);
            Ok((
                DependencyRegion::DirectoryName {
                    directory_id: *directory_id,
                    name,
                },
                allocation,
                bytes,
            ))
        }
        DependencyRegion::DirectoryRange {
            directory_id,
            after: Some(after),
            maximum_entries,
        } => {
            let (after, allocation) = super::persistent_io::clone_key::<TreeFormat>(
                after,
                u32::MAX,
                accounting,
                None,
                0,
            )?;
            let bytes = TreeFormat::key_owned_bytes(&after);
            Ok((
                DependencyRegion::DirectoryRange {
                    directory_id: *directory_id,
                    after: Some(after),
                    maximum_entries: *maximum_entries,
                },
                allocation,
                bytes,
            ))
        }
        _ => {
            let allocation = accounting.claim_owned(0, 0)?;
            Ok((region.clone(), allocation, 0))
        }
    }
}
