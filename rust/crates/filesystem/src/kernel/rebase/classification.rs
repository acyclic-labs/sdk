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
        Ok((decision, _allocation)) => Ok(RebaseReceipt { decision, work }),
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

    /// The single-link regular file `base` bound at `directory`/`name` and
    /// the new single-link regular file `candidate` bound there instead, when
    /// the candidate replaced one by the other.
    fn replacement_with_accounting(
        &self,
        base: GenerationId,
        candidate: GenerationId,
        directory: FileId,
        name: &LogicalName,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<Option<(FileId, FileId)>, ClassifyRebaseFailure<Self::Error>>>;

    /// Whether the checkout's own working tree no longer has `file`.
    fn removed_locally_with_accounting(
        &self,
        file: FileId,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<bool, ClassifyRebaseFailure<Self::Error>>>;
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

    // A standalone probe resolves regions only; it knows no names or
    // working tree, so every mismatch stays a conflict.
    async fn replacement_with_accounting(
        &self,
        _base: GenerationId,
        _candidate: GenerationId,
        _directory: FileId,
        _name: &LogicalName,
        _accounting: &mut super::allocation::KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<Option<(FileId, FileId)>, ClassifyRebaseFailure<Self::Error>> {
        Ok(None)
    }

    async fn removed_locally_with_accounting(
        &self,
        _file: FileId,
        _accounting: &mut super::allocation::KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<bool, ClassifyRebaseFailure<Self::Error>> {
        Ok(false)
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
    let (decision, allocation, settled) = classify_rebase_settling_with_accounting(
        probe,
        base,
        candidate,
        dependencies,
        maximum_conflicts,
        accounting,
        cancellation,
    )
    .await?;
    if let Some(settled) = settled {
        settled
            .retire(accounting)
            .map_err(ClassifyRebaseFailure::Accounting)?;
    }
    Ok((decision, allocation))
}

/// How a mismatch that a replacement settled carries into the rebased ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Settlement {
    /// The region keeps its key and now expects the candidate's state.
    Expect(DependencyState),
    /// A file region of the replaced identity now names its replacement.
    Follow(FileId),
}

/// Mismatches that a file replacement settled instead of conflicting. The
/// candidate merge decides them: when it merges, the ledger follows each
/// settlement; when it conflicts, the settled mismatches are the decision.
pub(crate) struct Settled<'d> {
    regions: super::persistent_point::Owned<Vec<(&'d DependencyRegion, Settlement)>>,
    conflicts: ConflictSink,
}

impl<'d> Settled<'d> {
    /// The settled mismatches as a conflicted decision, for a candidate merge
    /// that did not merge them.
    pub(crate) fn into_conflicts(
        self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(RebaseDecision, super::allocation::KernelOwnedAllocation), crate::FsError> {
        let Self { regions, conflicts } = self;
        if let Err(error) = regions.retire(accounting) {
            conflicts.conflicts.retire(accounting)?;
            return Err(error);
        }
        Ok((
            RebaseDecision::Conflicted {
                conflicts: conflicts.conflicts.value,
                truncated: conflicts.truncated,
            },
            conflicts.conflicts.allocation,
        ))
    }

    /// Releases the accounted settlement and returns its regions for the
    /// infallible ledger update that completes a rebase. Nothing allocates
    /// between this release and the vector's drop.
    pub(crate) fn release(
        self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Vec<(&'d DependencyRegion, Settlement)>, crate::FsError> {
        let Self { regions, conflicts } = self;
        let super::persistent_point::Owned {
            value,
            allocation,
            bytes,
        } = regions;
        let conflicts = conflicts.conflicts.retire(accounting);
        super::persistent_point::retire_allocation(allocation, bytes, accounting)?;
        conflicts.map(|()| value)
    }

    pub(crate) fn retire(
        self,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let regions = self.regions.retire(accounting);
        self.conflicts.conflicts.retire(accounting)?;
        regions
    }
}

fn region_file(region: &DependencyRegion) -> Option<FileId> {
    match region {
        DependencyRegion::FileRecord(file_id)
        | DependencyRegion::Metadata(file_id)
        | DependencyRegion::FileLength(file_id)
        | DependencyRegion::ContentRange { file_id, .. }
        | DependencyRegion::SparseSeek { file_id, .. } => Some(*file_id),
        DependencyRegion::DirectoryName { .. } | DependencyRegion::DirectoryRange { .. } => None,
    }
}

impl DependencyRegion {
    /// The same file region of another identity; directory regions are keyed
    /// by their directory and stay as they are.
    pub(crate) fn of_file(&self, file: FileId) -> Self {
        match self {
            Self::FileRecord(_) => Self::FileRecord(file),
            Self::Metadata(_) => Self::Metadata(file),
            Self::FileLength(_) => Self::FileLength(file),
            Self::ContentRange { offset, length, .. } => Self::ContentRange {
                file_id: file,
                offset: *offset,
                length: *length,
            },
            Self::SparseSeek { offset, target, .. } => Self::SparseSeek {
                file_id: file,
                offset: *offset,
                target: *target,
            },
            Self::DirectoryName { .. } | Self::DirectoryRange { .. } => self.clone(),
        }
    }
}

/// Bounded conflict output: the first `maximum` conflicts, then truncation.
struct ConflictSink {
    conflicts: super::persistent_point::Owned<Vec<RebaseConflict>>,
    maximum: usize,
    truncated: bool,
}

impl ConflictSink {
    fn new(
        maximum: u32,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        Ok(Self {
            conflicts: super::persistent_point::Owned::allocate(0, accounting)?,
            maximum: maximum as usize,
            truncated: false,
        })
    }

    fn push(
        &mut self,
        region: &DependencyRegion,
        (usage, expected, actual): States,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if self.conflicts.value.len() >= self.maximum {
            self.truncated = true;
            return Ok(());
        }
        self.conflicts.ensure_for_push(self.maximum, accounting)?;
        let (region, mut allocation, bytes) = clone_conflict_region(region, accounting)?;
        if let Err(error) = self.conflicts.allocation.absorb(&mut allocation) {
            drop(region);
            return Err(error);
        }
        self.conflicts.bytes = self
            .conflicts
            .bytes
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        self.conflicts.value.push(RebaseConflict {
            region,
            usage,
            expected,
            actual,
        });
        Ok(())
    }
}

/// Classifies like [`classify_rebase_with_accounting`] and also settles the
/// mismatches a file replacement explains, with the rule the kernel merge
/// uses: a name whose single-link regular file the candidate replaced by a
/// new single-link regular file still names the same logical file.
///
/// - The replaced name settles and expects the candidate's binding.
/// - A read of the replaced identity is checked against the same region of
///   its replacement; when it agrees it settles and follows the replacement.
/// - A compare-before-write of an identity that the candidate or the checkout
///   removed settles for the candidate merge to decide, whose replacement
///   pairing and byte merge are exact; it then expects the candidate state.
///
/// Every other mismatch conflicts as before.
pub(crate) async fn classify_rebase_settling_with_accounting<'d, P: AccountingRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &'d CheckoutDependencies,
    maximum_conflicts: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<
    (
        RebaseDecision,
        super::allocation::KernelOwnedAllocation,
        Option<Settled<'d>>,
    ),
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
            None,
        ));
    }
    if maximum_conflicts == 0 {
        return Err(ClassifyRebaseFailure::ZeroConflictLimit);
    }
    let mut work = Classification::allocate(maximum_conflicts, accounting)
        .map_err(ClassifyRebaseFailure::Accounting)?;
    let ledger = dependencies.len();
    let result = async {
        work.ledger_pass(probe, [base, candidate], dependencies, accounting, cancellation)
            .await?;
        work.deferred_reads(probe, candidate, ledger, accounting, cancellation)
            .await
    }
    .await;
    let Classification {
        conflicts,
        settled,
        pairs,
        reads,
    } = work;
    let retired = pairs.retire(accounting).and(reads.retire(accounting));
    if let Err(error) = result.and_then(|()| retired.map_err(ClassifyRebaseFailure::Accounting)) {
        let retired = conflicts.conflicts.retire(accounting);
        settled.retire(accounting).and(retired).map_err(ClassifyRebaseFailure::Accounting)?;
        return Err(error);
    }
    let ConflictSink {
        conflicts: Owned {
            value: mut conflicts,
            allocation,
            ..
        },
        truncated,
        ..
    } = conflicts;
    if conflicts.is_empty() {
        let settled = (!settled.regions.value.is_empty()).then_some(settled);
        return Ok((
            RebaseDecision::Safe {
                generation: candidate,
            },
            allocation,
            settled,
        ));
    }
    settled
        .retire(accounting)
        .map_err(ClassifyRebaseFailure::Accounting)?;
    // Deferred reads append after the ledger pass; report in ledger order.
    conflicts.sort_by(|left, right| left.region.cmp(&right.region));
    Ok((
        RebaseDecision::Conflicted {
            conflicts,
            truncated,
        },
        allocation,
        None,
    ))
}

type States = (DependencyUse, DependencyState, DependencyState);

/// The bounded working state of one settling classification.
struct Classification<'d> {
    conflicts: ConflictSink,
    settled: Settled<'d>,
    /// Replaced identity and its replacement, from settled names.
    pairs: super::persistent_point::Owned<Vec<(FileId, FileId)>>,
    /// Reads of an identity the candidate removed, checked once every
    /// replaced name is known.
    reads: super::persistent_point::Owned<Vec<(&'d DependencyRegion, States)>>,
}

impl Classification<'_> {
    fn allocate(
        maximum_conflicts: u32,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        use super::persistent_point::Owned;
        let conflicts = ConflictSink::new(maximum_conflicts, accounting)?;
        let settled = match ConflictSink::new(maximum_conflicts, accounting) {
            Ok(sink) => match Owned::allocate(0, accounting) {
                Ok(regions) => Settled {
                    regions,
                    conflicts: sink,
                },
                Err(error) => {
                    sink.conflicts.retire(accounting)?;
                    conflicts.conflicts.retire(accounting)?;
                    return Err(error);
                }
            },
            Err(error) => {
                conflicts.conflicts.retire(accounting)?;
                return Err(error);
            }
        };
        let pairs = match Owned::allocate(0, accounting) {
            Ok(pairs) => pairs,
            Err(error) => {
                settled.retire(accounting)?;
                conflicts.conflicts.retire(accounting)?;
                return Err(error);
            }
        };
        match Owned::allocate(0, accounting) {
            Ok(reads) => Ok(Self {
                conflicts,
                settled,
                pairs,
                reads,
            }),
            Err(error) => {
                pairs.retire(accounting)?;
                settled.retire(accounting)?;
                conflicts.conflicts.retire(accounting)?;
                Err(error)
            }
        }
    }
}

impl<'d> Classification<'d> {
    /// Probes every ledger region in `candidate`, settling or recording
    /// each mismatch; reads of removed identities wait in `reads`.
    async fn ledger_pass<P: AccountingRebaseProbe>(
        &mut self,
        probe: &P,
        [base, candidate]: [GenerationId; 2],
        dependencies: &'d CheckoutDependencies,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), ClassifyRebaseFailure<P::Error>> {
        let ledger = dependencies.len();
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
            let states = (captured.usage, captured.expected, actual);
            let settlement = match (region, region_file(region)) {
                (DependencyRegion::DirectoryName { directory_id, name }, _) => {
                    let pair = probe
                        .replacement_with_accounting(
                            base,
                            candidate,
                            *directory_id,
                            name,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    if let Some(pair) = pair {
                        self.pairs
                            .ensure_for_push(ledger, accounting)
                            .map_err(ClassifyRebaseFailure::Accounting)?;
                        self.pairs.value.push(pair);
                    }
                    pair.map(|_| Settlement::Expect(actual))
                }
                (_, Some(file)) if captured.usage == DependencyUse::Mutation => {
                    let removed = actual == DependencyState::Absent
                        || probe
                            .removed_locally_with_accounting(file, accounting, cancellation)
                            .await?;
                    removed.then_some(Settlement::Expect(actual))
                }
                (_, Some(_)) if actual == DependencyState::Absent => {
                    self.reads
                        .ensure_for_push(ledger, accounting)
                        .map_err(ClassifyRebaseFailure::Accounting)?;
                    self.reads.value.push((region, states));
                    continue;
                }
                _ => None,
            };
            self.record(region, states, settlement, ledger, accounting)
                .map_err(ClassifyRebaseFailure::Accounting)?;
        }
        Ok(())
    }

    /// Checks each read of a removed identity against the same region of
    /// its replacement, once every replaced name is known (the ledger orders
    /// names after file regions).
    async fn deferred_reads<P: AccountingRebaseProbe>(
        &mut self,
        probe: &P,
        candidate: GenerationId,
        ledger: usize,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), ClassifyRebaseFailure<P::Error>> {
        self.pairs.value.sort_unstable();
        for &(region, (usage, expected, absent)) in &self.reads.value {
            cancellation
                .check()
                .map_err(|error| ClassifyRebaseFailure::Accounting(error.into()))?;
            let replacement = region_file(region).and_then(|file| {
                let pairs = &self.pairs.value;
                pairs
                    .binary_search_by_key(&file, |(replaced, _)| *replaced)
                    .ok()
                    .and_then(|index| pairs.get(index))
                    .map(|(_, replacement)| *replacement)
            });
            let Some(replacement) = replacement else {
                self.conflicts
                    .push(region, (usage, expected, absent), accounting)
                    .map_err(ClassifyRebaseFailure::Accounting)?;
                continue;
            };
            accounting
                .charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(ClassifyRebaseFailure::Accounting)?;
            let actual = probe
                .probe_with_accounting(
                    candidate,
                    &region.of_file(replacement),
                    accounting,
                    cancellation,
                )
                .await?;
            let settlement = (actual == expected).then_some(Settlement::Follow(replacement));
            let settled = &mut self.settled;
            let conflicts = &mut self.conflicts;
            let states = (usage, expected, actual);
            match settlement {
                Some(settlement) => settle(settled, region, states, settlement, ledger, accounting),
                None => conflicts.push(region, states, accounting),
            }
            .map_err(ClassifyRebaseFailure::Accounting)?;
        }
        Ok(())
    }

    /// Settles a mismatch, or records it as a conflict.
    fn record(
        &mut self,
        region: &'d DependencyRegion,
        states: States,
        settlement: Option<Settlement>,
        ledger: usize,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        match settlement {
            Some(settlement) => settle(&mut self.settled, region, states, settlement, ledger, accounting),
            None => self.conflicts.push(region, states, accounting),
        }
    }
}

fn settle<'d>(
    settled: &mut Settled<'d>,
    region: &'d DependencyRegion,
    states: States,
    settlement: Settlement,
    ledger: usize,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    settled.regions.ensure_for_push(ledger, accounting)?;
    settled.regions.value.push((region, settlement));
    settled.conflicts.push(region, states, accounting)
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
