use super::*;

fn demand_error(error: MountSourceError) -> DemandError {
    match error {
        MountSourceError::Work(error) => DemandError::Work(error),
        MountSourceError::Cancelled => DemandError::Cancelled,
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(error)) => {
            DemandError::Work(error)
        }
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled) => {
            DemandError::Cancelled
        }
        error => DemandError::Mount(error),
    }
}

impl<A, O, D, S> SourceChangeSink for SourceChangeRecorder<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn source_changed(
        &self,
        changes: &[SourceChange],
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::demand::DemandResult<()> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation).map_err(
            |(error, work)| {
                crate::OperationFailure::new(
                    demand_error(MountSourceError::NativeWork(error)),
                    work,
                )
            },
        )?;
        let result = self.record(
            changes,
            crate::demand::NativeSourceWork::from_native_materialization_work(&work),
        );
        result
            .map(|value| crate::OperationReceipt {
                value,
                work: work.receipt(),
            })
            .map_err(|error| crate::OperationFailure::new(demand_error(error), work.receipt()))
    }

    fn source_changed_with_native_work(
        &self,
        changes: &[SourceChange],
        source: crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        self.record(changes, source).map_err(demand_error)
    }
}

impl<A, O, D, S> SourceChangeRecorder<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn record(
        &self,
        changes: &[SourceChange],
        source: crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), MountSourceError> {
        use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
        let work = source.materialization_work().map_err(mount_demand_error)?;
        work.admit(crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        let (Some(lazy), Some(authored)) = (self.lazy.upgrade(), self.authored.upgrade()) else {
            return Ok(());
        };
        authored
            .cancellation()
            .check()
            .map_err(|_| MountSourceError::Cancelled)?;
        let mut scratch_carrier = adapter::NativeDemandCarrier::Native(&work);
        let mut scratch = adapter::InputBuffers::new(&mut scratch_carrier)?;
        let mut effect = ViewEffect::default();
        let (nodes, subtrees, listings) =
            changes
                .iter()
                .try_fold((0usize, 0usize, 0usize), |(n, s, l), change| {
                    crate::native_host::NativeHostWork::check(&source)
                        .map_err(mount_demand_error)?;
                    work.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })
                    .map_err(MountSourceError::NativeWork)?;
                    let extra = match change {
                        SourceChange::Entry(path) => 1 + usize::from(!path.is_root()),
                        _ => 0,
                    };
                    Ok::<_, MountSourceError>((
                        n.checked_add(usize::from(matches!(change, SourceChange::Node(_)))),
                        s.checked_add(usize::from(matches!(change, SourceChange::Name(_)))),
                        l.checked_add(extra),
                    ))
                    .and_then(|(n, s, l)| {
                        n.zip(s)
                            .zip(l)
                            .map(|((n, s), l)| (n, s, l))
                            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))
                    })
                })?;
        let mut namespace_allocations = scratch.vec(
            subtrees
                .checked_add(listings)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?,
        )?;
        effect.nodes = scratch.vec(nodes)?;
        effect.subtrees = scratch.vec(subtrees)?;
        effect.listings = scratch.vec(listings)?;
        let mut unconfirmed = false;
        let mut accounting = KernelAccounting::native(&work, work.cancellation());
        for change in changes {
            crate::native_host::NativeHostWork::check(&source).map_err(mount_demand_error)?;
            authored
                .cancellation()
                .check()
                .map_err(|_| MountSourceError::Cancelled)?;
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
            match change {
                SourceChange::Name(path) | SourceChange::Entry(path) => {
                    let mut append = |depth, output: &mut Vec<crate::kernel::NamespacePath>| {
                        let (path, allocation) = path
                            .prefix_with_accounting(
                                depth,
                                lazy.workspace().limits(),
                                &mut accounting,
                                work.cancellation(),
                            )
                            .map_err(mount_engine_error)?;
                        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                            unreachable!()
                        };
                        namespace_allocations.push(allocation);
                        output.push(path);
                        Ok::<_, MountSourceError>(())
                    };
                    if matches!(change, SourceChange::Name(_)) {
                        append(path.depth(), &mut effect.subtrees)?;
                    } else {
                        if !path.is_root() {
                            append(path.depth() - 1, &mut effect.listings)?;
                        }
                        append(path.depth(), &mut effect.listings)?;
                    }
                }
                SourceChange::Node(identity) => effect.nodes.push(
                    lazy.source_file_id(identity, &mut accounting)
                        .map_err(lazy_error)?,
                ),
                SourceChange::Everything => effect.everything = true,
                SourceChange::Unconfirmed => unconfirmed = true,
            }
        }
        for change in [
            (!effect.is_empty()).then_some(ViewChange::Effect(&effect)),
            unconfirmed.then_some(ViewChange::Unconfirmed),
        ]
        .into_iter()
        .flatten()
        {
            authored.record_projection_change(
                &change,
                &mut adapter::NativeDemandCarrier::Native(&work),
                || {
                    crate::native_host::NativeHostWork::check(&source)
                        .map_err(mount_demand_error)?;
                    work.admit(crate::WorkCounters::UNCHARGED)
                        .map_err(MountSourceError::NativeWork)?;
                    authored
                        .cancellation()
                        .check()
                        .map_err(|_| MountSourceError::Cancelled)
                },
            )?;
        }
        crate::native_host::NativeHostWork::check(&source).map_err(mount_demand_error)
    }
}
