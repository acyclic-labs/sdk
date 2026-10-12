use super::*;

pub(super) async fn stage_mount_promotion<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    authored: &CheckoutMountSource<A, O>,
    path: &str,
    expected_source: FileId,
    carrier: &mut adapter::NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let cancellation = carrier.cancellation();
    for _ in 0..MAXIMUM_PROMOTION_RETRIES {
        carrier.with_work(|work| mount_items(work, 1))?;
        let (mut candidate, epoch, stamp) = authored
            .shared_checkout()
            .prepare_native_phase_with_carrier(
                carrier,
                &cancellation,
                Some(authored.cancellation()),
            )
            .await?;
        let original = carrier.original_native_work();
        let mut accounting = match &original {
            Some(original) => crate::kernel::KernelAccounting::native(original, &cancellation),
            None => crate::kernel::KernelAccounting::standalone(
                carrier
                    .with_work(|work| work.remaining())
                    .map_err(mount_demand_error)?,
                &cancellation,
            ),
        };
        let result = lazy
            .stage_into_checkout(
                &mut candidate.checkout,
                None,
                path,
                expected_source,
                MAXIMUM_PROMOTION_BYTES,
                &mut accounting,
            )
            .await;
        let changed = if original.is_some() {
            result.map_err(lazy_error)?
        } else {
            let result = crate::facade::measured(result, &accounting);
            carrier.with_work(|work| mount_accept(work, result, lazy_error))?
        };
        if !changed {
            return Ok(());
        }
        let (root, _allocation) = authored.namespace_path(&MountPath::root(), carrier)?;
        let (installed, _allocation) = adapter::installed_changes(
            &candidate.base,
            &candidate.checkout,
            InstallScope::Subtree(&root),
            carrier,
            &cancellation,
        )
        .await?;
        let installed = authored
            .shared_checkout()
            .finish_phase(
                candidate,
                epoch,
                stamp,
                carrier,
                &cancellation,
                Some(authored.cancellation()),
                |carrier| {
                    adapter::record_view_change(
                        &authored.shared_checkout().ledger,
                        &ViewChange::Effect(&installed),
                        carrier,
                    )
                },
                None,
                |publication, _| publication,
            )
            .await?;
        if installed.is_some() {
            return authored
                .shared_checkout()
                .publish_mutation_with_carrier(
                    carrier,
                    &cancellation,
                    Some(authored.cancellation()),
                    authored.callback_guard(),
                )
                .await;
        }
    }
    Err(MountSourceError::Stale)
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) async fn promote(
        &self,
        mounted: &MountPath,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError> {
        let (path, _allocation) = self.path(mounted, original)?;
        let resolution = self
            .resolve(mounted, &path, SourceViewGate::callback_owner(), original)
            .await?;
        let (lookup, _allocation) = match resolution {
            Resolution::Authored(_) => return Ok(()),
            Resolution::Absent => return Err(MountSourceError::NotFound),
            Resolution::Unauthored(lookup, _, allocation) => (lookup, allocation),
        };
        let expected_source = file_id(&self.lazy, &path, &lookup, original).await?;
        let mut work =
            crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, self.authored.cancellation());
        let mut carrier = match original {
            Some(work) => adapter::NativeDemandCarrier::Native(work),
            None => adapter::NativeDemandCarrier::Borrowed(&mut work),
        };
        stage_mount_promotion(
            &self.lazy,
            &self.authored,
            &path,
            expected_source,
            &mut carrier,
        )
        .await
    }

    pub(super) fn promote_parents_locked(
        &self,
        path: &MountPath,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        for length in 1..path.components().len() {
            let (parent, _allocation) = match original {
                Some(work) => {
                    let (path, allocation) = copy_path(&path.components()[..length], None, work)?;
                    (path, Some(allocation))
                }
                None => (
                    MountPath {
                        components: path.components()[..length].to_vec(),
                    },
                    None,
                ),
            };
            self.wait(|| self.promote(&parent, original))?;
        }
        Ok(())
    }

    pub(super) fn promote_subtree_locked(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), MountSourceError> {
        use crate::kernel::allocation::{
            KernelAccounting, KernelOwnedAllocation, LogicalVecCapacity,
        };
        let mut accounting = KernelAccounting::native(work, work.cancellation());
        let mut allocation = KernelOwnedAllocation::NativeOriginal(
            work.claim(0, 0, crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork)?,
        );
        let mut capacity = LogicalVecCapacity::default();
        let mut directories = Vec::<(MountPath, crate::NativeAllocation)>::new();
        let root = copy_path(path.components(), None, work)?;
        capacity
            .ensure_for_push_with_accounting(
                &mut directories,
                usize::MAX,
                &mut allocation,
                &mut accounting,
            )
            .map_err(mount_engine_error)?;
        directories.push(root);
        while let Some((directory, _allocation)) = directories.pop() {
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(MountSourceError::NativeWork)?;
            let mut unresolved_allocation = KernelOwnedAllocation::NativeOriginal(
                work.claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(MountSourceError::NativeWork)?,
            );
            let mut unresolved_capacity = LogicalVecCapacity::default();
            let mut unresolved = Vec::new();
            let mut cursor = None;
            loop {
                let page = self.directory_page(
                    &directory,
                    cursor.as_ref().map(
                        |(bytes, _): &(Vec<u8>, crate::demand::SourcePayloadOwnership)| {
                            bytes.as_slice()
                        },
                    ),
                    DIRECTORY_PROMOTION_PAGE,
                    work,
                )?;
                for entry in page.entries() {
                    let (child, allocation) =
                        copy_path(directory.components(), Some(&entry.name), work)?;
                    if self.authored.lookup_with_meter(&child, work)?.is_none() {
                        unresolved_capacity
                            .ensure_for_push_with_accounting(
                                &mut unresolved,
                                usize::MAX,
                                &mut unresolved_allocation,
                                &mut accounting,
                            )
                            .map_err(mount_engine_error)?;
                        unresolved.push((
                            child,
                            allocation,
                            entry.node.kind == MountNodeKind::Directory,
                        ));
                    }
                }
                let (_, next, ownership) = page.into_parts();
                cursor = next.map(|bytes| (bytes, ownership));
                if cursor.is_none() {
                    break;
                }
            }
            self.promote_locked(&directory, Some(work))?;
            for (child, child_allocation, directory) in unresolved.drain(..) {
                if directory {
                    capacity
                        .ensure_for_push_with_accounting(
                            &mut directories,
                            usize::MAX,
                            &mut allocation,
                            &mut accounting,
                        )
                        .map_err(mount_engine_error)?;
                    directories.push((child, child_allocation));
                } else {
                    self.promote_locked(&child, Some(work))?;
                }
            }
        }
        Ok(())
    }
}
