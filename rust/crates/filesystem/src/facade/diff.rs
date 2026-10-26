use super::*;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};

/// Terminal result of preparing an unpublished two-parent merge generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergePreparation {
    /// The checkout now holds this candidate and may commit or discard it.
    Prepared {
        /// Authenticated two-parent generation identity.
        generation_id: GenerationId,
    },
    /// No checkout state changed; exact conflicts are bounded by the caller.
    Conflicted {
        /// Stable conflicting regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the supplied bound.
        truncated: bool,
    },
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_join_changes<W: FsWork>(
        &self,
        base: GenerationId,
        source: &crate::Generation<A, O>,
        maximum_changes: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), crate::workspace::WorkspaceError> {
        use crate::workspace::WorkspaceError;
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if maximum_changes == 0 || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let config = source.workspace.volume.config;
            let (base_root, base_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: base.digest(),
                    },
                    config,
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::Filesystem)?;
            let bytes = crate::foundation::usize_to_u64(
                base_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let base_root = crate::kernel::Owned::new(base_root, base_root_allocation, bytes);
            let (source_root, source_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: source.id.digest(),
                    },
                    config,
                    &mut accounting,
                    cancellation,
                )
                .await
                .map_err(WorkspaceError::Filesystem)?;
            let bytes = crate::foundation::usize_to_u64(
                source_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let source_root = crate::kernel::Owned::new(source_root, source_root_allocation, bytes);
            if source_root.value.volume_id != source.workspace.volume.id
                || base_root.value.root_file_id != source_root.value.root_file_id
            {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let output = diff_generation_file_tables(
                &self.inner.objects,
                base_root.value.file_table,
                source_root.value.file_table,
                maximum_changes,
                DecodeLimits::for_volume(config),
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::Filesystem)?;
            base_root.retire(&mut accounting)?;
            source_root.retire(&mut accounting)?;
            if output.0.truncated {
                return Err(WorkspaceError::JoinLimit);
            }
            Ok(output)
        }
        .await;
        work.finish(result, &accounting)
    }
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    /// Computes one bounded semantic diff without scanning equal Merkle subtrees.
    pub async fn diff_generations(
        &self,
        before: GenerationId,
        after: GenerationId,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationDiff> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            if maximum_changes == 0 {
                return Err(FsError::InvalidDiff);
            }
            if before == after {
                return Ok(GenerationDiff {
                    files: Vec::new(),
                    bindings: Vec::new(),
                    truncated: false,
                });
            }
            let (before_root, before_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.fs.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: before.digest(),
                    },
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let bytes = crate::foundation::usize_to_u64(
                before_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let before_root = crate::kernel::Owned::new(before_root, before_root_allocation, bytes);
            let (after_root, after_root_allocation) =
                crate::kernel::read_generation_root_with_accounting(
                    &self.fs.inner.objects,
                    ObjectId {
                        kind: ObjectKind::GenerationRoot,
                        digest: after.digest(),
                    },
                    self.config,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let bytes = crate::foundation::usize_to_u64(
                after_root.parents.capacity() * size_of::<GenerationId>(),
            );
            let after_root = crate::kernel::Owned::new(after_root, after_root_allocation, bytes);
            if before_root.value.volume_id != self.id || after_root.value.volume_id != self.id {
                return Err(FsError::VolumeMismatch);
            }
            let (diff, _allocation) = diff_generation_file_tables(
                &self.fs.inner.objects,
                before_root.value.file_table,
                after_root.value.file_table,
                maximum_changes,
                DecodeLimits::for_volume(self.config),
                &mut accounting,
                cancellation,
            )
            .await?;
            before_root.retire(&mut accounting)?;
            after_root.retire(&mut accounting)?;
            Ok(diff)
        }
        .await;
        work::measured(result, &accounting)
    }
}
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    #[cfg(feature = "native-mount")]
    pub(crate) async fn candidate_diff<W: FsWork>(
        &self,
        candidate: &Self,
        maximum_changes: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(GenerationDiff, KernelOwnedAllocation), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if candidate.volume.id != self.volume.id {
                return Err(FsError::VolumeMismatch);
            }
            diff_generation_file_tables(
                &self.volume.fs.inner.objects,
                self.root.file_table,
                candidate.root.file_table,
                maximum_changes,
                DecodeLimits::for_volume(self.volume.config),
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        work.finish(result, &accounting)
    }
}
