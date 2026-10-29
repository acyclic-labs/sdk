use super::*;

/// Retains the authenticated frontier until the proof ledger's adoption lock.
pub(super) struct DisjointChanges {
    remote: crate::kernel::GenerationFrontier,
    local: Owned<GenerationDiff>,
}

impl DisjointChanges {
    pub(super) fn preserves(
        &self,
        dependencies: &CheckoutDependencies,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, FsError> {
        let empty = GenerationDiff {
            files: Vec::new(),
            bindings: Vec::new(),
            truncated: false,
        };
        self.remote.independent(
            crate::kernel::CommitFootprint {
                reads: Some(dependencies),
                writes: &empty,
            },
            accounting,
        )
    }

    pub(super) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let local = self.local.retire(accounting);
        self.remote.retire(accounting).and(local)
    }

    pub(super) async fn compose<O: AsyncObjectStore>(
        &self,
        objects: &O,
        candidate: ObjectId,
        root: &GenerationRoot,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(GenerationRoot, KernelOwnedAllocation), FsError> {
        let table = crate::kernel::apply_generation_delta(
            objects,
            root.file_table,
            &self.local.value,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let mut parents = Owned::<Vec<GenerationId>>::allocate(1, accounting)?;
        parents.value.push(GenerationId::from_root(candidate));
        let Owned {
            value: parents,
            allocation,
            ..
        } = parents;
        Ok((
            GenerationRoot {
                volume_id: root.volume_id,
                root_file_id: root.root_file_id,
                file_table: table,
                parents,
                required_features: root.continuation_features(),
            },
            allocation,
        ))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(super) async fn head_changes_disjoint(
        &self,
        candidate: ObjectId,
        candidate_head: Head,
        dependencies: &CheckoutDependencies,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<DisjointChanges>, FsError> {
        use crate::kernel::Owned;
        // An exact generation has no authenticated position in this authority.
        // Only the ordinary semantic rebase can establish its first head.
        let Some(base_head) = self.base_authority_head() else {
            return Ok(None);
        };
        let (root, allocation) = crate::kernel::read_generation_root_with_accounting(
            &self.volume.fs.inner.objects,
            candidate,
            self.volume.config,
            accounting,
            cancellation,
        )
        .await?;
        let bytes = root_parent_bytes(&root)?;
        let root = Owned::new(root, allocation, bytes);
        let result = async {
            if root.value.volume_id != self.volume.id
                || root.value.root_file_id != self.base_root.root_file_id
            {
                return Err(FsError::VolumeMismatch);
            }
            let maximum = u32::try_from(accounting.remaining()?.items_examined).unwrap_or(u32::MAX);
            if maximum == 0 {
                return Ok(None);
            }
            let changes = crate::kernel::generation_frontier(
                &self.volume.fs.inner.authority,
                &self.volume.fs.inner.objects,
                crate::kernel::FrontierRequest {
                    volume: self.volume.id,
                    base: (base_head, &self.base_root),
                    head: (candidate_head, candidate),
                    limits: DecodeLimits::for_volume(self.volume.config),
                },
                accounting,
                cancellation,
            )
            .await?;
            let local = diff_generation_file_tables(
                &self.volume.fs.inner.objects,
                self.base_root.file_table,
                self.root.file_table,
                maximum,
                DecodeLimits::for_volume(self.volume.config),
                accounting,
                cancellation,
            )
            .await;
            let result = match local {
                Ok((local, allocation)) => {
                    let bytes = local.retained_bytes()?;
                    let local = Owned::new(local, allocation, bytes);
                    let result = changes.independent(
                        crate::kernel::CommitFootprint {
                            reads: Some(dependencies),
                            writes: &local.value,
                        },
                        accounting,
                    );
                    if matches!(result, Ok(true)) {
                        return Ok(Some(DisjointChanges {
                            remote: changes,
                            local,
                        }));
                    }
                    local.retire(accounting).and(result)
                }
                Err(error) => Err(error),
            };
            changes.retire(accounting)?;
            result.map(|_| None)
        }
        .await;
        root.retire(accounting).and(result)
    }
}
