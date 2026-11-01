use super::*;
use crate::kernel::allocation::VisitedObjectSet;

#[derive(Clone, Copy)]
pub(crate) struct WorkspaceCommonAncestor {
    pub id: GenerationId,
    pub volume_id: VolumeId,
}

pub(crate) struct WorkspaceRebaseRequest<'a, A: 'static, O: 'static> {
    pub target: &'a Volume<A, O>,
    pub operation_id: OperationId,
    pub maximum_generations: Option<u32>,
    pub maximum_changes: Option<u32>,
    pub maximum_conflicts: Option<u32>,
}

/// Where a fork stands against the parent it was forked from.
pub(super) struct RebaseLineage<A: 'static, O: 'static> {
    pub(super) target_object: ObjectId,
    pub(super) target_head: Head,
    pub(super) target_root: GenerationRoot,
    /// The parent generation that the fork is based on.
    pub(super) base_object: ObjectId,
    pub(super) base_root: GenerationRoot,
    /// The parent.
    pub(super) source: Volume<A, O>,
    /// The parent's head.
    pub(super) source_object: ObjectId,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(crate) async fn workspace_common_ancestor_with_accounting(
        &self,
        source: &crate::Generation<A, O>,
        target: &crate::Generation<A, O>,
        maximum_generations: Option<u32>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceCommonAncestor, WorkspaceError> {
        if maximum_generations == Some(0)
            || !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || !Arc::ptr_eq(&self.inner, &target.workspace.volume.fs.inner)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if source.workspace.volume.config != target.workspace.volume.config {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        accounting.check()?;
        let (target_ancestors, target_truncated) = collect_generation_ancestors(
            &self.inner.objects,
            target.id,
            target.workspace.volume.config,
            maximum_generations,
            accounting,
            cancellation,
        )
        .await?;
        let result = find_first_generation_ancestor(
            &self.inner.objects,
            source.id,
            source.workspace.volume.config,
            maximum_generations,
            (&target_ancestors.value, target_truncated),
            accounting,
            cancellation,
        )
        .await;
        target_ancestors.retire(accounting)?;
        result
    }

    /// Computes the plaintext source-parent digest used by a true merge.
    pub(crate) async fn workspace_normalized_join_parent(
        &self,
        source: &crate::Generation<A, O>,
        target: &crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Digest, WorkspaceError> {
        accounting.check()?;
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || !Arc::ptr_eq(&self.inner, &target.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.workspace.volume.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let source_root = read_generation_root_with_accounting(
            &self.inner.objects,
            source.id.object_id(),
            source.workspace.volume.config,
            accounting,
            cancellation,
        )
        .await?;
        let target_root = read_generation_root_with_accounting(
            &self.inner.objects,
            target.id.object_id(),
            target.workspace.volume.config,
            accounting,
            cancellation,
        )
        .await?;
        if source_root.0.volume_id != source.workspace.volume.id
            || target_root.0.volume_id != target.workspace.volume.id
            || source_root.0.root_file_id != target_root.0.root_file_id
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let normalized = (
            GenerationRoot {
                volume_id: target.workspace.volume.id,
                root_file_id: target_root.0.root_file_id,
                file_table: source_root.0.file_table,
                required_features: source_root.0.continuation_features(),
                parents: source_root.0.parents,
            },
            source_root.1,
        );
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_generation_root(&normalized.0, observe, allocation_error)
            })?;
        let hashing = WorkCounters {
            bytes_hashed: (encoded.0.len() as u64),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let digest = object_digest(&encoded.0);
        accounting.record_completed(hashing)?;
        Ok(digest)
    }

    /// Resolves where a fork stands against the parent it was forked from:
    pub(super) async fn rebase_lineage(
        &self,
        target: &Volume<A, O>,
        maximum_generations: Option<u32>,
        cancellation: &CancellationToken,
    ) -> Result<RebaseLineage<A, O>, WorkspaceError> {
        let (target_object, target_head) = target
            .resolve_head_generation(
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation),
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let (target_root, _) = read_generation_root(
            &self.inner.objects,
            target_object,
            target.config,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;

        let mut cursor = target_object;
        let mut adopted = None;
        let mut filtered_baseline = None;
        let mut examined = 0_u64;
        loop {
            if maximum_generations.is_some_and(|maximum| examined >= u64::from(maximum)) {
                break;
            }
            examined = examined.checked_add(1).ok_or(WorkError::Overflow)?;
            let (root, _) = read_generation_root(
                &self.inner.objects,
                cursor,
                target.config,
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
            if root.volume_id != target.id {
                adopted = Some((cursor, root));
                break;
            }
            if filtered_baseline.is_none()
                && root.required_features & GenerationRoot::FILTERED_FORK != 0
            {
                filtered_baseline = Some(root.clone());
            }
            let Some(parent) = root.parents.first() else {
                return Err(WorkspaceError::NotFork);
            };
            cursor = parent.object_id();
        }
        let Some((base_object, base_root)) = adopted else {
            return Err(WorkspaceError::LineageLimit);
        };
        let source = self
            .open_volume(base_root.volume_id, WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(WorkspaceError::engine)?
            .value;
        if source.config != target.config || base_root.root_file_id != target_root.root_file_id {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let (source_object, _) = source
            .resolve_head_generation(
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation),
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(RebaseLineage {
            target_object,
            target_head,
            target_root,
            base_object,
            base_root: filtered_baseline.unwrap_or(base_root),
            source,
            source_object,
        })
    }

    /// The parent head that a fork would rebase onto, or `None` when the fork
    pub(crate) async fn parent_advance(
        &self,
        target: &Volume<A, O>,
        maximum_generations: Option<u32>,
    ) -> Result<Option<GenerationId>, WorkspaceError> {
        let lineage = self
            .rebase_lineage(target, maximum_generations, &CancellationToken::new())
            .await?;
        Ok((lineage.source_object != lineage.base_object)
            .then(|| GenerationId::from_root(lineage.source_object)))
    }
}

pub(super) fn append_lineage_value<T: Copy>(
    values: &mut Owned<Vec<T>>,
    value: T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    values.ensure_for_push(usize::MAX, accounting)?;
    let copied = WorkCounters {
        bytes_copied: usize_to_u64(size_of::<T>()),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    values.value.push(value);
    accounting.record_completed(copied)
}

pub(super) fn next_lineage_value<T: Copy>(
    pending: &[T],
    cursor: &mut usize,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<Option<T>, FsError> {
    let Some(value) = pending.get(*cursor) else {
        return Ok(None);
    };
    let copied = WorkCounters {
        items_examined: 1,
        bytes_copied: size_of::<T>() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let value = *value;
    *cursor += 1;
    accounting.record_completed(copied)?;
    Ok(Some(value))
}

pub(super) async fn collect_generation_ancestors<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: Option<u32>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<
    (
        crate::kernel::Owned<Vec<(GenerationId, Option<VolumeId>)>>,
        bool,
    ),
    crate::workspace::WorkspaceError,
> {
    let maximum = maximum_generations
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(usize::MAX);
    let mut visited =
        crate::kernel::Owned::<Vec<(GenerationId, Option<VolumeId>)>>::allocate(0, accounting)?;
    let mut pending = crate::kernel::Owned::<Vec<GenerationId>>::allocate(0, accounting)?;
    let mut seen = VisitedObjectSet::new_with_accounting(accounting)?;
    let mut cursor = 0;
    let result = async {
        append_lineage_value(&mut pending, start, accounting)?;
        while let Some(generation) = next_lineage_value(&pending.value, &mut cursor, accounting)? {
            if !VisitedObjectSet::insert_with_accounting(
                &mut seen,
                generation.object_id(),
                accounting,
            )?
            .inserted
            {
                continue;
            }
            if visited.value.len() == maximum {
                // Preserve the exact unread frontier for explicit finite policies.
                append_lineage_value(&mut visited, (generation, None), accounting)?;
                while let Some(frontier) =
                    next_lineage_value(&pending.value, &mut cursor, accounting)?
                {
                    if VisitedObjectSet::insert_with_accounting(
                        &mut seen,
                        frontier.object_id(),
                        accounting,
                    )?
                    .inserted
                    {
                        append_lineage_value(&mut visited, (frontier, None), accounting)?;
                    }
                }
                return Ok(true);
            }
            let decoded = crate::kernel::read_generation_root_with_accounting(
                objects,
                generation.object_id(),
                config,
                accounting,
                cancellation,
            )
            .await?;
            let bytes = root_parent_bytes(&decoded.0)?;
            let root = crate::kernel::Owned::new(decoded.0, decoded.1, bytes);
            let expanded = (|| -> Result<(), FsError> {
                append_lineage_value(
                    &mut visited,
                    (generation, Some(root.value.volume_id)),
                    accounting,
                )?;
                for parent in &root.value.parents {
                    append_lineage_value(&mut pending, *parent, accounting)?;
                }
                Ok(())
            })();
            root.retire(accounting)?;
            expanded?;
        }
        Ok::<_, crate::workspace::WorkspaceError>(false)
    }
    .await;
    pending.retire(accounting)?;
    seen.retire(accounting)?;
    let result = result.and_then(|truncated| {
        crate::kernel::sort::heap_sort(
            &mut visited.value,
            accounting,
            |left, right, accounting| {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                Ok::<_, FsError>(left.0.cmp(&right.0))
            },
            |values, left, right, accounting| {
                let copied = WorkCounters {
                    bytes_copied: (2 * size_of::<(GenerationId, Option<VolumeId>)>()) as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                values.swap(left, right);
                accounting.record_completed(copied)
            },
            |_| Ok(()),
        )?;
        Ok(truncated)
    });
    match result {
        Ok(truncated) => Ok((visited, truncated)),
        Err(error) => {
            visited.retire(accounting)?;
            Err(error)
        }
    }
}

pub(super) async fn find_first_generation_ancestor<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: Option<u32>,
    candidates: (&[(GenerationId, Option<VolumeId>)], bool),
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<WorkspaceCommonAncestor, crate::workspace::WorkspaceError> {
    let (candidates, candidates_truncated) = candidates;
    let maximum = maximum_generations
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(usize::MAX);
    let mut visited = VisitedObjectSet::new_with_accounting(accounting)?;
    let mut examined = 0_usize;
    let mut pending =
        crate::kernel::Owned::<Vec<(GenerationId, Option<WorkspaceCommonAncestor>)>>::allocate(
            0, accounting,
        )?;
    let mut cursor = 0;
    let result = async {
        append_lineage_value(&mut pending, (start, None), accounting)?;
        while let Some((generation, baseline)) =
            next_lineage_value(&pending.value, &mut cursor, accounting)?
        {
            let candidate = crate::kernel::binary_search_with_accounting(
                candidates,
                accounting,
                |candidate| candidate.0.cmp(&generation),
            )?
            .ok()
            .and_then(|index| candidates.get(index))
            .map(|candidate| candidate.1);
            if let Some(Some(volume_id)) = candidate {
                return Ok(baseline.unwrap_or(WorkspaceCommonAncestor {
                    id: generation,
                    volume_id,
                }));
            }
            let is_candidate_frontier = matches!(candidate, Some(None));
            if !VisitedObjectSet::insert_with_accounting(
                &mut visited,
                generation.object_id(),
                accounting,
            )?
            .inserted
            {
                continue;
            }
            if examined == maximum {
                return Err(crate::workspace::WorkspaceError::LineageLimit);
            }
            let decoded = crate::kernel::read_generation_root_with_accounting(
                objects,
                generation.object_id(),
                config,
                accounting,
                cancellation,
            )
            .await?;
            let bytes = root_parent_bytes(&decoded.0)?;
            let root = crate::kernel::Owned::new(decoded.0, decoded.1, bytes);
            let expanded = (|| -> Result<Option<WorkspaceCommonAncestor>, FsError> {
                examined = examined.checked_add(1).ok_or(WorkError::Overflow)?;
                if is_candidate_frontier {
                    return Ok(Some(baseline.unwrap_or(WorkspaceCommonAncestor {
                        id: generation,
                        volume_id: root.value.volume_id,
                    })));
                }
                // A filtered fork's initial namespace is its delta baseline.
                let boundary = if baseline.is_none()
                    && root.value.required_features & GenerationRoot::FILTERED_FORK != 0
                {
                    Some(WorkspaceCommonAncestor {
                        id: generation,
                        volume_id: root.value.volume_id,
                    })
                } else {
                    baseline
                };
                for parent in &root.value.parents {
                    append_lineage_value(&mut pending, (*parent, boundary), accounting)?;
                }
                Ok(None)
            })();
            root.retire(accounting)?;
            if let Some(ancestor) = expanded? {
                return Ok(ancestor);
            }
        }
        if candidates_truncated {
            Err(crate::workspace::WorkspaceError::LineageLimit)
        } else {
            Err(crate::workspace::WorkspaceError::NoCommonAncestor)
        }
    }
    .await;
    pending.retire(accounting)?;
    visited.retire(accounting)?;
    result
}
