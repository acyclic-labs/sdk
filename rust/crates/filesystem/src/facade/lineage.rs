use super::*;
use crate::kernel::allocation::FixedIndex;

type GenerationAncestors = FixedIndex<Option<VolumeId>, { ObjectId::ENCODED_LEN }>;

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
    pub(super) target_root: Owned<GenerationRoot>,
    /// The parent generation that the fork is based on.
    pub(super) base_object: ObjectId,
    pub(super) base_root: Owned<GenerationRoot>,
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
        find_first_generation_ancestor(
            &self.inner.objects,
            source.id,
            source.workspace.volume.config,
            maximum_generations,
            (&target_ancestors, target_truncated),
            accounting,
            cancellation,
        )
        .await
    }

    /// Computes a plaintext source digest with the target's scalar identity.
    pub(crate) async fn workspace_normalized_join_parent(
        &self,
        source: &crate::Generation<A, O>,
        target: &crate::Generation<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Digest, WorkspaceError> {
        let mut scope = accounting.allocation_scope()?;
        let accounting = &mut *scope;
        accounting.check()?;
        if !Arc::ptr_eq(&self.inner, &source.workspace.volume.fs.inner)
            || !Arc::ptr_eq(&self.inner, &target.workspace.volume.fs.inner)
            || source.workspace.volume.config != target.workspace.volume.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let mut source_root = read_generation_root_with_accounting(
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
        source_root.0.volume_id = target.workspace.volume.id;
        source_root.0.required_features = source_root.0.continuation_features();
        let encoded =
            crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| {
                crate::kernel::encode_generation_root(&source_root.0, observe, allocation_error)
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
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<RebaseLineage<A, O>, WorkspaceError> {
        let mut scope = accounting.allocation_scope()?;
        let accounting = &mut *scope;
        accounting.check()?;
        let (target_object, target_head) = target
            .resolve_head_generation(accounting, cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        let target_root = self
            .read_generation_owned_with_accounting(
                target_object,
                target.config,
                accounting,
                cancellation,
            )
            .await?;
        if target_root.value.volume_id != target.id {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }

        let mut cursor = target_object;
        let mut cursor_root = None;
        let mut adopted = None;
        let mut filtered_baseline = None;
        for depth in 0_u64.. {
            if maximum_generations.is_some_and(|maximum| depth >= u64::from(maximum)) {
                break;
            }
            if depth != 0 {
                cursor_root = Some(
                    self.read_generation_owned_with_accounting(
                        cursor,
                        target.config,
                        accounting,
                        cancellation,
                    )
                    .await?,
                );
            }
            let root = cursor_root.as_ref().unwrap_or(&target_root);
            if root.value.volume_id != target.id {
                adopted = Some((
                    cursor,
                    cursor_root
                        .take()
                        .ok_or(WorkspaceError::IncompatibleWorkspace)?,
                ));
                break;
            }
            if filtered_baseline.is_none()
                && root.value.required_features & GenerationRoot::FILTERED_FORK != 0
            {
                let (value, allocation) =
                    crate::kernel::clone_generation_root_with_accounting(&root.value, accounting)?;
                let bytes = root_parent_bytes(&value)?;
                filtered_baseline = Some(Owned::new(value, allocation, bytes));
            }
            let Some(parent) = root.value.parents.first() else {
                return Err(WorkspaceError::NotFork);
            };
            cursor = parent.object_id();
            if let Some(root) = cursor_root.take() {
                root.retire(accounting)?;
            }
        }
        let Some((base_object, base_root)) = adopted else {
            return Err(WorkspaceError::LineageLimit);
        };
        let source = self
            .open_volume_with_accounting(base_root.value.volume_id, accounting, cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        if source.config != target.config
            || base_root.value.root_file_id != target_root.value.root_file_id
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        let (source_object, _) = source
            .resolve_head_generation(accounting, cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        let base_root = if let Some(filtered) = filtered_baseline {
            base_root.retire(accounting)?;
            filtered
        } else {
            base_root
        };
        let bytes = target_root
            .bytes
            .checked_add(base_root.bytes)
            .ok_or(WorkError::Overflow)?;
        let lineage = RebaseLineage {
            target_object,
            target_head,
            target_root,
            base_object,
            base_root,
            source,
            source_object,
        };
        scope.keep_bytes(bytes)?;
        Ok(lineage)
    }

    /// The parent head that a fork would rebase onto, under the supplied work.
    pub(crate) async fn parent_advance<W: FsWork>(
        &self,
        target: &Volume<A, O>,
        maximum_generations: Option<u32>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<GenerationId>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut scope = accounting.allocation_scope()?;
            let lineage = self
                .rebase_lineage(target, maximum_generations, &mut scope, cancellation)
                .await?;
            Ok((lineage.source_object != lineage.base_object)
                .then(|| GenerationId::from_root(lineage.source_object)))
        }
        .await;
        work.finish(result, &accounting)
    }
}

fn append_lineage_value<T: Copy>(
    values: &mut Owned<Vec<T>>,
    value: T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    values.ensure_for_push(usize::MAX, accounting)?;
    let copied = WorkCounters {
        bytes_copied: size_of::<T>() as u64,
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
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationAncestors, bool), WorkspaceError> {
    let mut visited = GenerationAncestors::default();
    let mut pending = Owned::<Vec<GenerationId>>::allocate(0, accounting)?;
    let mut cursor = 0;
    let mut examined = 0_u64;
    let result = async {
        append_lineage_value(&mut pending, start, accounting)?;
        while let Some(generation) = next_lineage_value(&pending.value, &mut cursor, accounting)? {
            let entry = visited.get_or_try_insert_with(
                generation.object_id().index_key(),
                accounting,
                |accounting| lineage_index_value(None, accounting),
            )?;
            if entry.is_some() {
                continue;
            }
            if maximum_generations.is_some_and(|maximum| examined >= u64::from(maximum)) {
                // Keep every unread frontier identity for direction-independent joins.
                while let Some(frontier) =
                    next_lineage_value(&pending.value, &mut cursor, accounting)?
                {
                    visited.get_or_try_insert_with(
                        frontier.object_id().index_key(),
                        accounting,
                        |accounting| lineage_index_value(None, accounting),
                    )?;
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
            let root = Owned::new(decoded.0, decoded.1, bytes);
            let expanded = (|| -> Result<(), FsError> {
                let copied = WorkCounters {
                    bytes_copied: size_of::<Option<VolumeId>>() as u64,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                *entry = Some(root.value.volume_id);
                accounting.record_completed(copied)?;
                examined = examined.checked_add(1).ok_or(WorkError::Overflow)?;
                for parent in &root.value.parents {
                    append_lineage_value(&mut pending, *parent, accounting)?;
                }
                Ok(())
            })();
            root.retire(accounting)?;
            expanded?;
        }
        Ok::<_, WorkspaceError>(false)
    }
    .await;
    pending.retire(accounting)?;
    result.map(|truncated| (visited, truncated))
}

fn lineage_index_value<T: Copy>(
    value: T,
    accounting: &mut KernelAccounting<'_>,
) -> Result<T, FsError> {
    accounting.charge(WorkCounters {
        bytes_copied: (ObjectId::ENCODED_LEN + size_of::<T>()) as u64,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(value)
}

pub(super) async fn find_first_generation_ancestor<O: AsyncObjectStore>(
    objects: &O,
    start: GenerationId,
    config: VolumeConfig,
    maximum_generations: Option<u32>,
    candidates: (&GenerationAncestors, bool),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<WorkspaceCommonAncestor, WorkspaceError> {
    let (candidates, candidates_truncated) = candidates;
    let mut visited = FixedIndex::<(), { ObjectId::ENCODED_LEN }>::default();
    let mut pending =
        Owned::<Vec<(GenerationId, Option<WorkspaceCommonAncestor>)>>::allocate(0, accounting)?;
    let mut cursor = 0;
    let mut examined = 0_u64;
    let result = async {
        append_lineage_value(&mut pending, (start, None), accounting)?;
        while let Some((generation, baseline)) =
            next_lineage_value(&pending.value, &mut cursor, accounting)?
        {
            let key = generation.object_id().index_key();
            let candidate = candidates.get(key, accounting)?.copied();
            if let Some(Some(volume_id)) = candidate {
                return Ok(baseline.unwrap_or(WorkspaceCommonAncestor {
                    id: generation,
                    volume_id,
                }));
            }
            let is_candidate_frontier = matches!(candidate, Some(None));
            if maximum_generations.is_some_and(|maximum| examined >= u64::from(maximum)) {
                if visited.get(key, accounting)?.is_some() {
                    continue;
                }
                return Err(WorkspaceError::LineageLimit);
            }
            let mut fresh = false;
            visited.get_or_try_insert_with(key, accounting, |accounting| {
                lineage_index_value((), accounting)?;
                fresh = true;
                Ok(())
            })?;
            if !fresh {
                continue;
            }
            examined = examined.checked_add(1).ok_or(WorkError::Overflow)?;
            let decoded = crate::kernel::read_generation_root_with_accounting(
                objects,
                generation.object_id(),
                config,
                accounting,
                cancellation,
            )
            .await?;
            let bytes = root_parent_bytes(&decoded.0)?;
            let root = Owned::new(decoded.0, decoded.1, bytes);
            let expanded = (|| -> Result<Option<WorkspaceCommonAncestor>, FsError> {
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
            Err(WorkspaceError::LineageLimit)
        } else {
            Err(WorkspaceError::NoCommonAncestor)
        }
    }
    .await;
    pending.retire(accounting)?;
    result
}
