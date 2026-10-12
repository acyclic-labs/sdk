use super::*;

impl WorkspaceLineageStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.load_logged(workspace_id).await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_logged(workspace_id, expected_revision, replacement)
            .await
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        Option<NativeOwnedCoreState<WorkspaceLineageRecord>>,
        (
            crate::lineage::WorkspaceLineageError<Self::Error>,
            Option<crate::materializer::NativeAllocation>,
        ),
    > {
        native_records::check(original, cancellation)
            .map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))?;
        let cancellation = cancellation.clone();
        self.materialization(original.clone())
            .transaction(move |root| {
                let root = &root;
                let work = root._allocation.as_ref().expect("bound native work").work();
                root.with_log(|log| {
                    native_records::load_native_lineage(
                        &root,
                        log,
                        workspace_id,
                        work,
                        &cancellation,
                    )
                })
            })
            .await
            .map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: NativeOwnedCoreState<WorkspaceLineageRecord>,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        bool,
        (
            crate::lineage::WorkspaceLineageError<Self::Error>,
            Option<crate::materializer::NativeAllocation>,
        ),
    > {
        native_records::check(original, cancellation)
            .map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))?;
        let replacement = native_records::checked_packet(replacement, original, |value| {
            native_records::native_lineage_owned_bytes(value, original)
        })
        .map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))?;
        let cancellation = cancellation.clone();
        self.materialization(original.clone())
            .transaction(move |root| {
                let root = &root;
                let work = root._allocation.as_ref().expect("bound native work").work();
                root.with_log(|log| {
                    native_records::compare_native_lineage(
                        &root,
                        log,
                        workspace_id,
                        expected_revision,
                        replacement,
                        work,
                        &cancellation,
                    )
                })
            })
            .await
            .map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))
    }
}

impl WorkspaceContextStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        context_id: WorkspaceContextId,
    ) -> Result<Option<WorkspaceContext>, Self::Error> {
        self.load_logged(context_id).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        Option<NativeOwnedCoreState<WorkspaceContext>>,
        (
            crate::workspace_context::WorkspaceContextError<Self::Error>,
            Option<crate::materializer::NativeAllocation>,
        ),
    > {
        native_records::check(original, cancellation).map_err(|error| {
            (
                crate::workspace_context::WorkspaceContextError::Store(error),
                None,
            )
        })?;
        let cancellation = cancellation.clone();
        self.materialization(original.clone())
            .transaction(move |root| {
                let root = &root;
                let work = root._allocation.as_ref().expect("bound native work").work();
                root.with_log(|log| {
                    native_records::load_native_context(&root, log, context_id, work, &cancellation)
                })
            })
            .await
            .map_err(|error| {
                (
                    crate::workspace_context::WorkspaceContextError::Store(error),
                    None,
                )
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: NativeOwnedCoreState<WorkspaceContext>,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        bool,
        (
            crate::workspace_context::WorkspaceContextError<Self::Error>,
            Option<crate::materializer::NativeAllocation>,
        ),
    > {
        native_records::check(original, cancellation).map_err(|error| {
            (
                crate::workspace_context::WorkspaceContextError::Store(error),
                None,
            )
        })?;
        let replacement = native_records::checked_packet(replacement, original, |value| {
            native_records::native_context_owned_bytes(value, original)
        })
        .map_err(|error| {
            (
                crate::workspace_context::WorkspaceContextError::Store(error),
                None,
            )
        })?;
        let cancellation = cancellation.clone();
        self.materialization(original.clone())
            .transaction(move |root| {
                let root = &root;
                let work = root._allocation.as_ref().expect("bound native work").work();
                root.with_log(|log| {
                    native_records::compare_native_context(
                        &root,
                        log,
                        context_id,
                        expected_revision,
                        replacement,
                        work,
                        &cancellation,
                    )
                })
            })
            .await
            .map_err(|error| {
                (
                    crate::workspace_context::WorkspaceContextError::Store(error),
                    None,
                )
            })
    }

    async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
        self.transaction(move |root| {
            let root = &root;
            root.with_log(|log| {
                let contexts = load_all_contexts(root, log)?;
                if !context_children_index_ready(root, log)? {
                    log.commit(root, index_change(contexts.iter().cloned()))?;
                }
                Ok(contexts)
            })
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: WorkspaceContext,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_many(
            BTreeMap::from([(context_id, expected_revision)]),
            vec![replacement],
            false,
        )
        .await
    }

    async fn compare_and_swap_many(
        &self,
        expected_revisions: BTreeMap<WorkspaceContextId, u64>,
        replacements: Vec<WorkspaceContext>,
        require_exact_set: bool,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |root| {
            let root = &root;
            root.with_log(|log| {
                if require_exact_set
                    && live_context_ids(root, log)?.len() != expected_revisions.len()
                {
                    return Ok(false);
                }
                let mut before = BTreeMap::new();
                for (context_id, expected) in &expected_revisions {
                    let current = log.get::<WorkspaceContext>(root, *context_id)?;
                    if current.as_ref().map_or(0, Revisioned::revision) != *expected {
                        return Ok(false);
                    }
                    before.insert(*context_id, current);
                }
                let after = replacements
                    .into_iter()
                    .map(|replacement| (replacement.context_id, replacement))
                    .collect::<BTreeMap<_, _>>();
                if after
                    .keys()
                    .any(|context_id| !expected_revisions.contains_key(context_id))
                {
                    return Ok(false);
                }
                if !context_children_index_ready(root, log)? {
                    let contexts = load_all_contexts(root, log)?;
                    log.commit(root, index_change(contexts))?;
                }
                let affected_parents = before
                    .values()
                    .filter_map(|record| record.as_ref()?.parent_context_id)
                    .chain(after.values().filter_map(|record| record.parent_context_id))
                    .collect::<BTreeSet<_>>();
                let mut children = WorkspaceContextChildren::new();
                for parent in affected_parents {
                    let current = log.get::<ContextChildren>(root, parent)?;
                    children.insert(parent, current.unwrap_or_default().0);
                }
                for (context_id, replacement) in &after {
                    update_context_children(
                        &mut children,
                        before.get(context_id).and_then(Option::as_ref),
                        Some(replacement),
                    );
                }
                let mut change = Change::default();
                for (context_id, record) in after {
                    change.put(context_id, Some(record));
                }
                for (parent, descendants) in children {
                    change.put_children(parent, Some(descendants));
                }
                log.commit(root, change)?;
                Ok(true)
            })
        })
        .await
    }

    async fn discard_subtree(
        &self,
        parent: WorkspaceContextId,
        child: WorkspaceContextId,
        maximum: u32,
    ) -> Result<WorkspaceContextDiscardOutcome, Self::Error> {
        self.transaction(move |root_path| {
            let root_path = &root_path;
            root_path.with_log(|log| {
                if !context_children_index_ready(root_path, log)? {
                    return Ok(WorkspaceContextDiscardOutcome::IncompatibleState);
                }
                let root = log.get::<WorkspaceContext>(root_path, child)?;
                let discarded = match plan_local_context_subtree_discard(
                    root_path,
                    log,
                    root.as_ref(),
                    parent,
                    child,
                    maximum,
                )? {
                    Ok(discarded) => discarded,
                    Err(outcome) => return Ok(outcome),
                };
                let mut records = BTreeMap::from_iter(root.map(|record| (child, record)));
                for context_id in discarded.iter().copied().filter(|id| *id != child) {
                    let Some(record) = log.get::<WorkspaceContext>(root_path, context_id)? else {
                        return Ok(WorkspaceContextDiscardOutcome::IncompatibleState);
                    };
                    records.insert(context_id, record);
                }
                let selected_children = context_children(records.values().cloned());
                let outcome = discard_context_subtree(
                    &mut records,
                    &selected_children,
                    parent,
                    child,
                    maximum,
                );
                if !matches!(
                    &outcome,
                    WorkspaceContextDiscardOutcome::Discarded(actual) if actual == &discarded
                ) {
                    return Ok(match outcome {
                        WorkspaceContextDiscardOutcome::Discarded(_) => {
                            WorkspaceContextDiscardOutcome::IncompatibleState
                        }
                        outcome => outcome,
                    });
                }
                let WorkspaceContextDiscardOutcome::Discarded(discarded) = &outcome else {
                    return Ok(outcome);
                };
                let mut change = Change::default();
                for context_id in discarded {
                    let replacement = records.get(context_id).cloned().ok_or_else(|| {
                        LocalCoreStateStoreError::ContextTransaction(
                            "discarded context replacement is absent".to_owned(),
                        )
                    })?;
                    change.put(*context_id, Some(replacement));
                }
                log.commit(root_path, change)?;
                Ok(outcome)
            })
        })
        .await
    }
}

pub(super) const WORKSPACE_CONTEXT_FAMILY: &str = "workspace-contexts-v2";
pub(super) const CONTEXT_CHILDREN_FAMILY: &str = "workspace-context-children-v1";
pub(super) const CONTEXT_CHILDREN_INDEX_FAMILY: &str = "workspace-context-children-index-v1";
pub(super) const CONTEXT_CHILD_COUNT_FAMILY: &str = "workspace-context-child-counts-v1";
pub(super) const LINEAGE_FAMILY: &str = "lineage";
pub(super) const CORE_LOG_FAMILY: &str = "core-state-log-v1";
/// Commits an owner keeps in its log before writing their records back. Tests
pub(super) const OWNED_CHECKPOINT: u64 = if cfg!(test) { 3 } else { 256 };

impl LocalCoreStateStore {
    /// Loads one logged record, from an owner's memory when it is resident.
    pub(super) async fn load_logged<T: Logged>(
        &self,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        if let Ownership::Owned(owner) = &self.ownership
            && let Some(value) = owner.resident::<T>(id)
        {
            return Ok(value);
        }
        self.transaction(move |root| root.with_log(|log| log.get::<T>(&root, id)))
            .await
    }

    /// Commits `replacement` only while the record is at `expected_revision`
    pub(super) async fn compare_and_swap_logged<T: Logged + Revisioned>(
        &self,
        id: T::Id,
        expected_revision: u64,
        replacement: T,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.transaction(move |root| {
            let root = &root;
            root.with_log(|log| {
                if log.get::<T>(root, id)?.as_ref().map_or(0, T::revision) != expected_revision {
                    return Ok(false);
                }
                let mut change = Change::default();
                change.put(id, Some(replacement));
                log.commit(root, change)?;
                Ok(true)
            })
        })
        .await
    }
}

impl Owner {
    /// Returns a resident record (`Some(None)` for a known absence) without touching the log.
    pub(super) fn resident<T: Logged>(&self, id: T::Id) -> Option<Option<T>> {
        let residents = self.residents.read().ok()?;
        let value = residents.get(&T::key(id))?;
        if matches!(
            value,
            Some(RecordValue::FlatContext(_)) | Some(RecordValue::FlatChildren(_))
        ) {
            return None;
        }
        Some(value.as_ref().and_then(T::from_value).cloned())
    }

    pub(super) fn log(&self) -> MutexGuard<'_, Option<CoreLog>> {
        // A transaction that panics has taken the log, so a poisoned slot is
        // empty and the next transaction reopens and replays.
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Namespace {
    pub(super) fn with_log<T>(
        &self,
        operation: impl FnOnce(&mut CoreLog) -> Result<T, LocalCoreStateStoreError>,
    ) -> Result<T, LocalCoreStateStoreError> {
        match &self.admission {
            Admission::Shared { .. } => {
                let execute = |residents, allocation| {
                    let mut log = CoreLog::open(self, residents)?;
                    log.residents_control = allocation;
                    let result = operation(&mut log);
                    if !log.dirty.is_empty() && !log.broken {
                        drop(log.checkpoint(self));
                    }
                    result
                };
                let mut paths = self.record(CORE_LOG_FAMILY, &[0; 16])?;
                with_lock(&mut paths, |_| {
                    let control = self
                        ._allocation
                        .as_ref()
                        .map(|allocation| {
                            allocation.work().claim(
                                std::mem::size_of::<RwLock<Residents>>() as u64,
                                1,
                                crate::WorkCounters::UNCHARGED,
                            )
                        })
                        .transpose()?;
                    execute(Arc::default(), control)
                })
            }
            Admission::Owned(owner) => {
                let mut slot = owner.log();
                let mut log = match slot.take().filter(|log| !log.broken) {
                    Some(log) => log,
                    None => CoreLog::open(self, Arc::clone(&owner.residents))?,
                };
                let result = if log.sequence >= OWNED_CHECKPOINT {
                    log.checkpoint(self)
                } else {
                    Ok(())
                }
                .and_then(|()| operation(&mut log));
                *slot = Some(log);
                result
            }
        }
    }
}

pub(super) fn context_children_index_ready(
    root: &Namespace,
    log: &mut CoreLog,
) -> Result<bool, LocalCoreStateStoreError> {
    Ok(log.get::<ContextChildIndex>(root, ())?.is_some())
}

pub(super) fn plan_local_context_subtree_discard(
    root_path: &Namespace,
    log: &mut CoreLog,
    root: Option<&WorkspaceContext>,
    parent: WorkspaceContextId,
    child: WorkspaceContextId,
    maximum: u32,
) -> Result<Result<Vec<WorkspaceContextId>, WorkspaceContextDiscardOutcome>, LocalCoreStateStoreError>
{
    let mut selected = match plan_context_subtree_discard(
        root,
        &WorkspaceContextChildren::new(),
        parent,
        child,
        maximum,
    ) {
        Ok(selected) => selected,
        Err(outcome) => return Ok(Err(outcome)),
    };
    let maximum = maximum as usize;
    let mut pending = vec![child];
    let mut seen = BTreeSet::new();
    while let Some(context_id) = pending.pop() {
        if !seen.insert(context_id) || seen.len() > maximum {
            return Ok(Err(WorkspaceContextDiscardOutcome::TraversalLimit));
        }
        let count = log
            .get::<ContextChildCount>(root_path, context_id)?
            .map_or(0, |count| count.0);
        let remaining = maximum.saturating_sub(seen.len() + pending.len());
        if count > remaining as u64 {
            return Ok(Err(WorkspaceContextDiscardOutcome::TraversalLimit));
        }
        // UUIDs serialize as quoted 36-byte strings. This deliberately leaves
        // headroom for JSON delimiters while keeping allocation proportional
        // to the already-checked child count.
        let maximum_bucket_bytes = count
            .checked_mul(64)
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(|| {
                LocalCoreStateStoreError::ContextTransaction(
                    "workspace-context child bucket bound overflowed".to_owned(),
                )
            })?;
        let descendants = log
            .get_bounded::<ContextChildren>(root_path, context_id, Some(maximum_bucket_bytes))?
            .unwrap_or_default()
            .0;
        if usize::try_from(count) != Ok(descendants.len()) {
            return Ok(Err(WorkspaceContextDiscardOutcome::IncompatibleState));
        }
        pending.extend(descendants);
    }
    selected.clear();
    selected.extend(seen);
    Ok(Ok(selected))
}

/// Every context present in memory or on disk with a matching stored identity.
pub(super) fn live_context_ids(
    root: &Namespace,
    log: &mut CoreLog,
) -> Result<Vec<WorkspaceContextId>, LocalCoreStateStoreError> {
    let entries = match std::fs::read_dir(root.family(WORKSPACE_CONTEXT_FAMILY)) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let mut ids = BTreeSet::new();
    for entry in entries.into_iter().flatten() {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        let Ok(bytes) = hex::decode(stem) else {
            continue;
        };
        let Ok(bytes) = <[u8; 16]>::try_from(bytes) else {
            continue;
        };
        ids.insert(WorkspaceContextId::from_bytes(bytes));
    }
    ids.extend(log.resident_keys(|key| match key {
        RecordKey::Context(context_id) => Some(context_id),
        _ => None,
    }));
    let mut live = Vec::with_capacity(ids.len());
    for context_id in ids {
        if let Some(context) = log.get::<WorkspaceContext>(root, context_id)? {
            if context.context_id != context_id {
                return Err(LocalCoreStateStoreError::ContextTransaction(
                    "workspace context identity does not match its stored key".to_owned(),
                ));
            }
            live.push(context_id);
        }
    }
    live.sort_unstable_by_key(|id| id.into_bytes());
    Ok(live)
}

pub(super) fn load_all_contexts(
    root: &Namespace,
    log: &mut CoreLog,
) -> Result<Vec<WorkspaceContext>, LocalCoreStateStoreError> {
    live_context_ids(root, log)?
        .into_iter()
        .map(|id| {
            log.get::<WorkspaceContext>(root, id)?.ok_or_else(|| {
                LocalCoreStateStoreError::ContextTransaction(
                    "workspace context disappeared during locked discovery".to_owned(),
                )
            })
        })
        .collect()
}
