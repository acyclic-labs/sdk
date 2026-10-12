use super::*;

impl LocalCoreStateStore {
    /// Every file record a stored lazy shadow holds, which an object collection keeps: a lazy workspace may still resolve any of them.
    pub(crate) async fn lazy_shadow_records(
        &self,
    ) -> Result<Vec<crate::kernel::FileRecord>, LocalCoreStateStoreError> {
        self.transaction(|namespace| {
            let namespace = &namespace;
            let family = <crate::LazyShadow as LazyNode>::FAMILY;
            let entries = match std::fs::read_dir(namespace.family(family)) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error.into()),
            };
            let mut addresses = std::collections::BTreeSet::new();
            for entry in entries {
                let name = entry?.file_name();
                let Some(stem) = name.to_str().and_then(|name| name.strip_suffix(".json")) else {
                    continue;
                };
                let stem = stem.strip_suffix(".previous").unwrap_or(stem);
                if let Some(address) = hex::decode(stem)
                    .ok()
                    .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                {
                    addresses.insert(address);
                }
            }
            let mut records = Vec::new();
            for address in addresses {
                if let Some(crate::LazyShadow::Node { record, .. }) =
                    load_content_addressed::<crate::LazyShadow>(namespace, family, address)?
                {
                    records.push(
                        crate::kernel::decode_file_record(&record)
                            .map_err(|_| LocalCoreStateStoreError::Integrity)?,
                    );
                }
            }
            Ok(records)
        })
        .await
    }

    /// Exact generations retained by original forks and completed selected imports.
    pub(crate) async fn lazy_retained_generations(
        &self,
    ) -> Result<Vec<(crate::GenerationId, crate::model::VolumeConfig)>, LocalCoreStateStoreError>
    {
        self.transaction(|root| {
            let root = &root;
            root.with_log(|log| {
                let mut workspaces = log
                    .resident_keys(|key| match key {
                        RecordKey::LazyWorkspace(id) => Some(id),
                        _ => None,
                    })
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                for stem in family_stems(root, LAZY_WORKSPACE_FAMILY)? {
                    if let Ok(bytes) = <[u8; 16]>::try_from(stem.as_slice()) {
                        workspaces.insert(WorkspaceId::from_bytes(bytes));
                    }
                }
                let mut generations = Vec::new();
                for workspace in workspaces {
                    let Some(state) = log.get::<LazyWorkspaceState>(root, workspace)? else {
                        continue;
                    };
                    if let Some(origin) = state.fork_origin {
                        generations.push((origin.snapshot.authored_generation, origin.config));
                    }
                    if let Some(completed) = state.completed_selected_import {
                        generations.extend([
                            (completed.snapshot.authored_generation, completed.config),
                            (completed.destination.authored_generation, completed.config),
                            (
                                completed.source.authored_generation,
                                completed.source_config,
                            ),
                        ]);
                    }
                }
                Ok(generations)
            })
        })
        .await
    }
}

impl LocalCoreStateStore {
    /// Every workspace this store keeps a lineage, lazy binding, or Git-compatibility record for, with the parents those records name.
    pub(crate) async fn workspace_records(
        &self,
    ) -> Result<BTreeMap<WorkspaceId, Vec<WorkspaceId>>, LocalCoreStateStoreError> {
        self.transaction(|root| {
            let root = &root;
            root.with_log(|log| {
                let mut workspaces = log
                    .resident_keys(|key| match key {
                        RecordKey::Lineage(id) | RecordKey::LazyWorkspace(id) => Some(id),
                        _ => None,
                    })
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                for family in [LINEAGE_FAMILY, LAZY_WORKSPACE_FAMILY, GIT_COMPAT_NAMESPACE] {
                    for stem in family_stems(root, family)? {
                        if let Ok(bytes) = <[u8; 16]>::try_from(stem.as_slice()) {
                            workspaces.insert(WorkspaceId::from_bytes(bytes));
                        }
                    }
                }
                // A logged absence stays on disk as `null`.
                let mut recorded = BTreeMap::new();
                for workspace in workspaces {
                    let lineage = log.get::<WorkspaceLineageRecord>(root, workspace)?;
                    let lazy = log.get::<LazyWorkspaceState>(root, workspace)?;
                    let git = read_locked::<GitCompatState>(
                        &root.record(GIT_COMPAT_NAMESPACE, &workspace.into_bytes())?,
                    )?;
                    if lineage.is_none() && lazy.is_none() && git.is_none() {
                        continue;
                    }
                    // A durable freeze is retention metadata, not evidence that
                    // a destination filesystem workspace was ever created.
                    if lineage.is_none()
                        && git.is_none()
                        && lazy
                            .as_ref()
                            .and_then(|state| state.fork_origin)
                            .is_some_and(|origin| origin.phase == crate::LazyForkPhase::Prepared)
                    {
                        continue;
                    }
                    let mut parents: Vec<_> = lineage
                        .and_then(|record| record.parent_workspace_id)
                        .into_iter()
                        .chain(lazy.as_ref().and_then(|state| {
                            state
                                .fork_origin
                                .map(|origin| origin.snapshot.workspace_id)
                                .or(state.parent_workspace_id)
                        }))
                        .collect();
                    if let Some(completed) = lazy.and_then(|state| state.completed_selected_import)
                    {
                        for parent in [
                            completed.snapshot.workspace_id,
                            completed.destination.workspace_id,
                            completed.source.workspace_id,
                        ] {
                            if parent != workspace && !parents.contains(&parent) {
                                parents.push(parent);
                            }
                        }
                    }
                    recorded.insert(workspace, parents);
                }
                Ok(recorded)
            })
        })
        .await
    }

    /// Forgets every record of a deleted workspace: its lineage, its lazy binding, and its Git-compatibility state.
    pub(crate) async fn forget_workspace(
        &self,
        workspace: WorkspaceId,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.transaction(move |root| {
            let root = &root;
            root.with_log(|log| {
                let mut change = Change::default();
                change.put::<WorkspaceLineageRecord>(workspace, None);
                change.put::<LazyWorkspaceState>(workspace, None);
                log.commit(root, change)
            })?;
            // Unsynchronized: a removal a crash undoes is found again by the
            // next collection.
            let paths = root.record(GIT_COMPAT_NAMESPACE, &workspace.into_bytes())?;
            for path in [
                &paths.temporary,
                &paths.previous,
                &paths.current,
                &paths.lock,
            ] {
                remove_if_present(path)?;
            }
            Ok(())
        })
        .await
    }

    /// Removes every lazy overlay and shadow node no lazy workspace reaches, and returns how many it removed.
    pub async fn collect_lazy_nodes(&self) -> Result<u64, LocalCoreStateStoreError> {
        let nodes = Arc::clone(&self.nodes);
        self.transaction(move |root| {
            let root = &root;
            root.with_log(|log| {
                let mut workspaces = log
                    .resident_keys(|key| match key {
                        RecordKey::LazyWorkspace(id) => Some(id),
                        _ => None,
                    })
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                for stem in family_stems(root, LAZY_WORKSPACE_FAMILY)? {
                    if let Ok(bytes) = <[u8; 16]>::try_from(stem.as_slice()) {
                        workspaces.insert(WorkspaceId::from_bytes(bytes));
                    }
                }
                let mut overlays = Vec::new();
                let mut shadows = Vec::new();
                for workspace in workspaces {
                    let Some(state) = log.get::<LazyWorkspaceState>(root, workspace)? else {
                        continue;
                    };
                    overlays.push(state.overlay);
                    shadows.push(state.shadows);
                    if let Some(origin) = state.fork_origin {
                        overlays.push(origin.snapshot.overlay);
                        shadows.push(origin.snapshot.shadows);
                    }
                    if let Some(completed) = state.completed_selected_import {
                        overlays.extend([
                            completed.snapshot.overlay,
                            completed.destination.overlay,
                            completed.source.overlay,
                        ]);
                        shadows.extend([
                            completed.snapshot.shadows,
                            completed.destination.shadows,
                            completed.source.shadows,
                        ]);
                    }
                    if let Some(pending) = state.pending_remove {
                        overlays.extend([pending.prior_overlay, pending.prepared_overlay]);
                        shadows.push(pending.prior_shadows);
                    }
                }
                let overlays = reachable_nodes::<LazyOverlay>(root, overlays, |node| match node {
                    LazyOverlay::Node { left, right, .. } => {
                        vec![left.into_bytes(), right.into_bytes()]
                    }
                    LazyOverlay::Empty => Vec::new(),
                })?;
                let shadows =
                    reachable_nodes::<crate::LazyShadow>(root, shadows, |node| match node {
                        crate::LazyShadow::Node { left, right, .. } => {
                            vec![left.into_bytes(), right.into_bytes()]
                        }
                        crate::LazyShadow::Empty => Vec::new(),
                    })?;
                Ok(
                    sweep_nodes::<LazyOverlay>(root, &nodes, &overlays, LazyOverlayId::from_bytes)?
                        + sweep_nodes::<crate::LazyShadow>(
                            root,
                            &nodes,
                            &shadows,
                            crate::LazyShadowId::from_bytes,
                        )?,
                )
            })
        })
        .await
    }
}

/// The distinct record keys one family holds, whatever recovery copies accompany them.
pub(super) fn family_stems(
    root: &Namespace,
    family: &str,
) -> Result<BTreeSet<Vec<u8>>, LocalCoreStateStoreError> {
    let entries = match std::fs::read_dir(root.family(family)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error.into()),
    };
    let mut stems = BTreeSet::new();
    for entry in entries {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        let stem = name
            .strip_suffix(".previous.json")
            .or_else(|| name.strip_suffix(".next.json"))
            .or_else(|| name.strip_suffix(".json"))
            .or_else(|| name.strip_suffix(".lock"));
        if let Some(bytes) = stem.and_then(|stem| hex::decode(stem).ok()) {
            stems.insert(bytes);
        }
    }
    Ok(stems)
}

/// Every stored node of one family the `roots` reach.
pub(super) fn reachable_nodes<T: LazyNode + DeserializeOwned>(
    root: &Namespace,
    roots: Vec<T::Id>,
    children: impl Fn(T) -> Vec<[u8; 32]>,
) -> Result<std::collections::HashSet<[u8; 32]>, LocalCoreStateStoreError> {
    let empty = content_address(&T::default())?;
    let mut reachable = std::collections::HashSet::new();
    let mut pending = roots.into_iter().map(T::address).collect::<Vec<_>>();
    while let Some(address) = pending.pop() {
        if address == empty || !reachable.insert(address) {
            continue;
        }
        let node = load_content_addressed::<T>(root, T::FAMILY, address)?
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        pending.extend(children(node));
    }
    Ok(reachable)
}

/// Removes every stored node of one family outside `reachable` that was not put within the grace period.
pub(super) fn sweep_nodes<T: LazyNode>(
    root: &Namespace,
    nodes: &NodeMemo,
    reachable: &std::collections::HashSet<[u8; 32]>,
    id: impl Fn([u8; 32]) -> T::Id,
) -> Result<u64, LocalCoreStateStoreError> {
    let mut removed = 0;
    for stem in family_stems(root, T::FAMILY)? {
        let Ok(address) = <[u8; 32]>::try_from(stem.as_slice()) else {
            continue;
        };
        if reachable.contains(&address) {
            continue;
        }
        // Decided and removed under the memo lock a put records under.
        let mut memo = T::memo(nodes);
        if !memo.release(&id(address)) {
            continue;
        }
        // Unsynchronized: a removal a crash undoes leaves only garbage.
        let paths = root.record(T::FAMILY, &address)?;
        for path in [
            &paths.temporary,
            &paths.previous,
            &paths.current,
            &paths.lock,
        ] {
            remove_if_present(path)?;
        }
        removed += 1;
    }
    Ok(removed)
}

/// Loads an immutable record whose key is the BLAKE3 digest of its encoding.
pub(super) fn load_content_addressed<T: DeserializeOwned + Serialize>(
    namespace: &Namespace,
    family: &str,
    address: [u8; 32],
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let Some(value) = read_locked(&namespace.record(family, &address)?)? else {
        return Ok(None);
    };
    verify_content_address(address, &value)?;
    Ok(Some(value))
}

pub(super) fn verify_content_address<T: Serialize>(
    address: [u8; 32],
    value: &T,
) -> Result<(), LocalCoreStateStoreError> {
    if content_address(value)? != address {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    Ok(())
}

pub(super) fn content_address<T: Serialize>(
    value: &T,
) -> Result<[u8; 32], LocalCoreStateStoreError> {
    Ok(*blake3::hash(&serde_json::to_vec(value)?).as_bytes())
}
