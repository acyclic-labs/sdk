use super::*;
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Whether authored state differs between exact cuts. Source observations
    /// alone are reads, so they never require publishing a fork back to its parent.
    pub async fn has_authored_changes_between(
        &self,
        baseline: LazySnapshotRef,
        selected: LazySnapshotRef,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        if baseline.workspace_id != self.workspace.id()
            || selected.workspace_id != self.workspace.id()
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        self.validate_snapshot(baseline, ctx).await?;
        self.validate_snapshot(selected, ctx).await?;
        if selected.authored_generation != baseline.authored_generation
            || selected.shadows != baseline.shadows
        {
            return Ok(true);
        }
        let mut pending = Owned::allocate(1, ctx)?;
        pending.value.push(selected.overlay);
        let result = async {
            while let Some(id) = pending.value.pop() {
                if id == baseline.overlay {
                    continue;
                }
                let node = self
                    .store
                    .load_lazy_overlay(id, ctx)
                    .await?
                    .ok_or(LazyWorkspaceError::NotFound)?;
                let changed = async {
                    Ok::<_, LazyWorkspaceError>(
                        if let LazyOverlay::Node {
                            path,
                            change,
                            left,
                            right,
                            ..
                        } = &node.0
                        {
                            for child in [*left, *right] {
                                pending.ensure_for_push(usize::MAX, ctx)?;
                                pending.value.push(child);
                            }
                            matches!(change, LazyOverlayChange::Tombstone { .. })
                                && self
                                    .overlay_fact(baseline.overlay, path, ctx)
                                    .await?
                                    .as_ref()
                                    != Some(change)
                        } else {
                            false
                        },
                    )
                }
                .await;
                retire_node(node, ctx)?;
                if changed? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        .await;
        pending.retire(ctx)?;
        result
    }

    /// Lists deleted source versions under a bounded result count.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<(String, SourceVersion)>, KernelOwnedAllocation), LazyWorkspaceError> {
        let output = self.source_tombstones_owned(maximum, ctx).await?;
        Ok((output.value, output.allocation))
    }

    pub(super) async fn source_tombstones_owned(
        &self,
        maximum: usize,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Vec<(String, SourceVersion)>>, LazyWorkspaceError> {
        let state = self.state(ctx).await?;
        let mut pending = Owned::allocate(1, ctx)?;
        pending.value.push(state.overlay);
        let mut output = Owned::allocate(0, ctx)?;
        let result = async {
            while let Some(id) = pending.value.pop() {
                let mut node = self
                    .store
                    .load_lazy_overlay(id, ctx)
                    .await?
                    .ok_or(LazyWorkspaceError::NotFound)?;
                let prepared = (|| {
                    if let LazyOverlay::Node {
                        left,
                        right,
                        change,
                        ..
                    } = &node.0
                    {
                        for id in [*left, *right] {
                            pending.ensure_for_push(usize::MAX, ctx)?;
                            pending.value.push(id);
                        }
                        if matches!(change, LazyOverlayChange::Tombstone { removed: Some(_) }) {
                            output.ensure_for_push(maximum, ctx)?;
                            output.allocation.absorb(&mut node.1)?;
                        }
                    }
                    Ok::<_, LazyWorkspaceError>(())
                })();
                if let Err(error) = prepared {
                    retire_node(node, ctx)?;
                    return Err(error);
                }
                match node.0 {
                    LazyOverlay::Node {
                        path,
                        change:
                            LazyOverlayChange::Tombstone {
                                removed: Some(version),
                            },
                        ..
                    } => {
                        output.bytes += path.capacity() as u64;
                        output.value.push((path, version));
                    }
                    value => retire_node((value, node.1), ctx)?,
                }
            }
            output.value.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            Ok::<_, LazyWorkspaceError>(())
        }
        .await;
        pending.retire(ctx)?;
        if let Err(error) = result {
            output.retire(ctx)?;
            return Err(error);
        }
        Ok(output)
    }
    /// Reads the current source version without consulting authored state.
    pub async fn current_source_version(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let source = self.state(ctx).await?.source;
        match self.source_lookup(source, path, ctx).await {
            Ok(node) => Ok(node.map(|n| n.version)),
            Err(LazyWorkspaceError::Demand(DemandError::Absent | DemandError::NotDirectory)) => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
    pub(super) async fn removed_source_version(
        &self,
        state: &LazyWorkspaceState,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        match self.overlay_fact(state.overlay, path, ctx).await? {
            Some(LazyOverlayChange::Observe { source, node, .. }) if source == state.source => {
                Ok(Some(node.version))
            }
            Some(LazyOverlayChange::Tombstone { removed }) => Ok(removed),
            _ => match self.source_lookup(state.source, path, ctx).await {
                Ok(node) => Ok(node.map(|n| n.version)),
                Err(LazyWorkspaceError::Demand(
                    DemandError::Absent | DemandError::NotDirectory,
                )) => Ok(None),
                Err(error) => Err(error),
            },
        }
    }
}
