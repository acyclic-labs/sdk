use super::*;
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Lists deleted source versions under a bounded result count.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<(String, SourceVersion)>, KernelOwnedAllocation), LazyWorkspaceError> {
        let overlay = self.state(ctx).await?.overlay;
        let output = self
            .source_tombstones_owned(overlay, maximum, None, ctx)
            .await?;
        Ok((output.value, output.allocation))
    }

    pub(super) async fn source_tombstones_owned(
        &self,
        overlay: LazyOverlayId,
        maximum: usize,
        inherited: Option<LazyOverlayId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Vec<(String, SourceVersion)>>, LazyWorkspaceError> {
        let mut pending = Owned::allocate(1, ctx)?;
        pending.value.push((overlay, inherited));
        let mut output = Owned::allocate(0, ctx)?;
        let result = async {
            while let Some((id, original)) = pending.value.pop() {
                if original == Some(id) {
                    continue;
                }
                let mut node = self
                    .store
                    .load_lazy_overlay(id, ctx)
                    .await?
                    .ok_or(LazyWorkspaceError::NotFound)?;
                // Persistent subtrees retain their identities across edits and
                // rotations. Find the same key in the original cut before
                // descending, so inherited observations cost no subtree walk.
                let previous = match (&node.0, inherited) {
                    (LazyOverlay::Node { path, .. }, Some(root)) => {
                        match self.overlay_entry(root, path, ctx).await {
                            Ok(previous) => previous,
                            Err(error) => {
                                retire_node(node, ctx)?;
                                return Err(error);
                            }
                        }
                    }
                    _ => None,
                };
                if previous
                    .as_ref()
                    .is_some_and(|(original, _, _)| *original == id)
                {
                    retire_node(node, ctx)?;
                    continue;
                }
                let changed = match &node.0 {
                    LazyOverlay::Node { change, .. } => previous
                        .as_ref()
                        .is_none_or(|(_, original, _)| original != change),
                    LazyOverlay::Empty => false,
                };
                let prepared = (|| {
                    if let LazyOverlay::Node {
                        left,
                        right,
                        change,
                        ..
                    } = &node.0
                    {
                        for (index, id) in [*left, *right].into_iter().enumerate() {
                            pending.ensure_for_push(usize::MAX, ctx)?;
                            pending.value.push((
                                id,
                                previous.as_ref().map(|(_, _, children)| children[index]),
                            ));
                        }
                        if changed
                            && matches!(change, LazyOverlayChange::Tombstone { removed: Some(_) })
                        {
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
                    } if changed => {
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
        Ok(self
            .current_source_node(path, ctx)
            .await?
            .map(|node| node.version))
    }

    /// Observes the current source node without consulting authored state or
    /// retaining a new observation. Uncaptured host edits remain visible here.
    pub async fn current_source_node(
        &self,
        path: &str,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, LazyWorkspaceError> {
        let source = self.state(ctx).await?.source;
        match self.source_lookup(source, path, ctx).await {
            Ok(node) => Ok(node),
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
