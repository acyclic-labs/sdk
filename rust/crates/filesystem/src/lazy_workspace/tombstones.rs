use super::*;
use crate::kernel::{KernelAccounting, Owned};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Lists deleted source versions under an explicit result bound.
    pub async fn source_tombstones(
        &self,
        maximum: usize,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        (
            Vec<(String, SourceVersion)>,
            crate::kernel::allocation::KernelOwnedAllocation,
        ),
        LazyWorkspaceError,
    > {
        let state = self.state(accounting).await?;
        let mut pending = Owned::allocate(1, accounting)?;
        pending.value.push(state.overlay);
        let mut output = Owned::allocate(0, accounting)?;
        let result = async {
            while let Some(id) = pending.value.pop() {
                let Some(mut node) = self.store.load_lazy_overlay(id, accounting).await? else {
                    continue;
                };
                let result = (|| -> Result<(), LazyWorkspaceError> {
                    if let LazyOverlay::Node {
                        left,
                        right,
                        change,
                        ..
                    } = &node.0
                    {
                        pending.ensure_for_push(usize::MAX, accounting)?;
                        pending.value.push(*left);
                        pending.ensure_for_push(usize::MAX, accounting)?;
                        pending.value.push(*right);
                        if matches!(change, LazyOverlayChange::Tombstone { removed: Some(_) }) {
                            if output.value.len() == maximum {
                                return Err(crate::WorkError::BudgetExceeded {
                                    counter: "source_tombstones",
                                    observed: (maximum as u64)
                                        .checked_add(1)
                                        .ok_or(crate::WorkError::Overflow)?,
                                    maximum: maximum as u64,
                                }
                                .into());
                            }
                            output.ensure_for_push(maximum, accounting)?;
                            output.allocation.absorb(&mut node.1)?;
                        }
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    retire_node(node, accounting)?;
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
                        output.bytes = output
                            .bytes
                            .checked_add(path.capacity() as u64)
                            .ok_or(crate::WorkError::Overflow)?;
                        output.value.push((path, version));
                    }
                    value => retire_node((value, node.1), accounting)?,
                }
            }
            output.value.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            Ok(())
        }
        .await;
        pending.retire(accounting)?;
        if let Err(error) = result {
            output.retire(accounting)?;
            return Err(error);
        }
        Ok((output.value, output.allocation))
    }

    /// Reads the current source version without consulting authored state.
    pub async fn current_source_version(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        let state = self.state(accounting).await?;
        match self.source_lookup(state.source, path, accounting).await {
            Ok(node) => Ok(node.map(|node| node.version)),
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
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceVersion>, LazyWorkspaceError> {
        match self.overlay_fact(state.overlay, path, accounting).await? {
            Some(LazyOverlayChange::Observe { node, .. }) => Ok(Some(node.version)),
            Some(LazyOverlayChange::Tombstone { removed }) => Ok(removed),
            None => match self.source_lookup(state.source, path, accounting).await {
                Ok(node) => Ok(node.map(|node| node.version)),
                Err(LazyWorkspaceError::Demand(
                    DemandError::Absent | DemandError::NotDirectory,
                )) => Ok(None),
                Err(error) => Err(error),
            },
        }
    }
}
