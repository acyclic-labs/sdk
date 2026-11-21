use super::*;

/// An immutable lazy node whose backing can be copied on an accounting ledger.
#[doc(hidden)]
pub trait MemoryLazyNode: Serialize + Eq + Sized + Send + Sync {
    /// Bytes of owned backing carried by the node's allocation.
    fn owned_bytes(&self) -> u64;
    /// Retains independently shared backing decoded with the node.
    #[cfg(not(target_arch = "wasm32"))]
    fn retain_decoded(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>;
    /// Copies the immutable node and returns its original custody.
    fn copy(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError>;
}

fn copy_node<T: MemoryLazyNode>(
    mut requested: u64,
    bytes_copied: u64,
    accounting: &mut KernelAccounting<'_>,
    copy: impl FnOnce(
        &mut KernelOwnedAllocation,
        &mut u64,
        &mut KernelAccounting<'_>,
    ) -> Result<T, crate::FsError>,
) -> Result<(T, KernelOwnedAllocation), LazyWorkspaceError> {
    accounting.check()?;
    let mut guard = accounting.claim_owned(requested, u64::from(requested != 0))?;
    let copied = WorkCounters {
        bytes_copied,
        ..WorkCounters::UNCHARGED
    };
    if let Err(error) = accounting.admit(copied) {
        accounting.release_owned(&mut guard, requested)?;
        return Err(error.into());
    }
    let value = match copy(&mut guard, &mut requested, accounting) {
        Ok(value) => value,
        Err(error) => {
            accounting.release_owned(&mut guard, requested)?;
            return Err(error.into());
        }
    };
    let actual = value.owned_bytes();
    let checked = accounting
        .reconcile_owned(&mut guard, requested, actual)
        .and_then(|()| accounting.record_completed(copied));
    if let Err(error) = checked {
        drop(value);
        accounting.release_owned(&mut guard, actual)?;
        return Err(error.into());
    }
    Ok((value, guard))
}

impl MemoryLazyNode for LazyOverlay {
    #[cfg(not(target_arch = "wasm32"))]
    fn retain_decoded(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if let Self::Node {
            change: LazyOverlayChange::Observe(observed),
            ..
        } = self
        {
            observed.source.retain_decoded(allocation, ctx)?;
        }
        Ok(())
    }
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { path, change, .. } => path.capacity() as u64 + change.owned_bytes(),
        }
    }

    fn copy(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError> {
        let buffers = match self {
            Self::Empty => 0,
            Self::Node { path, .. } => path.capacity() as u64,
        };
        copy_node(
            buffers,
            self.owned_bytes(),
            ctx,
            |allocation, bytes, ctx| {
                let Self::Node {
                    path,
                    change,
                    priority,
                    left,
                    right,
                } = self
                else {
                    return Ok(Self::Empty);
                };
                let change = match change {
                    LazyOverlayChange::Observe(observed) => {
                        let copy = LazyObservation {
                            source: observed.source.clone(),
                            node: observed.node,
                            complete: observed.complete,
                        };
                        LazyOverlayChange::Observe(LazyBox::fund(copy, allocation, bytes, ctx)?)
                    }
                    LazyOverlayChange::Tombstone { removed } => {
                        LazyOverlayChange::Tombstone { removed: *removed }
                    }
                };
                Ok(Self::Node {
                    path: path.clone(),
                    change,
                    priority: *priority,
                    left: *left,
                    right: *right,
                })
            },
        )
    }
}

impl MemoryLazyNode for LazyShadow {
    #[cfg(not(target_arch = "wasm32"))]
    fn retain_decoded(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if let Self::Node {
            observed: Some(observed),
            ..
        } = self
        {
            observed.source.retain_decoded(allocation, ctx)?;
        }
        Ok(())
    }
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node {
                authored, observed, ..
            } => {
                authored
                    .as_ref()
                    .map_or(0, |record| record.bytes() + record.record.capacity() as u64)
                    + observed.as_ref().map_or(0, |record| {
                        record.bytes() + record.content.record.capacity() as u64
                    })
            }
        }
    }

    fn copy(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError> {
        let buffers = match self {
            Self::Empty => 0,
            Self::Node {
                authored, observed, ..
            } => {
                authored
                    .as_ref()
                    .map_or(0, |record| record.record.capacity() as u64)
                    + observed
                        .as_ref()
                        .map_or(0, |record| record.content.record.capacity() as u64)
            }
        };
        copy_node(
            buffers,
            self.owned_bytes(),
            ctx,
            |allocation, bytes, ctx| {
                let Self::Node {
                    file_id,
                    priority,
                    authored,
                    observed,
                    left,
                    right,
                } = self
                else {
                    return Ok(Self::Empty);
                };
                let authored = authored
                    .as_deref()
                    .map(|record| LazyBox::fund(record.clone(), allocation, bytes, ctx))
                    .transpose()?;
                let observed = observed
                    .as_deref()
                    .map(|record| LazyBox::fund(record.clone(), allocation, bytes, ctx))
                    .transpose()?;
                Ok(Self::Node {
                    file_id: *file_id,
                    priority: *priority,
                    authored,
                    observed,
                    left: *left,
                    right: *right,
                })
            },
        )
    }
}

impl LazyOverlayChange {
    pub(super) fn owned_bytes(&self) -> u64 {
        match self {
            Self::Observe(observed) => observed.bytes(),
            Self::Tombstone { .. } => 0,
        }
    }

    pub(crate) fn observe(
        source: SourceReference,
        node: SourceNode,
        complete: bool,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Self>, LazyWorkspaceError> {
        let mut allocation = ctx.claim_owned(0, 0)?;
        let mut bytes = 0;
        let observation = LazyObservation {
            source,
            node,
            complete,
        };
        let observation = LazyBox::fund(observation, &mut allocation, &mut bytes, ctx)?;
        Ok(Owned::new(Self::Observe(observation), allocation, bytes))
    }

    pub(super) fn tombstone(
        removed: Option<SourceVersion>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Self>, LazyWorkspaceError> {
        Ok(Owned::new(
            Self::Tombstone { removed },
            ctx.claim_owned(0, 0)?,
            0,
        ))
    }
}

pub(super) fn observation(
    fact: Option<&LazyOverlayChange>,
) -> Option<(SourceReference, SourceNode, bool)> {
    match fact {
        Some(LazyOverlayChange::Observe(observed)) => {
            Some((observed.source.clone(), observed.node, observed.complete))
        }
        _ => None,
    }
}
