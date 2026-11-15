use super::*;

pub(crate) trait MemoryLazyNode: Serialize + Eq + Clone + Send + Sync {
    fn owned_bytes(&self) -> u64;
    fn retain_decoded(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>;
    fn copy(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Self, KernelOwnedAllocation), LazyWorkspaceError> {
        accounting.check()?;
        let requested = self.owned_bytes();
        let mut guard = accounting.claim_owned(requested, u64::from(requested != 0))?;
        let copied = WorkCounters {
            bytes_copied: requested,
            ..WorkCounters::UNCHARGED
        };
        if let Err(error) = accounting.admit(copied) {
            accounting.release_owned(&mut guard, requested)?;
            return Err(error.into());
        }
        let value = self.clone();
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
}

impl MemoryLazyNode for LazyOverlay {
    fn retain_decoded(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if let Self::Node {
            change: LazyOverlayChange::Observe { source, .. },
            ..
        } = self
        {
            source.retain_decoded(allocation, ctx)?;
        }
        Ok(())
    }
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { path, .. } => path.capacity() as u64,
        }
    }
}

impl MemoryLazyNode for LazyShadow {
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
                    .map_or(0, |record| record.record.capacity() as u64)
                    + observed
                        .as_ref()
                        .map_or(0, |record| record.content.record.capacity() as u64)
            }
        }
    }
}
