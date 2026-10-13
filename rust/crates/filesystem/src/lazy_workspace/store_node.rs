use super::*;

pub(crate) trait MemoryLazyNode: Serialize + Eq + Clone {
    fn owned_bytes(&self) -> u64;
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
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { path, .. } => path.capacity() as u64,
        }
    }
}

impl MemoryLazyNode for LazyShadow {
    fn owned_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Node { record, .. } => record.capacity() as u64,
        }
    }
}
