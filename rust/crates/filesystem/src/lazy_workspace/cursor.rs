use super::*;
use crate::kernel::{KernelAccounting, allocation::KernelOwnedAllocation};

/// Continuation bound to an immutable authored/source cut and directory.
pub struct LazyDirectoryCursor {
    pub(super) cut: LazySnapshotRef,
    pub(super) directory: NamespacePath,
    pub(super) phase: LazyDirectoryPhase,
    pub(super) allocation: KernelOwnedAllocation,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum LazyDirectoryPhase {
    Source(Option<SourceCursor>),
    Authored(Option<LogicalName>),
}

impl std::fmt::Debug for LazyDirectoryCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyDirectoryCursor")
            .field("cut", &self.cut)
            .field("directory", &self.directory)
            .field("phase", &self.phase)
            .finish()
    }
}

impl PartialEq for LazyDirectoryCursor {
    fn eq(&self, other: &Self) -> bool {
        self.cut == other.cut && self.directory == other.directory && self.phase == other.phase
    }
}
impl Eq for LazyDirectoryCursor {}

impl LazyDirectoryCursor {
    /// Actual retained directory, continuation and name backing.
    pub fn retained_bytes(&self) -> Result<u64, LazyWorkspaceError> {
        self.directory
            .retained_bytes()
            .checked_add(match &self.phase {
                LazyDirectoryPhase::Source(cursor) => {
                    cursor.as_ref().map_or(0, SourceCursor::retained_bytes)
                }
                LazyDirectoryPhase::Authored(name) => {
                    name.as_ref().map_or(0, |name| name.retained_bytes() as u64)
                }
            })
            .ok_or_else(|| crate::WorkError::Overflow.into())
    }

    /// Clones continuation data after admission on the supplied context.
    pub fn clone(&self, accounting: &mut KernelAccounting<'_>) -> Result<Self, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        let (directory, mut allocation) = self
            .directory
            .clone_with_accounting(accounting, cancellation)?;
        let phase = match &self.phase {
            LazyDirectoryPhase::Source(None) => LazyDirectoryPhase::Source(None),
            LazyDirectoryPhase::Source(Some(cursor)) => {
                let bytes = cursor.retained_bytes();
                let copying = WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copying)?;
                let mut claim = accounting.claim_owned(bytes, self.directory.depth() as u64 + 1)?;
                let mut cursor = Clone::clone(cursor);
                accounting.reconcile_owned(&mut claim, bytes, cursor.retained_bytes())?;
                accounting.record_completed(copying)?;
                allocation.absorb(&mut claim)?;
                LazyDirectoryPhase::Source(Some(cursor))
            }
            LazyDirectoryPhase::Authored(None) => LazyDirectoryPhase::Authored(None),
            LazyDirectoryPhase::Authored(Some(name)) => {
                let (name, mut claim) = name.clone_with_accounting(accounting, cancellation)?;
                allocation.absorb(&mut claim)?;
                LazyDirectoryPhase::Authored(Some(name))
            }
        };
        Ok(Self {
            cut: self.cut,
            directory,
            phase,
            allocation,
        })
    }

    /// Drops continuation data before releasing its backing.
    pub fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), LazyWorkspaceError> {
        let bytes = self.retained_bytes()?;
        drop(self.directory);
        drop(self.phase);
        crate::kernel::retire_allocation(self.allocation, bytes, accounting).map_err(Into::into)
    }
}
