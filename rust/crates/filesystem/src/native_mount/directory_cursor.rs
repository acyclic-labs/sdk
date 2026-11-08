use super::{
    MountSourceError, adapter::CaptureProducer, view_gate::ViewGate, view_ledger::ViewStamp,
};
use crate::kernel::{KernelOwnedAllocation, LogicalName};
use std::sync::{Arc, Mutex};

/// An owned continuation for one directory listing.
///
/// Keeping another listing open never invalidates this continuation. Dropping it
/// releases its physical source state and original allocation credit.
pub struct MountDirectoryCursor {
    pub(super) position: DirectoryPosition,
    // The value, including its Box backing, retires before its original credit.
    pub(super) _allocation: KernelOwnedAllocation,
}

pub(super) enum DirectoryPosition {
    Authored(LogicalName),
    Lazy(Box<LazyDirectoryPosition>),
}

pub(super) struct LazyDirectoryPosition {
    pub(super) source: Arc<ViewGate>,
    pub(super) generation: (u64, u64),
    pub(super) stamp: ViewStamp,
    pub(super) cursor: Mutex<Option<crate::LazyDirectoryCursor>>,
    // Source cleanup waits for the actual directory state to retire above.
    pub(super) _producer: CaptureProducer,
}

impl MountDirectoryCursor {
    pub(super) fn authored(&self) -> Result<&LogicalName, MountSourceError> {
        match &self.position {
            DirectoryPosition::Authored(name) => Ok(name),
            DirectoryPosition::Lazy(_) => Err(MountSourceError::Stale),
        }
    }

    pub(super) fn take_lazy(
        &self,
        source: &Arc<ViewGate>,
        generation: (u64, u64),
    ) -> Result<(ViewStamp, crate::LazyDirectoryCursor), MountSourceError> {
        let DirectoryPosition::Lazy(position) = &self.position else {
            return Err(MountSourceError::Stale);
        };
        if !Arc::ptr_eq(&position.source, source) || position.generation != generation {
            return Err(MountSourceError::Stale);
        }
        let cursor = position
            .cursor
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .take()
            .ok_or(MountSourceError::Stale)?;
        Ok((position.stamp, cursor))
    }
}

impl std::fmt::Debug for MountDirectoryCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountDirectoryCursor")
            .finish_non_exhaustive()
    }
}
