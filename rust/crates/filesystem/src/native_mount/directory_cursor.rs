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
    Source(Box<dyn std::any::Any + Send + Sync>),
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
    /// Retains a custom source's continuation, admitting its boxed state on the
    /// original invocation before allocating. Any backing already owned by
    /// `state` must retain its own allocation custody inside that state.
    ///
    /// The source must validate its directory and generation identity when
    /// resuming, just as it validates any other source-owned handle.
    pub fn from_source_state<T: std::any::Any + Send + Sync>(
        state: T,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, MountSourceError> {
        let bytes = std::mem::size_of::<T>() as u64;
        let allocation = work
            .claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)
            .map_err(MountSourceError::NativeWork)?;
        Ok(Self {
            position: DirectoryPosition::Source(Box::new(state)),
            _allocation: KernelOwnedAllocation::NativeOriginal(allocation),
        })
    }

    /// Borrows this source's exact continuation state. A cursor from another
    /// source type, or an SDK source, has no state of type `T`.
    #[must_use]
    pub fn source_state<T: std::any::Any>(&self) -> Option<&T> {
        match &self.position {
            DirectoryPosition::Source(state) => state.downcast_ref(),
            _ => None,
        }
    }

    pub(super) fn authored(&self) -> Result<&LogicalName, MountSourceError> {
        match &self.position {
            DirectoryPosition::Authored(name) => Ok(name),
            DirectoryPosition::Lazy(_) | DirectoryPosition::Source(_) => {
                Err(MountSourceError::Stale)
            }
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
