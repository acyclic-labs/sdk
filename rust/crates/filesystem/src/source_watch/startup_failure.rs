use super::{NativeSourceWatch, arc_control_bytes};
use crate::demand::{DemandError, NativeSourceWork, SourceDemandReadWork, SourcePayloadOwnership};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, atomic::{AtomicU64, Ordering}};

/// A failed native startup retaining its actual owner until cleanup succeeds.
pub struct SourceWatchStartupFailure {
    pub(super) cause: OnceLock<DemandError>,
    pub(super) watch: Mutex<Option<NativeSourceWatch>>,
    pub(super) owned_bytes: AtomicU64,
    pub(super) ownership: SourcePayloadOwnership,
}

impl std::fmt::Debug for SourceWatchStartupFailure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.debug_struct("SourceWatchStartupFailure").field("cause", &self.cause.get()).finish_non_exhaustive()
    }
}
impl std::fmt::Display for SourceWatchStartupFailure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.cause.get() { Some(cause) => std::fmt::Display::fmt(cause, output), None => output.write_str("native startup has not failed") }
    }
}
impl std::error::Error for SourceWatchStartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.get().map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

impl SourceWatchStartupFailure {
    pub(super) fn retain_failed_platform(self: &Arc<Self>, error: DemandError, owner: NativeSourceWatch) -> DemandError {
        owner.delivery().exact.store(false, Ordering::Release);
        assert!(self.cause.set(error).is_ok(), "one startup owner");
        self.owned_bytes.store(owner.logical_owned_bytes(), Ordering::Release);
        *self.watch.lock().unwrap_or_else(PoisonError::into_inner) = Some(owner);
        DemandError::SourceWatchStartup(self.clone())
    }

    fn with_owner(&self, invoke: impl FnOnce(&NativeSourceWatch) -> Result<(), DemandError>) -> Result<(), DemandError> {
        let (slot, poisoned) = match self.watch.try_lock() {
            Ok(slot) => (slot, false),
            Err(std::sync::TryLockError::Poisoned(error)) => (error.into_inner(), true),
            Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
        };
        let owner = slot.as_ref().ok_or(DemandError::WorkerUnavailable)?;
        let result = invoke(owner);
        self.owned_bytes.store(owner.logical_owned_bytes(), Ordering::Release);
        if poisoned { result.and(Err(DemandError::OriginalWorkPoisoned)) } else { result }
    }

    /// Quiesces the retained owner with the current native invocation.
    pub fn stop_with_native_work(&self, work: &crate::NativeMaterializationWork) -> Result<(), DemandError> {
        self.stop_with_work(&mut NativeSourceWork::from_native_materialization_work(work))
    }
    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<(), DemandError> {
        self.with_owner(|owner| owner.stop_with_work(work))
    }

    /// Uses birth-paid cleanup while the supplied token controls waiting.
    pub fn stop_with_cleanup_wait(&self, wait: &crate::CancellationToken) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        self.with_owner(|owner| owner.stop_with_cleanup_wait(wait))
    }
    /// Retires only this owner's accepted native resources.
    pub fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.with_owner(NativeSourceWatch::stop_accepted_cleanup)
    }
    /// True only after the actual retained owner has stopped.
    pub fn is_stopped(&self) -> bool {
        match self.watch.try_lock() {
            Ok(slot) => slot.as_ref().is_some_and(NativeSourceWatch::is_stopped),
            Err(std::sync::TryLockError::Poisoned(error)) => error.get_ref().as_ref().is_some_and(NativeSourceWatch::is_stopped),
            Err(std::sync::TryLockError::WouldBlock) => false,
        }
    }
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        arc_control_bytes::<Self>() + self.owned_bytes.load(Ordering::Acquire)
    }
}
