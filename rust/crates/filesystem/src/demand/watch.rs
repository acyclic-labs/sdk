use super::*;

/// Read-only physical custody of one newly allocated native watch-registration
pub struct SourceWatchAllocationCustody {
    retained: std::sync::atomic::AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::demand) ownership: SourcePayloadOwnership,
}

impl SourceWatchAllocationCustody {
    /// Actual logical buffer bytes still retained by this original backing.
        #[must_use]
        pub fn retained_bytes(&self) -> u64 {
            self.retained.load(std::sync::atomic::Ordering::Acquire)
        }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        pub(in crate::demand) fn new(ownership: SourcePayloadOwnership) -> Self {
            Self { retained: std::sync::atomic::AtomicU64::new(0), ownership }
        }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        pub(in crate::demand) fn publish(&self, bytes: u64) {
            self.retained.store(bytes, std::sync::atomic::Ordering::Release);
        }

    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
        pub(in crate::demand) fn retired(&self) {
            self.retained.store(0, std::sync::atomic::Ordering::Release);
        }
}

/// Live observation of every change made to one source outside every view
pub trait SourceWatch: Send + Sync {

    /// Delivers preceding changes within the accounting allowance.
    fn fence(&self, accounting: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError>;

    /// Quiesces this subscription within the accounting allowance.
    fn stop(&self, accounting: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError>;

    /// Retires this owned subscription against its original birth-paid cleanup.
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)?;
        Err(DemandError::NativeWorkUnavailable)
    }

    /// Completes only the already-accepted native physical retirement plan,
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        Err(DemandError::NativeWorkUnavailable)
    }

    /// True only after this provider has observed physical watch quiescence.
    fn is_stopped(&self) -> bool { false }

    /// SDK logical payload retained by this returned owner. Existing borrowed
    fn logical_owned_bytes(&self) -> u64;

    /// Only this call's newly created registration backing, not a preexisting
    fn allocation_custody(&self) -> Option<std::sync::Arc<SourceWatchAllocationCustody>>;

    /// Whether every change still reaches the sink. False once the host
    fn is_exact(&self) -> bool;
}

/// Fences a watch using standalone or original native accounting.
pub fn fence_watch<W: crate::facade::FsWork>(
    watch: &dyn SourceWatch, work: W, cancellation: &CancellationToken,
) -> W::Output<(), DemandError> {
    let mut accounting = work.accounting(cancellation);
    let result = watch.fence(&mut accounting);
    work.finish(result, &accounting)
}

/// Stops a watch using standalone or original native accounting.
pub fn stop_watch<W: crate::facade::FsWork>(
    watch: &dyn SourceWatch, work: W, cancellation: &CancellationToken,
) -> W::Output<(), DemandError> {
    let mut accounting = work.accounting(cancellation);
    let result = watch.stop(&mut accounting);
    work.finish(result, &accounting)
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(in crate::demand) fn cancellation_error(error: crate::cancellation::CancellationWaitError) -> DemandError {
    match error {
        crate::cancellation::CancellationWaitError::Work(error) => error.into(),
        crate::cancellation::CancellationWaitError::AllocationFailed => {
            std::io::Error::from(std::io::ErrorKind::OutOfMemory).into()
        }
        crate::cancellation::CancellationWaitError::Native(error) => crate::native_name::original_name_error(error),
    }
}
