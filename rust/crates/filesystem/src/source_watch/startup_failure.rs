use super::*;

struct FailedWatchLease<'a> {
    owner: Option<NativeSourceWatch>,
    failure: &'a SourceWatchStartupFailure,
}

impl Drop for FailedWatchLease<'_> {
    fn drop(&mut self) {
        if let Some(watch) = self.owner.take() {
            self.failure
                .stopped
                .store(watch.is_stopped(), Ordering::Release);
            *self
                .failure
                .watch
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watch);
        }
    }
}

impl std::fmt::Debug for SourceWatchStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("SourceWatchStartupFailure");
        if let Some(cause) = self.cause.get() {
            debug.field("cause", cause);
        }
        debug.finish_non_exhaustive()
    }
}

impl std::fmt::Display for SourceWatchStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.cause.get() {
            Some(cause) => std::fmt::Display::fmt(cause, formatter),
            None => formatter.write_str("native source startup has not failed"),
        }
    }
}

impl std::error::Error for SourceWatchStartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .get()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

impl SourceWatchStartupFailure {
    #[cfg(any(windows, target_os = "linux"))]
    pub(super) fn retain_failed_platform(
        self: &Arc<Self>,
        error: DemandError,
        platform: NativeSourceWatch,
    ) -> DemandError {
        platform.delivery().exact.store(false, Ordering::Release);
        assert!(
            self.cause.set(error).is_ok(),
            "one startup control retains one actual native owner"
        );
        let watch = platform;
        self.stopped.store(watch.is_stopped(), Ordering::Release);
        *self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(watch);
        DemandError::SourceWatchStartup(Arc::clone(self))
    }

    /// Quiesces the retained owner using the current cleanup invocation.
    pub fn stop_with_native_work(
        &self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        self.stop_retained_with_work(&mut crate::NativeMaterializationWork::clone(work), true)
    }

    /// Retires the retained original watch using only its birth-paid cleanup.
    pub fn stop_with_cleanup_wait(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<(), DemandError> {
        self.stop_original_with_wait(Some(wait))
    }

    /// Retires only the retained owner's already-accepted physical resources.
    pub fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.stop_original_with_wait(None)
    }

    fn stop_original_with_wait(
        &self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<(), DemandError> {
        if let Some(wait) = wait {
            wait.check().map_err(|_| DemandError::Cancelled)?;
        }
        let (owner, poisoned) = match self.watch.try_lock() {
            Ok(mut custody) => (custody.take(), false),
            Err(std::sync::TryLockError::WouldBlock) => return Err(DemandError::WorkerUnavailable),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                (poisoned.into_inner().take(), true)
            }
        };
        let Some(owner) = owner else {
            return if poisoned {
                Err(DemandError::OriginalWorkPoisoned)
            } else if self.is_stopped() {
                Ok(())
            } else {
                Err(DemandError::WorkerUnavailable)
            };
        };
        let lease = FailedWatchLease {
            owner: Some(owner),
            failure: self,
        };
        let native = lease
            .owner
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        let result = match wait {
            Some(wait) => native.stop_with_cleanup_wait(wait),
            None => native.stop_accepted_cleanup(),
        };
        if poisoned {
            result.and(Err(DemandError::OriginalWorkPoisoned))
        } else {
            result
        }
    }

    /// Uses the same retained-owner lease and original native cleanup engine.
    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        self.stop_retained_with_work(work, false)
    }

    fn stop_retained_with_work<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
        retiring: bool,
    ) -> Result<(), DemandError> {
        work.check()?;
        let owner = self
            .watch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(owner) = owner else {
            return if self.is_stopped() {
                Ok(())
            } else {
                Err(DemandError::WorkerUnavailable)
            };
        };
        let lease = FailedWatchLease {
            owner: Some(owner),
            failure: self,
        };
        let owner = lease
            .owner
            .as_ref()
            .ok_or(DemandError::NativeWorkUnavailable)?;
        if retiring {
            owner.retire_with_work(work)
        } else {
            owner.stop_with_work(work)
        }
    }

    /// True only after the retained native owner has actually quiesced.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}
