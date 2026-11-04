use super::*;

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
        platform.delivery().notify_lost();
        assert!(
            self.cause.set(error).is_ok(),
            "one startup control retains one actual native owner"
        );
        assert!(
            self.watch.set(platform).is_ok(),
            "one startup control retains one actual native owner"
        );
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
        self.owner()?.stop_with_cleanup_wait(wait)
    }

    /// Retires only the retained owner's already-accepted physical resources.
    pub fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.owner()?.stop_accepted_cleanup()
    }

    fn owner(&self) -> Result<&NativeSourceWatch, DemandError> {
        self.watch.get().ok_or(DemandError::NativeWorkUnavailable)
    }

    /// Uses the same retained owner and original native cleanup engine.
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
        let owner = self.owner()?;
        if retiring {
            owner.retire_with_work(work)
        } else {
            owner.stop_with_work(work)
        }
    }

    /// True only after the retained native owner has actually quiesced.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.watch.get().is_some_and(NativeSourceWatch::is_stopped)
    }
}
