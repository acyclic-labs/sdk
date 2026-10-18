use super::*;

/// Actual failed constructor and retained owners.
pub struct NativeMountStartupFailure {
    pub(super) error: NativeMountError,
    pub(super) owner: NativeMountSession,
    pub(super) _custody: crate::NativeAllocation,
}

impl NativeMountStartupFailure {
    /// Original typed constructor failure.
    #[must_use]
    pub fn error(&self) -> &NativeMountError {
        &self.error
    }
    /// Original driver and physical destination fence.
    #[must_use]
    pub fn owner(&self) -> &NativeMountSession {
        &self.owner
    }
    /// Retains the original owner for a bounded cleanup retry.
    pub fn owner_mut(&mut self) -> &mut NativeMountSession {
        &mut self.owner
    }
}

impl std::fmt::Debug for NativeMountStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeMountStartupFailure")
            .field("error", self.error())
            .field("owner", &self.owner().cleanup_state())
            .finish()
    }
}
impl std::fmt::Display for NativeMountStartupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), formatter)
    }
}
impl std::error::Error for NativeMountStartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error())
    }
}
/// Actual preadmitted failure storage.
pub struct PreparedNativeMountStart {
    pub(super) buffer: Box<std::mem::MaybeUninit<NativeMountStartupFailure>>,
    pub(super) custody: crate::NativeAllocation,
}
impl PreparedNativeMountStart {
    pub(super) fn new(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountStartupAdmissionError> {
        let custody = work.claim(
            std::mem::size_of::<NativeMountStartupFailure>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Self {
            buffer: Box::new(std::mem::MaybeUninit::uninit()),
            custody,
        })
    }
    pub(super) fn failed(
        self,
        error: NativeMountError,
        owner: NativeMountSession,
    ) -> Box<NativeMountStartupFailure> {
        let Self { buffer, custody } = self;
        Box::write(
            buffer,
            NativeMountStartupFailure {
                error,
                owner,
                _custody: custody,
            },
        )
    }
}

/// Admits startup failure storage on the original ledger.
pub fn prepare_native_mount_start(
    work: &crate::NativeMaterializationWork,
) -> Result<PreparedNativeMountStart, NativeMountStartupAdmissionError> {
    let _services = adapter::CallbackRuntime::existing_startup()?;
    work.admit(crate::WorkCounters::UNCHARGED)?;
    PreparedNativeMountStart::new(work)
}

impl PreparedNativeMountStart {
    /// Starts the same actual constructor on an already operative original Arc.
    pub fn start(
        self,
        request: NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &NativeMaterializationWork,
        destination_custody: Option<crate::NativeAllocation>,
    ) -> Result<NativeMountSession, NativeMountError> {
        start_native_mount_prepared_with_meter(
            request,
            source,
            false,
            work,
            self,
            destination_custody,
        )
    }

    /// Places an original failed source constructor into this precreated owner.
    #[must_use]
    pub fn retain_source_startup_failure(
        self,
        request: NativeMountRequest,
        destination_custody: crate::NativeAllocation,
        error: NativeMountError,
    ) -> NativeMountError {
        let (startup_source_watch, error) = match error {
            NativeMountError::SourceWatchStartupFailed { cause, owner } => {
                (Some(owner), NativeMountError::Demand(cause))
            }
            error => (None, error),
        };
        let mut owner = NativeMountSession::new(request, None, Some(destination_custody));
        owner.startup_source_watch = startup_source_watch;
        owner.measured_cleanup_failed = true;
        NativeMountError::StartupFailed(self.failed(error, owner))
    }
}

/// Upper lifecycle payload held by the existing physical driver loop.
pub(super) struct NativeMountRetirement {
    pub(super) retained_source: Option<Arc<dyn MountFilesystem>>,
    pub(super) destination_guard: Option<MountDestinationGuard>,
    pub(super) startup_source_watch: Option<lazy::NativeMountSourceWatchOwner>,
    #[cfg(windows)]
    pub(super) warm_namespace: Option<projfs::ProjFsNamespaceQualification>,
}

impl NativeMountRetirement {
    /// Called only after the driver has actually drained namespace callbacks.
    pub(super) fn retire(
        &mut self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, NativeMountError> {
        let mut changed = self.retire_source_and_watch_with_cleanup(wait)?;
        if let Some(guard) = self.destination_guard.as_mut() {
            guard.remove_destination()?;
            guard.release_cleanup(wait)?;
            drop(self.destination_guard.take());
            changed = true;
        }
        Ok(changed)
    }

    /// Drains source producers after callback admission closes, before the driver's own namespace teardown.
    pub(super) fn retire_source_and_watch_with_cleanup(
        &mut self,
        wait: Option<&crate::CancellationToken>,
    ) -> Result<bool, NativeMountError> {
        let mut changed = false;
        if let Some(source) = self.retained_source.as_ref() {
            match wait {
                Some(wait) => source.shutdown_with_cleanup_wait(wait),
                None => source.shutdown_accepted_cleanup(),
            }?;
            drop(self.retained_source.take());
            changed = true;
        }
        if let Some(watch) = self.startup_source_watch.as_mut() {
            let result = if watch.is_stopped() {
                Ok(false)
            } else {
                match wait {
                    Some(wait) => watch.stop_with_cleanup_wait(wait),
                    None => watch.stop_accepted_cleanup(),
                }
            };
            if watch.is_stopped() {
                drop(self.startup_source_watch.take());
                changed = true;
            }
            result?;
        }
        Ok(changed)
    }

    pub(super) fn is_retired(&self) -> bool {
        self.retained_source.is_none()
            && self.startup_source_watch.is_none()
            && self.destination_guard.is_none()
    }
}
