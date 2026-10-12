use super::*;

/// Actual failed constructor and retained owners.
pub struct NativeMountStartupFailure {
    pub(super) error: NativeMountError,
    pub(super) owner: NativeMountSession,
    pub(super) retired: Option<NativeMountSession>,
    pub(super) custody: crate::NativeAllocation,
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
    /// Original warm-continuation owner, including all unresolved DATA.
    #[must_use]
    pub fn retired_owner(&self) -> Option<&NativeMountSession> {
        self.retired.as_ref()
    }
    /// Moves the original cause, new source owner, and complete old outcome.
    #[must_use]
    pub fn into_parts(
        self: Box<Self>,
    ) -> (
        NativeMountError,
        NativeMountSession,
        Option<NativeMountSession>,
    ) {
        let Self {
            error,
            owner,
            retired,
            custody,
        } = *self;
        drop(custody);
        (error, owner, retired)
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
    pub(super) retired: Option<NativeMountSession>,
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
            retired: None,
            custody,
        })
    }
    pub(super) fn failed(
        self,
        error: NativeMountError,
        owner: NativeMountSession,
    ) -> Box<NativeMountStartupFailure> {
        let Self {
            buffer,
            retired,
            custody,
        } = self;
        Box::write(
            buffer,
            NativeMountStartupFailure {
                error,
                owner,
                retired,
                custody,
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

    /// Continues only this SDK owner's retired Windows namespace with a freshly constructed stock source.
    pub fn restart_with_meter(
        mut self,
        retired: NativeMountSession,
        source: Arc<dyn MountFilesystem>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeMountSession, NativeMountError> {
        let mut next = NativeMountSession::new(
            NativeMountRequest {
                destination: PathBuf::new(),
                ..retired.request
            },
            Some(source.clone()),
            None,
        );
        self.retired = Some(retired);
        let admitted = (|| -> Result<(), NativeMountError> {
            let original = work.clone();
            let storage_work = self.custody.work();
            if !storage_work.shares_ledger(&original)
                || !storage_work.shares_resource_allowance(&original)
            {
                return Err(NativeMountError::Lifecycle(
                    "restart differs from its preborn original work",
                ));
            }
            original.admit(crate::WorkCounters::UNCHARGED)?;
            let old = self.retired.as_ref().expect("complete original owner");
            if !old.cleanup_state().cleanup_complete() {
                return Err(NativeMountError::ControlPending);
            }
            #[cfg(not(windows))]
            return Err(NativeMountError::UnsupportedTarget);
            #[cfg(windows)]
            {
                let outcome = old
                    .namespace_outcome
                    .as_ref()
                    .ok_or(NativeMountError::Lifecycle(
                        "restart has no original SDK namespace outcome",
                    ))?;
                if outcome.qualification().is_none() || !outcome.continuation_ready() {
                    return Err(NativeMountError::Lifecycle(
                        "original namespace capture DATA is not settled",
                    ));
                }
                let lineage =
                    source
                        .native_namespace_lineage()
                        .ok_or(NativeMountError::Lifecycle(
                            "restart source has no sealed stock lineage",
                        ))?;
                if !adapter::namespace_gate_is_fresh(&lineage)
                    || !old
                        .namespace_binding
                        .as_ref()
                        .is_some_and(|binding| binding.matches(&lineage))
                {
                    return Err(NativeMountError::Lifecycle(
                        "restart requires fresh capture gate on original checkout/root/work",
                    ));
                }
                if old
                    .retired_callback_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some()
                {
                    return Err(NativeMountError::Lifecycle(
                        "original callback outcome is not settled",
                    ));
                }
                let (destination, custody) =
                    crate::native_host::clone_path(&old.request.destination, &mut &original)?;
                next.request.destination = destination;
                next._destination_custody = Some(custody);
                Ok(())
            }
        })();
        if let Err(error) = admitted {
            return Err(NativeMountError::StartupFailed(self.failed(error, next)));
        }
        start_native_mount_prepared_with_meter(
            next.request,
            source,
            true,
            work,
            self,
            next._destination_custody,
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
