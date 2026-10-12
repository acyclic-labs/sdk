use super::*;

pub(super) fn callback_errno(error: &NativeMountError) -> i32 {
    match error {
        NativeMountError::CallbackDenied { .. } => libc::EACCES,
        NativeMountError::Source(error)
        | NativeMountError::Demand(crate::DemandError::Mount(error)) => source_errno(error),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) => {
            libc::ECANCELED
        }
        NativeMountError::Materialization(error) => {
            crate::native_mount::native_materialization_errno(error)
        }
        NativeMountError::Io(error) => error.raw_os_error().unwrap_or(libc::EIO),
        NativeMountError::Demand(crate::DemandError::Cancelled) => libc::ECANCELED,
        NativeMountError::Allocation(_) => libc::ENOMEM,
        _ => libc::EIO,
    }
}

fn source_errno(error: &MountSourceError) -> i32 {
    match error.semantic_error() {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) | MountSourceError::NamespacePath(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) | MountSourceError::NativeUnsupported => libc::EOPNOTSUPP,
        MountSourceError::Stale
        | MountSourceError::Filesystem(crate::FsError::SourceSnapshotStale) => libc::ESTALE,
        MountSourceError::Cancelled
        | MountSourceError::Filesystem(crate::FsError::Cancelled(_)) => libc::ECANCELED,
        MountSourceError::Allocation(_) => libc::ENOMEM,
        MountSourceError::Io(error, _) => error.raw_os_error().unwrap_or(libc::EIO),
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled) => {
            libc::ECANCELED
        }
        MountSourceError::NativeWork(error) => {
            crate::native_mount::native_materialization_errno(error)
        }
        MountSourceError::WorkspaceCause(error) => {
            crate::native_mount::native_workspace_errno(error)
        }
        MountSourceError::WorkspacePrefixFailure { source, .. } => {
            crate::native_mount::native_workspace_errno(source)
        }
        MountSourceError::NativeCallbackDenied { .. } => libc::EACCES,
        MountSourceError::NativeWorkspaceCause(error, _) => {
            crate::native_mount::native_workspace_errno(error)
        }
        _ => libc::EIO,
    }
}

pub(super) fn fatal(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Work(_)
        | NativeMountError::Demand(crate::DemandError::Cancelled)
        | NativeMountError::ControlPending
        | NativeMountError::CallbackDenied { .. }
        | NativeMountError::DriverPanicked { .. } => true,
        NativeMountError::Materialization(error) => {
            crate::native_mount::native_materialization_failure_is_fatal(error)
        }
        NativeMountError::Source(error)
        | NativeMountError::Demand(crate::DemandError::Mount(error)) => {
            match error.semantic_error() {
                MountSourceError::NativeWork(error) => {
                    crate::native_mount::native_materialization_failure_is_fatal(error)
                }
                MountSourceError::WorkspaceCause(error) => {
                    crate::native_mount::native_workspace_failure_is_fatal(error)
                }
                MountSourceError::WorkspacePrefixFailure { source, accounting } => {
                    crate::native_mount::native_workspace_failure_is_fatal(source)
                        || crate::native_mount::native_workspace_failure_is_fatal(accounting)
                }
                MountSourceError::NativeWorkspaceCause(error, _) => {
                    crate::native_mount::native_workspace_failure_is_fatal(error)
                }
                MountSourceError::Cancelled
                | MountSourceError::Work(_)
                | MountSourceError::OriginalSnapshotUnavailable
                | MountSourceError::Indeterminate
                | MountSourceError::Filesystem(
                    crate::FsError::Cancelled(_) | crate::FsError::Work(_),
                ) => true,
                #[cfg(feature = "native-watch")]
                MountSourceError::SourceWatchStartup(_)
                | MountSourceError::NativeWatchStartupFailed { .. }
                | MountSourceError::NativeWatchSourceStartupFailed { .. } => true,
                _ => false,
            }
        }
        _ => false,
    }
}

pub(super) fn context(address: usize) -> Result<&'static DarwinMountContext, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    // SAFETY: the bridge borrows the context retained by the live SDK session.
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

pub(super) fn ffi<T: From<i32>>(
    address: usize,
    span: &tracing::span::EnteredSpan,
    authored_write: bool,
    operation: impl FnOnce(&DarwinMountContext, &Work) -> Result<T, NativeMountError>,
) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let context = context(address)?;
        let _callback = context.callbacks.enter();
        let _origin = context.origin.enter();
        context.run(authored_write, |work| operation(context, work))
    }))
    .unwrap_or(Err(libc::EIO))
    .unwrap_or_else(|error| {
        span.record("errno", error);
        T::from(-error)
    })
}

impl DarwinMountContext {
    pub(super) fn record_failure(&self, cause: NativeMountError, work: WorkCounters) -> i32 {
        let status = callback_errno(&cause);
        if fatal(&cause) {
            let mut state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.first_fatal.is_none() {
                state.first_fatal = Some((Some(cause), work));
            }
        }
        status
    }

    pub(super) fn run<T>(
        &self,
        authored_write: bool,
        operation: impl FnOnce(&Work) -> Result<T, NativeMountError>,
    ) -> Result<T, i32> {
        let active = self.callback_meter().map_err(|cause| {
            let work = self
                .source_original_work
                .as_ref()
                .map_or(WorkCounters::UNCHARGED, |work| work.state.lock().work);
            self.record_failure(cause, work)
        })?;
        {
            let state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some((cause, _)) = &state.first_fatal {
                return Err(cause.as_ref().map_or(libc::EIO, callback_errno));
            }
        }
        let operation = |work: &Work| {
            work.admit(WorkCounters::UNCHARGED)?;
            if authored_write {
                self.admit_write(work)?;
            }
            let value = operation(work)?;
            if authored_write {
                self.admit_write(work)?;
            }
            Ok(value)
        };
        let future_request = active.is_none();
        let work = match active {
            Some(work) => work,
            None => Work::new(self.handler_budget, &self.handler_cancellation)
                .map_err(|(cause, work)| self.record_failure(cause.into(), work))?,
        };
        let result = crate::native_mount::run_native_mount_handler_on_meter(&work, operation);
        let completed = work.receipt();
        if future_request {
            if let Err(overflow) = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .completed_future_request_work
                .try_add_assign(&completed)
            {
                match &result {
                    Ok(_) => return Err(self.record_failure(overflow.into(), completed)),
                    Err(cause) if !fatal(cause) => {
                        self.record_failure(overflow.into(), completed);
                    }
                    Err(_) => {}
                }
            }
        }
        result.map_err(|cause| self.record_failure(cause, completed))
    }
}
