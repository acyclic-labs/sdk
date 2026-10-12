use super::*;

/// One process-owned high-level Darwin mount session.
pub(in crate::native_mount) struct DarwinMountSession {
    pub(super) resources: Option<Arc<DriverSessionResources>>,
    pub(super) destination: PathBuf,
    pub(super) thread: Option<JoinHandle<c_int>>,
    pub(super) events: Arc<LoopEvents>,
    pub(super) loop_finished_before_teardown: Option<bool>,
    pub(super) loop_result: Option<MountLoopResult>,
    pub(super) teardown_complete: bool,
}

#[derive(Debug)]
pub(super) enum MountLoopResult {
    Exited(c_int),
    Panicked,
    TimedOut,
}

/// Both the session and mount loop own these pointers. A timed-out loop may
/// outlive the session, but neither pointer is freed until that loop exits.
pub(super) struct DriverSessionResources {
    pub(super) source_context: usize,
    pub(super) driver_session: usize,
}

impl DriverSessionResources {
    pub(in crate::native_mount) fn context(&self) -> &DarwinMountContext {
        // SAFETY: the context outlives every owner of these resources.
        unsafe { &*(self.source_context as *const DarwinMountContext) }
    }
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        // SAFETY: the last owner has released these exact allocations, and
        // the mount loop retains an owner through every native callback.
        unsafe {
            acyclic_fs_darwin_mount_session_free(self.driver_session as *mut c_void);
            drop(Arc::from_raw(
                self.source_context as *const DarwinMountContext,
            ));
        }
    }
}

impl DarwinMountSession {
    pub(in crate::native_mount) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, DriverStartFailure> {
        static STARTUP: OnceLock<Mutex<()>> = OnceLock::new();
        let _startup = STARTUP.get_or_init(|| Mutex::new(())).lock().map_err(|_| {
            NativeMountError::Driver("Darwin mount startup lock is poisoned".to_owned())
        })?;
        let root = source
            .lookup(&MountPath::root())
            .map_err(source_error)?
            .ok_or_else(|| NativeMountError::Driver("volume root is absent".to_owned()))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(
                NativeMountError::Driver("volume root is not a directory".to_owned()).into(),
            );
        }
        let destination_metadata = request
            .destination
            .metadata()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let parent_metadata = request
            .destination
            .parent()
            .and_then(|parent| parent.metadata().ok())
            .ok_or_else(|| NativeMountError::Driver("mount parent is unavailable".to_owned()))?;
        let supports_named_attributes = source.supports_posix_named_attributes();
        let context = DarwinMountContext::new(
            source,
            request.writable,
            &destination_metadata,
            root.node.file_id,
        )
        .observing_source();

        let destination = request.destination.clone();
        let destination_c = path_cstring(&destination).map_err(driver_errno)?;
        let options = mount_options(request.writable, supports_named_attributes);
        let arguments = fuse_arguments(&options);
        let argument_count = c_int::try_from(arguments.len())
            .map_err(|_| NativeMountError::Driver("too many Darwin mount arguments".to_owned()))?;
        // Transfer the Rust callback context only after every fallible argument
        // conversion has completed, then bind one C interrupt handle to this
        // exact loop. The bridge registry keeps native control state scoped to
        // this session, so independent mounts do not share an interrupt target.
        let source_context = Arc::into_raw(context) as usize;
        // SAFETY: the session allocator has no preconditions; null is handled below.
        let driver_session = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
        if driver_session == 0 {
            // SAFETY: `source_context` came from `Arc::into_raw` above and was never
            // handed to the bridge, so this reclaims its only reference once.
            unsafe { drop(Arc::from_raw(source_context as *const DarwinMountContext)) };
            return Err(NativeMountError::Driver(
                "Darwin mount session allocation failed".to_owned(),
            )
            .into());
        }
        let resources = Arc::new(DriverSessionResources {
            source_context,
            driver_session,
        });
        let loop_resources = Arc::clone(&resources);
        let events = Arc::new(LoopEvents::default());
        let exit_report = ExitReport(Arc::clone(&events));
        let thread = std::thread::Builder::new()
            .name("acyclic-fs-darwin-nfs".to_owned())
            .spawn(move || {
                let exit_report = exit_report;
                let pointers = arguments
                    .iter()
                    .map(|argument| argument.as_ptr())
                    .collect::<Vec<_>>();
                let mut interrupted_before_namespace = 0;
                // SAFETY: this closure owns `arguments`, `pointers` and `destination_c` for
                // the whole blocking loop; `loop_resources` keeps the session and context
                // allocations alive and `exit_report` keeps the reported events alive.
                unsafe {
                    acyclic_fs_darwin_mount_run(
                        loop_resources.driver_session as *mut c_void,
                        argument_count,
                        pointers.as_ptr(),
                        destination_c.as_ptr(),
                        loop_resources.source_context,
                        report_mounted,
                        Arc::as_ptr(&exit_report.0).cast_mut().cast(),
                        &mut interrupted_before_namespace,
                    )
                }
            })
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self {
            resources: Some(resources),
            destination,
            thread: Some(thread),
            events,
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: false,
        }
        .await_mount(&parent_metadata)
    }

    pub(in crate::native_mount) fn await_mount(mut self, parent_metadata: &Metadata) -> Result<Self, DriverStartFailure> {
        let reported = self.events.wait_until(MOUNT_VISIBILITY_TIMEOUT, |state| {
            state.mounted || state.exited
        });
        if !reported.mounted {
            if !reported.exited {
                return Err(self.failed_start(NativeMountError::Driver(format!(
                    "Darwin mount did not become visible within {} seconds",
                    MOUNT_VISIBILITY_TIMEOUT.as_secs()
                ))));
            }
            let status = self.finish_thread();
            self.loop_finished_before_teardown = Some(true);
            let description = format!("{status:?}");
            self.loop_result = Some(status);
            return Err(self.failed_start(NativeMountError::Driver(format!(
                "Darwin mount exited before the mount became visible: {description}"
            ))));
        }
        // The loop reports only a mount the kernel accepted; the mount table
        // confirms that it is this destination's.
        match is_mounted(&self.destination, parent_metadata) {
            Ok(true) => {}
            Ok(false) => {
                return Err(self.failed_start(NativeMountError::Driver(
                    "Darwin mount reported a mount the mount table does not show".to_owned(),
                )));
            }
            Err(error) => return Err(self.failed_start(NativeMountError::Driver(error))),
        }
        // A successful NFS mount does not prove the caller can use it: macOS
        // privacy policy can allow metadata lookups while denying every open
        // on the mounted network volume. Opening the root reads no entries or
        // content, and fails before exposing an unusable child workspace.
        if let Err(error) = std::fs::File::open(&self.destination) {
            let message = if error.raw_os_error() == Some(libc::EPERM) {
                format!(
                    "macOS denied opening the mounted network volume: {error}; for SSH sessions, enable Remote Login → Allow full disk access for remote users"
                )
            } else {
                format!("Darwin mount root could not be opened: {error}")
            };
            return Err(self.failed_start(NativeMountError::Driver(message)));
        }
        Ok(self)
    }

    pub(in crate::native_mount) fn failed_start(mut self, error: NativeMountError) -> DriverStartFailure {
        match self.stop() {
            Ok(()) => error.into(),
            Err(cleanup) => {
                // The caller will preserve the destination fence. Do not let
                // Drop retry teardown without owning that fence: its result
                // could otherwise contradict the caller's ownership decision.
                std::mem::forget(self);
                DriverStartFailure::preserving_destination_fence(NativeMountError::Driver(format!(
                    "{error}; teardown failed: {cleanup}"
                )))
            }
        }
    }

    /// Marks one mount-relative path (leading `/` optional) changed by a
    /// projection change the source did not record, such as a removed
    /// route. NFS offers no server-initiated invalidation without
    /// delegations: this forgets everything the mount cached about the view
    /// (the change may reach beyond `path`), so the client drops its cached
    /// names, attributes, and data at its next revalidation, which
    /// [`Self::revalidate`] waits for and every open makes immediately.
    pub(in crate::native_mount) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let resources = self.resources()?;
        let mut bytes = Vec::with_capacity(path.len() + 1);
        if path.first() != Some(&b'/') {
            bytes.push(b'/');
        }
        bytes.extend_from_slice(path);
        let path = CString::new(bytes)
            .map_err(|_| NativeMountError::Driver("path contains NUL".to_owned()))?;
        // Reject continuation snapshots before the external invalidation can
        // overlap a directory read, and make the client's next revalidation
        // discard what it cached.
        resources.context().forget_view();
        // SAFETY: `driver_session` is the live bridge session this struct
        // owns until teardown, and `path` is a NUL-terminated string.
        let status = unsafe {
            acyclic_fs_darwin_mount_invalidate(
                resources.driver_session as *mut c_void,
                path.as_ptr(),
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(NativeMountError::Driver(format!(
                "invalidate returned {status}"
            )))
        }
    }

    /// See [`DarwinMountContext::revalidate`].
    pub(in crate::native_mount) fn revalidate(&self) -> Result<(), NativeMountError> {
        let context = self.resources()?.context();
        // Every change made to the source before this call is recorded, and
        // so counted as made around the mount, before the wait begins.
        context
            .source
            .fence_changes()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        context.revalidate();
        Ok(())
    }

    pub(in crate::native_mount) fn resources(&self) -> Result<&DriverSessionResources, NativeMountError> {
        self.resources
            .as_deref()
            .ok_or_else(|| NativeMountError::Driver("Darwin mount session has stopped".to_owned()))
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "keeps the fallible stop signature the other mount drivers share"
    )]
    pub(in crate::native_mount) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.teardown_complete {
            return Ok(());
        }
        // Observe whether the provider loop failed independently before any
        // teardown action can make it exit. A detach closes the transport,
        // which ends the loop, so sampling after it races a successful detach
        // and misclassifies it as a pre-existing failure.
        if self.loop_result.is_none() {
            self.loop_finished_before_teardown =
                Some(self.thread.is_none() || self.events.current().exited);
        }
        let unmount = bounded_unmount(&self.destination);
        if self
            .loop_result
            .as_ref()
            .is_none_or(|result| matches!(result, MountLoopResult::TimedOut))
        {
            if let Some(resources) = &self.resources {
                // SAFETY: `resources` keeps the bridge session alive; interrupting it only
                // signals that session's loop.
                unsafe {
                    acyclic_fs_darwin_mount_interrupt(resources.driver_session as *mut c_void);
                }
            }
            let join = self.finish_thread();
            self.loop_result = Some(join);
        }
        let unmount = unmount.map_err(NativeMountError::Driver)?;
        let loop_finished_before_teardown =
            self.loop_finished_before_teardown.ok_or_else(|| {
                NativeMountError::Driver("Darwin mount teardown state is absent".to_owned())
            })?;
        let join = self.loop_result.as_ref().ok_or_else(|| {
            NativeMountError::Driver("Darwin mount loop result is absent".to_owned())
        })?;
        let result = classify_detached_loop(join, unmount, loop_finished_before_teardown);
        if result.is_ok() {
            self.teardown_complete = true;
        }
        result
    }

    pub(in crate::native_mount) fn finish_thread(&mut self) -> MountLoopResult {
        self.finish_thread_within(MOUNT_LOOP_EXIT_TIMEOUT)
    }

    pub(in crate::native_mount) fn finish_thread_within(&mut self, timeout: Duration) -> MountLoopResult {
        if self.thread.is_none() {
            return MountLoopResult::Exited(0);
        }
        if !self.events.wait_until(timeout, |state| state.exited).exited {
            // Keep the join handle, callback resources, and destination
            // fence so a later stop can retry once callbacks have exited.
            return MountLoopResult::TimedOut;
        }
        // The loop has returned: joining waits only for its thread to end.
        let Some(thread) = self.thread.take() else {
            return MountLoopResult::Panicked;
        };
        let status = match thread.join() {
            Ok(status) => MountLoopResult::Exited(status),
            Err(_) => MountLoopResult::Panicked,
        };
        self.resources.take();
        status
    }
}

pub(super) fn classify_detached_loop(
    join: &MountLoopResult,
    unmount: UnmountEvidence,
    loop_finished_before_teardown: bool,
) -> Result<(), NativeMountError> {
    match (join, unmount, loop_finished_before_teardown) {
        (MountLoopResult::Exited(0), _, false)
        | (MountLoopResult::Exited(0), UnmountEvidence::AlreadyUnmounted, true) => Ok(()),
        // An external forced detach is a mount-loss event, not a provider
        // failure.  The destination evidence distinguishes it from a
        // provider loop that died while its stale mount was still present.
        (MountLoopResult::Exited(0), _, true) => Err(NativeMountError::Driver(
            "Darwin mount loop exited before teardown began".to_owned(),
        )),
        // A detached namespace does not prove that in-flight callbacks have
        // finished. Publishing this destination now could race a late write.
        (MountLoopResult::TimedOut, _, _) => Err(NativeMountError::Driver(
            "Darwin mount callbacks have not finished; destination remains fenced".to_owned(),
        )),
        (MountLoopResult::Exited(status), _, _) => Err(NativeMountError::Driver(format!(
            "Darwin mount loop exited with status {status}"
        ))),
        (MountLoopResult::Panicked, _, _) => Err(NativeMountError::Driver(
            "Darwin mount loop thread panicked".to_owned(),
        )),
    }
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Only options the loopback NFS mount honors: `DarwinFUSE` rejects any
/// other rather than dropping it.
pub(super) fn mount_options(writable: bool, supports_named_attributes: bool) -> Vec<&'static CStr> {
    let mut options = vec![c"nobrowse"];
    if !writable {
        options.push(c"ro");
    }
    if supports_named_attributes {
        options.push(c"namedattr");
    }
    options
}

pub(super) fn fuse_arguments(options: &[&CStr]) -> Vec<CString> {
    let mut arguments = Vec::with_capacity(options.len().saturating_mul(2).saturating_add(1));
    arguments.push(c"acyclic-fs".to_owned());
    for option in options {
        arguments.push(c"-o".to_owned());
        arguments.push((*option).to_owned());
    }
    arguments
}
