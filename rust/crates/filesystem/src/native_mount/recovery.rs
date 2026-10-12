use super::*;

/// Real cleanup custody and driver evidence for one retained native session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMountCleanupState<'a> {
    /// Stable identity of the actual mounted owner.
    pub mount_id: MountId,
    /// Original destination, borrowed without creating another path carrier.
    pub destination: &'a Path,
    /// Driver, callback, or outstanding native-control custody remains owned.
    pub driver_retained: bool,
    /// The original destination fence remains owned.
    pub destination_fence_retained: bool,
    /// The supplied source and any actually owned watcher remain retained.
    pub source_retained: bool,
    /// The driver has actual successful namespace-detach evidence.
    pub namespace_detach_confirmed: bool,
    /// The last measured cleanup failed; retained custody requires explicit retry.
    pub measured_cleanup_failed: bool,
}

impl NativeMountCleanupState<'_> {
    /// Whether driver, source/watcher, and destination fence were released.
    pub fn cleanup_complete(&self) -> bool { !self.driver_retained && !self.destination_fence_retained && !self.source_retained }
}

/// Borrowed actual recovery custody, not a physical root-presence probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMountRecoveryCleanupState<'a> {
    /// Original destination.
    pub destination: &'a Path,
    /// Identity queried from the actual original held fence file.
    pub fence_identity: crate::NativeRootIdentity,
    /// Original destination fence remains owned.
    pub destination_fence_retained: bool,
    /// Actual native task/process/SDK cleanup remains owned.
    pub controls_pending: bool,
    /// Actual driver evidence; false never invents a namespace-presence result.
    pub namespace_detach_confirmed: bool,
    /// Requested empty-directory removal completed or proved the name absent.
    pub destination_removed: bool,
    /// Last actual measured action failed.
    pub measured_cleanup_failed: bool,
}

#[cfg(target_os = "linux")]
pub(super) type NativeRecoveryControl = fuse::FuseRecoveryControl;
#[cfg(target_os = "macos")]
pub(super) type NativeRecoveryControl = darwin_mount::DarwinRecoveryControl;
#[cfg(target_os = "windows")]
pub(super) type NativeRecoveryControl = projfs::ProjFsRecoveryControl;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
#[derive(Default)]
pub(super) struct NativeRecoveryControl;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
impl NativeRecoveryControl {
    pub(super) fn controls_pending(&self) -> bool { false }
    pub(super) fn namespace_detach_confirmed(&self) -> bool { false }
    pub(super) fn retire_controls_with_work(&mut self, _original: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        Ok(())
    }
}

pub(super) struct NativeRecoveryState {
    #[cfg(windows)]
    pub(super) destination_identity: Option<crate::NativeRootIdentity>,
    pub(super) guard: Option<MountDestinationGuard>,
    pub(super) control: NativeRecoveryControl,
    pub(super) driver_step_complete: bool,
    pub(super) destination_removed: bool,
    pub(super) measured_cleanup_failed: bool,
    pub(super) panic: Option<Box<dyn std::any::Any + Send>>,
    pub(super) retirement_error: Option<NativeMountError>,
}

#[derive(Clone, Copy)]
pub(super) struct NativeRecoverySnapshot {
    pub(super) fence_identity: crate::NativeRootIdentity,
    pub(super) destination_fence_retained: bool,
    pub(super) controls_pending: bool,
    pub(super) namespace_detach_confirmed: bool,
    pub(super) destination_removed: bool,
    pub(super) measured_cleanup_failed: bool,
}

pub(super) enum NativeRecoveryCommand {
    Action { remove_destination: bool, work: crate::NativeMaterializationWork },
    LastDrop,
}

pub(super) struct NativeRecoveryMailbox {
    pub(super) command: Option<NativeRecoveryCommand>,
    pub(super) reply: Option<Result<bool, NativeMountError>>,
    pub(super) snapshot: Option<NativeRecoverySnapshot>,
    // Actual state DATA has dropped; this is not a std thread-join proof.
    pub(super) state_retired: bool,
}

pub(super) struct NativeRecoveryShared {
    pub(super) destination: PathBuf,
    pub(super) mailbox: std::sync::Mutex<NativeRecoveryMailbox>,
    pub(super) wake: std::sync::Condvar,
    // Physical path, slot, and reply DATA precede their original custody.
    pub(super) _custody: crate::NativeAllocation,
}

pub(super) struct NativeRecoveryWorker {
    pub(super) shared: Arc<NativeRecoveryShared>,
    pub(super) state: NativeRecoveryState,
    pub(super) original: crate::NativeMaterializationWork,
    pub(super) final_close: crate::NativeWorkReservation,
    pub(super) _closure_custody: crate::NativeAllocation,
}

/// Original recovery control and destination fence, acquired once before action.
pub struct NativeMountRecoveryOwner {
    pub(super) shared: Arc<NativeRecoveryShared>,
}

impl std::fmt::Debug for NativeMountRecoveryOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeMountRecoveryOwner")
            .field("cleanup", &self.cleanup_state()).finish()
    }
}

impl NativeMountRecoveryOwner {
    /// Administratively acquires the existing destination fence once.
    pub fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).map_err(|(error, _)| NativeMountError::from(error))?;
        Self::acquire_with_original(destination, original)
    }

    pub(super) fn acquire_with_original(
        destination: &Path, original: crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let mut destination = crate::native_host::clone_path_with_work(destination, &mut &original)?;
        let mut shared_custody = original.claim(std::mem::size_of::<NativeRecoveryShared>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        destination.1.absorb(&mut shared_custody)?;
        let closure_custody = original.claim(std::mem::size_of::<NativeRecoveryWorker>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        let final_close = original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        let control = NativeRecoveryControl::default();
        #[cfg(windows)]
        let control = {
            let mut control = control;
            control.prepare_retirement(&original)?;
            control
        };
        let shared = Arc::new(NativeRecoveryShared {
            destination: destination.0,
            mailbox: std::sync::Mutex::new(NativeRecoveryMailbox {
                command: None, reply: None, snapshot: None, state_retired: false,
            }),
            wake: std::sync::Condvar::new(),
            _custody: destination.1,
        });
        let worker = NativeRecoveryWorker {
            shared: Arc::clone(&shared),
            state: NativeRecoveryState {
                #[cfg(windows)]
                destination_identity: None,
                guard: None, control, driver_step_complete: false,
                destination_removed: false, measured_cleanup_failed: false, panic: None, retirement_error: None,
            },
            original, final_close,
            _closure_custody: closure_custody,
        };
        // std's scheduler, native thread stack, Arc headers, and JoinHandle
        // internals are opaque; known SDK closure/slot/path DATA are funded.
        crate::native_host::NativeHostWork::started_io(&mut &worker.original, crate::WorkCounters {
            backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
        })?;
        drop(std::thread::Builder::new().spawn(move || worker.run())?);
        let owner = Self { shared };
        let result = {
            let mut mailbox = owner.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            while mailbox.reply.is_none() {
                mailbox = owner.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
            }
            mailbox.reply.take().expect("actual acquisition reply")
        };
        result?;
        Ok(owner)
    }

    /// Borrows the real original identity, evidence, and retained resources.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountRecoveryCleanupState<'_> {
        let state = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner())
            .snapshot.expect("only an actually acquired owner is published");
        NativeMountRecoveryCleanupState {
            destination: &self.shared.destination,
            fence_identity: state.fence_identity,
            destination_fence_retained: state.destination_fence_retained,
            controls_pending: state.controls_pending,
            namespace_detach_confirmed: state.namespace_detach_confirmed,
            destination_removed: state.destination_removed,
            measured_cleanup_failed: state.measured_cleanup_failed,
        }
    }
    /// Recovers through the same engine with an explicit unbounded allowance.
    pub fn recover(&mut self) -> Result<bool, NativeMountError> { self.ordinary_action(true) }

    /// Detaches while preserving the underlying destination and authored residue.
    pub fn detach(&mut self) -> Result<bool, NativeMountError> { self.ordinary_action(false) }

    pub(super) fn ordinary_action(&mut self, remove_destination: bool) -> Result<bool, NativeMountError> {
        if !self.cleanup_state().destination_fence_retained { return Ok(false); }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.mark_failed();
            return Err(error.into());
        }
        self.measured_action(remove_destination, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value).map_err(|(error, _work)| error)
    }

    /// Detaches on finite work without removing the destination.
    pub fn detach_measured(
        &mut self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        self.measured_action(false, budget, cancellation)
    }

    pub(super) fn measured_action(
        &mut self, remove_destination: bool, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, crate::WorkCounters)> {
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((error, work)) => {
                self.mark_failed();
                return Err((error.into(), work));
            }
        };
        {
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            debug_assert!(mailbox.command.is_none() && mailbox.reply.is_none());
            mailbox.command = Some(NativeRecoveryCommand::Action { remove_destination, work: work.clone() });
            self.shared.wake.notify_one();
        }
        let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
        while mailbox.reply.is_none() {
            mailbox = self.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
        }
        let result = mailbox.reply.take().expect("actual action reply");
        drop(mailbox);
        let spent = work.receipt();
        result.map(|value| crate::OperationReceipt { value, work: spent }).map_err(|error| (error, spent))
    }

    pub(super) fn mark_failed(&mut self) {
        if let Some(snapshot) = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner()).snapshot.as_mut() {
            snapshot.measured_cleanup_failed = true;
        }
    }
}

impl NativeRecoveryState {
    pub(super) fn finish_with_work(
        &mut self, destination: &Path, remove_destination: bool, work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            if self.guard.is_none() { return Ok(false); }
            if !self.driver_step_complete {
                recover_driver_with_work(&mut self.control, destination, remove_destination, work)?;
                if self.control.controls_pending() { return Err(NativeMountError::ControlPending); }
                self.driver_step_complete = true;
            }
            if remove_destination && !self.destination_removed {
                #[cfg(windows)]
                projfs::remove_empty_recovery_destination_measured(
                    destination, self.destination_identity, &mut &*work,
                )?;
                #[cfg(not(windows))]
                remove_empty_recovery_destination(destination, work)?;
                self.destination_removed = true;
            }
            self.guard.as_mut().expect("original retained fence")
                .release(&mut &*work)?;
            self.guard.take();
            Ok(true)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }

    pub(super) fn snapshot(&self, identity: crate::NativeRootIdentity) -> NativeRecoverySnapshot {
        NativeRecoverySnapshot {
            fence_identity: identity,
            destination_fence_retained: self.guard.is_some(),
            controls_pending: self.control.controls_pending(),
            namespace_detach_confirmed: self.control.namespace_detach_confirmed(),
            destination_removed: self.destination_removed,
            measured_cleanup_failed: self.measured_cleanup_failed,
        }
    }
}

impl NativeRecoveryWorker {
    pub(super) fn acquire(&mut self) -> Result<crate::NativeRootIdentity, NativeMountError> {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        MountDestinationGuard::acquire_with_work(
            &self.shared.destination, true, &self.original, &mut self.state.guard, true,
        )?;
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        return Err(NativeMountError::UnsupportedTarget);
        let (identity, _) = crate::native_host::file_identity_and_length_with_work(
            self.state.guard.as_ref().and_then(|guard| guard.file.as_ref())
                .ok_or(NativeMountError::InvalidDestination)?, &mut &self.original,
        )?;
        #[cfg(windows)]
        {
            self.state.destination_identity = match crate::native_host::root_path_identity_with_work(
                &self.shared.destination, &mut &self.original,
            ) {
                Ok(identity) => Some(identity),
                Err(crate::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if let Some(identity) = self.state.destination_identity { self.state.control.bind_root_identity(identity); }
        }
        Ok(identity)
    }

    pub(super) fn retire(&mut self) {
        // This is the same real termination/reap engine, never namespace
        // recovery. An uninterruptible native task keeps its actual fence.
        while self.state.control.controls_pending() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                { self.state.control.retire_controls(&self.original) }
                #[cfg(not(any(target_os = "linux", target_os = "macos")))]
                { self.state.control.retire_controls_with_work(&self.original) }
            })) {
                Ok(Ok(())) if !self.state.control.controls_pending() => break,
                Ok(Err(error)) => {
                    if self.state.retirement_error.is_none() { self.state.retirement_error = Some(error); }
                }
                Ok(Ok(())) => {}
                Err(panic) => self.state.panic = Some(panic),
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        if let Some(guard) = self.state.guard.as_mut() {
            guard.close_administrative_fence(&mut self.final_close);
        }
        // No unlink, directory removal, or namespace-detach assertion.
        self.state.guard.take();
    }

    pub(super) fn run(mut self) {
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.acquire())) {
            Ok(result) => result,
            Err(panic) => {
                self.state.panic = Some(panic);
                Err(NativeMountError::Lifecycle("administrative acquisition panicked"))
            }
        };
        let identity = match result {
            Ok(identity) => identity,
            Err(error) => {
                self.retire();
                drop(self.state);
                let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                mailbox.reply = Some(Err(error));
                mailbox.state_retired = true;
                self.shared.wake.notify_all();
                return;
            }
        };
        {
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            mailbox.snapshot = Some(self.state.snapshot(identity));
            mailbox.reply = Some(Ok(true));
            self.shared.wake.notify_all();
        }
        loop {
            let command = {
                let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                while mailbox.command.is_none() {
                    mailbox = self.shared.wake.wait(mailbox).unwrap_or_else(|error| error.into_inner());
                }
                mailbox.command.take().expect("original administrative slot")
            };
            let reply = match command {
                NativeRecoveryCommand::Action { remove_destination, work } => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.state.finish_with_work(&self.shared.destination, remove_destination, &work)
                    }));
                    match result {
                        Ok(result) => result,
                        Err(panic) => {
                            self.state.panic = Some(panic);
                            self.state.measured_cleanup_failed = true;
                            Err(NativeMountError::Lifecycle("administrative action panicked"))
                        }
                    }
                }
                NativeRecoveryCommand::LastDrop => {
                    self.retire();
                    // Drop state data outside the mutex, before the worker's allocation guard.
                    drop(self.state);
                    let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
                    mailbox.state_retired = true;
                    self.shared.wake.notify_all();
                    return;
                }
            };
            let snapshot = self.state.snapshot(identity);
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            mailbox.snapshot = Some(snapshot);
            mailbox.reply = Some(reply);
            self.shared.wake.notify_all();
        }
    }
}

impl Drop for NativeMountRecoveryOwner {
    fn drop(&mut self) {
        {
            let mut mailbox = self.shared.mailbox.lock().unwrap_or_else(|error| error.into_inner());
            if !mailbox.state_retired {
                mailbox.command = Some(NativeRecoveryCommand::LastDrop);
                self.shared.wake.notify_one();
            }
        }
    }
}

pub(super) fn recover_driver_with_work(
    control: &mut NativeRecoveryControl, destination: &Path,
    disposable: bool, work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    { let _ = disposable; control.recover(destination, work) }
    #[cfg(target_os = "windows")]
    { control.recover_measured(destination, disposable, &mut &*work) }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    { let _ = (control, destination, disposable, work); Err(NativeMountError::UnsupportedTarget) }
}

#[cfg(not(windows))]
pub(super) fn remove_empty_recovery_destination(
    destination: &Path, work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let parent = destination.parent().ok_or(NativeMountError::InvalidDestination)?;
    let name = destination.file_name().ok_or(NativeMountError::InvalidDestination)?;
    let mut host = work;
    let result = crate::native_host::open_root_directory_with_work(parent, &mut host)
        .and_then(|directory| crate::native_host::remove_empty_directory_held_with_work(
            &directory, Path::new(name), &mut host,
        ));
    match result {
        Ok(()) => Ok(()),
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
