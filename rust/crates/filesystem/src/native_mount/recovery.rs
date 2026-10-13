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
    pub fn cleanup_complete(&self) -> bool {
        !self.driver_retained && !self.destination_fence_retained && !self.source_retained
    }
}

/// Actual recovery resources and completed driver actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeMountRecoveryCleanupState {
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
    pub(super) fn controls_pending(&self) -> bool {
        false
    }
    pub(super) fn namespace_detach_confirmed(&self) -> bool {
        false
    }
    pub(super) fn retire_controls_with_work(
        &mut self,
        _original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        Ok(())
    }
}

pub(super) struct NativeRecoveryState {
    identity: Option<crate::NativeRootIdentity>,
    #[cfg(windows)]
    destination_identity: Option<crate::NativeRootIdentity>,
    guard: Option<MountDestinationGuard>,
    control: NativeRecoveryControl,
    driver_step_complete: bool,
    destination_removed: bool,
    measured_cleanup_failed: bool,
    final_close: crate::NativeWorkReservation,
}

pub(super) struct NativeRecoveryShared {
    destination: PathBuf,
    pub(super) state: std::sync::Mutex<Option<NativeRecoveryState>>,
    wake: std::sync::Condvar,
    // Path and state data retire before their allocation.
    _custody: crate::NativeAllocation,
}

/// Original recovery control and destination fence, acquired once before action.
pub struct NativeMountRecoveryOwner {
    pub(super) shared: Arc<NativeRecoveryShared>,
    // Closing the only sender asks the prestarted worker to retire the owner.
    retirement: std::sync::mpsc::SyncSender<()>,
}

impl std::fmt::Debug for NativeMountRecoveryOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeMountRecoveryOwner")
            .field("cleanup", &self.cleanup_state())
            .finish()
    }
}

impl NativeMountRecoveryOwner {
    /// Administratively acquires the existing destination fence once.
    pub fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| NativeMountError::from(error))?;
        Self::acquire_with_original(destination, &original)
    }

    pub(super) fn acquire_with_original(
        destination: &Path,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let mut destination = crate::native_host::clone_path(destination, &mut &*original)?;
        destination
            .1
            .grow(std::mem::size_of::<NativeRecoveryShared>() as u64)?;
        let closure = original.claim(
            std::mem::size_of::<(
                Arc<NativeRecoveryShared>,
                std::sync::mpsc::Receiver<()>,
                crate::NativeAllocation,
            )>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut control = NativeRecoveryControl::default();
        #[cfg(windows)]
        control.prepare_retirement(original)?;
        let final_close = original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1,
            ..Default::default()
        })?;
        let shared = Arc::new(NativeRecoveryShared {
            destination: destination.0,
            state: std::sync::Mutex::new(Some(NativeRecoveryState {
                identity: None,
                #[cfg(windows)]
                destination_identity: None,
                guard: None,
                control,
                driver_step_complete: false,
                destination_removed: false,
                measured_cleanup_failed: false,
                final_close,
            })),
            wake: std::sync::Condvar::new(),
            _custody: destination.1,
        });
        let (retirement, receiver) = std::sync::mpsc::sync_channel(1);
        let worker = (Arc::clone(&shared), receiver, closure);
        crate::native_host::NativeHostWork::started_io(
            &mut worker.2.work(),
            crate::WorkCounters {
                backend_write_operations: 1,
                ..Default::default()
            },
        )?;
        drop(std::thread::Builder::new().spawn(move || {
            let (shared, receiver, _closure) = worker;
            let _ = receiver.recv();
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .as_mut()
                .expect("original recovery state")
                .retire(shared._custody.work());
            drop(state.take());
            shared.wake.notify_all();
            drop(state);
            drop((receiver, shared));
            drop(_closure);
        })?);
        let owner = Self { shared, retirement };
        let result = {
            let mut state = owner
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                state
                    .as_mut()
                    .expect("original recovery state")
                    .acquire(&owner.shared.destination, owner.shared._custody.work())
            }))
            .unwrap_or_else(|panic| {
                Err(NativeMountError::DriverPanicked {
                    operation: "administrative acquisition",
                    panic: std::sync::Mutex::new(panic),
                })
            })
        };
        if let Err(error) = result {
            // Constructor rejection waits for the physical acquisition prefix to retire.
            let _ = owner.retirement.send(());
            let mut state = owner
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while state.is_some() {
                state = owner
                    .shared
                    .wake
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            return Err(error);
        }
        Ok(owner)
    }

    /// Returns the original identity and actual recovery state.
    #[must_use]
    pub fn cleanup_state(&self) -> NativeMountRecoveryCleanupState {
        self.shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("an active recovery owner")
            .snapshot()
    }

    /// Recovers through the same engine with an explicit unbounded allowance.
    pub fn recover(&mut self) -> Result<bool, NativeMountError> {
        self.ordinary_action(true)
    }

    /// Detaches while preserving the destination and authored residue.
    pub fn detach(&mut self) -> Result<bool, NativeMountError> {
        self.ordinary_action(false)
    }

    pub(super) fn ordinary_action(
        &mut self,
        remove_destination: bool,
    ) -> Result<bool, NativeMountError> {
        if !self.cleanup_state().destination_fence_retained {
            return Ok(false);
        }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.mark_failed();
            return Err(error.into());
        }
        self.measured_action(
            remove_destination,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map(|receipt| receipt.value)
        .map_err(|(error, _)| error)
    }

    /// Detaches on finite work without removing the destination.
    pub fn detach_measured(
        &mut self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, Box<crate::WorkCounters>)> {
        self.measured_action(false, budget, cancellation)
    }

    pub(super) fn measured_action(
        &mut self,
        remove_destination: bool,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, (NativeMountError, Box<crate::WorkCounters>)> {
        let work = match crate::NativeMaterializationWork::new(budget, cancellation) {
            Ok(work) => work,
            Err((error, work)) => {
                self.mark_failed();
                return Err((error.into(), work));
            }
        };
        let result = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = state.as_mut().expect("an active recovery owner");
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                state.finish_with_work(&self.shared.destination, remove_destination, &work)
            }))
            .unwrap_or_else(|panic| {
                state.measured_cleanup_failed = true;
                Err(NativeMountError::DriverPanicked {
                    operation: "administrative action",
                    panic: std::sync::Mutex::new(panic),
                })
            })
        };
        let spent = work.receipt();
        result
            .map(|value| crate::OperationReceipt { value, work: spent })
            .map_err(|error| (error, Box::new(spent)))
    }

    pub(super) fn mark_failed(&mut self) {
        if let Some(state) = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            state.measured_cleanup_failed = true;
        }
    }
}

impl NativeRecoveryState {
    fn acquire(
        &mut self,
        destination: &Path,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        MountDestinationGuard::acquire_with_work(destination, true, work, &mut self.guard, true)?;
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        return Err(NativeMountError::UnsupportedTarget);
        let (identity, _) = crate::native_host::file_identity_and_length(
            self.guard
                .as_ref()
                .and_then(|guard| guard.file.as_ref())
                .ok_or(NativeMountError::InvalidDestination)?,
            &mut &*work,
        )?;
        self.identity = Some(identity);
        #[cfg(windows)]
        {
            self.destination_identity =
                match crate::native_host::root_path_identity(destination, &mut &*work) {
                    Ok(identity) => Some(identity),
                    Err(crate::NativeTreeMaterializationError::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        None
                    }
                    Err(error) => return Err(error.into()),
                };
            if let Some(identity) = self.destination_identity {
                self.control.bind_root_identity(identity);
            }
        }
        Ok(())
    }

    fn retire(&mut self, _original: &crate::NativeMaterializationWork) {
        // Retirement keeps the actual fence through every pending native control.
        while self.control.controls_pending() {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                {
                    self.control.retire_controls(_original)
                }
                #[cfg(windows)]
                {
                    self.control.retire_controls()
                }
                #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
                {
                    self.control.retire_controls_with_work(_original)
                }
            })) {
                Ok(Ok(())) if !self.control.controls_pending() => break,
                _ => {}
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        if let Some(guard) = self.guard.as_mut() {
            guard.close_administrative_fence(&mut self.final_close);
        }
        self.guard.take();
    }

    pub(super) fn finish_with_work(
        &mut self,
        destination: &Path,
        remove_destination: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let result = (|| {
            if self.guard.is_none() {
                return Ok(false);
            }
            if !self.driver_step_complete {
                recover_driver_with_work(&mut self.control, destination, remove_destination, work)?;
                if self.control.controls_pending() {
                    return Err(NativeMountError::ControlPending);
                }
                self.driver_step_complete = true;
            }
            if remove_destination && !self.destination_removed {
                #[cfg(windows)]
                projfs::remove_empty_recovery_destination(
                    destination,
                    self.destination_identity,
                    &mut &*work,
                )?;
                #[cfg(not(windows))]
                remove_empty_recovery_destination(destination, work)?;
                self.destination_removed = true;
            }
            self.guard
                .as_mut()
                .expect("original retained fence")
                .release(&mut &*work)?;
            self.guard.take();
            Ok(true)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }

    fn snapshot(&self) -> NativeMountRecoveryCleanupState {
        NativeMountRecoveryCleanupState {
            fence_identity: self.identity.expect("an acquired fence identity"),
            destination_fence_retained: self.guard.is_some(),
            controls_pending: self.control.controls_pending(),
            namespace_detach_confirmed: self.control.namespace_detach_confirmed(),
            destination_removed: self.destination_removed,
            measured_cleanup_failed: self.measured_cleanup_failed,
        }
    }
}

pub(super) fn recover_driver_with_work(
    control: &mut NativeRecoveryControl,
    destination: &Path,
    disposable: bool,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let _ = disposable;
        control.recover(destination, work)
    }
    #[cfg(target_os = "windows")]
    {
        control.recover(destination, disposable, &mut &*work)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = (control, destination, disposable, work);
        Err(NativeMountError::UnsupportedTarget)
    }
}

#[cfg(not(windows))]
pub(super) fn remove_empty_recovery_destination(
    destination: &Path,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let parent = destination
        .parent()
        .ok_or(NativeMountError::InvalidDestination)?;
    let name = destination
        .file_name()
        .ok_or(NativeMountError::InvalidDestination)?;
    let mut host = work;
    let result = crate::native_host::open_root_directory(parent, &mut host)
        .map(|(directory, _)| directory)
        .and_then(|directory| {
            crate::native_host::remove_leaf(&directory, Path::new(name), true, &mut host)
        });
    match result {
        Ok(()) => Ok(()),
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
