use super::*;
use crate::sync::native::{Arc as RecoveryArc, Condvar, Mutex, RetirementSender};

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
    // Lock order: recovery state -> work ledger -> native syscall boundary.
    // The retirement worker and helper never acquire this lock in reverse.
    pub(super) state: Mutex<Option<NativeRecoveryState>>,
    wake: Condvar,
    // Path and state data retire before their allocation.
    _custody: crate::NativeAllocation,
}

/// Original recovery control and destination fence, acquired once before action.
pub struct NativeMountRecoveryOwner {
    pub(super) shared: RecoveryArc<NativeRecoveryShared>,
    // Closing the only sender asks the prestarted worker to retire the owner.
    retirement: RetirementSender,
}

impl std::fmt::Debug for NativeMountRecoveryOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeMountRecoveryOwner")
            .field("cleanup", &self.cleanup_state())
            .finish()
    }
}

impl Drop for NativeMountRecoveryOwner {
    fn drop(&mut self) {
        let _ = self.retirement.send(());
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
        Self::acquire_bound(destination, original, None)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn acquire_in_parent(
        destination: &Path,
        parent: &std::os::fd::OwnedFd,
    ) -> Result<Self, NativeMountError> {
        let original = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| NativeMountError::from(error))?;
        original.charge(crate::materializer::NativeResourceCredit::ReadOperations.delta(1))?;
        let directory = cap_std::fs::Dir::from_std_file(parent.try_clone()?.into());
        Self::acquire_bound(destination, &original, Some(directory))
    }

    fn acquire_bound(
        destination: &Path,
        original: &crate::NativeMaterializationWork,
        parent: Option<cap_std::fs::Dir>,
    ) -> Result<Self, NativeMountError> {
        let mut destination = crate::native_host::clone_path(destination, &mut &*original)?;
        destination
            .1
            .grow(std::mem::size_of::<NativeRecoveryShared>() as u64)?;
        let closure = original.claim(
            std::mem::size_of::<(
                RecoveryArc<NativeRecoveryShared>,
                crate::sync::native::mpsc::Receiver<()>,
                crate::NativeAllocation,
            )>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        #[cfg(windows)]
        let mut control = NativeRecoveryControl::default();
        #[cfg(not(windows))]
        let control = NativeRecoveryControl::default();
        #[cfg(windows)]
        control.prepare_retirement(original)?;
        let final_close = original.reserve_work(crate::WorkCounters {
            backend_write_operations: 1,
            ..Default::default()
        })?;
        let shared = RecoveryArc::new(NativeRecoveryShared {
            destination: destination.0,
            state: Mutex::new(Some(NativeRecoveryState {
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
            wake: Condvar::new(),
            _custody: destination.1,
        });
        let (retirement, receiver) = crate::sync::native::retirement_channel();
        let worker = (RecoveryArc::clone(&shared), receiver, closure);
        crate::native_host::NativeHostWork::started_io(
            &mut worker.2.work(),
            crate::WorkCounters {
                backend_write_operations: 1,
                ..Default::default()
            },
        )?;
        drop(crate::sync::native::thread::Builder::new().spawn(move || {
            retire_recovery(worker);
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
                    .ok_or(NativeMountError::ControlPending)?
                    .acquire(
                        &owner.shared.destination,
                        parent,
                        owner.shared._custody.work(),
                    )
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
    pub fn cleanup_state(&self) -> Result<NativeMountRecoveryCleanupState, NativeMountError> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?
            .snapshot()
    }

    /// Recovers through the same engine with an explicit unbounded allowance.
    pub fn recover(&mut self) -> Result<bool, NativeMountError> {
        if !self.cleanup_state()?.destination_fence_retained {
            return Ok(false);
        }
        #[cfg(target_os = "macos")]
        if let Err(error) = acyclic_native_runtime::initialize_native_io_services() {
            self.mark_failed();
            return Err(error.into());
        }
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| {
            self.mark_failed();
            NativeMountError::from(error)
        })?;
        self.finish(true, &work)
    }

    /// Detaches on the caller's original work while preserving authored residue.
    pub fn detach(
        &mut self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        self.finish(false, work)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn detach_pinned(
        &mut self,
        file: File,
        parent: File,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let control = &mut state
                .as_mut()
                .ok_or(NativeMountError::ControlPending)?
                .control;
            control.pinned = Some(file);
            control.pinned_parent = Some(parent);
        }
        self.finish(false, work)
    }

    fn finish(
        &mut self,
        remove_destination: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = state.as_mut().ok_or(NativeMountError::ControlPending)?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.finish_with_work(&self.shared.destination, remove_destination, work)
        }))
        .unwrap_or_else(|panic| {
            state.measured_cleanup_failed = true;
            Err(NativeMountError::DriverPanicked {
                operation: "administrative action",
                panic: std::sync::Mutex::new(panic),
            })
        })
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
        parent: Option<cap_std::fs::Dir>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        if let Some(directory) = parent {
            MountDestinationGuard::acquire_in_directory(
                destination,
                directory,
                work,
                &mut self.guard,
                true,
            )?;
        } else {
            MountDestinationGuard::acquire_with_work(
                destination,
                true,
                work,
                &mut self.guard,
                true,
            )?;
        }
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
            #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
            if super::disconnected::kernel::current().is_some_and(|kernel| kernel.early_release()) {
                self.guard
                    .as_mut()
                    .ok_or(NativeMountError::ControlPending)?
                    .release(&mut &*work)?;
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
                remove_empty_recovery_destination(
                    destination,
                    self.guard
                        .as_ref()
                        .and_then(|guard| guard.parent_directory.as_ref())
                        .ok_or(NativeMountError::InvalidDestination)?,
                    work,
                )?;
                self.destination_removed = true;
            }
            self.guard
                .as_mut()
                .ok_or(NativeMountError::ControlPending)?
                .release(&mut &*work)?;
            self.guard.take();
            Ok(true)
        })();
        self.measured_cleanup_failed = result.is_err();
        result
    }

    fn snapshot(&self) -> Result<NativeMountRecoveryCleanupState, NativeMountError> {
        Ok(NativeMountRecoveryCleanupState {
            fence_identity: self.identity.ok_or(NativeMountError::ControlPending)?,
            destination_fence_retained: self.guard.is_some(),
            controls_pending: self.control.controls_pending(),
            namespace_detach_confirmed: self.control.namespace_detach_confirmed(),
            destination_removed: self.destination_removed,
            measured_cleanup_failed: self.measured_cleanup_failed,
        })
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
    parent: &cap_std::fs::Dir,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let name = destination
        .file_name()
        .ok_or(NativeMountError::InvalidDestination)?;
    let mut host = work;
    let result = crate::native_host::remove_leaf(parent, Path::new(name), true, &mut host);
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

fn retire_recovery(
    (shared, receiver, _closure): (
        RecoveryArc<NativeRecoveryShared>,
        crate::sync::native::mpsc::Receiver<()>,
        crate::NativeAllocation,
    ),
) {
    let _ = receiver.recv();
    let mut state = shared
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(state) = state.as_mut() {
        state.retire(shared._custody.work());
    }
    drop(state.take());
    shared.wake.notify_all();
    drop(state);
    drop((receiver, shared));
    drop(_closure);
}
