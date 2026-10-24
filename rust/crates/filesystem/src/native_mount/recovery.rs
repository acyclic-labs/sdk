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
    #[cfg(windows)]
    preservation: Option<PendingPreservation>,
    guard: Option<MountDestinationGuard>,
    control: NativeRecoveryControl,
    driver_step_complete: bool,
    destination_removed: bool,
    measured_cleanup_failed: bool,
    final_close: crate::NativeWorkReservation,
}

#[cfg(windows)]
struct PendingPreservation {
    parent: cap_std::fs::Dir,
    name: PathBuf,
    _custody: crate::NativeAllocation,
    close: crate::NativeWorkReservation,
    flush: crate::NativeWorkReservation,
}

#[cfg(windows)]
impl PendingPreservation {
    fn flush_work() -> crate::WorkCounters {
        crate::WorkCounters {
            backend_read_operations: 2,
            durability_operations: 2,
            items_examined: 2,
            ..crate::WorkCounters::UNCHARGED
        }
    }

    fn settle(&mut self, origin: &cap_std::fs::Dir) -> Result<(), NativeMountError> {
        self.flush.refill_cleanup(Self::flush_work())?;
        let mut work = super::fence::NativeFenceReleaseWork {
            credit: &self.flush,
            wait: None,
            pending: None,
        };
        crate::native_host::sync_directory_held(&self.parent, &mut work)?;
        crate::native_host::sync_directory_held(origin, &mut work)?;
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for PendingPreservation {
    fn drop(&mut self) {
        self.close
            .started_drop_io(crate::materializer::NativeResourceCredit::WriteOperations.delta(1));
    }
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
    // Last drop notifies the prestarted worker to retire the owner.
    retirement: Option<RetirementSender>,
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
        if let Some(retirement) = self.retirement.take() {
            let _ = retirement.send(());
        }
    }
}

impl NativeMountRecoveryOwner {
    /// Acquires the destination fence relative to an already held parent.
    /// The displayed destination supplies only its leaf name; it is never used
    /// to reopen the parent. All resources belong to the caller's original work.
    #[cfg(windows)]
    pub fn acquire_in_directory(
        destination: &Path,
        parent: &cap_std::fs::Dir,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        super::registry::initialize()?;
        crate::native_host::require_leaf(Path::new(
            destination
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?,
        ))?;
        use crate::native_host::NativeHostWork as _;
        let parent = (&mut &*original).attempt_io(
            crate::WorkCounters {
                backend_read_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            },
            || parent.try_clone(),
        )?;
        Self::acquire_bound(destination, original, Some(parent))
    }

    /// Preserves a crashed ProjFS root by an identity-checked, no-replace rename
    /// between held directories. A live provider's fence prevents acquisition.
    /// Only physically recorded roots of the expected volume with a proven
    /// dead process incarnation can move. Foreign reparse points are refused.
    #[cfg(windows)]
    pub fn preserve_crashed_in_directory(
        &mut self,
        residue_parent: &cap_std::fs::Dir,
        residue_name: &Path,
        volume: crate::VolumeId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        crate::native_host::require_leaf(residue_name)?;
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = state.as_mut().ok_or(NativeMountError::ControlPending)?;
        let result = state.preserve_crashed_in_directory(
            &self.shared.destination,
            residue_parent,
            residue_name,
            volume,
            work,
        );
        state.measured_cleanup_failed = result.is_err();
        result
    }

    /// Administratively acquires the existing destination fence once.
    pub fn acquire(destination: &Path) -> Result<Self, NativeMountError> {
        super::registry::initialize()?;
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
        super::registry::initialize()?;
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
                #[cfg(windows)]
                preservation: None,
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
        let mut owner = Self {
            shared,
            retirement: Some(retirement),
        };
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
            if let Some(retirement) = owner.retirement.take() {
                let _ = retirement.send(());
            }
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
        file: std::fs::File,
        parent: std::fs::File,
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
            self.destination_identity = if let Some(parent) = self
                .guard
                .as_ref()
                .and_then(|guard| guard.parent_directory.as_ref())
            {
                projfs::open_recovery_leaf(
                    parent,
                    Path::new(
                        destination
                            .file_name()
                            .ok_or(NativeMountError::InvalidDestination)?,
                    ),
                    work,
                )?
                .map(|(_, identity)| identity)
            } else {
                match crate::native_host::root_path_identity(destination, &mut &*work) {
                    Ok(identity) => Some(identity),
                    Err(crate::NativeTreeMaterializationError::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        None
                    }
                    Err(error) => return Err(error.into()),
                }
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
        #[cfg(windows)]
        while self.destination_removed && self.preservation.is_some() {
            let result = self
                .preservation
                .as_mut()
                .zip(self.guard.as_ref())
                .and_then(|(pending, guard)| {
                    guard
                        .parent_directory
                        .as_ref()
                        .map(|parent| (pending, parent))
                })
                .ok_or(NativeMountError::ControlPending)
                .and_then(|(pending, origin)| pending.settle(origin));
            match result {
                Ok(()) => {
                    self.preservation.take();
                }
                Err(_) => {
                    self.measured_cleanup_failed = true;
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
        if let Some(guard) = self.guard.as_mut() {
            guard.close_administrative_fence(&mut self.final_close);
        }
        self.guard.take();
    }

    #[cfg(windows)]
    fn preserve_crashed_in_directory(
        &mut self,
        destination: &Path,
        residue_parent: &cap_std::fs::Dir,
        residue_name: &Path,
        volume: crate::VolumeId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let guard = self
            .guard
            .as_mut()
            .ok_or(NativeMountError::ControlPending)?;
        // A preceding refused disposable recovery may retain its probe handle.
        // Retire that control while this same destination fence is still held.
        self.control.retire_controls()?;
        let parent = guard
            .parent_directory
            .as_ref()
            .ok_or(NativeMountError::InvalidDestination)?;
        let leaf = Path::new(
            destination
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?,
        );
        let receipt = guard.root_ownership(work)?;
        let opened = if self.destination_removed {
            let pending = self
                .preservation
                .as_ref()
                .ok_or(NativeMountError::ControlPending)?;
            projfs::open_recovery_leaf(&pending.parent, &pending.name, work)?
        } else {
            projfs::open_recovery_leaf(parent, leaf, work)?
        };
        let Some((file, identity)) = opened else {
            if self.destination_identity.is_some() {
                return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
            }
            guard.release(&mut &*work)?;
            self.guard.take();
            return Ok(false);
        };
        if Some(identity) != self.destination_identity {
            return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
        }
        let receipt = receipt.ok_or(NativeMountError::RecoveryOwnershipUnknown)?;
        if receipt.root != identity {
            return Err(NativeMountError::RecoveryOwnershipUnknown);
        }
        if receipt.volume != volume {
            return Err(NativeMountError::RecoveryVolumeMismatch);
        }
        projfs::require_dead_process_owner(receipt.pid, receipt.started, work)?;
        match projfs::reparse_tag_held(&file, &mut &*work)? {
            Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS) | None => {}
            Some(tag) => {
                return Err(NativeMountError::UnsafeRecoveryDestination {
                    reparse_tag: Some(tag),
                });
            }
        }
        if self.preservation.is_none() {
            use crate::native_host::NativeHostWork as _;
            let (name, custody) = work.path(residue_name, None)?;
            let close = work.reserve_work(
                crate::materializer::NativeResourceCredit::WriteOperations.delta(1),
            )?;
            // Pre-fund physical completion on the acquisition's original ledger.
            // Losing this action's caller or budget cannot revoke the flushes.
            let flush = self
                .final_close
                .work()
                .reserve_work(PendingPreservation::flush_work())?;
            let parent = (&mut &*work).attempt_io(
                crate::materializer::NativeResourceCredit::ReadOperations.delta(1),
                || residue_parent.try_clone(),
            )?;
            self.preservation = Some(PendingPreservation {
                parent,
                name,
                _custody: custody,
                close,
                flush,
            });
        }
        let pending = self
            .preservation
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?;
        if !self.destination_removed {
            // Rename rechecks identity on its own no-follow source handle.
            crate::native_host::with_wide_path(leaf, &mut &*work, |leaf, work| {
                crate::native_host::with_wide_path(&pending.name, work, |name, work| {
                    crate::native_host::rename_held(
                        parent,
                        leaf,
                        &pending.parent,
                        name,
                        acyclic_native_runtime::RenameMode::NoReplace,
                        Some(identity),
                        work,
                    )
                })
            })?;
            // A failed flush can retry the moved root with this same custody.
            self.destination_removed = true;
        }
        // Authored residue remains owned by the caller's held recovery directory.
        // Never release the destination fence before both namespace writes settle.
        crate::native_host::sync_directory_held(&pending.parent, &mut &*work)?;
        crate::native_host::sync_directory_held(parent, &mut &*work)?;
        self.driver_step_complete = true;
        self.destination_removed = true;
        guard.release(&mut &*work)?;
        self.guard.take();
        self.preservation.take();
        Ok(true)
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
            #[cfg(windows)]
            if self.preservation.is_some() {
                // Only preservation may settle the retained target's flushes.
                return Err(NativeMountError::ControlPending);
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
        let controls_pending = self.control.controls_pending();
        #[cfg(windows)]
        let controls_pending = controls_pending || self.preservation.is_some();
        Ok(NativeMountRecoveryCleanupState {
            fence_identity: self.identity.ok_or(NativeMountError::ControlPending)?,
            destination_fence_retained: self.guard.is_some(),
            controls_pending,
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

#[cfg(all(test, windows))]
pub(super) fn wait_for_retirement(shared: &NativeRecoveryShared) {
    let mut state = shared
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while state.is_some() {
        state = shared
            .wake
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
}
