#[cfg(test)]
use super::mount_probe::same_mount;
use super::*;
use crate::native_mount::NativeMountRecoveryOwner;
use std::os::unix::fs::MetadataExt;

// Read the kernel's cached mount table, never stat or enumerate a potentially
// dead NFS root. getmntinfo's process-global buffer shares the probe's lock.
pub(super) fn mount_table() -> Result<Vec<libc::statfs>, NativeMountError> {
    let _table = MOUNT_TABLE.lock().unwrap_or_else(PoisonError::into_inner);
    let mut mounts = ptr::null_mut();
    // SAFETY: libc returns count initialized records, copied before releasing MOUNT_TABLE.
    let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
    if count <= 0 || mounts.is_null() {
        return Err(std::io::Error::last_os_error().into());
    }
    let count = usize::try_from(count).map_err(|_| driver_errno(libc::EOVERFLOW))?;
    // SAFETY: the table remains valid under MOUNT_TABLE until it has been copied.
    Ok(unsafe { std::slice::from_raw_parts(mounts, count) }.to_vec())
}

pub(super) fn field(name: &[c_char]) -> Result<&CStr, NativeMountError> {
    // SAFETY: c_char and u8 have identical layout; the fixed field is borrowed.
    let bytes = unsafe { std::slice::from_raw_parts(name.as_ptr().cast(), name.len()) };
    CStr::from_bytes_until_nul(bytes).map_err(|_| driver_errno(libc::EINVAL))
}

fn is_loopback_mount(mount: &libc::statfs, uid: u32) -> Result<bool, NativeMountError> {
    let source = field(&mount.f_mntfromname)?.to_bytes();
    let socket_id = source
        .strip_prefix(b"</tmp/acyclic-fs.")
        .and_then(|source| source.strip_suffix(b"/nfs>:/"));
    Ok(mount.f_owner == uid
        && field(&mount.f_fstypename)?.to_bytes() == b"nfs"
        && socket_id.is_some_and(|id| !id.is_empty() && !id.contains(&b'/')))
}

fn direct_unmount(destination: &CStr) -> std::io::Result<()> {
    // SAFETY: destination borrows the terminated kernel mount name.
    if unsafe { libc::unmount(destination.as_ptr(), libc::MNT_FORCE) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

unsafe extern "C" {
    fn acyclic_fs_darwin_retire_mount(destination: *const c_char, dissent: *mut c_int) -> c_int;
}

// Every Darwin owner uses this retirement mechanism and its birth-paid
// operation reservation. Test faults belong to this invocation, so they
// follow the original owner into the blocking worker without ambient state.
#[derive(Clone, Default)]
pub(super) struct RetirementOperations {
    #[cfg(test)]
    denied_direct: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl RetirementOperations {
    #[cfg(test)]
    pub(super) fn denying_direct(counter: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            denied_direct: Some(counter),
        }
    }

    pub(super) fn retire_mount(
        &self,
        destination: &CStr,
        mut attempt: impl FnMut(&dyn Fn() -> std::io::Result<()>) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let direct = || {
            #[cfg(test)]
            if let Some(counter) = &self.denied_direct {
                counter.fetch_add(1, Ordering::Relaxed);
                return Err(std::io::Error::from_raw_os_error(libc::EPERM));
            }
            direct_unmount(destination)
        };
        match attempt(&direct) {
            Err(error) if error.raw_os_error() == Some(libc::EPERM) => {
                attempt(&|| arbitration_unmount(destination))
            }
            result => result,
        }
    }
}

// Disk Arbitration is the sole fallback after a denied unmount.
// diskutil's StorageKit path discovery opens the dead NFS root and can block.
// The caller retains the authenticated destination fence throughout this call.
fn arbitration_unmount(destination: &CStr) -> std::io::Result<()> {
    let mut dissent = 0;
    // SAFETY: the cached mount record retains its terminated path. The bridge
    // awaits the terminal callback before its storage or this fence can die.
    let status = unsafe { acyclic_fs_darwin_retire_mount(destination.as_ptr(), &raw mut dissent) };
    if dissent != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("Darwin mount retirement denied: {dissent:#x}"),
        ));
    }
    if status != 0 {
        return Err(std::io::Error::from_raw_os_error(status));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn detach_stale_mount(mount: &libc::statfs) -> Result<(), NativeMountError> {
    // No operation may target a changed kernel identity. The destination
    // fence excludes a replacing SDK owner until terminal completion.
    if !mount_table()?
        .iter()
        .any(|current| same_mount(mount, current))
    {
        return Ok(());
    }
    RetirementOperations::default()
        .retire_mount(field(&mount.f_mntonname)?, |operation| operation())?;
    verify_detached(mount)
}

#[cfg(test)]
fn verify_detached(mount: &libc::statfs) -> Result<(), NativeMountError> {
    if mount_table()?
        .iter()
        .any(|current| same_mount(mount, current))
    {
        return Err(std::io::Error::other("stale mount remained after retirement").into());
    }
    Ok(())
}

// Bootstrap is deliberately outside a bounded callback invocation. The fence
// is the durable SDK ownership record and its kernel lock is the process
// liveness witness, including after SIGKILL and across PID reuse.
pub(in crate::native_mount) fn reclaim_stale_mounts() -> Result<(), NativeMountError> {
    reclaim_stale_mounts_with_retirement(RetirementOperations::default())
}

pub(super) fn reclaim_stale_mounts_with_retirement(
    retirement: RetirementOperations,
) -> Result<(), NativeMountError> {
    acyclic_native_runtime::initialize_native_io_services()?;
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let work = Work::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| NativeMountError::from(error))?;
    let mounts = mount_table()?;
    let observed = mounts
        .iter()
        .map(|mount| {
            let destination = field(&mount.f_mntonname)?;
            Ok(super::super::recovery_order::ObservedMount {
                destination: Path::new(std::ffi::OsStr::from_bytes(destination.to_bytes())),
                native: mount,
            })
        })
        .collect::<Result<Vec<_>, NativeMountError>>()?;
    super::super::recovery_order::recover_mounts(&observed, |mount| {
        reclaim_mount(mount, uid, &work, retirement.clone())
    })
    .map_err(NativeMountError::RecoveryBatchFailed)
}

fn reclaim_mount(
    mount: &libc::statfs,
    uid: u32,
    work: &Work,
    retirement: RetirementOperations,
) -> Result<bool, NativeMountError> {
    if !is_loopback_mount(mount, uid)? {
        return Ok(false);
    }
    let destination = field(&mount.f_mntonname)?;
    let path = Path::new(std::ffi::OsStr::from_bytes(destination.to_bytes()));
    let mut owner = match NativeMountRecoveryOwner::acquire_existing_canonical(path, work) {
        Ok(owner) => owner,
        Err(NativeMountError::DestinationBusy) => return Ok(false),
        Err(NativeMountError::Demand(crate::DemandError::Io(error)))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(false);
        }
        Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Io(
            error,
        ))) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let authentication = owner.configure_control(|control, fence| {
        work.charge(READ_WORK)?;
        let metadata = fence.metadata()?;
        if !metadata.is_file() || metadata.uid() != uid || metadata.nlink() != 1 {
            return Ok(false);
        }
        control.bind_mount(mount, work, retirement)?;
        Ok(true)
    });
    let authenticated = match authentication {
        Ok(authenticated) => authenticated,
        Err(error) => {
            owner.mark_failed();
            return Err(NativeMountError::RecoveryFailed {
                error: Box::new(error),
                owner,
            });
        }
    };
    if !authenticated {
        return Ok(false);
    }
    // The original fence and kernel identity now belong to the same core
    // owner used by explicit recovery. Its driver adapter performs every
    // direct attempt, Disk Arbitration fallback, and physical retirement.
    if let Err(error) = owner.detach(work) {
        owner.mark_failed();
        return Err(NativeMountError::RecoveryFailed {
            error: Box::new(error),
            owner,
        });
    }
    Ok(true)
}
