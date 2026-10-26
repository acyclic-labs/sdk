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

fn same_mount(left: &libc::statfs, right: &libc::statfs) -> bool {
    // SAFETY: Darwin's fsid_t consists of two initialized int32_t values.
    let identity = |mount: &libc::statfs| unsafe {
        std::mem::transmute::<libc::fsid_t, [i32; 2]>(mount.f_fsid)
    };
    identity(left) == identity(right)
        && left.f_owner == right.f_owner
        && left.f_mntonname == right.f_mntonname
        && left.f_mntfromname == right.f_mntfromname
}

#[cfg(test)]
std::thread_local! {
    static DENIED_DIRECT_UNMOUNTS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(super) fn with_direct_unmount_denied<T>(operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            DENIED_DIRECT_UNMOUNTS.set(self.0);
        }
    }
    let _restore = Restore(DENIED_DIRECT_UNMOUNTS.replace(Some(0)));
    let result = operation();
    assert!(DENIED_DIRECT_UNMOUNTS.get().is_some_and(|count| count > 0));
    result
}

fn direct_unmount(destination: &CStr) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(count) = DENIED_DIRECT_UNMOUNTS.get() {
        DENIED_DIRECT_UNMOUNTS.set(Some(count + 1));
        eprintln!("RECLAIM_DIRECT_UNMOUNT_DENIED errno=EPERM");
        return Err(std::io::Error::from_raw_os_error(libc::EPERM));
    }
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

// Every Darwin owner uses this same retirement mechanism. The lifecycle
// adapter supplies its birth-paid operation reservation; bootstrap is outside
// a callback invocation and calls the operations directly.
type RetirementOperation = fn(&CStr) -> std::io::Result<()>;

pub(super) fn retire_mount(
    destination: &CStr,
    mut attempt: impl FnMut(RetirementOperation, &CStr) -> std::io::Result<()>,
) -> std::io::Result<()> {
    match attempt(direct_unmount, destination) {
        Err(error) if error.raw_os_error() == Some(libc::EPERM) => {
            attempt(arbitration_unmount, destination)
        }
        result => result,
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

pub(super) fn detach_stale_mount(mount: &libc::statfs) -> Result<(), NativeMountError> {
    // No operation may target a changed kernel identity. The destination
    // fence excludes a replacing SDK owner until terminal completion.
    if !mount_table()?
        .iter()
        .any(|current| same_mount(mount, current))
    {
        return Ok(());
    }
    retire_mount(field(&mount.f_mntonname)?, |operation, path| {
        operation(path)
    })?;
    verify_detached(mount)
}

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
        reclaim_mount(mount, uid, &work)
    })
    .map_err(NativeMountError::RecoveryBatchFailed)
}

fn reclaim_mount(mount: &libc::statfs, uid: u32, work: &Work) -> Result<bool, NativeMountError> {
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
        control.bind_mount(mount, work)?;
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

#[cfg(test)]
use crate::native_mount::fence::{MountDestinationGuard, acquire_existing_mount_lock};

#[cfg(test)]
pub(super) fn reclaim_stale_mounts_direct_only() -> Result<(), NativeMountError> {
    reclaim_stale_mounts_with_retirement(|mount| {
        direct_unmount(field(&mount.f_mntonname)?).map_err(Into::into)
    })
}

#[cfg(test)]
fn reclaim_stale_mounts_with_retirement(
    retire: impl Fn(&libc::statfs) -> Result<(), NativeMountError>,
) -> Result<(), NativeMountError> {
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    for mount in mount_table()? {
        if !is_loopback_mount(&mount, uid)? {
            continue;
        }
        let destination = field(&mount.f_mntonname)?;
        let path = Path::new(std::ffi::OsStr::from_bytes(destination.to_bytes()));
        let (fence, _) = MountDestinationGuard::paths_for_canonical(path)?;
        let owner = match acquire_existing_mount_lock(&fence) {
            Ok(owner) => owner,
            Err(NativeMountError::DestinationBusy) => continue,
            Err(NativeMountError::Demand(crate::DemandError::Io(error)))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                continue;
            }
            Err(NativeMountError::Materialization(crate::NativeTreeMaterializationError::Io(
                error,
            ))) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(NativeMountError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(error) => return Err(error),
        };
        let metadata = owner.metadata()?;
        if !metadata.is_file() || metadata.uid() != uid || metadata.nlink() != 1 {
            continue;
        }
        // A live or replacing SDK owner cannot pass the still-held fence.
        // Refuse changed kernel identities even if their destination is equal.
        if !mount_table()?
            .iter()
            .any(|current| same_mount(&mount, current))
        {
            continue;
        }
        retire(&mount)?;
        // Keep the fence file: other processes may already hold its inode.
        drop(owner);
    }
    Ok(())
}
