use super::*;
use crate::native_mount::fence::{MountDestinationGuard, acquire_existing_mount_lock};
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

// Bootstrap is deliberately outside a bounded callback invocation. The fence
// is the durable SDK ownership record and its kernel lock is the process
// liveness witness, including after SIGKILL and across PID reuse.
pub(in crate::native_mount) fn reclaim_stale_mounts() -> Result<(), NativeMountError> {
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
        // Adopt the locked inode into the normal recovery owner. This is the
        // same permission-aware detach and control retirement used by explicit
        // recovery; direct unmount alone requires privileges on macOS.
        acyclic_native_runtime::initialize_native_io_services()?;
        let work = Work::new(
            crate::performance::BOUNDARY_BUDGET,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| NativeMountError::from(error))?;
        let guard = MountDestinationGuard::from_locked_existing(owner, fence);
        let mut recovery = crate::native_mount::NativeMountRecoveryOwner::from_locked_existing(
            path, guard, &work,
        )?;
        // Recheck after creating the retirement owner, still under the same
        // fence. A different kernel mount must never inherit this authority.
        if mount_table()?
            .iter()
            .any(|current| same_mount(&mount, current))
        {
            recovery.detach(&work)?;
            if mount_table()?
                .iter()
                .any(|current| same_mount(&mount, current))
            {
                return Err(std::io::Error::other("stale mount remained after retirement").into());
            }
        }
        // Retirement keeps the original fence until all native controls drain;
        // it never unlinks the shared fence inode or the destination directory.
    }
    Ok(())
}
