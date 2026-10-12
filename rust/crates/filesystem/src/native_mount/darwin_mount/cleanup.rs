use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnmountEvidence {
    AlreadyUnmounted,
    Detached,
}

pub(super) fn is_mounted(destination: &Path, _parent: &Metadata) -> Result<bool, String> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt as _;

    let mut mounts = std::ptr::null_mut::<libc::statfs>();
    // SAFETY: `getmntinfo` initializes `mounts` to an OS-owned array retained
    // until the next call; this function consumes it before returning.
    let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
    if count <= 0 || mounts.is_null() {
        return Err(format!(
            "getmntinfo failed while querying {}: {}",
            destination.display(),
            std::io::Error::last_os_error()
        ));
    }
    let count = usize::try_from(count)
        .map_err(|_| "getmntinfo returned an invalid mount count".to_owned())?;
    // SAFETY: a positive return value is the exact initialized array length.
    let mounts = unsafe { std::slice::from_raw_parts(mounts, count) };
    let normalized = destination
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .and_then(|parent| destination.file_name().map(|name| parent.join(name)));
    Ok(mounts.iter().any(|mount| {
        // SAFETY: Darwin guarantees that `f_mntonname` is NUL terminated.
        let mounted_at = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) };
        mounted_at.to_bytes() == destination.as_os_str().as_bytes()
            || normalized
                .as_deref()
                .is_some_and(|path| mounted_at.to_bytes() == path.as_os_str().as_bytes())
    }))
}

/// Detaches `destination` directly, falling back to `diskutil unmount
/// force` only when the direct detach leaves it mounted.
pub(super) fn bounded_unmount(destination: &Path) -> Result<UnmountEvidence, String> {
    let parent = destination
        .parent()
        .and_then(|path| path.metadata().ok())
        .ok_or_else(|| "mount parent disappeared during teardown".to_owned())?;
    if !is_mounted(destination, &parent)? {
        return Ok(UnmountEvidence::AlreadyUnmounted);
    }
    if bounded_direct_unmount(destination, &parent)? {
        return Ok(UnmountEvidence::Detached);
    }
    let mut child = acyclic_native_runtime::spawn_process_tree(
        Command::new("/usr/sbin/diskutil")
            .arg("unmount")
            .arg("force")
            .arg(destination)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            child.terminate().map_err(|error| error.to_string())?;
            if !is_mounted(destination, &parent)? {
                return Ok(UnmountEvidence::Detached);
            }
            return Err(format!(
                "diskutil unmount exceeded its {}-second bound",
                DISKUTIL_UNMOUNT_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    child.terminate().map_err(|error| error.to_string())?;
    if !is_mounted(destination, &parent)? {
        return Ok(UnmountEvidence::Detached);
    }
    if !status.success() {
        return Err(format!("diskutil unmount failed with status {status}"));
    }
    let verify_deadline = Instant::now() + DISKUTIL_VISIBILITY_TIMEOUT;
    while is_mounted(destination, &parent)? {
        if Instant::now() >= verify_deadline {
            return Err("Darwin mount remained visible after diskutil unmount".to_owned());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(UnmountEvidence::Detached)
}

pub(in crate::native_mount) fn recover_destination(destination: &Path) -> Result<(), NativeMountError> {
    bounded_unmount(destination)
        .map(|_| ())
        .map_err(NativeMountError::Driver)
}

/// Force-detaches with `unmount(2)`, as `umount -f` would without its
/// process launch, and reports whether the destination is detached. The
/// kernel detaches within the call, so its return is the answer; a helper
/// thread bounds a call that a wedged transport blocks, after which the
/// caller's fallback takes over.
pub(super) fn bounded_direct_unmount(destination: &Path, parent: &Metadata) -> Result<bool, String> {
    let path = path_cstring(destination)
        .map_err(|error| std::io::Error::from_raw_os_error(error).to_string())?;
    let (detached, outcome) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("acyclic-fs-darwin-unmount".to_owned())
        .spawn(move || {
            // SAFETY: `path` is a NUL-terminated path this thread owns.
            let status = unsafe { libc::unmount(path.as_ptr(), libc::MNT_FORCE) };
            let _ = detached.send(status);
        })
        .map_err(|error| error.to_string())?;
    // The mount table decides either way: a failed or still-blocked call
    // leaves the destination mounted for the fallback.
    let _ = outcome.recv_timeout(DIRECT_UNMOUNT_TIMEOUT);
    is_mounted(destination, parent).map(|mounted| !mounted)
}

