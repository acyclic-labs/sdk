use super::*;
use crate::native_host::NativeHostWork;

/// Exchanges entries under the caller's original work and cancellation.
pub fn exchange_native_entries(left: &Path, right: &Path, work: &crate::NativeMaterializationWork) -> Result<(), NativeExchangeError> {
    if left == right { return Err(NativeExchangeError::InvalidLayout); }
    let mut work = work;
    let left_parent = crate::native_host::open_root_directory(left.parent().ok_or(NativeExchangeError::InvalidLayout)?, &mut work).map(|(directory, _)| directory)?;
    let right_parent = crate::native_host::open_root_directory(right.parent().ok_or(NativeExchangeError::InvalidLayout)?, &mut work).map(|(directory, _)| directory)?;
    exchange_native_entries_held(&left_parent, Path::new(left.file_name().ok_or(NativeExchangeError::InvalidLayout)?), &right_parent, Path::new(right.file_name().ok_or(NativeExchangeError::InvalidLayout)?), &mut work).map_err(Into::into)
}

/// Recovers the original physical entries named by a durable witness.
pub fn recover_native_entry_exchange(live: &Path, work: &crate::NativeMaterializationWork) -> Result<(), NativeExchangeError> {
    let mut work = work;
    let parent = crate::native_host::open_root_directory(live.parent().ok_or(NativeExchangeError::InvalidLayout)?, &mut work).map(|(directory, _)| directory)?;
    recover_native_entry_exchange_held(&parent, Path::new(live.file_name().ok_or(NativeExchangeError::InvalidLayout)?), &mut work).map_err(Into::into)
}

pub(super) fn attempted<W: NativeHostWork, T>(
    work: &mut W,
    delta: crate::WorkCounters,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, W::Error> {
    work.admit(delta)?;
    // The noncancellable fixed charge immediately precedes the actual call;
    // cancellation arriving during it cannot erase the attempted operation.
    work.record(delta)?;
    operation().map_err(Into::into)
}
/// Exchanges entries through their original held parents and the existing scratch/witness protocol.
pub(crate) fn exchange_native_entries_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error>
where W::Error: From<NativeExchangeError> {
    validate_leaf(left_name).map_err(W::Error::from)?;
    validate_leaf(right_name).map_err(W::Error::from)?;
    #[cfg(unix)]
    {
        let roots = [
            crate::native_host::entry_identity(left, left_name, work)?.ok_or(NativeExchangeError::InvalidLayout)?,
            crate::native_host::entry_identity(right, right_name, work)?.ok_or(NativeExchangeError::InvalidLayout)?,
        ];
        exchange_kernel_held(left, left_name, right, right_name, work)?;
        if crate::native_host::entry_identity(left, left_name, work)? != Some(roots[1])
            || crate::native_host::entry_identity(right, right_name, work)? != Some(roots[0])
        {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        crate::native_host::sync_directory_held(left, work)?;
        crate::native_host::sync_directory_held(right, work)?;
        Ok(())
    }
    #[cfg(windows)]
    exchange_windows_held(left, left_name, right, right_name, work)
}
pub(super) fn validate_leaf(path: &Path) -> Result<(), NativeExchangeError> {
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(NativeExchangeError::InvalidLayout);
    }
    Ok(())
}
#[cfg(unix)]
#[allow(unsafe_code, reason = "atomic exchange uses live held parents and single native leaf names")]
pub(super) fn exchange_kernel_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir, left_name: &Path,
    right: &cap_std::fs::Dir, right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error> {
    use std::os::fd::AsRawFd as _;
    crate::native_host::unix_name(left_name.as_os_str(), work, |left_name, work| {
        crate::native_host::unix_name(right_name.as_os_str(), work, |right_name, work| {
            attempted(work, crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            }, || {
                // SAFETY: held descriptors and NUL-terminated leaf names remain
                // live for the whole operation; neither parent is reconstructed.
                #[cfg(target_os = "linux")]
                let result = unsafe { libc::syscall(libc::SYS_renameat2,
                    left.as_raw_fd(), left_name.as_ptr(), right.as_raw_fd(),
                    right_name.as_ptr(), libc::RENAME_EXCHANGE) };
                #[cfg(target_os = "macos")]
                let result = unsafe { libc::renameatx_np(left.as_raw_fd(), left_name.as_ptr(),
                    right.as_raw_fd(), right_name.as_ptr(), libc::RENAME_SWAP) } as libc::c_long;
                if result == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
            })
        })
    })
}
/// Recovers only the physical preimages named by the surviving entry witness.
pub(crate) fn recover_native_entry_exchange_held<W: NativeHostWork>(
    parent: &cap_std::fs::Dir, name: &Path, work: &mut W,
) -> Result<(), W::Error>
where W::Error: From<NativeExchangeError> {
    validate_leaf(name).map_err(W::Error::from)?;
    #[cfg(windows)]
    {
        with_service_name(name, ".acyclic-exchange", work, |scratch_name, work|
            with_service_name(name, ".acyclic-exchange-witness", work, |witness_name, work| {
                if crate::native_host::entry_identity(parent, witness_name, work)?.is_none() {
                    if crate::native_host::entry_identity(parent, scratch_name, work)?.is_some() {
                        return Err(NativeExchangeError::IncompatibleJournal.into());
                    }
                    return Ok(());
                }
                recover_entry_held(parent, name, scratch_name, witness_name, None, work).map(|_| ())
            }))
    }
    #[cfg(unix)]
    {
        let _ = (parent, work);
        Ok(())
    }
}
