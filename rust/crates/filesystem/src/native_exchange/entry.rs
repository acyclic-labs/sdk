use super::*;
use crate::native_host::NativeHostWork;

pub(super) fn attempted<W: NativeHostWork, T>(
    work: &mut W,
    delta: crate::WorkCounters,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, W::Error> {
    work.admit(delta)?;
    // Charge the attempt immediately before the syscall.
    work.record(delta)?;
    operation().map_err(Into::into)
}
/// Exchanges entries through their original held parents and the existing scratch/witness protocol.
pub(crate) fn exchange_native_entries<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error>
where
    W::Error: From<NativeExchangeError>,
{
    validate_leaf(left_name).map_err(W::Error::from)?;
    validate_leaf(right_name).map_err(W::Error::from)?;
    #[cfg(unix)]
    {
        let roots = [
            crate::native_host::entry_identity(left, left_name, work)?
                .ok_or(NativeExchangeError::InvalidLayout)?,
            crate::native_host::entry_identity(right, right_name, work)?
                .ok_or(NativeExchangeError::InvalidLayout)?,
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
#[allow(
    unsafe_code,
    reason = "atomic exchange uses live held parents and single native leaf names"
)]
pub(super) fn exchange_kernel_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error> {
    use std::os::fd::AsRawFd as _;
    crate::native_host::unix_name(left_name.as_os_str(), work, |left_name, work| {
        crate::native_host::unix_name(right_name.as_os_str(), work, |right_name, work| {
            attempted(
                work,
                crate::WorkCounters {
                    backend_write_operations: 1,
                    ..crate::WorkCounters::UNCHARGED
                },
                || {
                    // SAFETY: held descriptors and NUL-terminated leaf names remain
                    // live for the whole operation; neither parent is reconstructed.
                    #[cfg(target_os = "linux")]
                    let result = unsafe {
                        libc::syscall(
                            libc::SYS_renameat2,
                            left.as_raw_fd(),
                            left_name.as_ptr(),
                            right.as_raw_fd(),
                            right_name.as_ptr(),
                            libc::RENAME_EXCHANGE,
                        )
                    };
                    #[cfg(target_os = "macos")]
                    let result = unsafe {
                        libc::renameatx_np(
                            left.as_raw_fd(),
                            left_name.as_ptr(),
                            right.as_raw_fd(),
                            right_name.as_ptr(),
                            libc::RENAME_SWAP,
                        )
                    } as libc::c_long;
                    if result == 0 {
                        Ok(())
                    } else {
                        Err(std::io::Error::last_os_error())
                    }
                },
            )
        })
    })
}
/// Recovers only the physical preimages named by the surviving entry witness.
pub(crate) fn recover_native_entry_exchange<W: NativeHostWork>(
    parent: &cap_std::fs::Dir,
    name: &Path,
    work: &mut W,
) -> Result<(), W::Error>
where
    W::Error: From<NativeExchangeError>,
{
    validate_leaf(name).map_err(W::Error::from)?;
    #[cfg(windows)]
    {
        let scratch = service_name(name, ".acyclic-exchange", work)?;
        let scratch_name = scratch.0.as_path();
        let result = (|| {
            let witness = service_name(name, ".acyclic-exchange-witness", work)?;
            let witness_name = witness.0.as_path();
            let result = (|| {
                if crate::native_host::entry_identity(parent, witness_name, work)?.is_none() {
                    if crate::native_host::entry_identity(parent, scratch_name, work)?.is_some() {
                        return Err(NativeExchangeError::IncompatibleJournal.into());
                    }
                    return Ok(());
                }
                recover_entry_held(parent, name, scratch_name, witness_name, None, work).map(|_| ())
            })();
            drop(witness.0);
            work.release(witness.1)?;
            result
        })();
        drop(scratch.0);
        work.release(scratch.1)?;
        result
    }
    #[cfg(unix)]
    {
        let _ = (parent, work);
        Ok(())
    }
}
