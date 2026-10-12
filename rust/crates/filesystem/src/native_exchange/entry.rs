use super::*;
use crate::demand::{DemandError, SourceDemandReadWork};
use crate::native_host::{self as host};

/// Exchanges two entries through their original held parents.
pub(crate) fn exchange_native_entries<W: SourceDemandReadWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), DemandError> {
    validate_leaf(left_name)?;
    validate_leaf(right_name)?;
    #[cfg(windows)]
    {
        exchange_windows_held(left, left_name, right, right_name, work)
    }
    #[cfg(unix)]
    {
        let roots = [
            host::entry_identity(left, left_name, work)?
                .ok_or(NativeExchangeError::InvalidLayout)?,
            host::entry_identity(right, right_name, work)?
                .ok_or(NativeExchangeError::InvalidLayout)?,
        ];
        exchange_kernel(left, left_name, right, right_name, work)?;
        if host::entry_identity(left, left_name, work)? != Some(roots[1])
            || host::entry_identity(right, right_name, work)? != Some(roots[0])
        {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        host::sync_directory_held(left, work)?;
        host::sync_directory_held(right, work)
    }
}

pub(super) fn validate_leaf(name: &Path) -> Result<(), NativeExchangeError> {
    let mut parts = name.components();
    if matches!(parts.next(), Some(std::path::Component::Normal(_))) && parts.next().is_none() {
        Ok(())
    } else {
        Err(NativeExchangeError::InvalidLayout)
    }
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "held descriptors and checked leaf names stay live"
)]
fn exchange_kernel<W: SourceDemandReadWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::fd::AsRawFd;
    host::unix_name(left_name.as_os_str(), work, |left_name, work| {
        host::unix_name(right_name.as_os_str(), work, |right_name, work| {
            work.attempt_io(
                crate::WorkCounters {
                    backend_write_operations: 1,
                    ..crate::WorkCounters::UNCHARGED
                },
                || {
                    // SAFETY: held parent descriptors and NUL-terminated names outlive the call.
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

/// Recovers the exact entry request recorded under the held parent.
pub(crate) fn recover_native_entry_exchange<W: SourceDemandReadWork>(
    parent: &cap_std::fs::Dir,
    name: &Path,
    work: &mut W,
) -> Result<(), DemandError> {
    validate_leaf(name)?;
    #[cfg(windows)]
    {
        with_service_name(name, ".acyclic-exchange", work, |scratch, work| {
            with_service_name(name, ".acyclic-exchange-witness", work, |witness, work| {
                if host::entry_identity(parent, witness, work)?.is_none() {
                    return if host::entry_identity(parent, scratch, work)?.is_some() {
                        Err(NativeExchangeError::IncompatibleJournal.into())
                    } else {
                        Ok(())
                    };
                }
                recover_entry_held(parent, name, scratch, witness, None, work).map(|_| ())
            })
        })
    }
    #[cfg(unix)]
    {
        let _ = (parent, work);
        Ok(())
    }
}
