use crate::*;

/// Destination behavior for one durable rename.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenameMode {
    /// Fail atomically when the destination already exists.
    NoReplace,
    /// Atomically replace a compatible destination entry.
    Replace,
}

#[cfg(unix)]
pub(crate) type NamespacePathUnit = u8;

#[cfg(windows)]
pub(crate) type NamespacePathUnit = u16;

#[cfg(any(unix, windows))]
pub(crate) fn namespace_path(
    path: &Path,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<(Vec<NamespacePathUnit>, Option<ReadAllocation>)> {
    if let Some(meter) = meter {
        meter.check()?;
        meter.admit_examination(1, 0)?;
    }
    #[cfg(unix)]
    let length = {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = path.as_os_str().as_bytes();
        if bytes.contains(&0) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        bytes.len()
    };
    #[cfg(windows)]
    let length = {
        use std::os::windows::ffi::OsStrExt as _;
        let mut length = 0_usize;
        for unit in path.as_os_str().encode_wide() {
            if unit == 0 {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            length = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        }
        length
    };
    if let Some(meter) = meter {
        meter.record_examination(1, 0)?;
    }
    let capacity = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
    let bytes = capacity
        .checked_mul(std::mem::size_of::<NamespacePathUnit>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    let mut allocation = meter
        .map(|meter| ReadAllocation::new(meter, bytes))
        .transpose()?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(capacity) {
        return Err(match meter {
            Some(meter) => meter.allocation_failed(error),
            None => io::ErrorKind::OutOfMemory.into(),
        });
    }
    if let Some(allocation) = &mut allocation {
        allocation
            .reconcile_capacity(buffer.capacity() * std::mem::size_of::<NamespacePathUnit>())?;
    }
    #[cfg(unix)]
    let (encoded, copied) = (0, length);
    #[cfg(windows)]
    let (encoded, copied) = (length * 2, 0);
    if let Some(meter) = meter {
        meter.admit_codec(encoded as u64, copied as u64, 0)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        buffer.extend_from_slice(path.as_os_str().as_bytes());
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        buffer.extend(path.as_os_str().encode_wide());
    }
    buffer.push(0);
    if let Some(meter) = meter {
        meter.record_codec(encoded as u64, copied as u64, 0)?;
    }
    Ok((buffer, allocation))
}

/// Publishes a new entry without replacing an existing one.
#[allow(unsafe_code)]
pub fn publish_new(
    from: &Path,
    to: &Path,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    #[cfg(any(unix, windows))]
    {
        let from_path = namespace_path(from, meter)?;
        let to_path = namespace_path(to, meter)?;
        loop {
            if let Some(meter) = meter {
                meter.check()?;
                meter.admit_write(0)?;
            }
            #[cfg(unix)]
            let failed =
                unsafe { libc::link(from_path.0.as_ptr().cast(), to_path.0.as_ptr().cast()) } != 0;
            #[cfg(windows)]
            let failed = unsafe {
                windows_sys::Win32::Storage::FileSystem::CreateHardLinkW(
                    to_path.0.as_ptr(),
                    from_path.0.as_ptr(),
                    std::ptr::null(),
                )
            } == 0;
            if failed {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Ok(());
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        if let Some(meter) = meter {
            meter.check()?;
            meter.admit_write(0)?;
        }
        std::fs::hard_link(from, to)
    }
}

/// Renames an entry and durably publishes the affected namespace.
#[allow(unsafe_code)]
pub fn durable_rename(
    from: &Path,
    to: &Path,
    mode: RenameMode,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        let from_path = namespace_path(from, meter)?;
        let to_path = namespace_path(to, meter)?;
        loop {
            if let Some(meter) = meter {
                meter.check()?;
                meter.admit_write(0)?;
                #[cfg(windows)]
                meter.admit_durability()?;
            }
            #[cfg(target_os = "linux")]
            let failed = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    libc::AT_FDCWD,
                    from_path.0.as_ptr(),
                    libc::AT_FDCWD,
                    to_path.0.as_ptr(),
                    if mode == RenameMode::NoReplace {
                        libc::RENAME_NOREPLACE
                    } else {
                        0
                    },
                )
            } != 0;
            #[cfg(target_os = "macos")]
            let failed = unsafe {
                libc::renamex_np(
                    from_path.0.as_ptr().cast(),
                    to_path.0.as_ptr().cast(),
                    if mode == RenameMode::NoReplace {
                        libc::RENAME_EXCL
                    } else {
                        0
                    },
                )
            } != 0;
            #[cfg(windows)]
            let failed = unsafe {
                use windows_sys::Win32::Storage::FileSystem::{
                    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
                };
                MoveFileExW(
                    from_path.0.as_ptr(),
                    to_path.0.as_ptr(),
                    MOVEFILE_WRITE_THROUGH
                        | if mode == RenameMode::Replace {
                            MOVEFILE_REPLACE_EXISTING
                        } else {
                            0
                        },
                )
            } == 0;
            if !failed {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
        #[cfg(unix)]
        sync_rename_parents(from, to, meter)?;
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (from, to, mode, meter);
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(unix)]
pub(crate) fn sync_rename_parents(
    from: &Path,
    to: &Path,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    fn namespace_parent(path: &Path) -> &Path {
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    }
    let synchronize = |parent: &Path| -> io::Result<()> {
        if let Some(meter) = meter {
            meter.check()?;
            meter.admit_read(0)?;
            let directory = File::open(parent)?;
            sync_file(&directory, Durability::Full, Some(meter.as_ref()))
        } else {
            sync_parent(parent, Durability::Full)
        }
    };
    let to_parent = namespace_parent(to);
    synchronize(to_parent)?;
    let from_parent = namespace_parent(from);
    if from_parent != to_parent {
        synchronize(from_parent)?;
    }
    Ok(())
}
