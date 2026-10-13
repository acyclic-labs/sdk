use super::*;

pub(in crate::native_mount::materialize) fn restore_artifact_name(
    prefix: &str,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<PathBuf, MaterializeError> {
    use std::fmt::Write as _;
    let mut hasher = blake3::Hasher::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = relative.as_os_str().as_bytes();
        work.started_io(WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        hasher.update(bytes);
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        for unit in relative.as_os_str().encode_wide() {
            work.started_io(WorkCounters {
                bytes_hashed: 2,
                bytes_encoded: 2,
                ..WorkCounters::UNCHARGED
            })
            .map_err(restore_demand_error)?;
            hasher.update(&unit.to_le_bytes());
        }
    }
    let bytes = prefix.len().checked_add(64).ok_or(WorkError::Overflow)?;
    let delta = WorkCounters {
        bytes_encoded: bytes as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(delta).map_err(restore_demand_error)?;
    work.claim_allocation(bytes as u64, u64::from(bytes != 0), WorkCounters::UNCHARGED)
        .map_err(restore_demand_error)?;
    let mut name = String::with_capacity(bytes);
    write!(name, "{prefix}{}", hasher.finalize().to_hex()).expect("String formatting cannot fail");
    if let Err(error) = work.record(delta) {
        drop(name);
        work.release(bytes as u64).map_err(restore_demand_error)?;
        return Err(restore_demand_error(error));
    }
    Ok(PathBuf::from(name))
}

pub(in crate::native_mount::materialize) fn with_restore_artifact<
    T,
    W: crate::demand::SourceDemandReadWork,
>(
    prefix: &str,
    relative: &Path,
    work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, MaterializeError>,
) -> Result<T, MaterializeError> {
    let name = restore_artifact_name(prefix, relative, work)?;
    let bytes = name.as_os_str().as_encoded_bytes().len() as u64;
    let result = operation(&name, work);
    drop(name);
    work.release(bytes).map_err(restore_demand_error)?;
    result
}

pub(in crate::native_mount::materialize) fn acquire_restore_lock(
    parent: &HostRoot,
    relative: &Path,
    blocking: bool,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<File, MaterializeError> {
    with_restore_artifact(".acyclic-restore-lock-", relative, work, |name, work| {
        let file = crate::native_host::open_leaf(
            &parent.directory,
            name,
            &crate::native_host::NativeOpenOptions::ReadWriteCreate,
            work,
        )
        .map_err(restore_demand_error)?;
        loop {
            work.started_io(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(restore_demand_error)?;
            match fs2::FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(file),
                Err(error) if blocking && error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
}

pub(in crate::native_mount::materialize) const REMOVE_RESTORE_WITNESS: &[u8] =
    b"acyclic-remove-restore-v1\n";
pub(in crate::native_mount::materialize) const LIVE_RESTORE_WITNESS: &[u8] =
    b"acyclic-live-restore-v1\n";

pub(in crate::native_mount::materialize) fn create_restore_witness(
    parent: &HostRoot,
    name: &Path,
    contents: &[u8],
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    use std::io::Write as _;
    let mut file = crate::native_host::open_leaf(
        &parent.directory,
        name,
        &crate::native_host::NativeOpenOptions::CreateNew,
        work,
    )
    .map_err(restore_demand_error)?;
    let mut remaining = contents;
    while !remaining.is_empty() {
        work.admit(WorkCounters {
            output_bytes: remaining.len() as u64,
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        work.started_io(WorkCounters {
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        match file.write(remaining) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "restore witness write made no progress",
                )
                .into());
            }
            Ok(written) => {
                work.record(WorkCounters {
                    output_bytes: written as u64,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(restore_demand_error)?;
                remaining = &remaining[written..];
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    work.started_io(WorkCounters {
        durability_operations: 1,
        ..WorkCounters::UNCHARGED
    })
    .map_err(restore_demand_error)?;
    file.sync_all()?;
    crate::native_host::sync_directory_held(&parent.directory, work).map_err(restore_demand_error)
}

pub(in crate::native_mount::materialize) fn validate_restore_witness(
    parent: &HostRoot,
    name: &Path,
    expected: &[u8],
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<bool, MaterializeError> {
    use std::io::Read as _;
    let mut file = match crate::native_host::open_leaf(
        &parent.directory,
        name,
        &crate::native_host::NativeOpenOptions::Read,
        work,
    ) {
        Ok(file) => file,
        Err(crate::demand::DemandError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(false);
        }
        Err(error) => return Err(restore_demand_error(error)),
    };
    let mut contents = [0_u8; 64];
    let bound = expected
        .len()
        .checked_add(1)
        .filter(|bound| *bound <= contents.len())
        .ok_or(MaterializeError::InvalidOptions)?;
    let mut read = 0;
    while read < bound {
        work.admit(WorkCounters {
            source_bytes_read: (bound - read) as u64,
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        work.started_io(WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(restore_demand_error)?;
        match file.read(&mut contents[read..bound]) {
            Ok(0) => break,
            Ok(bytes) => {
                work.record(WorkCounters {
                    source_bytes_read: bytes as u64,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(restore_demand_error)?;
                read += bytes;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    if &contents[..read] == expected {
        Ok(true)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "restore artifact ownership witness is invalid",
        )
        .into())
    }
}

pub(in crate::native_mount::materialize) fn remove_restore_witness(
    parent: &HostRoot,
    name: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    remove_entry(parent, name, work).map_err(restore_demand_error)?;
    crate::native_host::sync_directory_held(&parent.directory, work).map_err(restore_demand_error)
}
