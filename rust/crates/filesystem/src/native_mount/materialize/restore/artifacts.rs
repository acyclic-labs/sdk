use super::*;

pub(in crate::native_mount::materialize) fn restore_artifact_name(
    prefix: &str,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<PathBuf, MaterializeError> {
    let mut hasher = blake3::Hasher::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = relative.as_os_str().as_bytes();
        work.started_io(WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        })
        .map_err(MaterializeError::from)?;
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
            .map_err(MaterializeError::from)?;
            hasher.update(&unit.to_le_bytes());
        }
    }
    let bytes = prefix.len().checked_add(64).ok_or(WorkError::Overflow)?;
    let delta = WorkCounters {
        bytes_encoded: bytes as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(delta).map_err(MaterializeError::from)?;
    work.claim_allocation(bytes as u64, u64::from(bytes != 0), WorkCounters::UNCHARGED)
        .map_err(MaterializeError::from)?;
    let mut name = String::with_capacity(bytes);
    name.push_str(prefix);
    name.push_str(hasher.finalize().to_hex().as_str());
    if let Err(error) = work.record(delta) {
        drop(name);
        work.release(bytes as u64).map_err(MaterializeError::from)?;
        return Err(MaterializeError::from(error));
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
    work.release(bytes).map_err(MaterializeError::from)?;
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
        .map_err(MaterializeError::from)?;
        lock_restore_file(file, blocking, work, fs2::FileExt::try_lock_exclusive)
    })
}

pub(in crate::native_mount::materialize) fn lock_restore_file(
    file: File,
    blocking: bool,
    work: &mut impl crate::demand::SourceDemandReadWork,
    mut try_lock: impl FnMut(&File) -> std::io::Result<()>,
) -> Result<File, MaterializeError> {
    loop {
        work.started_io(crate::materializer::NativeResourceCredit::WriteOperations.delta(1))
            .map_err(MaterializeError::from)?;
        match try_lock(&file) {
            Ok(()) => return Ok(file),
            Err(error) if blocking && crate::native_mount::lock_is_contended(&error) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
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
    .map_err(MaterializeError::from)?;
    let mut remaining = contents;
    while !remaining.is_empty() {
        work.admit(WorkCounters {
            output_bytes: remaining.len() as u64,
            backend_write_operations: 1,
            ..WorkCounters::UNCHARGED
        })
        .map_err(MaterializeError::from)?;
        work.started_io(crate::materializer::NativeResourceCredit::WriteOperations.delta(1))
            .map_err(MaterializeError::from)?;
        match file.write(remaining) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "restore witness write made no progress",
                )
                .into());
            }
            Ok(written) => {
                work.record(
                    crate::materializer::NativeResourceCredit::WriteBytes.delta(written as u64),
                )
                .map_err(MaterializeError::from)?;
                remaining = remaining.get(written..).ok_or(WorkError::Overflow)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    work.started_io(crate::materializer::NativeResourceCredit::DurabilityOperations.delta(1))
        .map_err(MaterializeError::from)?;
    file.sync_all()?;
    crate::native_host::sync_directory_held(&parent.directory, work).map_err(MaterializeError::from)
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
        Err(error) => return Err(MaterializeError::from(error)),
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
        .map_err(MaterializeError::from)?;
        work.started_io(crate::materializer::NativeResourceCredit::ReadOperations.delta(1))
            .map_err(MaterializeError::from)?;
        match file.read(contents.get_mut(read..bound).ok_or(WorkError::Overflow)?) {
            Ok(0) => break,
            Ok(bytes) => {
                work.record(
                    crate::materializer::NativeResourceCredit::ReadBytes.delta(bytes as u64),
                )
                .map_err(MaterializeError::from)?;
                read += bytes;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    if contents.get(..read).ok_or(WorkError::Overflow)? == expected {
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
    remove_entry(parent, name, work).map_err(MaterializeError::from)?;
    crate::native_host::sync_directory_held(&parent.directory, work).map_err(MaterializeError::from)
}
