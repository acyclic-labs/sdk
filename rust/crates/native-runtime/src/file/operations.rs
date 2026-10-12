use crate::*;

/// Required durability boundary for one file operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Flush file contents and metadata to stable storage.
    Full,
    /// Order earlier writes before later writes without forcing the device cache to drain.
    Barrier,
}

/// Flushes file contents and metadata according to `durability`.
pub fn sync_file(
    file: &File,
    durability: Durability,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<()> {
    if let Some(meter) = meter {
        #[cfg(not(target_vendor = "apple"))]
        if durability == Durability::Barrier {
            return barrier_sync(file);
        }
        loop {
            meter.check()?;
            meter.admit_durability()?;
            #[cfg(target_os = "linux")]
            let result = linux::sync_once(file);
            #[cfg(target_vendor = "apple")]
            let result = apple::sync_once(file, durability);
            #[cfg(windows)]
            let result = windows::sync_once(file);
            #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
            let result = sync_file_unsequenced(file, durability);
            match result {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return result.and_then(|()| meter.check()),
            }
        }
    }
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_file",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.in_scope(|| {
        operation.finish(with_file_admission(file, || {
            sync_file_unsequenced(file, durability)
        }))
    })
}

pub(crate) fn sync_file_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_all(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes file data according to `durability`.
pub fn sync_data(file: &File, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_data",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.in_scope(|| {
        operation.finish(with_file_admission(file, || match durability {
            Durability::Full => file.sync_data(),
            Durability::Barrier => barrier_sync(file),
        }))
    })
}

/// Flushes the directory entry namespace on platforms exposing directory flushes.
pub fn sync_parent(path: &Path, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_parent",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.in_scope(|| operation.finish(sync_parent_impl(path, durability)))
}

/// Reads at an absolute offset without changing the file cursor.
pub fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    with_file_admission(file, || read_at_impl(file, offset, destination))
}

/// Reads one declared source prefix into caller-admitted storage without changing the cursor or allocating.
#[cfg(unix)]
pub fn read_declared_source_bytes(
    file: &std::fs::File,
    offset: u64,
    destination: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    meter.check()?;
    if destination.is_empty() {
        return Ok(0);
    }
    #[cfg(target_os = "linux")]
    linux::validate_io_range(offset, destination.len())?;
    #[cfg(not(target_os = "linux"))]
    {
        let length = u64::try_from(destination.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let end = offset
            .checked_add(length)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if offset > i64::MAX as u64 || end > i64::MAX as u64 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
    }
    let mut operation = NativeOperationReservation::new(meter, NativeOperationKind::Read, 1)?;
    let mut bytes =
        NativeByteReservation::new(meter, NativeOperationKind::Read, destination.len())?;
    meter.check()?;
    #[cfg(target_os = "linux")]
    let (result, recorded_operation) =
        operation.attempt(|| linux::read_at_validated(file, offset, destination));
    #[cfg(not(target_os = "linux"))]
    let (result, recorded_operation) =
        operation.attempt(|| read_at_impl(file, offset, destination));
    let recorded_bytes = bytes.record(result.as_ref().copied().unwrap_or(0));
    recorded_bytes?;
    recorded_operation?;
    let count = result?;
    meter.check()?;
    Ok(count)
}

/// Writes positionally; a metered held handle must support positional I/O and own its file admission.
pub fn write_all_at(
    file: &File,
    offset: u64,
    bytes: &[u8],
    meter: Option<(&dyn NativeReadMeter, bool)>,
) -> io::Result<()> {
    let Some((meter, held)) = meter else {
        return with_file_admission(file, || write_all_at_impl(file, offset, bytes));
    };
    offset
        .checked_add(bytes.len() as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    #[cfg(windows)]
    if !held {
        return windows::with_overlapped(file, 0x0012_0116, false, Some(meter), |file| {
            write_bounded_in_place(file, offset, bytes, meter)
        });
    }
    #[cfg(not(windows))]
    let _ = held;
    write_bounded_in_place(file, offset, bytes, meter)
}

/// Completes one device request on a held handle, optionally admitting its operation kind.
#[cfg(windows)]
pub fn device_control_in_place(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    meter: Option<(&dyn NativeReadMeter, NativeOperationKind)>,
) -> io::Result<usize> {
    if let Some((meter, _)) = meter {
        meter.check()?;
    }
    windows::control_in_place(file, code, input, output, meter)
}

/// One owned positional write whose buffer remains live through completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWrite {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Bytes written completely or reported as an error.
    pub bytes: Bytes,
}

/// One owned positional read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedRead {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Maximum bytes returned for this request.
    pub length: usize,
}

/// Submits an owned read batch to the shared native completion worker.
pub fn read_batch_async(file: File, reads: Vec<OwnedRead>) -> NativeCompletion<Vec<Bytes>> {
    let file = Arc::new(NativeFilePayload {
        file: file,
        _allocation: None,
    });
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        match shared_file_health(&file, None) {
            Ok(health) => NativeCompletion::<Vec<Bytes>>::submit(
                file,
                reads,
                Some(health),
                obs::file_span!(TRACE, "acyclic.runtime.read_batch"),
                #[cfg(windows)]
                false,
            ),
            Err(error) => NativeCompletion::<Vec<Bytes>>::failed(error),
        }
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    NativeCompletion::<Vec<Bytes>>::submit(
        file,
        reads,
        None,
        obs::file_span!(TRACE, "acyclic.runtime.read_batch"),
    )
}

/// Resizes an already-held, ordered handle. Each retry is separately admitted.
pub fn set_len(file: &File, size: u64, meter: &dyn NativeReadMeter) -> io::Result<()> {
    i64::try_from(size).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    loop {
        meter.check()?;
        meter.admit_write(0)?;
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let result = {
            use std::os::fd::AsRawFd as _;
            let size = libc::off_t::try_from(size).map_err(|_| io::ErrorKind::InvalidInput)?;
            // SAFETY: ftruncate acts only on this live descriptor.
            #[allow(unsafe_code)]
            if unsafe { libc::ftruncate(file.as_raw_fd(), size) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        };
        #[cfg(windows)]
        let result = windows::set_len_once(file, size);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result = file.set_len(size);
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result.and_then(|()| meter.check()),
        }
    }
}

pub(crate) fn write_bounded_in_place(
    file: &File,
    mut offset: u64,
    mut bytes: &[u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    while !bytes.is_empty() {
        meter.check()?;
        let length = bytes.len().min(1024 * 1024);
        let chunk = &bytes[..length];
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let result = (|| {
            use std::os::fd::AsRawFd as _;
            validate_prepared_read_range(offset, length)?;
            let mut transfer =
                NativeByteReservation::new(meter, NativeOperationKind::Write, length)?;
            meter.admit_write(0)?;
            // SAFETY: the validated range and borrowed payload remain live through pwrite.
            #[allow(unsafe_code)]
            let count = unsafe {
                libc::pwrite(
                    file.as_raw_fd(),
                    chunk.as_ptr().cast(),
                    length,
                    offset as libc::off_t,
                )
            };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            transfer.record(count as usize)?;
            Ok(count as usize)
        })();
        #[cfg(windows)]
        let result = windows::write_at_known_overlapped(file, offset, chunk, meter);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result: io::Result<usize> = Err(io::Error::from(io::ErrorKind::Unsupported));
        let count = match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count > length {
            std::process::abort();
        }
        meter.check()?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        offset += count as u64;
        bytes = &bytes[count..];
    }
    Ok(())
}

pub(crate) fn validate_write_batch(writes: &[OwnedWrite]) -> io::Result<()> {
    let mut ranges = Vec::new();
    ranges.try_reserve_exact(writes.len())?;
    for write in writes {
        if write.offset == u64::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch write offset aliases the shared file cursor",
            ));
        }
        let length = u64::try_from(write.bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "batch write length is invalid")
        })?;
        let end = write.offset.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "batch write range overflows")
        })?;
        if !write.bytes.is_empty() {
            ranges.push((write.offset, end));
        }
    }
    ranges.sort_unstable();
    if ranges
        .windows(2)
        .any(|pair| matches!(pair, [first, second] if first.1 > second.0))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "overlapping batch write ranges",
        ));
    }
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
pub(crate) fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::fd::AsRawFd as _;
    let offset = libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: the held file and writable destination live through this syscall.
    let result = unsafe {
        libc::pread(
            file.as_raw_fd(),
            destination.as_mut_ptr().cast(),
            destination.len(),
            offset,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[cfg(all(unix, not(target_os = "linux"), not(target_vendor = "apple")))]
pub(crate) fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt as _;
    file.read_at(destination, offset)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    file.write_all_at(bytes, offset)
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code, reason = "F_BARRIERFSYNC is reachable only through fcntl")]
pub(crate) fn barrier_sync(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: `F_BARRIERFSYNC` acts only on the live descriptor.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_vendor = "apple"))]
pub(crate) fn barrier_sync(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "ordered write barrier is unavailable on this platform",
    ))
}

#[cfg(unix)]
pub(crate) fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    sync_file(&File::open(path)?, durability, None)
}

#[cfg(windows)]
pub(crate) fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
    let directory = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ_WRITE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    sync_file(&directory, durability, None)
}
