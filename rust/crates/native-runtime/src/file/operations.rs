#[cfg(unix)]
use super::ReadDestination;
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
///
/// On Apple platforms, Full requires `F_FULLFSYNC`; an unsupported full flush
/// returns directly without performing a weaker barrier. An interruption is
/// returned to the caller without reissuing the operation.
pub fn sync_file(
    file: &File,
    durability: Durability,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync", || sync_file_impl(file, durability, meter));
    #[cfg(not(feature = "test-support"))]
    sync_file_impl(file, durability, meter)
}

fn sync_file_impl(
    file: &File,
    durability: Durability,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<()> {
    if let Some(meter) = meter {
        #[cfg(not(target_vendor = "apple"))]
        if durability == Durability::Barrier {
            return barrier_sync(file);
        }
        #[cfg(target_vendor = "apple")]
        return crate::persistence_syscall(|| {
            apple::sync_once(file, durability, || {
                meter.check()?;
                meter.admit_durability()
            })
        })
        .and_then(|()| meter.check());
        #[cfg(any(windows, target_os = "linux"))]
        return sync_full(file, Some(meter));
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        {
            meter.check()?;
            meter.admit_durability()?;
            return crate::persistence_syscall(|| sync_file_unsequenced(file, durability))
                .and_then(|()| meter.check());
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
            crate::persistence_syscall(|| sync_file_unsequenced(file, durability))
        }))
    })
}

/// One admission and native full flush on the original held descriptor.
#[cfg(any(windows, target_os = "linux"))]
fn sync_full(file: &File, meter: Option<&dyn NativeReadMeter>) -> io::Result<()> {
    if let Some(meter) = meter {
        meter.check()?;
        meter.admit_durability()?;
    }
    #[cfg(target_os = "linux")]
    let descriptor = {
        use std::os::fd::AsFd as _;
        file.as_fd()
    };
    #[cfg(windows)]
    let descriptor = {
        use std::os::windows::io::AsHandle as _;
        file.as_handle()
    };
    crate::persistence_syscall(|| sync_descriptor_once(descriptor)).and_then(|()| match meter {
        Some(meter) => meter.check(),
        None => Ok(()),
    })
}

pub(crate) fn sync_file_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    return apple::sync_once(file, durability, || Ok(()));
    #[cfg(not(target_vendor = "apple"))]
    match durability {
        Durability::Full => {
            #[cfg(any(windows, target_os = "linux"))]
            return sync_full(file, None);
            #[cfg(not(any(windows, target_os = "linux")))]
            file.sync_all()
        }
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes file data according to `durability`.
pub fn sync_data(file: &File, durability: Durability) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_data", || sync_data_impl(file, durability));
    #[cfg(not(feature = "test-support"))]
    sync_data_impl(file, durability)
}

fn sync_data_impl(file: &File, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_data",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.in_scope(|| {
        operation.finish(with_file_admission(file, || {
            crate::persistence_syscall(|| {
                #[cfg(target_vendor = "apple")]
                return apple::sync_once(file, durability, || Ok(()));
                #[cfg(not(target_vendor = "apple"))]
                match durability {
                    Durability::Full => {
                        #[cfg(windows)]
                        return sync_file_unsequenced(file, durability);
                        #[cfg(not(windows))]
                        super::durable::flush_file_policy(file, true)
                    }
                    Durability::Barrier => barrier_sync(file),
                }
            })
        }))
    })
}

/// Flushes every dirty file and directory of the filesystem holding `path`
/// in one call where the platform has one, Linux `syncfs`; `Unsupported`
/// elsewhere, where a caller flushes each file instead.
pub fn sync_filesystem(path: &Path) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_filesystem", || sync_filesystem_impl(path));
    #[cfg(not(feature = "test-support"))]
    sync_filesystem_impl(path)
}

fn sync_filesystem_impl(path: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;
        let file = File::open(path)?;
        // SAFETY: syncfs acts only on this live descriptor.
        #[allow(unsafe_code, reason = "syncfs acts only on the held descriptor")]
        if unsafe { libc::syncfs(file.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err(io::ErrorKind::Unsupported.into())
    }
}

/// Flushes the directory entry namespace on platforms exposing directory flushes.
pub fn sync_parent(path: &Path, durability: Durability) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_parent", || {
        sync_parent_ordered(path, durability)
    });
    #[cfg(not(feature = "test-support"))]
    sync_parent_ordered(path, durability)
}

fn sync_parent_ordered(path: &Path, durability: Durability) -> io::Result<()> {
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
    with_file_admission(file, || {
        #[cfg(target_vendor = "apple")]
        return read_at_impl(file, offset, ReadDestination::Slice(destination));
        #[cfg(not(target_vendor = "apple"))]
        read_at_impl(file, offset, destination)
    })
}

/// Reads one declared source prefix into caller-admitted storage without changing the cursor or allocating.
#[cfg(unix)]
pub fn read_declared_source_bytes(
    file: &std::fs::File,
    offset: u64,
    destination: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    read_declared_source(file, offset, ReadDestination::Slice(destination), meter)
}

#[cfg(unix)]
pub(crate) fn read_declared_source_spare(
    file: &File,
    offset: u64,
    data: &mut Vec<u8>,
    length: usize,
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    read_declared_source(file, offset, ReadDestination::Spare { data, length }, meter)
}

#[cfg(unix)]
fn read_declared_source(
    file: &File,
    offset: u64,
    mut destination: ReadDestination<'_>,
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    meter.check()?;
    let (_, length) = destination.parts()?;
    if length == 0 {
        return Ok(0);
    }
    #[cfg(target_os = "linux")]
    linux::validate_io_range(offset, length)?;
    #[cfg(not(target_os = "linux"))]
    {
        let length = u64::try_from(length).map_err(|_| io::ErrorKind::InvalidInput)?;
        let end = offset
            .checked_add(length)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if offset > i64::MAX as u64 || end > i64::MAX as u64 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
    }
    let mut operation = NativeOperationReservation::new(meter, NativeOperationKind::Read, 1)?;
    let mut bytes = NativeByteReservation::new(meter, NativeOperationKind::Read, length)?;
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
/// Successful short writes advance through the payload. The first native error,
/// including interruption, ends the request; an earlier prefix may be written.
pub fn write_all_at(
    file: &File,
    offset: u64,
    bytes: &[u8],
    meter: Option<(&dyn NativeReadMeter, bool)>,
) -> io::Result<()> {
    let Some((meter, held)) = meter else {
        return with_file_admission(file, || {
            #[cfg(windows)]
            return windows::write_all_at(file, offset, bytes);
            #[cfg(not(windows))]
            {
                write_all_in_place(file, offset, bytes, None)
            }
        });
    };
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
    match NativeFile::from_file(file, None) {
        Ok(file) => file.read_batch_async(reads),
        Err((error, _)) => NativeCompletion::ready(Err(error)),
    }
}

/// Resizes an already-held, ordered handle with one admission and native call.
/// An interruption returns directly to the caller.
pub fn set_len(file: &File, size: u64, meter: &dyn NativeReadMeter) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:set_len", || resize_file(file, size, Some(meter)));
    #[cfg(not(feature = "test-support"))]
    resize_file(file, size, Some(meter))
}

pub(super) fn resize_file(
    file: &File,
    size: u64,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<()> {
    let size = i64::try_from(size).map_err(|_| io::ErrorKind::InvalidInput)?;
    #[cfg(unix)]
    let size = libc::off_t::try_from(size).map_err(|_| io::ErrorKind::InvalidInput)?;
    if let Some(meter) = meter {
        meter.check()?;
        meter.admit_write(0)?;
    }
    #[cfg(unix)]
    let result = {
        use std::os::fd::AsRawFd as _;
        // SAFETY: ftruncate acts only on this live descriptor with the validated length.
        #[allow(unsafe_code, reason = "ftruncate acts only on the held descriptor")]
        if unsafe { libc::ftruncate(file.as_raw_fd(), size) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    };
    #[cfg(windows)]
    let result = windows::set_len_once(file, size);
    #[cfg(not(any(windows, unix)))]
    let result = file.set_len(size as u64);
    result.and_then(|()| match meter {
        Some(meter) => meter.check(),
        None => Ok(()),
    })
}

pub(crate) fn write_bounded_in_place(
    file: &File,
    offset: u64,
    bytes: &[u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    write_all_in_place(file, offset, bytes, Some(meter))
}

pub(crate) fn write_all_in_place(
    file: &File,
    mut offset: u64,
    mut bytes: &[u8],
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<()> {
    let end = offset
        .checked_add(bytes.len() as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    #[cfg(unix)]
    if offset > libc::off_t::MAX as u64 || end > libc::off_t::MAX as u64 {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(not(unix))]
    let _ = end;
    while !bytes.is_empty() {
        if let Some(meter) = meter {
            meter.check()?;
        }
        // Metered requests retain their original physical chunk size. An
        // unmetered request offers its entire remaining native-sized range.
        let length = if meter.is_some() {
            bytes.len().min(1024 * 1024)
        } else {
            bytes.len().min(isize::MAX as usize)
        };
        #[cfg(windows)]
        let length = length.min(u32::MAX as usize);
        let chunk = bytes.get(..length).ok_or(io::ErrorKind::InvalidData)?;
        let count = write_file_with(chunk, |chunk| {
            #[cfg(unix)]
            let result = (|| {
                let length = chunk.len();
                use std::os::fd::AsRawFd as _;
                let mut transfer = meter
                    .map(|meter| {
                        NativeByteReservation::new(meter, NativeOperationKind::Write, length)
                    })
                    .transpose()?;
                if let Some(meter) = meter {
                    meter.admit_write(0)?;
                }
                #[allow(
                    unsafe_code,
                    reason = "writes the validated borrowed range on a held descriptor"
                )]
                // SAFETY: the validated range and borrowed payload remain live through pwrite.
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
                let count = usize::try_from(count).map_err(|_| io::ErrorKind::InvalidData)?;
                if let Some(transfer) = &mut transfer {
                    transfer.record(count)?;
                }
                Ok(count)
            })();
            #[cfg(windows)]
            let result = windows::write_at_known_overlapped(file, offset, chunk, meter);
            #[cfg(not(any(windows, unix)))]
            let result: io::Result<usize> = Err(io::Error::from(io::ErrorKind::Unsupported));
            result
        })?;
        if count > length {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let Some(meter) = meter {
            meter.check()?;
        }
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        offset += count as u64;
        bytes = bytes.get(count..).ok_or(io::ErrorKind::InvalidData)?;
    }
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "reads the validated destination on a held descriptor"
)]
pub(crate) fn read_at_impl(
    file: &File,
    offset: u64,
    mut destination: ReadDestination<'_>,
) -> io::Result<usize> {
    use std::os::fd::AsRawFd as _;
    let offset = libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
    let (pointer, length) = destination.parts()?;
    // SAFETY: the held file and writable destination live through this syscall.
    let result = unsafe { libc::pread(file.as_raw_fd(), pointer.cast(), length, offset) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        let count = usize::try_from(result).map_err(|_| io::ErrorKind::InvalidData)?;
        if count > length {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let ReadDestination::Spare { data, .. } = destination {
            // SAFETY: pread initialized count bytes in the validated spare slice.
            unsafe { data.set_len(data.len() + count) };
        }
        Ok(count)
    }
}

#[cfg(all(unix, not(target_os = "linux"), not(target_vendor = "apple")))]
pub(crate) fn read_at_impl(
    file: &File,
    offset: u64,
    destination: ReadDestination<'_>,
) -> io::Result<usize> {
    use std::os::unix::fs::FileExt as _;
    match destination {
        ReadDestination::Slice(data) => file.read_at(data, offset),
        ReadDestination::Spare { .. } => Err(io::ErrorKind::Unsupported.into()),
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
    // sync_parent owns the fault boundary; never wrap the flush a second time.
    sync_file_impl(&File::open(path)?, durability, None)
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
    sync_file_impl(&directory, durability, None)
}
