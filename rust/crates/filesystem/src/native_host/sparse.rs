use super::*;

/// Network, clustered, and user-space filesystems, by `statfs(2)` magic.
#[cfg(target_os = "linux")]
pub(super) const REMOTE_FILESYSTEMS: [u64; 13] = [
    0x6969,      // NFS
    0x517b,      // SMB
    0xff53_4d42, // CIFS
    0xfe53_4d42, // SMB2
    0x5346_414f, // AFS
    0x00c3_6400, // Ceph
    0x6573_5546, // FUSE
    0x6a65_6a63, // virtiofs, which a hypervisor's host changes unseen
    0x0102_1997, // 9P
    0x564c,      // NCP
    0x7375_7245, // Coda
    0x0bd0_0bd0, // Lustre
    0x4750_4653, // GPFS
];

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn filesystem_is_local<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<bool, W::Error> {
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    work.started_io(READ)?;
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Ok(false);
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let stats = unsafe { stats.assume_init() };
    #[cfg(target_os = "linux")]
    {
        Ok(u64::try_from(stats.f_type).is_ok_and(|kind| !REMOTE_FILESYSTEMS.contains(&kind)))
    }
    #[cfg(target_os = "macos")]
    {
        Ok(u32::try_from(libc::MNT_LOCAL).is_ok_and(|local| stats.f_flags & local != 0))
    }
}

#[cfg(windows)]
pub(super) fn filesystem_is_local<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<bool, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{
        FileFsDeviceInformation, NtQueryVolumeInformationFile,
    };
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    /// The characteristic of a volume on a network redirector.
    const FILE_REMOTE_DEVICE: u32 = 0x10;
    let mut device = [0_u32; 2];
    let mut status = IO_STATUS_BLOCK::default();
    // SAFETY: the handle is live for the call, and the buffer is exactly one `FILE_FS_DEVICE_INFORMATION` of the length passed.
    work.started_io(READ)?;
    let queried = unsafe {
        NtQueryVolumeInformationFile(
            HANDLE(directory.as_raw_handle()),
            &raw mut status,
            device.as_mut_ptr().cast(),
            8,
            FileFsDeviceInformation,
        )
    };
    let [_, characteristics] = device;
    Ok(queried.is_ok() && characteristics & FILE_REMOTE_DEVICE == 0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn filesystem_is_local<W: NativeHostWork>(
    _directory: &Dir,
    work: &mut W,
) -> Result<bool, W::Error> {
    work.check()?;
    Ok(false)
}

/// Deallocates an already-zero range using the supplied native meter.
#[cfg(target_os = "macos")]
pub fn punch_hole(
    file: &impl std::os::fd::AsRawFd,
    offset: u64,
    length: u64,
    meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
) -> io::Result<()> {
    let fd = file.as_raw_fd();
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    loop {
        if let Some(meter) = meter {
            meter.admit_read(0)?;
        }
        // SAFETY: fstatfs fills live, correctly sized storage for the held descriptor.
        if unsafe { libc::fstatfs(fd, stats.as_mut_ptr()) } == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let block = u64::from(unsafe { stats.assume_init() }.f_bsize);
    if block == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "host filesystem reports a zero allocation unit",
        ));
    }
    let Some(start) = offset
        .checked_add(block - 1)
        .map(|edge| edge / block * block)
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole start overflow",
        ));
    };
    let Some(end) = offset.checked_add(length).map(|edge| edge / block * block) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole end overflow",
        ));
    };
    if end <= start {
        return Ok(());
    }
    let (Ok(fp_offset), Ok(fp_length)) = (i64::try_from(start), i64::try_from(end - start)) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole range exceeds host limits",
        ));
    };
    let arg = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset,
        fp_length,
    };
    loop {
        if let Some(meter) = meter {
            meter.admit_write(0)?;
        }
        // SAFETY: the argument struct outlives this value-only fcntl call.
        if unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &arg) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(windows)]
pub(super) fn clone_windows_file<W: SourceDemandReadWork>(
    source: &File,
    target: &File,
    length: u64,
    work: &mut W,
) -> Result<bool, DemandError> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::{
        DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE, FSCTL_SET_SPARSE,
    };
    const CLONE_CHUNK: u64 = 1024 * 1024 * 1024;
    for alignment in [4 * 1024, 64 * 1024] {
        work.charge(ITEM)?;
        let cloned: Result<(), DemandError> = (|| {
            let clone_length = length / alignment * alignment;
            if clone_length == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file is smaller than clone alignment",
                )
                .into());
            }
            work.charge(WRITE)?;
            // SAFETY: live synchronous target handle; this control has no buffers.
            unsafe {
                DeviceIoControl(
                    HANDLE(target.as_raw_handle()),
                    FSCTL_SET_SPARSE,
                    None,
                    0,
                    None,
                    0,
                    None,
                    None,
                )
                .map_err(io::Error::from)?;
            }
            truncate_file(target, length, work)?;
            let mut offset = 0;
            while offset < clone_length {
                work.charge(ITEM)?;
                let count = (clone_length - offset).min(CLONE_CHUNK);
                let duplicate = DUPLICATE_EXTENTS_DATA {
                    FileHandle: HANDLE(source.as_raw_handle()),
                    SourceFileOffset: i64::try_from(offset)
                        .map_err(|_| crate::WorkError::Overflow)?,
                    TargetFileOffset: i64::try_from(offset)
                        .map_err(|_| crate::WorkError::Overflow)?,
                    ByteCount: i64::try_from(count).map_err(|_| crate::WorkError::Overflow)?,
                };
                work.charge(WRITE)?;
                // SAFETY: both held handles and complete fixed-size input remain live.
                unsafe {
                    DeviceIoControl(
                        HANDLE(target.as_raw_handle()),
                        FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                        Some((&raw const duplicate).cast()),
                        std::mem::size_of::<DUPLICATE_EXTENTS_DATA>() as u32,
                        None,
                        0,
                        None,
                        None,
                    )
                    .map_err(io::Error::from)?;
                }
                offset += count;
            }
            if clone_length < length {
                let ranges =
                    allocated_data_ranges(source, clone_length, length - clone_length, 64, work)?;
                let ranges_bytes =
                    (ranges.capacity() * std::mem::size_of::<HostDataRange>()) as u64;
                let result: Result<(), DemandError> = (|| {
                    let mut buffer = [0_u8; 64 * 1024];
                    for range in &ranges {
                        work.charge(ITEM)?;
                        let size = usize::try_from(range.length)
                            .map_err(|_| crate::WorkError::Overflow)?;
                        let buffer = buffer.get_mut(..size).ok_or(DemandError::InvalidRequest)?;
                        windows_copy_tail(source, target, range.offset, buffer, work)?;
                    }
                    Ok(())
                })();
                drop(ranges);
                work.release(ranges_bytes)?;
                result?;
            }
            Ok(())
        })();
        match cloned {
            Ok(()) => return Ok(true),
            Err(DemandError::Io(_)) => truncate_file(target, 0, work)?,
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

pub(crate) fn truncate_file<W: NativeHostWork>(
    file: &File,
    length: u64,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    {
        let length = libc::off_t::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
        loop {
            let delta = WRITE;
            work.admit(delta)?;
            work.record(delta)?;
            // SAFETY: ftruncate acts only on the live held descriptor.
            if unsafe { libc::ftruncate(file.as_raw_fd(), length) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
    }
    #[cfg(windows)]
    {
        use windows::Wdk::Storage::FileSystem::FileEndOfFileInformation;
        use windows::Win32::Storage::FileSystem::FILE_END_OF_FILE_INFO;
        let information = FILE_END_OF_FILE_INFO {
            EndOfFile: i64::try_from(length).map_err(|_| crate::WorkError::Overflow)?,
        };
        work.attempt_io(WRITE, || {
            windows_set_information(file, &information, FileEndOfFileInformation)
        })
    }
}

#[cfg(windows)]
pub(super) fn windows_copy_tail<W: SourceDemandReadWork>(
    source: &File,
    target: &File,
    offset: u64,
    buffer: &mut [u8],
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{NtReadFile, NtWriteFile};
    use windows::Win32::Foundation::HANDLE;
    let mut consumed = 0usize;
    while consumed < buffer.len() {
        let length = buffer.len() - consumed;
        work.admit(WorkCounters {
            source_bytes_read: length as u64,
            ..WorkCounters::UNCHARGED
        })?;
        work.started_io(READ)?;
        let position = i64::try_from(
            offset
                .checked_add(consumed as u64)
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        // SAFETY: synchronous held handle and writable admitted tail slice.
        let status = unsafe {
            NtReadFile(
                HANDLE(source.as_raw_handle()),
                None,
                None,
                None,
                &raw mut block,
                buffer[consumed..].as_mut_ptr().cast(),
                length as u32,
                Some(&position),
                None,
            )
        };
        if status.is_err() {
            return Err(status_error(status).into());
        }
        let count = block.Information;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "clone source tail shortened",
            )
            .into());
        }
        if count > length {
            return Err(DemandError::InvalidRequest);
        }
        work.record(WorkCounters {
            source_bytes_read: count as u64,
            ..WorkCounters::UNCHARGED
        })?;
        consumed += count;
    }
    let mut consumed = 0usize;
    while consumed < buffer.len() {
        work.charge(WRITE)?;
        let position = i64::try_from(
            offset
                .checked_add(consumed as u64)
                .ok_or(crate::WorkError::Overflow)?,
        )
        .map_err(|_| crate::WorkError::Overflow)?;
        let mut block = windows::Win32::System::IO::IO_STATUS_BLOCK::default();
        // SAFETY: synchronous held handle and initialized tail slice remain live.
        let status = unsafe {
            NtWriteFile(
                HANDLE(target.as_raw_handle()),
                None,
                None,
                None,
                &raw mut block,
                buffer[consumed..].as_ptr().cast(),
                (buffer.len() - consumed) as u32,
                Some(&position),
                None,
            )
        };
        if status.is_err() {
            return Err(status_error(status).into());
        }
        let count = block.Information;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "clone tail write made no progress",
            )
            .into());
        }
        if count > buffer.len() - consumed {
            return Err(DemandError::InvalidRequest);
        }
        consumed += count;
    }
    Ok(())
}
