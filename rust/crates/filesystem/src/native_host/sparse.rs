use super::*;

/// Network, clustered, and user-space filesystems, by `statfs(2)` magic.
#[cfg(all(target_os = "linux", any(test, feature = "native-watch")))]
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
#[cfg(any(test, feature = "native-watch"))]
pub(crate) fn filesystem_is_local<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<bool, W::Error> {
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    work.started_io(READ)?;
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
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
#[cfg(any(test, feature = "native-watch"))]
pub(crate) fn filesystem_is_local<W: NativeHostWork>(
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
    work.started_io(READ)?;
    // SAFETY: the live handle and buffers match the sizes passed to this synchronous call.
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
#[cfg(any(test, feature = "native-watch"))]
pub(crate) fn filesystem_is_local<W: NativeHostWork>(
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

#[cfg(all(windows, any(test, feature = "native-mount")))]
pub(super) fn clone_windows_file<W: SourceDemandReadWork>(
    source: &File,
    target: &File,
    length: u64,
    work: &mut W,
) -> Result<bool, DemandError> {
    use acyclic_native_runtime::NativeOperationKind::Write;
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::System::Ioctl::{
        DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE, FSCTL_SET_SPARSE,
    };
    const CLONE_CHUNK: u64 = 1024 * 1024 * 1024;
    for alignment in [4 * 1024, 64 * 1024] {
        work.started_io(ITEM)?;
        let cloned = (|| {
            let clone_length = length / alignment * alignment;
            if clone_length == 0 {
                return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
            }
            source_control(target, FSCTL_SET_SPARSE, &[], &mut [], Write, work)?;
            truncate_file(target, length, work)?;
            let mut offset = 0;
            while offset < clone_length {
                work.started_io(ITEM)?;
                let count = (clone_length - offset).min(CLONE_CHUNK);
                let duplicate = DUPLICATE_EXTENTS_DATA {
                    FileHandle: win::HANDLE(source.as_raw_handle()),
                    SourceFileOffset: i64::try_from(offset)
                        .map_err(|_| crate::WorkError::Overflow)?,
                    TargetFileOffset: i64::try_from(offset)
                        .map_err(|_| crate::WorkError::Overflow)?,
                    ByteCount: i64::try_from(count).map_err(|_| crate::WorkError::Overflow)?,
                };
                // SAFETY: this native record has four initialized, padding-free machine words.
                let input = unsafe {
                    std::slice::from_raw_parts(
                        std::ptr::from_ref(&duplicate).cast::<u8>(),
                        std::mem::size_of_val(&duplicate),
                    )
                };
                source_control(
                    target,
                    FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                    input,
                    &mut [],
                    Write,
                    work,
                )?;
                offset += count;
            }
            if clone_length < length {
                let ranges =
                    allocated_data_ranges(source, clone_length, length - clone_length, 64, work)?;
                let allocation = (ranges.capacity() * std::mem::size_of::<HostDataRange>()) as u64;
                let result = (|| {
                    let mut buffer = [0_u8; 64 * 1024];
                    for range in &ranges {
                        work.started_io(ITEM)?;
                        let length = usize::try_from(range.length)
                            .map_err(|_| crate::WorkError::Overflow)?;
                        let output = buffer
                            .get_mut(..length)
                            .ok_or(DemandError::InvalidRequest)?;
                        windows_copy_tail(source, target, range.offset, output, work)?;
                    }
                    Ok::<_, DemandError>(())
                })();
                drop(ranges);
                work.release(allocation)?;
                result?;
            }
            Ok::<_, DemandError>(())
        })();
        match cloned {
            Ok(()) => return Ok(true),
            Err(DemandError::Io(_)) => truncate_file(target, 0, work)?,
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

#[cfg(any(windows, test, feature = "local"))]
pub(crate) fn truncate_file<W: NativeHostWork>(
    file: &File,
    length: u64,
    work: &mut W,
) -> Result<(), W::Error> {
    i64::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
    loop {
        match work.attempt_io(WRITE, || file.set_len(length)) {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => continue,
            result => return result,
        }
    }
}

#[cfg(windows)]
pub(super) fn windows_copy_tail<W: SourceDemandReadWork>(
    source: &File,
    target: &File,
    mut offset: u64,
    buffer: &mut [u8],
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::windows::fs::FileExt as _;
    let target_offset = offset;
    let mut remaining = &mut *buffer;
    // Each partial native read is admitted; read_exact would hide its retries.
    while !remaining.is_empty() {
        work.admit(WorkCounters {
            source_bytes_read: remaining.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let count = match work.attempt_io(READ, || source.seek_read(remaining, offset)) {
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        work.record(WorkCounters {
            source_bytes_read: count as u64,
            ..WorkCounters::UNCHARGED
        })?;
        offset = offset
            .checked_add(count as u64)
            .ok_or(crate::WorkError::Overflow)?;
        remaining = remaining
            .get_mut(count..)
            .ok_or(DemandError::InvalidRequest)?;
    }
    let mut offset = target_offset;
    let mut input = &*buffer;
    while !input.is_empty() {
        let count = match work.attempt_io(WRITE, || target.seek_write(input, offset)) {
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero).into());
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or(crate::WorkError::Overflow)?;
        input = input.get(count..).ok_or(DemandError::InvalidRequest)?;
    }
    Ok(())
}

#[cfg(all(windows, any(test, feature = "native-mount")))]
pub(super) fn clonable_windows_length(length: u64) -> bool {
    length >= 4 * 1024 && i64::try_from(length).is_ok()
}
