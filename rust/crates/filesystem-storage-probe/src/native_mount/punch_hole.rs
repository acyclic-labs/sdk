//! Deallocating discarded byte ranges without changing file length.

/// Deallocates a discarded range without changing file length or other bytes.
#[cfg(target_os = "linux")]
#[allow(unsafe_code, reason = "deallocates a discarded range with fallocate")]
pub fn punch_hole(
    file: &impl std::os::fd::AsRawFd,
    offset: u64,
    length: u64,
    meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<()> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let offset = i64::try_from(offset)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let length = i64::try_from(end)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?
        - offset;
    if length == 0 {
        return Ok(());
    }
    if let Some(meter) = meter {
        meter.admit_write(0)?;
    }
    // SAFETY: the live descriptor is borrowed for this value-only call and
    // the checked range contains only discarded bytes.
    if unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset,
            length,
        )
    } == 0
    {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Deallocates a discarded range without changing file length or other bytes.
#[cfg(windows)]
pub fn punch_hole(
    file: &std::fs::File,
    offset: u64,
    length: u64,
    meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<()> {
    use acyclic_native_runtime::{NativeOperationKind::Write, device_control_in_place};
    use windows::Win32::System::Ioctl::{FSCTL_SET_SPARSE, FSCTL_SET_ZERO_DATA};
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= i64::MAX as u64)
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    if length == 0 {
        return Ok(());
    }
    device_control_in_place(file, FSCTL_SET_SPARSE, &[], &mut [], Write, meter)?.into_complete()?;
    let mut range = [0; 16];
    range[..8].copy_from_slice(&offset.to_le_bytes());
    range[8..].copy_from_slice(&end.to_le_bytes());
    device_control_in_place(file, FSCTL_SET_ZERO_DATA, &range, &mut [], Write, meter)?
        .into_complete()?;
    Ok(())
}

/// Deallocates whole allocation units within a discarded range.
#[cfg(target_os = "macos")]
#[allow(
    unsafe_code,
    reason = "deallocates whole allocation units with F_PUNCHHOLE"
)]
pub fn punch_hole(
    file: &impl std::os::fd::AsRawFd,
    offset: u64,
    length: u64,
    meter: Option<&dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<()> {
    let fd = file.as_raw_fd();
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if let Some(meter) = meter {
        meter.admit_read(0)?;
    }
    // SAFETY: fstatfs fills live, correctly sized storage for the held descriptor.
    if unsafe { libc::fstatfs(fd, stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let block = u64::from(unsafe { stats.assume_init() }.f_bsize);
    if block == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "host filesystem reports a zero allocation unit",
        ));
    }
    let Some(start) = offset
        .checked_add(block - 1)
        .map(|edge| edge / block * block)
    else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hole start overflow",
        ));
    };
    let Some(end) = offset.checked_add(length).map(|edge| edge / block * block) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hole end overflow",
        ));
    };
    if end <= start {
        return Ok(());
    }
    let (Ok(fp_offset), Ok(fp_length)) = (i64::try_from(start), i64::try_from(end - start)) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hole range exceeds host limits",
        ));
    };
    let arg = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset,
        fp_length,
    };
    if let Some(meter) = meter {
        meter.admit_write(0)?;
    }
    // SAFETY: the argument struct outlives this value-only fcntl call.
    if unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &arg) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
