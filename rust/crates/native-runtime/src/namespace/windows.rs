//! Windows namespace publication and DOS path normalization.
use super::*;

/// Replace a name while original readers retain the old file. The source is
/// opened write-through: NTFS flushes rename metadata as part of that request,
/// rather than requiring a separate caller flush after publication.
/// See the native [caching contract](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew#caching-behavior).
#[allow(
    unsafe_code,
    reason = "passes held paths and an aligned bounded rename packet to Win32"
)]
pub(super) fn rename(
    from: &[u16],
    to: &[u16],
    mode: RenameMode,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
    use windows_sys::Wdk::Storage::FileSystem::{
        FILE_RENAME_POSIX_SEMANTICS, FILE_RENAME_REPLACE_IF_EXISTS,
    };
    use windows_sys::Win32::{
        Foundation::INVALID_HANDLE_VALUE,
        Storage::FileSystem::{
            CreateFileW, DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_FLAG_WRITE_THROUGH, FILE_RENAME_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ,
            FILE_SHARE_WRITE, FileRenameInfoEx, OPEN_EXISTING, SYNCHRONIZE,
            SetFileInformationByHandle,
        },
    };

    let name = to.strip_suffix(&[0]).ok_or(io::ErrorKind::InvalidInput)?;
    let name_bytes = std::mem::size_of_val(name);
    let packet_bytes = std::mem::offset_of!(FILE_RENAME_INFO, FileName)
        .checked_add(name_bytes)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let length = u32::try_from(packet_bytes).map_err(|_| io::ErrorKind::InvalidInput)?;
    // The flexible UTF-16 name follows a pointer-aligned native header.
    let words = packet_bytes.div_ceil(std::mem::size_of::<usize>());
    let capacity = words
        .checked_mul(std::mem::size_of::<usize>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    let mut allocation = meter
        .map(|meter| ReadAllocation::new(meter, capacity))
        .transpose()?;
    let mut packet = Vec::<usize>::new();
    packet
        .try_reserve_exact(words)
        .map_err(|error| match meter {
            Some(meter) => meter.allocation_failed(error),
            None => io::ErrorKind::OutOfMemory.into(),
        })?;
    if let Some(allocation) = &mut allocation {
        allocation.reconcile_capacity(packet.capacity() * std::mem::size_of::<usize>())?;
    }
    let copied = capacity
        .checked_add(name_bytes)
        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<u32>()))
        .ok_or(io::ErrorKind::InvalidInput)?;
    if let Some(meter) = meter {
        meter.check()?;
        meter.admit_codec(0, copied as u64, 0)?;
    }
    packet.resize(words, 0);
    // SAFETY: the initialized allocation has FILE_RENAME_INFO alignment and
    // covers its fixed fields and the complete checked, non-terminated name.
    unsafe {
        let header = packet.as_mut_ptr().cast::<FILE_RENAME_INFO>();
        (*header).Anonymous.Flags = if mode == RenameMode::Replace {
            FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS
        } else {
            0
        };
        (*header).FileNameLength =
            u32::try_from(name_bytes).map_err(|_| io::ErrorKind::InvalidInput)?;
        std::ptr::copy_nonoverlapping(
            name.as_ptr(),
            std::ptr::addr_of_mut!((*header).FileName).cast::<u16>(),
            name.len(),
        );
    }
    if let Some(meter) = meter {
        meter.record_codec(0, copied as u64, 0)?;
        meter.admit_read(0)?;
    }
    // SAFETY: namespace_path supplied a held NUL-terminated source. Opening
    // the entry itself preserves symlink rename semantics and allows folders.
    let handle = unsafe {
        CreateFileW(
            from.as_ptr(),
            DELETE | SYNCHRONIZE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_WRITE_THROUGH | FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: transfer the successful open to exactly one closing owner.
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    // SAFETY: the write-through handle and the initialized, aligned packet
    // remain live through the single atomic rename. No retry or unlink occurs.
    if unsafe {
        SetFileInformationByHandle(
            handle.as_raw_handle(),
            FileRenameInfoEx,
            packet.as_ptr().cast(),
            length,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Keep allocation custody for both the encoded input and the normalized
/// output until each buffer is actually retired. Verbatim/device paths
/// already specify their namespace and must not be normalized a second time.
#[allow(
    unsafe_code,
    reason = "resolve a held NUL-terminated DOS path with GetFullPathNameW"
)]
pub(super) fn extended_path(
    input: (Vec<u16>, Option<ReadAllocation>),
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<(Vec<u16>, Option<ReadAllocation>)> {
    use windows_sys::Win32::Storage::FileSystem::GetFullPathNameW;
    if input.0.starts_with(&[92, 92, 63, 92]) || input.0.starts_with(&[92, 92, 46, 92]) {
        return Ok(input);
    }
    // SAFETY: input owns its terminating NUL. A zero-sized null output asks
    // only for the required size, including its terminating NUL.
    let required = unsafe {
        GetFullPathNameW(
            input.0.as_ptr(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if required == 0 {
        return Err(io::Error::last_os_error());
    }
    let capacity = (required as usize)
        .checked_add(8)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let bytes = capacity.checked_mul(2).ok_or(io::ErrorKind::InvalidInput)?;
    let mut allocation = meter
        .map(|meter| ReadAllocation::new(meter, bytes))
        .transpose()?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|error| match meter {
            Some(meter) => meter.allocation_failed(error),
            None => io::ErrorKind::OutOfMemory.into(),
        })?;
    if let Some(allocation) = &mut allocation {
        allocation.reconcile_capacity(output.capacity() * 2)?;
    }
    // Account for zero initialization, OS output, moving the completed path,
    // and writing its namespace prefix before performing those copies.
    let maximum_copied = bytes
        .checked_add((required as usize) * 4 + 16)
        .ok_or(io::ErrorKind::InvalidInput)?;
    if let Some(meter) = meter {
        meter.check()?;
        meter.admit_codec(0, maximum_copied as u64, 0)?;
    }
    output.resize(capacity, 0);
    if let Some(meter) = meter {
        meter.record_codec(0, bytes as u64, 0)?;
    }
    // SAFETY: the input is held; output[8..] has exactly `required` writable
    // UTF-16 units. The reserved prefix is outside the API's output range.
    let length = unsafe {
        GetFullPathNameW(
            input.0.as_ptr(),
            required,
            output.as_mut_ptr().add(8),
            std::ptr::null_mut(),
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    // If a process-wide cwd change grows the required output, fail before
    // publication rather than dispatching a truncated path or repeating it.
    if length >= required {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let length = length as usize;
    if let Some(meter) = meter {
        meter.record_codec(0, ((length + 1) * 2) as u64, 0)?;
    }
    let resolved = output
        .get(8..8 + length)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let (prefix, skip): (&[u16], usize) =
        if resolved.starts_with(&[92, 92, 46, 92]) || resolved.starts_with(&[92, 92, 63, 92]) {
            (&[], 0)
        } else if resolved.starts_with(&[92, 92]) {
            (&[92, 92, 63, 92, 85, 78, 67, 92], 2)
        } else if output.get(9..11) == Some(&[58, 92]) {
            (&[92, 92, 63, 92], 0)
        } else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
    output.copy_within(8 + skip..8 + length + 1, prefix.len());
    output
        .get_mut(..prefix.len())
        .ok_or(io::ErrorKind::InvalidInput)?
        .copy_from_slice(prefix);
    output.truncate(prefix.len() + length + 1 - skip);
    if let Some(meter) = meter {
        let copied = (length + 1 - skip + prefix.len()) * 2;
        meter.record_codec(0, copied as u64, 0)?;
    }
    Ok((output, allocation))
}
