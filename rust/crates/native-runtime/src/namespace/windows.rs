//! Normalize DOS paths before entering Win32 namespace publication APIs.
use super::*;

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
