use super::*;
use crate::native_host::NativeHostWork;

#[cfg(windows)]
pub(super) const ENTRY_WITNESS: &[u8; 32] = b"acyclic-entry-exchange-v2\0\0\0\0\0\0\0";

#[cfg(windows)]
pub(super) fn write_all<W: NativeHostWork>(
    file: &mut std::fs::File, mut bytes: &[u8], work: &mut W,
) -> Result<(), W::Error> {
    use std::io::Write as _;
    while !bytes.is_empty() {
        let maximum = crate::WorkCounters {
            backend_write_operations: 1, output_bytes: bytes.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(maximum)?;
        work.record(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED })?;
        match file.write(bytes) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
            Ok(length) => {
                work.record(crate::WorkCounters { output_bytes: length as u64, ..crate::WorkCounters::UNCHARGED })?;
                bytes = bytes.get(length..).ok_or_else(|| std::io::Error::other("invalid native write length"))?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
#[cfg(windows)]
pub(super) fn read_exact<W: NativeHostWork>(
    file: &mut std::fs::File, mut bytes: &mut [u8], work: &mut W,
) -> Result<(), W::Error> {
    use std::io::Read as _;
    while !bytes.is_empty() {
        work.admit(crate::WorkCounters {
            backend_read_operations: 1, source_bytes_read: bytes.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        work.record(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED })?;
        match file.read(bytes) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()),
            Ok(length) => {
                work.record(crate::WorkCounters { source_bytes_read: length as u64, ..crate::WorkCounters::UNCHARGED })?;
                bytes = bytes.get_mut(length..).ok_or_else(|| std::io::Error::other("invalid native read length"))?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
#[cfg(windows)]
pub(super) fn write_identity<W: NativeHostWork>(
    file: &mut std::fs::File, identity: crate::NativeRootIdentity, work: &mut W,
) -> Result<(), W::Error> {
    let delta = crate::WorkCounters { bytes_encoded: 16, ..crate::WorkCounters::UNCHARGED };
    work.admit(delta)?;
    let bytes = identity.to_bytes();
    work.record(delta)?;
    write_all(file, &bytes, work)
}
#[cfg(windows)]
pub(super) fn read_identity<W: NativeHostWork>(
    file: &mut std::fs::File, work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
    let mut bytes = [0u8; 16];
    read_exact(file, &mut bytes, work)?;
    Ok(crate::NativeRootIdentity::from_bytes(bytes))
}
#[cfg(windows)]
pub(super) fn open_witness<W: NativeHostWork>(
    directory: &cap_std::fs::Dir, name: &Path, create: bool, work: &mut W,
) -> Result<std::fs::File, W::Error> {
    with_wide_leaf(name, work, |name, work| {
        if create {
            crate::native_host::create_windows_provisional_file_with_work(directory, name, work)
        } else {
            crate::native_host::open_windows_leaf_with_options_with_work(directory, name,
                &crate::native_host::NativeOpenOptions::Read, work)
        }
    })
}
#[cfg(windows)]
pub(super) fn with_service_name<W: NativeHostWork, T>(
    name: &Path, suffix: &str, work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    let bytes = name.as_os_str().as_encoded_bytes().len()
        .checked_add(suffix.len()).and_then(|bytes| bytes.checked_add(1))
        .ok_or(crate::WorkError::Overflow)?;
    let allocation = work.claim(bytes as u64)?;
    let result = (|| {
        work.admit(crate::WorkCounters { bytes_copied: bytes as u64, ..crate::WorkCounters::UNCHARGED })?;
        let mut service_name = std::ffi::OsString::new();
        service_name.try_reserve_exact(bytes).map_err(std::io::Error::other)?;
        service_name.push(".");
        service_name.push(name);
        service_name.push(suffix);
        work.record(crate::WorkCounters { bytes_copied: bytes as u64, ..crate::WorkCounters::UNCHARGED })?;
        operation(Path::new(&service_name), work)
    })();
    work.release(allocation)?;
    result
}
#[cfg(windows)]
pub(super) fn with_wide_leaf<W: NativeHostWork, T>(
    name: &Path, work: &mut W,
    operation: impl FnOnce(&[u16], &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut length = 0usize;
    for _ in name.as_os_str().encode_wide() {
        work.admit(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        work.record(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    let bytes = length.checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64;
    let allocation = work.claim(bytes)?;
    let result = (|| {
        work.admit(crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::UNCHARGED })?;
        let mut units = Vec::new();
        units.try_reserve_exact(length).map_err(std::io::Error::other)?;
        units.extend(name.as_os_str().encode_wide());
        work.record(crate::WorkCounters { bytes_encoded: bytes, ..crate::WorkCounters::UNCHARGED })?;
        operation(&units, work)
    })();
    work.release(allocation)?;
    result
}
#[cfg(windows)]
pub(super) fn write_native_path<W: NativeHostWork>(
    file: &mut std::fs::File, path: &Path, work: &mut W,
) -> Result<(), W::Error> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut units = 0u64;
    for _ in path.as_os_str().encode_wide() {
        work.admit(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        work.record(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        units = units.checked_add(1).ok_or(crate::WorkError::Overflow)?;
    }
    let delta = crate::WorkCounters { bytes_encoded: 8, ..crate::WorkCounters::UNCHARGED };
    work.admit(delta)?;
    let encoded_length = units.to_le_bytes();
    work.record(delta)?;
    write_all(file, &encoded_length, work)?;
    let bytes = units.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let allocation = work.claim(bytes)?;
    let result = (|| {
        let mut buffered = Vec::new();
        buffered.try_reserve_exact(usize::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?)
            .map_err(std::io::Error::other)?;
        for unit in path.as_os_str().encode_wide() {
            work.admit(crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED })?;
            let encoded = unit.to_le_bytes();
            buffered.extend_from_slice(&encoded);
            work.record(crate::WorkCounters { bytes_encoded: 2, ..crate::WorkCounters::UNCHARGED })?;
        }
        write_all(file, &buffered, work)
    })();
    work.release(allocation)?;
    result
}
#[cfg(windows)]
pub(super) fn with_read_native_path<W: NativeHostWork, T>(
    file: &mut std::fs::File, remaining: &mut u64, work: &mut W,
    operation: impl FnOnce(&Path, &mut std::fs::File, &mut u64, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error>
where W::Error: From<NativeExchangeError> {
    if *remaining < 8 { return Err(NativeExchangeError::IncompatibleJournal.into()); }
    let mut encoded_length = [0u8; 8];
    read_exact(file, &mut encoded_length, work)?;
    let length = u64::from_le_bytes(encoded_length);
    let bytes = length.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    let consumed = bytes.checked_add(8).ok_or(crate::WorkError::Overflow)?;
    if consumed > *remaining { return Err(NativeExchangeError::IncompatibleJournal.into()); }
    let allocation = work.claim(bytes)?;
    let result = (|| {
        let length = usize::try_from(length).map_err(|_| crate::WorkError::Overflow)?;
        let mut units = Vec::new();
        units.try_reserve_exact(length).map_err(std::io::Error::other)?;
        units.resize(length, 0u16);
        read_exact(file, utf16_bytes_mut(&mut units), work)?;
        #[cfg(target_endian = "big")]
        for unit in &mut units { *unit = u16::from_le(*unit); }
        *remaining -= consumed;
        with_windows_path(&units, work, |path, work| operation(path, file, remaining, work))
    })();
    work.release(allocation)?;
    result
}
#[cfg(windows)]
#[allow(unsafe_code, reason = "initialized u16 storage has no padding and may be filled through its byte view")]
pub(super) fn utf16_bytes_mut(units: &mut [u16]) -> &mut [u8] {
    // SAFETY: a valid u16 slice owns exactly two initialized bytes per element.
    // Every possible result of the read is a valid u16 value, and the exclusive
    // borrow prevents simultaneous access through the original typed slice.
    unsafe { std::slice::from_raw_parts_mut(units.as_mut_ptr().cast::<u8>(), units.len() * 2) }
}
#[cfg(windows)]
pub(super) fn with_windows_path<W: NativeHostWork, T>(
    units: &[u16], work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::ffi::OsStringExt as _;
    let logical_max = units.len().checked_mul(3).ok_or(crate::WorkError::Overflow)?;
    let mut allocation = work.reserve_opaque_decode(logical_max as u64)?;
    let result = (|| {
        work.admit(crate::WorkCounters {
            bytes_encoded: logical_max as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let path = PathBuf::from(std::ffi::OsString::from_wide(units));
        let encoded = path.as_os_str().as_encoded_bytes().len() as u64;
        let reconciled = work.reconcile_allocation(&mut allocation, path.capacity() as u64);
        let recorded = work.record(crate::WorkCounters {
            bytes_encoded: encoded,
            ..crate::WorkCounters::UNCHARGED
        });
        reconciled?;
        recorded?;
        operation(&path, work)
    })();
    work.release(allocation)?;
    result
}
#[cfg(windows)]
#[allow(unsafe_code, reason = "reads the physical path from a live held directory handle")]
pub(super) fn with_directory_path<W: NativeHostWork, T>(
    directory: &cap_std::fs::Dir, work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use ::windows::Win32::Foundation::HANDLE;
    use ::windows::Win32::Storage::FileSystem::{GetFinalPathNameByHandleW, FILE_NAME_NORMALIZED};
    let handle = HANDLE(directory.as_raw_handle());
    let mut capacity = attempted(work, crate::WorkCounters {
        backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
    }, || {
        // SAFETY: zero-length output probes the required path capacity.
        let length = unsafe { GetFinalPathNameByHandleW(handle, &mut [], FILE_NAME_NORMALIZED) };
        if length == 0 { Err(std::io::Error::last_os_error()) } else { Ok(length) }
    })?;
    let mut operation = Some(operation);
    loop {
        let allocation = work.claim(u64::from(capacity).checked_mul(2).ok_or(crate::WorkError::Overflow)?)?;
        let result = (|| {
            let mut units = Vec::new();
            units.try_reserve_exact(capacity as usize).map_err(std::io::Error::other)?;
            units.resize(capacity as usize, 0u16);
            let length = attempted(work, crate::WorkCounters {
                backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
            }, || {
                // SAFETY: the slice is writable for its advertised capacity.
                let length = unsafe { GetFinalPathNameByHandleW(handle, &mut units, FILE_NAME_NORMALIZED) };
                if length == 0 { Err(std::io::Error::last_os_error()) } else { Ok(length) }
            })?;
            if length >= capacity { return Ok(Err(length.checked_add(1).ok_or(crate::WorkError::Overflow)?)); }
            units.truncate(length as usize);
            let operation = operation.take().ok_or_else(|| std::io::Error::other("path witness consumed twice"))?;
            with_windows_path(&units, work, operation).map(Ok)
        })();
        work.release(allocation)?;
        match result? {
            Ok(value) => return Ok(value),
            Err(next) => capacity = next,
        }
    }
}
