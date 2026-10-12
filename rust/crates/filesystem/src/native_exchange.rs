//! Crash-recoverable whole-tree publication for native consumers.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

use acyclic_native_runtime::durable_rename;

use crate::native_host::NativeHostWork;

impl From<crate::demand::DemandError> for NativeExchangeError {
    fn from(error: crate::demand::DemandError) -> Self {
        match error {
            crate::demand::DemandError::Cancelled => Self::Cancelled,
            crate::demand::DemandError::Work(error) => Self::Work(error),
            crate::demand::DemandError::Io(error) => exchange_io_error(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

impl From<crate::materializer::NativeTreeMaterializationError> for NativeExchangeError {
    fn from(error: crate::materializer::NativeTreeMaterializationError) -> Self {
        match error {
            crate::materializer::NativeTreeMaterializationError::Cancelled => Self::Cancelled,
            crate::materializer::NativeTreeMaterializationError::Work(error) => Self::Work(error),
            crate::materializer::NativeTreeMaterializationError::Io(error) => exchange_io_error(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

impl From<NativeExchangeError> for crate::demand::DemandError {
    fn from(error: NativeExchangeError) -> Self {
        match error {
            NativeExchangeError::Cancelled => Self::Cancelled,
            NativeExchangeError::Work(error) => Self::Work(error),
            NativeExchangeError::Io(error) => Self::Io(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

impl From<NativeExchangeError> for crate::materializer::NativeTreeMaterializationError {
    fn from(error: NativeExchangeError) -> Self {
        match error {
            NativeExchangeError::Cancelled => Self::Cancelled,
            NativeExchangeError::Work(error) => Self::Work(error),
            NativeExchangeError::Io(error) => Self::Io(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

fn exchange_io_error(error: std::io::Error) -> NativeExchangeError {
    if error.get_ref().is_some_and(|inner| inner.is::<NativeExchangeError>()) {
        match error.into_inner() {
            Some(inner) => match inner.downcast::<NativeExchangeError>() {
                Ok(error) => *error,
                Err(inner) => NativeExchangeError::Io(std::io::Error::other(inner)),
            },
            None => NativeExchangeError::Io(std::io::Error::other("exchange error lost its payload")),
        }
    } else {
        NativeExchangeError::Io(error)
    }
}

fn attempted<W: NativeHostWork, T>(
    work: &mut W,
    delta: crate::WorkCounters,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> Result<T, W::Error> {
    work.admit(delta)?;
    // The noncancellable fixed charge immediately precedes the actual call;
    // cancellation arriving during it cannot erase the attempted operation.
    work.record(delta)?;
    operation().map_err(Into::into)
}

fn held_identity<W: NativeHostWork>(
    parent: &cap_std::fs::Dir,
    name: &Path,
    work: &mut W,
) -> Result<Option<crate::NativeRootIdentity>, W::Error> {
    crate::native_host::entry_identity(parent, name, work)
}

/// Exchanges entries through their original held parents and the existing scratch/witness protocol.
pub(crate) fn exchange_native_entries_held_measured<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error>
where W::Error: From<NativeExchangeError> {
    validate_leaf(left_name).map_err(W::Error::from)?;
    validate_leaf(right_name).map_err(W::Error::from)?;
    #[cfg(unix)]
    {
        let roots = [
            held_identity(left, left_name, work)?.ok_or(NativeExchangeError::InvalidLayout)?,
            held_identity(right, right_name, work)?.ok_or(NativeExchangeError::InvalidLayout)?,
        ];
        exchange_kernel_held(left, left_name, right, right_name, work)?;
        if held_identity(left, left_name, work)? != Some(roots[1])
            || held_identity(right, right_name, work)? != Some(roots[0])
        {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        crate::native_host::sync_directory_held(left, work)?;
        crate::native_host::sync_directory_held(right, work)?;
        Ok(())
    }
    #[cfg(windows)]
    exchange_windows_held(left, left_name, right, right_name, work)
}

fn validate_leaf(path: &Path) -> Result<(), NativeExchangeError> {
    let mut components = path.components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(NativeExchangeError::InvalidLayout);
    }
    Ok(())
}

#[cfg(unix)]
#[allow(unsafe_code, reason = "atomic exchange uses live held parents and single native leaf names")]
fn exchange_kernel_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir, left_name: &Path,
    right: &cap_std::fs::Dir, right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error> {
    use std::os::fd::AsRawFd as _;
    crate::native_host::unix_name(left_name.as_os_str(), work, |left_name, work| {
        crate::native_host::unix_name(right_name.as_os_str(), work, |right_name, work| {
            attempted(work, crate::WorkCounters {
                backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED
            }, || {
                // SAFETY: held descriptors and NUL-terminated leaf names remain
                // live for the whole operation; neither parent is reconstructed.
                #[cfg(target_os = "linux")]
                let result = unsafe { libc::syscall(libc::SYS_renameat2,
                    left.as_raw_fd(), left_name.as_ptr(), right.as_raw_fd(),
                    right_name.as_ptr(), libc::RENAME_EXCHANGE) };
                #[cfg(target_os = "macos")]
                let result = unsafe { libc::renameatx_np(left.as_raw_fd(), left_name.as_ptr(),
                    right.as_raw_fd(), right_name.as_ptr(), libc::RENAME_SWAP) } as libc::c_long;
                if result == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
            })
        })
    })
}

#[cfg(windows)]
fn write_all_measured<W: NativeHostWork>(
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
fn read_exact_measured<W: NativeHostWork>(
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
fn write_identity_measured<W: NativeHostWork>(
    file: &mut std::fs::File, identity: crate::NativeRootIdentity, work: &mut W,
) -> Result<(), W::Error> {
    let delta = crate::WorkCounters { bytes_encoded: 16, ..crate::WorkCounters::UNCHARGED };
    work.admit(delta)?;
    let bytes = identity.to_bytes();
    work.record(delta)?;
    write_all_measured(file, &bytes, work)
}

#[cfg(windows)]
fn read_identity_measured<W: NativeHostWork>(
    file: &mut std::fs::File, work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
    let mut bytes = [0u8; 16];
    read_exact_measured(file, &mut bytes, work)?;
    Ok(crate::NativeRootIdentity::from_bytes(bytes))
}

#[cfg(windows)]
fn open_witness<W: NativeHostWork>(
    directory: &cap_std::fs::Dir, name: &Path, create: bool, work: &mut W,
) -> Result<std::fs::File, W::Error> {
    with_wide_leaf(name, work, |name, work| {
        if create {
            crate::native_host::create_windows_provisional_file(directory, name, work)
        } else {
            crate::native_host::open_windows_leaf_with_options(directory, name,
                &crate::native_host::NativeOpenOptions::Read, work)
        }
    })
}

#[cfg(windows)]
const MEASURED_ENTRY_WITNESS: &[u8; 32] = b"acyclic-entry-exchange-v2\0\0\0\0\0\0\0";

#[cfg(windows)]
fn with_service_name<W: NativeHostWork, T>(
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
fn with_wide_leaf<W: NativeHostWork, T>(
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
fn write_native_path<W: NativeHostWork>(
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
    write_all_measured(file, &encoded_length, work)?;
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
        write_all_measured(file, &buffered, work)
    })();
    work.release(allocation)?;
    result
}

#[cfg(windows)]
fn with_read_native_path<W: NativeHostWork, T>(
    file: &mut std::fs::File, remaining: &mut u64, work: &mut W,
    operation: impl FnOnce(&Path, &mut std::fs::File, &mut u64, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error>
where W::Error: From<NativeExchangeError> {
    if *remaining < 8 { return Err(NativeExchangeError::IncompatibleJournal.into()); }
    let mut encoded_length = [0u8; 8];
    read_exact_measured(file, &mut encoded_length, work)?;
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
        read_exact_measured(file, utf16_bytes_mut(&mut units), work)?;
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
fn utf16_bytes_mut(units: &mut [u16]) -> &mut [u8] {
    // SAFETY: a valid u16 slice owns exactly two initialized bytes per element.
    // Every possible result of the read is a valid u16 value, and the exclusive
    // borrow prevents simultaneous access through the original typed slice.
    unsafe { std::slice::from_raw_parts_mut(units.as_mut_ptr().cast::<u8>(), units.len() * 2) }
}

#[cfg(windows)]
fn with_windows_path<W: NativeHostWork, T>(
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
fn with_directory_path<W: NativeHostWork, T>(
    directory: &cap_std::fs::Dir, work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{GetFinalPathNameByHandleW, FILE_NAME_NORMALIZED};
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

#[cfg(windows)]
fn parent_identity<W: NativeHostWork>(
    directory: &cap_std::fs::Dir, work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
    crate::native_host::directory_identity(directory, work)
}

#[cfg(windows)]
fn rename_durable_held<W: NativeHostWork>(
    from: &cap_std::fs::Dir, from_name: &Path,
    to: &cap_std::fs::Dir, to_name: &Path, expected: crate::NativeRootIdentity, work: &mut W,
) -> Result<(), W::Error> {
    crate::native_host::rename_no_replace_held_if_identity(from, from_name, to, to_name,
        expected, work)?;
    crate::native_host::sync_directory_held(from, work)?;
    crate::native_host::sync_directory_held(to, work)
}

#[cfg(windows)]
fn remove_owned_witness<W: NativeHostWork>(
    parent: &cap_std::fs::Dir, name: &Path, identity: crate::NativeRootIdentity, work: &mut W,
) -> Result<(), W::Error>
where W::Error: From<NativeExchangeError> {
    if held_identity(parent, name, work)? != Some(identity) {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    with_wide_leaf(name, work, |name, work|
        crate::native_host::remove_windows_leaf_if_identity(parent, name, false,
            identity, work))?;
    crate::native_host::sync_directory_held(parent, work)
}

#[cfg(windows)]
fn create_measured_entry_witness<W: NativeHostWork>(
    left: &cap_std::fs::Dir, left_name: &Path,
    right: &cap_std::fs::Dir, right_name: &Path,
    witness_name: &Path, roots: [crate::NativeRootIdentity; 2], work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
    // The newly and exclusively created handle remains delete-on-close until
    // every witness byte is durable. No failed prefix can become restart authority.
    let mut file = open_witness(left, witness_name, true, work)?;
    write_all_measured(&mut file, MEASURED_ENTRY_WITNESS, work)?;
    write_identity_measured(&mut file, parent_identity(left, work)?, work)?;
    write_identity_measured(&mut file, parent_identity(right, work)?, work)?;
    for identity in roots { write_identity_measured(&mut file, identity, work)?; }
    with_directory_path(left, work, |path, work| write_native_path(&mut file, path, work))?;
    write_native_path(&mut file, left_name, work)?;
    with_directory_path(right, work, |path, work| write_native_path(&mut file, path, work))?;
    write_native_path(&mut file, right_name, work)?;
    attempted(work, crate::WorkCounters {
        durability_operations: 1, ..crate::WorkCounters::UNCHARGED
    }, || file.sync_all())?;
    crate::native_host::keep_windows_created_file(&file, work)?;
    crate::native_host::sync_directory_held(left, work)?;
    attempted(work, crate::WorkCounters {
        backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED
    }, || crate::NativeRootIdentity::from_file(&file))
}

#[cfg(windows)]
fn exchange_windows_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir, left_name: &Path,
    right: &cap_std::fs::Dir, right_name: &Path, work: &mut W,
) -> Result<(), W::Error>
where W::Error: From<NativeExchangeError> {
    with_service_name(left_name, ".acyclic-exchange", work, |scratch_name, work| {
        with_service_name(left_name, ".acyclic-exchange-witness", work, |witness_name, work| {
            if held_identity(left, witness_name, work)?.is_some()
                && recover_entry_held(left, left_name, scratch_name, witness_name,
                    Some((right, right_name)), work)?
            {
                // A surviving completed witness acknowledges its original
                // decision; a recovered rollback proceeds with this request.
                return Ok(());
            }
            if held_identity(left, scratch_name, work)?.is_some() {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            let roots = [
                held_identity(left, left_name, work)?.ok_or(NativeExchangeError::InvalidLayout)?,
                held_identity(right, right_name, work)?.ok_or(NativeExchangeError::InvalidLayout)?,
            ];
            let witness = create_measured_entry_witness(left, left_name, right, right_name,
                witness_name, roots, work)?;
            // Revalidate the original physical preimages at each mutation;
            // replacements never become newly adopted durable authority.
            if held_identity(left, left_name, work)? != Some(roots[0])
                || held_identity(right, right_name, work)? != Some(roots[1])
                || held_identity(left, witness_name, work)? != Some(witness)
            {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            rename_durable_held(left, left_name, left, scratch_name, roots[0], work)?;
            if held_identity(right, right_name, work)? != Some(roots[1]) {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            rename_durable_held(right, right_name, left, left_name, roots[1], work)?;
            if held_identity(left, scratch_name, work)? != Some(roots[0]) {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            rename_durable_held(left, scratch_name, right, right_name, roots[0], work)?;
            remove_owned_witness(left, witness_name, witness, work)
        })
    })
}

#[cfg(windows)]
fn recover_entry_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir, left_name: &Path,
    scratch_name: &Path, witness_name: &Path,
    requested_right: Option<(&cap_std::fs::Dir, &Path)>, work: &mut W,
) -> Result<bool, W::Error>
where W::Error: From<NativeExchangeError> {
    let mut file = open_witness(left, witness_name, false, work)?;
    let (witness_identity, witness_length) =
        crate::native_host::file_identity_and_length(&file, work)?;
    let mut magic = [0u8; 32];
    read_exact_measured(&mut file, &mut magic, work)?;
    if &magic != MEASURED_ENTRY_WITNESS {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let parents = [read_identity_measured(&mut file, work)?, read_identity_measured(&mut file, work)?];
    let roots = [read_identity_measured(&mut file, work)?, read_identity_measured(&mut file, work)?];
    if parent_identity(left, work)? != parents[0] {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let mut remaining = witness_length.checked_sub(96).ok_or(NativeExchangeError::IncompatibleJournal)?;
    let published = with_read_native_path(&mut file, &mut remaining, work, |left_parent_path, file, remaining, work| {
        if !left_parent_path.is_absolute() { return Err(NativeExchangeError::IncompatibleJournal.into()); }
        let named_left = crate::native_host::open_root_directory(left_parent_path, work).map(|(directory, _)| directory)?;
        if parent_identity(&named_left, work)? != parents[0] {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        with_read_native_path(file, remaining, work, |original_name, file, remaining, work| {
        if original_name != left_name { return Err(NativeExchangeError::IncompatibleJournal.into()); }
        with_read_native_path(file, remaining, work, |right_parent_path, file, remaining, work| {
            if !right_parent_path.is_absolute() { return Err(NativeExchangeError::IncompatibleJournal.into()); }
            let right = crate::native_host::open_root_directory(right_parent_path, work).map(|(directory, _)| directory)?;
            if parent_identity(&right, work)? != parents[1] {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            with_read_native_path(file, remaining, work, |right_name, file, remaining, work| {
                validate_leaf(right_name).map_err(W::Error::from)?;
                if let Some((requested_parent, requested_name)) = requested_right {
                    if requested_name != right_name || parent_identity(requested_parent, work)? != parents[1] {
                        return Err(NativeExchangeError::IncompatibleJournal.into());
                    }
                }
                let (identity, length) = crate::native_host::file_identity_and_length(file, work)?;
                if *remaining != 0 || length != witness_length || identity != witness_identity {
                    return Err(NativeExchangeError::IncompatibleJournal.into());
                }
                let current_left = held_identity(left, left_name, work)?;
                let current_right = held_identity(&right, right_name, work)?;
                let current_scratch = held_identity(left, scratch_name, work)?;
                let published = match (current_left, current_right, current_scratch) {
                    (Some(original), Some(replacement), None)
                        if [original, replacement] == roots => false,
                    (None, Some(replacement), Some(original))
                        if [original, replacement] == roots => {
                            rename_durable_held(left, scratch_name, left, left_name, roots[0], work)?;
                            false
                        }
                    (Some(replacement), None, Some(original))
                        if [original, replacement] == roots => {
                            rename_durable_held(left, scratch_name, &right, right_name, roots[0], work)?;
                            true
                        }
                    (Some(replacement), Some(original), None)
                        if [original, replacement] == roots => true,
                    _ => return Err(NativeExchangeError::IncompatibleJournal.into()),
                };
                Ok(published)
            })
        })
        })
    })?;
    // On Windows the named deletion is complete only after the reader releases
    // its share-delete handle. The subsequent barrier covers actual cleanup.
    drop(file);
    remove_owned_witness(left, witness_name, witness_identity, work)?;
    Ok(published)
}

/// Recovers only the physical preimages named by the surviving entry witness.
pub(crate) fn recover_native_entry_exchange_held_measured<W: NativeHostWork>(
    parent: &cap_std::fs::Dir, name: &Path, work: &mut W,
) -> Result<(), W::Error>
where W::Error: From<NativeExchangeError> {
    validate_leaf(name).map_err(W::Error::from)?;
    #[cfg(windows)]
    {
        with_service_name(name, ".acyclic-exchange", work, |scratch_name, work|
            with_service_name(name, ".acyclic-exchange-witness", work, |witness_name, work| {
                if held_identity(parent, witness_name, work)?.is_none() {
                    if held_identity(parent, scratch_name, work)?.is_some() {
                        return Err(NativeExchangeError::IncompatibleJournal.into());
                    }
                    return Ok(());
                }
                recover_entry_held(parent, name, scratch_name, witness_name, None, work).map(|_| ())
            }))
    }
    #[cfg(unix)]
    {
        let _ = (parent, work);
        Ok(())
    }
}

/// Bounded native entry exchange retaining the complete attempted prefix.
#[cfg(feature = "native-watch")]
pub fn exchange_native_entries_with_budget(
    left: &Path, right: &Path, budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<()>, (NativeExchangeError, crate::WorkCounters)> {
    let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| (NativeExchangeError::from(cause), work))?;
    exchange_native_entries_measured(left, right, &work)
        .map(|value| crate::OperationReceipt { value, work: work.receipt() })
        .map_err(|error| (error.into(), work.receipt()))
}

/// Uses the caller's operation-wide ledger rather than starting a nested meter.
#[cfg(feature = "native-watch")]
pub(crate) fn exchange_native_entries_measured(
    left: &Path, right: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    if left == right { return Err(NativeExchangeError::InvalidLayout.into()); }
    let left_parent = left.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let right_parent = right.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let left_name = left.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    let right_name = right.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    let mut meter = work;
    let left = crate::native_host::open_root_directory(left_parent, &mut meter).map(|(directory, _)| directory)?;
    let right = crate::native_host::open_root_directory(right_parent, &mut meter).map(|(directory, _)| directory)?;
    exchange_native_entries_held_measured(&left, Path::new(left_name), &right, Path::new(right_name), &mut meter)
}

#[cfg(feature = "native-watch")]
pub(crate) fn recover_native_entry_exchange_measured(
    live: &Path, work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let name = live.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    let mut meter = work;
    let parent = crate::native_host::open_root_directory(parent, &mut meter).map(|(directory, _)| directory)?;
    recover_native_entry_exchange_held_measured(&parent, Path::new(name), &mut meter)
}

/// Bounded restart recovery using the existing entry exchange witness.
#[cfg(feature = "native-watch")]
pub fn recover_native_entry_exchange_with_budget(
    live: &Path, budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<()>, (NativeExchangeError, crate::WorkCounters)> {
    let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| (NativeExchangeError::from(cause), work))?;
    recover_native_entry_exchange_measured(live, &work)
        .map(|value| crate::OperationReceipt { value, work: work.receipt() })
        .map_err(|error| (error.into(), work.receipt()))
}

const NATIVE_EXCHANGE_JOURNAL_VERSION: u32 = 1;

/// Durable whole-tree exchange phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeExchangePhase {
    /// Excluded paths are moving from the live tree into the prepared tree.
    Carrying,
    /// Whole-tree exchange is in flight.
    Exchanging,
}

/// Complete recovery state for one whole-tree publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeExchangeJournal {
    /// Version of the durable journal encoding.
    pub version: u32,
    /// Stable identity of the caller-visible publication request.
    pub operation: crate::IdempotencyKey,
    /// Opaque caller recovery bytes retained with the durable request.
    pub recovery: Vec<u8>,
    /// Current live root.
    pub live: PathBuf,
    /// Prepared tree before publication and displaced tree after publication.
    pub prepared: PathBuf,
    /// Exact relative paths carried from the live tree.
    pub carried: Vec<PathBuf>,
    /// Stable identities of the live and prepared roots before exchange.
    pub roots: [[u8; 16]; 2],
    /// Current durable phase.
    pub phase: NativeExchangePhase,
}

/// Result of a successful or recovered exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeExchangeOutcome {
    /// Whether the prepared tree was published.
    pub published: bool,
    /// Complete displaced tree when publication completed.
    pub displaced: Option<PathBuf>,
    /// Opaque consumer state stored with the recovered publication request.
    pub recovery: Vec<u8>,
}

/// Persists an exchange request before a caller changes any related durable state.
pub fn prepare_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
) -> Result<(), NativeExchangeError> {
    prepare_native_exchange_with_recovery(
        journal_path,
        live,
        prepared,
        operation,
        carried,
        Vec::new(),
    )
}

/// Persists an exchange request with opaque bytes needed by consumer recovery.
pub fn prepare_native_exchange_with_recovery(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
    recovery: Vec<u8>,
) -> Result<(), NativeExchangeError> {
    validate_layout(live, prepared)?;
    validate_carried_paths(&carried)?;
    let journal = NativeExchangeJournal {
        version: NATIVE_EXCHANGE_JOURNAL_VERSION,
        operation,
        recovery,
        live: live.to_path_buf(),
        prepared: prepared.to_path_buf(),
        carried,
        roots: [root_identity(live)?, root_identity(prepared)?],
        phase: NativeExchangePhase::Carrying,
    };
    match read_journal(journal_path) {
        Ok(existing) if same_prepared_request(&existing, &journal) => Ok(()),
        Ok(_) => Err(NativeExchangeError::IncompatibleJournal),
        Err(NativeExchangeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            write_journal(journal_path, &journal)
        }
        Err(error) => Err(error),
    }
}

/// Whole-tree exchange failure.
#[derive(Debug, Error)]
pub enum NativeExchangeError {
    /// Paths are not two distinct sibling directories.
    #[error("native exchange paths must be distinct sibling directories")]
    InvalidLayout,
    /// Durable journal is malformed or does not match its requested roots.
    #[error("native exchange journal is incompatible")]
    IncompatibleJournal,
    /// Durable journal was written by an unsupported format version.
    #[error("native exchange journal version {0} is unsupported")]
    UnsupportedJournalVersion(u32),
    /// Both copies of one carried path exist, so recovery cannot discard either.
    #[error("both copies of carried path exist: {0}")]
    CarriedPathConflict(PathBuf),
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Journal serialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Cancellation stopped the next native boundary.
    #[error("native exchange cancelled")]
    Cancelled,
    /// The next actual operation exceeded its finite allowance.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
}

/// Exchanges a prepared sibling tree with a live root.
pub fn publish_native_exchange(
    journal_path: &Path,
    live: &Path,
    prepared: &Path,
    operation: crate::IdempotencyKey,
    carried: Vec<PathBuf>,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    validate_layout(live, prepared)?;
    if journal_path.exists() {
        let journal = read_journal(journal_path)?;
        if journal.live != live || journal.prepared != prepared {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        if journal.operation != operation || journal.carried != carried {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
        let recovered = recover_native_exchange(journal_path)?;
        if recovered.published {
            return Ok(recovered);
        }
    }
    prepare_native_exchange(journal_path, live, prepared, operation, carried)?;
    let mut journal = read_journal(journal_path)?;
    if let Err(error) = carry(live, prepared, &journal.carried) {
        move_back(prepared, live, &journal.carried)?;
        remove_journal(journal_path)?;
        return Err(error);
    }
    journal.phase = NativeExchangePhase::Exchanging;
    write_journal(journal_path, &journal)?;
    if let Err(error) = exchange(live, prepared) {
        return match recover_native_exchange(journal_path) {
            Ok(outcome) if outcome.published => Ok(outcome),
            Ok(_) => Err(error),
            Err(recovery) => Err(recovery),
        };
    }
    remove_journal(journal_path)?;
    Ok(NativeExchangeOutcome {
        published: true,
        displaced: Some(prepared.to_path_buf()),
        recovery: journal.recovery,
    })
}

fn same_prepared_request(
    existing: &NativeExchangeJournal,
    requested: &NativeExchangeJournal,
) -> bool {
    existing.operation == requested.operation
        && existing.recovery == requested.recovery
        && existing.live == requested.live
        && existing.prepared == requested.prepared
        && existing.carried == requested.carried
        && existing.roots == requested.roots
        && existing.phase == requested.phase
}

/// Exchanges two existing sibling filesystem entries without flattening either entry's metadata, links, sparse allocation, or native name.
pub fn exchange_native_entries(left: &Path, right: &Path) -> Result<(), NativeExchangeError> {
    #[cfg(feature = "native-watch")]
    {
        exchange_native_entries_with_budget(left, right, crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()).map(|_| ()).map_err(|failure| failure.0)
    }
    #[cfg(not(feature = "native-watch"))]
    {
        if left == right || left.parent().is_none() || right.parent().is_none() {
            return Err(NativeExchangeError::InvalidLayout);
        }
        if !entry_exists(left)? || !entry_exists(right)? {
            return Err(NativeExchangeError::InvalidLayout);
        }
        exchange(left, right)?;
        sync_parent(left)?;
        if left.parent() != right.parent() { sync_parent(right)?; }
        Ok(())
    }
}

/// Resolves the durable Windows three-rename entry exchange.
#[cfg(windows)]
pub(crate) fn recover_native_entry_exchange(live: &Path) -> Result<(), NativeExchangeError> {
    #[cfg(feature = "native-watch")]
    {
        let work = crate::materializer::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(cause, _)| NativeExchangeError::from(cause))?;
        return recover_native_entry_exchange_measured(live, &work).map_err(Into::into);
    }
    #[cfg(all(windows, not(feature = "native-watch")))]
    {
        let scratch = scratch(live)?;
        let witness = entry_exchange_witness(live)?;
        if !entry_exists(&witness)? {
            if entry_exists(&scratch)? {
                return Err(NativeExchangeError::IncompatibleJournal);
            }
            return Ok(());
        }
        validate_entry_exchange_witness(&witness)?;
        if entry_exists(&scratch)? && entry_exists(live)? {
            remove_entry(&scratch)?;
            sync_parent(live)?;
        } else if entry_exists(&scratch)? {
            durable_rename(
                &scratch,
                live,
                acyclic_native_runtime::RenameMode::NoReplace, None,
            )?;
        }
        remove_entry_exchange_witness(&witness)?;
    }
    #[cfg(not(feature = "native-watch"))]
    Ok(())
}

/// Recovers one interrupted whole-tree publication.
pub fn recover_native_exchange(
    journal_path: &Path,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let journal = read_journal(journal_path)?;
    validate_layout(&journal.live, &journal.prepared)?;
    match journal.phase {
        NativeExchangePhase::Carrying => {
            if [
                root_identity(&journal.live)?,
                root_identity(&journal.prepared)?,
            ] != journal.roots
            {
                return Err(NativeExchangeError::IncompatibleJournal);
            }
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            remove_journal(journal_path)?;
            Ok(NativeExchangeOutcome {
                published: false,
                displaced: None,
                recovery: journal.recovery,
            })
        }
        NativeExchangePhase::Exchanging => recover_exchange(journal_path, &journal),
    }
}

fn read_journal(path: &Path) -> Result<NativeExchangeJournal, NativeExchangeError> {
    let journal: NativeExchangeJournal = serde_json::from_slice(&std::fs::read(path)?)?;
    match journal.version {
        NATIVE_EXCHANGE_JOURNAL_VERSION => {
            validate_carried_paths(&journal.carried)?;
            Ok(journal)
        }
        version => Err(NativeExchangeError::UnsupportedJournalVersion(version)),
    }
}

fn validate_layout(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    if live == prepared || live.parent().is_none() || live.parent() != prepared.parent() {
        return Err(NativeExchangeError::InvalidLayout);
    }
    Ok(())
}

fn validate_carried_paths(relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(NativeExchangeError::IncompatibleJournal);
        }
    }
    Ok(())
}

fn carry(from: &Path, into: &Path, relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        let source = from.join(path);
        if !entry_exists(&source)? {
            continue;
        }
        let destination = into.join(path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        remove_entry(&destination)?;
        std::fs::rename(source, destination)?;
    }
    Ok(())
}

fn move_back(from: &Path, into: &Path, relative: &[PathBuf]) -> Result<(), NativeExchangeError> {
    for path in relative {
        let source = from.join(path);
        if !entry_exists(&source)? {
            continue;
        }
        let destination = into.join(path);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        #[cfg(test)]
        BEFORE_ROLLBACK_RENAME.with(|hook| {
            if let Some(barrier) = hook.take() {
                barrier.wait();
                barrier.wait();
            }
        });
        match durable_rename(
            &source,
            &destination,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        ) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(NativeExchangeError::CarriedPathConflict(path.clone()));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    // Schedule a real user binding at the rollback's final publication boundary.
    static BEFORE_ROLLBACK_RENAME: std::cell::RefCell<Option<std::sync::Arc<std::sync::Barrier>>> =
        const { std::cell::RefCell::new(None) };
}

fn write_journal(path: &Path, journal: &NativeExchangeJournal) -> Result<(), NativeExchangeError> {
    use std::io::Write;
    let temporary = path.with_extension("next");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(journal)?)?;
    file.sync_all()?;
    drop(file);
    durable_rename(
        &temporary,
        path,
        acyclic_native_runtime::RenameMode::Replace, None,
    )?;
    Ok(())
}

fn remove_journal(path: &Path) -> Result<(), NativeExchangeError> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path).map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn entry_exists(path: &Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn root_identity(path: &Path) -> Result<[u8; 16], NativeExchangeError> {
    let root = cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
    Ok(crate::NativeRootIdentity::from_metadata(&root.dir_metadata()?)?.to_bytes())
}

fn remove_entry(path: &Path) -> Result<(), std::io::Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("path has no parent"))?;
    acyclic_native_runtime::sync_parent(parent, acyclic_native_runtime::Durability::Full)
}

#[cfg(windows)]
fn scratch(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let name = live.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    Ok(parent.join(format!(".{}.acyclic-exchange", name.to_string_lossy())))
}

#[cfg(all(windows, not(feature = "native-watch")))]
const ENTRY_EXCHANGE_WITNESS: &[u8] = b"acyclic-native-entry-exchange-v1\n";

#[cfg(windows)]
fn entry_exchange_witness(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let name = live.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    Ok(parent.join(format!(
        ".{}.acyclic-exchange-witness",
        name.to_string_lossy()
    )))
}

#[cfg(all(windows, not(feature = "native-watch")))]
fn validate_entry_exchange_witness(path: &Path) -> Result<(), NativeExchangeError> {
    if std::fs::read(path)? == ENTRY_EXCHANGE_WITNESS {
        Ok(())
    } else {
        Err(NativeExchangeError::IncompatibleJournal)
    }
}

#[cfg(all(windows, not(feature = "native-watch")))]
fn create_entry_exchange_witness(path: &Path) -> Result<(), NativeExchangeError> {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(ENTRY_EXCHANGE_WITNESS)?;
    file.sync_all()?;
    sync_parent(path)?;
    Ok(())
}

#[cfg(all(windows, not(feature = "native-watch")))]
fn remove_entry_exchange_witness(path: &Path) -> Result<(), NativeExchangeError> {
    remove_entry(path)?;
    sync_parent(path)?;
    Ok(())
}

#[cfg(all(windows, not(feature = "native-watch")))]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    recover_native_entry_exchange(live)?;
    let scratch = scratch(live)?;
    let witness = entry_exchange_witness(live)?;
    create_entry_exchange_witness(&witness)?;
    durable_rename(
        live,
        &scratch,
        acyclic_native_runtime::RenameMode::NoReplace, None,
    )?;
    if let Err(error) = durable_rename(
        prepared,
        live,
        acyclic_native_runtime::RenameMode::NoReplace, None,
    ) {
        let _ = durable_rename(
            &scratch,
            live,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        );
        if entry_exists(live)? && !entry_exists(&scratch)? {
            remove_entry_exchange_witness(&witness)?;
        }
        return Err(error.into());
    }
    durable_rename(
        &scratch,
        prepared,
        acyclic_native_runtime::RenameMode::NoReplace, None,
    )?;
    remove_entry_exchange_witness(&witness)?;
    Ok(())
}

#[cfg(windows)]
fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let scratch = scratch(&journal.live)?;
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    let scratch_identity = entry_exists(&scratch)?
        .then(|| root_identity(&scratch))
        .transpose()?;
    let original = journal.roots[0];
    let replacement = journal.roots[1];
    let displaced = if live == Some(original)
        && prepared == Some(replacement)
        && scratch_identity.is_none()
    {
        move_back(&journal.prepared, &journal.live, &journal.carried)?;
        None
    } else if live.is_none() && prepared == Some(replacement) && scratch_identity == Some(original)
    {
        move_back(&journal.prepared, &scratch, &journal.carried)?;
        durable_rename(
            &scratch,
            &journal.live,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )?;
        None
    } else if live == Some(replacement) && prepared.is_none() && scratch_identity == Some(original)
    {
        durable_rename(
            &scratch,
            &journal.prepared,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )?;
        Some(journal.prepared.clone())
    } else if live == Some(replacement) && prepared == Some(original) && scratch_identity.is_none()
    {
        Some(journal.prepared.clone())
    } else if live == Some(replacement) && scratch_identity.is_none() {
        None
    } else {
        return Err(NativeExchangeError::IncompatibleJournal);
    };
    #[cfg(feature = "native-watch")]
    if entry_exists(&entry_exchange_witness(&journal.live)?)? {
        recover_native_entry_exchange(&journal.live)?;
    }
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published: live == Some(replacement),
        displaced,
        recovery: journal.recovery.clone(),
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn recover_exchange(
    path: &Path,
    journal: &NativeExchangeJournal,
) -> Result<NativeExchangeOutcome, NativeExchangeError> {
    let live = entry_exists(&journal.live)?
        .then(|| root_identity(&journal.live))
        .transpose()?;
    let prepared = entry_exists(&journal.prepared)?
        .then(|| root_identity(&journal.prepared))
        .transpose()?;
    let (published, displaced) = match (live, prepared) {
        (Some(live), Some(prepared)) if [live, prepared] == journal.roots => {
            move_back(&journal.prepared, &journal.live, &journal.carried)?;
            (false, None)
        }
        (Some(live), Some(prepared))
            if [live, prepared] == [journal.roots[1], journal.roots[0]] =>
        {
            (true, Some(journal.prepared.clone()))
        }
        (Some(live), _) if live == journal.roots[1] => (true, None),
        _ => return Err(NativeExchangeError::IncompatibleJournal),
    };
    remove_journal(path)?;
    Ok(NativeExchangeOutcome {
        published,
        displaced,
        recovery: journal.recovery.clone(),
    })
}

#[cfg(feature = "native-watch")]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    exchange_native_entries_with_budget(live, prepared, crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new()).map(|_| ()).map_err(|failure| failure.0)
}

#[cfg(all(target_os = "macos", not(feature = "native-watch")))]
#[allow(unsafe_code, reason = "renamex_np receives two live C paths")]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    use std::os::unix::ffi::OsStrExt;
    let live = std::ffi::CString::new(live.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    let prepared = std::ffi::CString::new(prepared.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    // SAFETY: both paths are NUL-terminated `CString`s that outlive the call.
    if unsafe { libc::renamex_np(live.as_ptr(), prepared.as_ptr(), libc::RENAME_SWAP) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(all(target_os = "linux", not(feature = "native-watch")))]
#[allow(unsafe_code, reason = "renameat2 receives two live C paths")]
fn exchange(live: &Path, prepared: &Path) -> Result<(), NativeExchangeError> {
    use std::os::unix::ffi::OsStrExt;
    let live = std::ffi::CString::new(live.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    let prepared = std::ffi::CString::new(prepared.as_os_str().as_bytes())
        .map_err(|_| NativeExchangeError::InvalidLayout)?;
    // SAFETY: both paths are NUL-terminated `CString`s that outlive the syscall.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            live.as_ptr(),
            libc::AT_FDCWD,
            prepared.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "native-watch")]
    fn finite_entry_budget() -> crate::WorkBudget {
        crate::WorkCounters {
            backend_read_operations: 20_000,
            backend_write_operations: 20_000,
            durability_operations: 100,
            bytes_copied: 1_000_000,
            bytes_encoded: 1_000_000,
            source_bytes_read: 1_000_000,
            source_path_components: 1_000,
            output_bytes: 1_000_000,
            items_examined: 20_000,
            allocation_operations: 20_000,
            peak_allocation_bytes: 1_000_000,
            ..crate::WorkCounters::default()
        }
    }

    #[cfg(all(windows, feature = "native-watch"))]
    fn measured_entry_witness_fixture(live: &Path, prepared: &Path, witness: &Path) {
        let cancellation = crate::CancellationToken::new();
        let ledger = crate::materializer::NativeMaterializationWork::new(finite_entry_budget(), &cancellation)
            .expect("original native meter");
        let mut work = &ledger;
        let parent = crate::native_host::open_root_directory(live.parent().expect("parent"), &mut work).map(|(directory, _)| directory)
            .expect("held fixture parent");
        let prepared_parent = crate::native_host::open_root_directory(
            prepared.parent().expect("prepared parent"), &mut work).map(|(directory, _)| directory).expect("held prepared parent");
        let live_name = Path::new(live.file_name().expect("live name"));
        let prepared_name = Path::new(prepared.file_name().expect("prepared name"));
        let roots = [
            held_identity(&parent, live_name, &mut work).expect("original identity").expect("original"),
            held_identity(&prepared_parent, prepared_name, &mut work).expect("prepared identity").expect("prepared"),
        ];
        create_measured_entry_witness(&parent, live_name, &prepared_parent, prepared_name,
            Path::new(witness.file_name().expect("witness name")), roots, &mut work)
            .expect("durable original witness");
    }

    #[cfg(feature = "native-watch")]
    #[test]
    fn bounded_entry_exchange_rejects_zero_and_cancelled_work_before_io() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        let cancellation = crate::CancellationToken::new();
        let failure = exchange_native_entries_with_budget(&live, &prepared,
            crate::WorkBudget::default(), &cancellation).expect_err("zero allowance");
        assert_eq!(failure.1, crate::WorkCounters::default());
        cancellation.cancel();
        let failure = exchange_native_entries_with_budget(&live, &prepared,
            finite_entry_budget(), &cancellation).expect_err("cancelled");
        assert!(matches!(failure.0, NativeExchangeError::Cancelled));
        assert_eq!(failure.1, crate::WorkCounters::default());
        assert_eq!(std::fs::read(&live).expect("original preserved"), b"before");
        assert_eq!(std::fs::read(&prepared).expect("prepared preserved"), b"after");
    }

    #[cfg(feature = "native-watch")]
    #[test]
    fn bounded_entry_exchange_succeeds_at_its_exact_actual_allowance() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        let cancellation = crate::CancellationToken::new();
        let measured = exchange_native_entries_with_budget(&live, &prepared,
            finite_entry_budget(), &cancellation).expect("measured exchange");
        assert!(measured.work.backend_write_operations > 0);
        assert!(measured.work.durability_operations > 0);
        let exact = exchange_native_entries_with_budget(&live, &prepared,
            measured.work, &cancellation).expect("exact allowance");
        assert_eq!(exact.work, measured.work);
        assert_eq!(std::fs::read(&live).expect("original restored"), b"before");
        assert_eq!(std::fs::read(&prepared).expect("prepared restored"), b"after");
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_journal_failure_cleans_only_its_provisional_handle() {
        for deny_bytes in [true, false] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let live = temporary.path().join("live");
            let prepared = temporary.path().join("prepared");
            let witness = entry_exchange_witness(&live).expect("witness");
            let scratch = scratch(&live).expect("scratch");
            std::fs::write(&live, b"before").expect("original");
            std::fs::write(&prepared, b"after").expect("prepared");
            let mut budget = finite_entry_budget();
            if deny_bytes { budget.output_bytes = (MEASURED_ENTRY_WITNESS.len() - 1) as u64; }
            else { budget.durability_operations = 0; }
            let cancellation = crate::CancellationToken::new();
            let failure = exchange_native_entries_with_budget(&live, &prepared, budget, &cancellation)
                .expect_err("journal admission denied before physical edits");
            assert!(matches!(failure.0, NativeExchangeError::Work(_)));
            assert!(failure.1.backend_write_operations > 0);
            assert_eq!(failure.1.durability_operations, 0);
            if deny_bytes { assert_eq!(failure.1.output_bytes, 0); }
            else { assert!(failure.1.output_bytes > 0); }
            assert!(!witness.exists());
            assert!(!scratch.exists());
            assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
            assert_eq!(std::fs::read(&prepared).expect("prepared retained"), b"after");
            exchange_native_entries_with_budget(&live, &prepared, finite_entry_budget(), &cancellation)
                .expect("the real durable request remains retryable");
            assert_eq!(std::fs::read(&live).expect("published"), b"after");
            assert_eq!(std::fs::read(&prepared).expect("displaced"), b"before");
        }
    }

    #[cfg(all(feature = "native-watch", unix))]
    #[test]
    fn bounded_entry_exchange_failure_retains_the_physical_edit_prefix() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        let mut budget = finite_entry_budget();
        budget.durability_operations = 0;
        let failure = exchange_native_entries_with_budget(&live, &prepared,
            budget, &crate::CancellationToken::new()).expect_err("durability denied");
        assert!(matches!(failure.0, NativeExchangeError::Work(_)));
        assert_eq!(failure.1.backend_write_operations, 1);
        assert_eq!(failure.1.durability_operations, 0);
        assert_eq!(std::fs::read(&live).expect("actual published bytes"), b"after");
        assert_eq!(std::fs::read(&prepared).expect("actual displaced bytes"), b"before");
    }

    #[cfg(all(feature = "native-watch", unix))]
    #[test]
    fn bounded_exchange_keeps_original_parents_when_the_lexical_root_is_replaced() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let saved = temporary.path().join("saved");
        std::fs::create_dir(&root).expect("root");
        std::fs::write(root.join("live"), b"before").expect("original");
        std::fs::write(root.join("prepared"), b"after").expect("prepared");
        let ledger = crate::materializer::NativeMaterializationWork::new(
            finite_entry_budget(), &crate::CancellationToken::new()).expect("original native meter");
        let mut work = &ledger;
        let parent = crate::native_host::open_root_directory(&root, &mut work).map(|(directory, _)| directory).expect("original held parent");
        std::fs::rename(&root, &saved).expect("displace original root");
        std::fs::create_dir(&root).expect("user replacement root");
        std::fs::write(root.join("live"), b"user-live").expect("user live");
        std::fs::write(root.join("prepared"), b"user-prepared").expect("user prepared");
        exchange_native_entries_held_measured(&parent, Path::new("live"),
            &parent, Path::new("prepared"), &mut work).expect("held physical exchange");
        assert_eq!(std::fs::read(saved.join("live")).expect("held published"), b"after");
        assert_eq!(std::fs::read(saved.join("prepared")).expect("held displaced"), b"before");
        assert_eq!(std::fs::read(root.join("live")).expect("user live preserved"), b"user-live");
        assert_eq!(std::fs::read(root.join("prepared")).expect("user prepared preserved"), b"user-prepared");
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_retry_rejects_a_different_request_without_adopting_it() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        let other = temporary.path().join("other");
        let witness = entry_exchange_witness(&live).expect("witness");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        std::fs::write(&other, b"unrelated request").expect("other request");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        let cancellation = crate::CancellationToken::new();
        let failure = exchange_native_entries_with_budget(&live, &other, finite_entry_budget(), &cancellation)
            .expect_err("original request path identity remains authoritative");
        assert!(matches!(failure.0, NativeExchangeError::IncompatibleJournal));
        assert_eq!(failure.1.backend_write_operations, 0);
        assert!(witness.exists());
        assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
        assert_eq!(std::fs::read(&prepared).expect("original preparation retained"), b"after");
        assert_eq!(std::fs::read(&other).expect("other request retained"), b"unrelated request");
        exchange_native_entries_with_budget(&live, &prepared, finite_entry_budget(), &cancellation)
            .expect("original rolled-back request proceeds to actual publication");
        assert_eq!(std::fs::read(&live).expect("original request published"), b"after");
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_recovery_finishes_in_the_original_other_parent() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live_parent = temporary.path().join("checkout");
        let prepared_parent = temporary.path().join("operation");
        std::fs::create_dir(&live_parent).expect("checkout parent");
        std::fs::create_dir(&prepared_parent).expect("operation parent");
        let live = live_parent.join("live");
        let prepared = prepared_parent.join("prepared");
        let witness = entry_exchange_witness(&live).expect("witness");
        let scratch = scratch(&live).expect("scratch");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        std::fs::rename(&live, &scratch).expect("first physical edit");
        std::fs::rename(&prepared, &live).expect("publication before interrupted displacement");
        recover_native_entry_exchange_with_budget(&live, finite_entry_budget(),
            &crate::CancellationToken::new()).expect("held original other-parent recovery");
        assert_eq!(std::fs::read(&live).expect("published entry"), b"after");
        assert_eq!(std::fs::read(&prepared).expect("original displaced entry"), b"before");
        assert!(!scratch.exists());
        assert!(!witness.exists());
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_recovery_keeps_the_same_lost_ack_decision() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        let witness = entry_exchange_witness(&live).expect("witness");
        let scratch = scratch(&live).expect("scratch");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        std::fs::rename(&live, &scratch).expect("first rename");
        std::fs::rename(&prepared, &live).expect("publication");
        std::fs::rename(&scratch, &prepared).expect("displacement before lost acknowledgement");
        let cancellation = crate::CancellationToken::new();
        let recovered = recover_native_entry_exchange_with_budget(&live, finite_entry_budget(), &cancellation)
            .expect("restart recovers original decision");
        assert!(recovered.work.backend_write_operations > 0);
        assert!(!witness.exists());
        recover_native_entry_exchange_with_budget(&live, finite_entry_budget(), &cancellation)
            .expect("reopened cleanup is idempotent");
        assert_eq!(std::fs::read(&live).expect("published identity retained"), b"after");
        assert_eq!(std::fs::read(&prepared).expect("original preimage retained"), b"before");
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_recovery_never_adopts_a_replacement_preimage() {
        for completed in [false, true] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let live = temporary.path().join("live");
            let prepared = temporary.path().join("prepared");
            let witness = entry_exchange_witness(&live).expect("witness");
            let scratch = scratch(&live).expect("scratch");
            let saved = temporary.path().join("saved");
            std::fs::write(&live, b"before").expect("original");
            std::fs::write(&prepared, b"after").expect("prepared");
            measured_entry_witness_fixture(&live, &prepared, &witness);
            if completed {
                std::fs::rename(&live, &scratch).expect("first rename");
                std::fs::rename(&prepared, &live).expect("publication");
            }
            std::fs::rename(&live, &saved).expect("retain replaced physical entry");
            std::fs::write(&live, b"user replacement").expect("user replacement");
            let failure = recover_native_entry_exchange_with_budget(&live, finite_entry_budget(),
                &crate::CancellationToken::new()).expect_err("replacement is not original authority");
            assert!(matches!(failure.0, NativeExchangeError::IncompatibleJournal));
            assert!(failure.1.backend_read_operations > 0);
            assert_eq!(failure.1.backend_write_operations, 0);
            assert_eq!(std::fs::read(&live).expect("user replacement retained"), b"user replacement");
            assert_eq!(std::fs::read(&saved).expect("original binding retained"),
                if completed { b"after".as_slice() } else { b"before".as_slice() });
            assert!(witness.exists());
            if completed { assert_eq!(std::fs::read(&scratch).expect("original preimage retained"), b"before"); }
        }
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_recovery_preserves_a_replaced_physical_parent() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("root");
        let saved = temporary.path().join("saved");
        std::fs::create_dir(&root).expect("root");
        let live = root.join("live");
        let prepared = root.join("prepared");
        let witness = entry_exchange_witness(&live).expect("witness");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        let ledger = crate::materializer::NativeMaterializationWork::new(
            finite_entry_budget(), &crate::CancellationToken::new()).expect("original native meter");
        let mut work = &ledger;
        let parent = crate::native_host::open_root_directory(&root, &mut work).map(|(directory, _)| directory).expect("original held root");
        std::fs::rename(&root, &saved).expect("displace original physical parent");
        std::fs::create_dir(&root).expect("user replacement root");
        std::fs::write(root.join("live"), b"user-live").expect("user live");
        std::fs::write(root.join("prepared"), b"user-prepared").expect("user prepared");
        recover_native_entry_exchange_held_measured(&parent, Path::new("live"), &mut work)
            .expect_err("persisted right parent no longer names the original root");
        assert_eq!(std::fs::read(root.join("live")).expect("user live preserved"), b"user-live");
        assert_eq!(std::fs::read(root.join("prepared")).expect("user prepared preserved"), b"user-prepared");
        assert_eq!(std::fs::read(saved.join("live")).expect("original preserved"), b"before");
        assert!(saved.join(witness.file_name().expect("witness name")).exists());
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_cleanup_failure_reports_its_entire_read_prefix() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        let witness = entry_exchange_witness(&live).expect("witness");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        let cancellation = crate::CancellationToken::new();
        let baseline = recover_native_entry_exchange_with_budget(&live, finite_entry_budget(), &cancellation)
            .expect("measure actual cleanup");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        let mut exact_prefix = baseline.work;
        exact_prefix.backend_write_operations = 0;
        let failure = recover_native_entry_exchange_with_budget(&live, exact_prefix, &cancellation)
            .expect_err("reject only the actual cleanup mutation");
        assert!(matches!(failure.0, NativeExchangeError::Work(_)));
        assert_eq!(failure.1.backend_write_operations, 0);
        assert_eq!(failure.1.backend_read_operations, baseline.work.backend_read_operations);
        assert_eq!(failure.1.source_bytes_read, baseline.work.source_bytes_read);
        assert!(witness.exists());
        assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
        assert_eq!(std::fs::read(&prepared).expect("prepared retained"), b"after");
        recover_native_entry_exchange_with_budget(&live, finite_entry_budget(), &cancellation)
            .expect("retry completes owned cleanup");
        assert!(!witness.exists());
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_recovery_rejects_unbounded_lengths_before_buffer_allocation() {
        use std::io::{Seek as _, Write as _};
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        let witness = entry_exchange_witness(&live).expect("witness");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        let mut journal = std::fs::OpenOptions::new().write(true).open(&witness).expect("fixture journal");
        journal.seek(std::io::SeekFrom::Start(96)).expect("first native path length");
        journal.write_all(&u64::MAX.to_le_bytes()).expect("hostile durable length");
        drop(journal);
        let failure = recover_native_entry_exchange_with_budget(&live, finite_entry_budget(),
            &crate::CancellationToken::new()).expect_err("overflowing buffer length rejected");
        assert!(matches!(failure.0, NativeExchangeError::Work(crate::WorkError::Overflow)));
        assert_eq!(failure.1.backend_write_operations, 0);
        assert!(failure.1.peak_allocation_bytes < finite_entry_budget().peak_allocation_bytes);
        assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
        assert_eq!(std::fs::read(&prepared).expect("prepared retained"), b"after");
        assert!(witness.exists());
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_recovery_retains_journal_replacements_and_trailing_data() {
        use std::io::Write as _;
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        let witness = entry_exchange_witness(&live).expect("witness");
        std::fs::write(&live, b"before").expect("original");
        std::fs::write(&prepared, b"after").expect("prepared");
        measured_entry_witness_fixture(&live, &prepared, &witness);
        let mut journal = std::fs::OpenOptions::new().append(true).open(&witness).expect("fixture journal");
        journal.write_all(b"user trailing bytes").expect("external journal change");
        drop(journal);
        let failure = recover_native_entry_exchange_with_budget(&live, finite_entry_budget(),
            &crate::CancellationToken::new()).expect_err("noncanonical witness retained");
        assert!(matches!(failure.0, NativeExchangeError::IncompatibleJournal));
        assert_eq!(failure.1.backend_write_operations, 0);
        assert!(std::fs::read(&witness).expect("journal retained").ends_with(b"user trailing bytes"));
        assert_eq!(std::fs::read(&live).expect("original retained"), b"before");
        assert_eq!(std::fs::read(&prepared).expect("prepared retained"), b"after");
    }

    #[cfg(all(feature = "native-watch", windows))]
    #[test]
    fn bounded_entry_exchange_preserves_raw_windows_names() {
        use std::os::windows::ffi::OsStringExt as _;
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live_name: Vec<_> = "live".encode_utf16().chain([0xd800]).collect();
        let prepared_name: Vec<_> = "prepared".encode_utf16().chain([0xdc00]).collect();
        let live = temporary.path().join(std::ffi::OsString::from_wide(&live_name));
        let prepared = temporary.path().join(std::ffi::OsString::from_wide(&prepared_name));
        std::fs::write(&live, b"before").expect("raw original name");
        std::fs::write(&prepared, b"after").expect("raw prepared name");
        exchange_native_entries_with_budget(&live, &prepared, finite_entry_budget(),
            &crate::CancellationToken::new()).expect("raw native exchange");
        assert_eq!(std::fs::read(&live).expect("raw published binding"), b"after");
        assert_eq!(std::fs::read(&prepared).expect("raw displaced binding"), b"before");
    }

    #[test]
    fn carrying_recovery_keeps_a_concurrently_recreated_live_path() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("private"), b"original").expect("original entry");
        let journal = temporary.path().join("exchange.json");
        let operation = crate::IdempotencyKey::from_bytes([30; 16]);
        let carried = vec![PathBuf::from("private")];
        prepare_native_exchange(&journal, &live, &prepared, operation, carried.clone())
            .expect("prepare exchange");
        carry(&live, &prepared, &carried).expect("crash after carry");

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        BEFORE_ROLLBACK_RENAME.with(|hook| {
            hook.replace(Some(barrier.clone()));
        });
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                assert!(!live.join("private").exists());
                std::fs::write(live.join("private"), b"user edit").expect("user edit");
                barrier.wait();
            });
            assert!(matches!(
                recover_native_exchange(&journal),
                Err(NativeExchangeError::CarriedPathConflict(path)) if path == carried[0]
            ));
        });
        assert_eq!(
            std::fs::read(live.join("private")).expect("user entry"),
            b"user edit"
        );
        assert_eq!(
            std::fs::read(prepared.join("private")).expect("original entry"),
            b"original"
        );
        assert_eq!(
            read_journal(&journal).expect("retained journal").operation,
            operation
        );

        std::fs::rename(live.join("private"), live.join("saved-user-edit"))
            .expect("resolve conflict without discarding user edit");
        assert!(
            !recover_native_exchange(&journal)
                .expect("retry recovery")
                .published
        );
        assert_eq!(
            std::fs::read(live.join("private")).expect("restored entry"),
            b"original"
        );
        assert_eq!(
            std::fs::read(live.join("saved-user-edit")).expect("saved user entry"),
            b"user edit"
        );
        assert!(!journal.exists());
    }

    #[test]
    fn carrying_recovery_replays_a_partially_completed_rollback() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        let carried = vec![PathBuf::from("first"), PathBuf::from("second")];
        for path in &carried {
            std::fs::write(live.join(path), b"original").expect("original entry");
        }
        let journal = temporary.path().join("exchange.json");
        prepare_native_exchange(
            &journal,
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([33; 16]),
            carried.clone(),
        )
        .expect("prepare exchange");
        carry(&live, &prepared, &carried).expect("carry entries");
        move_back(&prepared, &live, &carried[..1]).expect("crash after first rollback rename");
        std::fs::write(live.join("first"), b"later user edit").expect("edit returned entry");
        assert!(
            !recover_native_exchange(&journal)
                .expect("resume rollback")
                .published
        );
        assert_eq!(
            std::fs::read(live.join("first")).expect("returned entry"),
            b"later user edit"
        );
        assert_eq!(
            std::fs::read(live.join("second")).expect("remaining entry"),
            b"original"
        );
        assert!(!journal.exists());
    }

    #[test]
    fn carrying_recovery_rejects_replaced_roots_without_mutation() {
        for replace_live in [true, false] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let live = temporary.path().join("live");
            let prepared = temporary.path().join("prepared");
            std::fs::create_dir(&live).expect("live tree");
            std::fs::create_dir(&prepared).expect("prepared tree");
            std::fs::write(live.join("private"), b"original").expect("original entry");
            let journal = temporary.path().join("exchange.json");
            let carried = vec![PathBuf::from("private")];
            prepare_native_exchange(
                &journal,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([31; 16]),
                carried.clone(),
            )
            .expect("prepare exchange");
            carry(&live, &prepared, &carried).expect("crash after carry");
            let replaced = if replace_live { &live } else { &prepared };
            let saved = temporary.path().join("saved");
            std::fs::rename(replaced, &saved).expect("displace original root");
            std::fs::create_dir(replaced).expect("replacement root");
            std::fs::write(replaced.join("private"), b"unrelated").expect("replacement entry");
            assert!(matches!(
                recover_native_exchange(&journal),
                Err(NativeExchangeError::IncompatibleJournal)
            ));
            assert_eq!(
                std::fs::read(replaced.join("private")).expect("replacement entry"),
                b"unrelated"
            );
            let original = if replace_live { &prepared } else { &saved };
            assert_eq!(
                std::fs::read(original.join("private")).expect("original entry"),
                b"original"
            );
            assert!(journal.exists());
        }
    }

    #[test]
    fn invalid_carried_paths_fail_before_preparation_or_recovery_effects() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(prepared.join("valid"), b"untouched").expect("prepared entry");
        let journal_path = temporary.path().join("exchange.json");
        let mut invalid = vec![
            PathBuf::new(),
            PathBuf::from("."),
            PathBuf::from("../outside"),
            PathBuf::from("nested/../../outside"),
            live.clone(),
        ];
        #[cfg(windows)]
        invalid.extend([PathBuf::from("C:relative"), PathBuf::from("\\rooted")]);
        for path in invalid.drain(..) {
            let carried = vec![PathBuf::from("valid"), path];
            assert!(matches!(
                prepare_native_exchange(
                    &journal_path,
                    &live,
                    &prepared,
                    crate::IdempotencyKey::from_bytes([32; 16]),
                    carried.clone()
                ),
                Err(NativeExchangeError::IncompatibleJournal)
            ));
            assert!(!journal_path.exists());
            let journal = NativeExchangeJournal {
                version: NATIVE_EXCHANGE_JOURNAL_VERSION,
                operation: crate::IdempotencyKey::from_bytes([32; 16]),
                recovery: Vec::new(),
                live: live.clone(),
                prepared: prepared.clone(),
                carried,
                roots: [
                    root_identity(&live).expect("live identity"),
                    root_identity(&prepared).expect("prepared identity"),
                ],
                phase: NativeExchangePhase::Carrying,
            };
            write_journal(&journal_path, &journal).expect("malformed persisted journal");
            assert!(matches!(
                recover_native_exchange(&journal_path),
                Err(NativeExchangeError::IncompatibleJournal)
            ));
            assert_eq!(
                std::fs::read(prepared.join("valid")).expect("untouched entry"),
                b"untouched"
            );
            assert!(!live.join("valid").exists());
            std::fs::remove_file(&journal_path).expect("remove fixture journal");
        }
    }

    #[cfg(windows)]
    #[test]
    fn entry_exchange_recovery_restores_a_missing_live_entry() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let scratch = scratch(&live).expect("scratch path");
        let witness = entry_exchange_witness(&live).expect("witness path");
        #[cfg(not(feature = "native-watch"))]
        {
            create_entry_exchange_witness(&witness).expect("exchange witness");
            std::fs::write(&scratch, b"before").expect("scratch entry");
        }
        #[cfg(feature = "native-watch")]
        {
            let prepared = temporary.path().join("prepared");
            std::fs::write(&live, b"before").expect("original entry");
            std::fs::write(&prepared, b"after").expect("prepared entry");
            measured_entry_witness_fixture(&live, &prepared, &witness);
            std::fs::rename(&live, &scratch).expect("crash after first rename");
        }

        recover_native_entry_exchange(&live).expect("recover exchange");

        assert_eq!(std::fs::read(&live).expect("restored entry"), b"before");
        assert!(!scratch.exists());
    }

    #[cfg(windows)]
    #[test]
    fn entry_exchange_recovery_keeps_a_published_live_entry() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let scratch = scratch(&live).expect("scratch path");
        let witness = entry_exchange_witness(&live).expect("witness path");
        #[cfg(not(feature = "native-watch"))]
        {
            create_entry_exchange_witness(&witness).expect("exchange witness");
            std::fs::write(&live, b"after").expect("published entry");
            std::fs::write(&scratch, b"before").expect("scratch entry");
        }
        #[cfg(feature = "native-watch")]
        {
            let prepared = temporary.path().join("prepared");
            std::fs::write(&live, b"before").expect("original entry");
            std::fs::write(&prepared, b"after").expect("prepared entry");
            measured_entry_witness_fixture(&live, &prepared, &witness);
            std::fs::rename(&live, &scratch).expect("first physical edit");
            std::fs::rename(&prepared, &live).expect("crash after publication");
        }

        recover_native_entry_exchange(&live).expect("recover exchange");

        assert_eq!(std::fs::read(&live).expect("published entry"), b"after");
        assert!(!scratch.exists());
        #[cfg(feature = "native-watch")]
        assert_eq!(std::fs::read(temporary.path().join("prepared")).expect("preserved preimage"), b"before");
    }

    #[cfg(windows)]
    #[test]
    fn entry_exchange_recovery_preserves_an_unowned_scratch_collision() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let scratch = scratch(&live).expect("scratch path");
        std::fs::write(&live, b"live").expect("live entry");
        std::fs::write(&scratch, b"unrelated").expect("scratch collision");

        assert!(matches!(
            recover_native_entry_exchange(&live),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(std::fs::read(&live).expect("live entry"), b"live");
        assert_eq!(
            std::fs::read(&scratch).expect("scratch collision"),
            b"unrelated"
        );
    }

    #[test]
    fn recovery_recognizes_an_exchange_completed_before_journal_removal() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([1; 16]),
            recovery: vec![7; 16],
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Exchanging,
        };
        write_journal(&journal_path, &journal).expect("journal");
        exchange(&live, &prepared).expect("exchange");

        let outcome = publish_native_exchange(
            &journal_path,
            &live,
            &prepared,
            journal.operation,
            Vec::new(),
        )
        .expect("recover publication retry");
        assert!(outcome.published);
        assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
        assert_eq!(outcome.recovery, vec![7; 16]);
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!journal_path.exists());
    }

    #[test]
    fn prepared_exchange_is_idempotent_and_rejects_other_requests() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        let journal_path = temporary.path().join("exchange.json");
        let operation = crate::IdempotencyKey::from_bytes([12; 16]);
        let carried = vec![PathBuf::from("private")];

        prepare_native_exchange_with_recovery(
            &journal_path,
            &live,
            &prepared,
            operation,
            carried.clone(),
            vec![21; 32],
        )
        .expect("prepare exchange");
        prepare_native_exchange_with_recovery(
            &journal_path,
            &live,
            &prepared,
            operation,
            carried.clone(),
            vec![21; 32],
        )
        .expect("retry preparation");

        let journal = read_journal(&journal_path).expect("read journal");
        assert_eq!(journal.operation, operation);
        assert_eq!(journal.carried, carried);
        assert_eq!(journal.recovery, vec![21; 32]);
        assert_eq!(journal.phase, NativeExchangePhase::Carrying);
        assert!(matches!(
            prepare_native_exchange(
                &journal_path,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([13; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &live,
                &prepared,
                crate::IdempotencyKey::from_bytes([13; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(
            read_journal(&journal_path)
                .expect("preserved journal")
                .operation,
            operation
        );
    }

    #[test]
    fn publication_rejects_a_journal_for_different_roots() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let first_live = temporary.path().join("first-live");
        let first_prepared = temporary.path().join("first-prepared");
        let second_live = temporary.path().join("second-live");
        let second_prepared = temporary.path().join("second-prepared");
        for path in [&first_live, &first_prepared, &second_live, &second_prepared] {
            std::fs::create_dir(path).expect("exchange tree");
        }
        std::fs::write(second_live.join("old"), b"old").expect("old file");
        std::fs::write(second_prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        write_journal(
            &journal_path,
            &NativeExchangeJournal {
                version: NATIVE_EXCHANGE_JOURNAL_VERSION,
                operation: crate::IdempotencyKey::from_bytes([5; 16]),
                recovery: Vec::new(),
                live: first_live.clone(),
                prepared: first_prepared.clone(),
                carried: Vec::new(),
                roots: [
                    root_identity(&first_live).expect("live identity"),
                    root_identity(&first_prepared).expect("prepared identity"),
                ],
                phase: NativeExchangePhase::Carrying,
            },
        )
        .expect("journal");

        assert!(matches!(
            publish_native_exchange(
                &journal_path,
                &second_live,
                &second_prepared,
                crate::IdempotencyKey::from_bytes([6; 16]),
                Vec::new(),
            ),
            Err(NativeExchangeError::IncompatibleJournal)
        ));
        assert_eq!(std::fs::read(second_live.join("old")).expect("old"), b"old");
        assert_eq!(
            std::fs::read(second_prepared.join("new")).expect("new"),
            b"new"
        );
        assert!(journal_path.exists());
    }

    #[test]
    fn non_v1_journal_versions_fail_closed_without_mutation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        for obsolete_version in [0, 2, 3, u32::MAX] {
            let journal = NativeExchangeJournal {
                version: obsolete_version,
                operation: crate::IdempotencyKey::from_bytes([10; 16]),
                recovery: Vec::new(),
                live: live.clone(),
                prepared: prepared.clone(),
                carried: Vec::new(),
                roots: [
                    root_identity(&live).expect("live identity"),
                    root_identity(&prepared).expect("prepared identity"),
                ],
                phase: NativeExchangePhase::Carrying,
            };
            std::fs::write(
                &journal_path,
                serde_json::to_vec(&journal).expect("encode future journal"),
            )
            .expect("write future journal");

            assert!(matches!(
                publish_native_exchange(
                    &journal_path,
                    &live,
                    &prepared,
                    crate::IdempotencyKey::from_bytes([11; 16]),
                    Vec::new(),
                ),
                Err(NativeExchangeError::UnsupportedJournalVersion(version))
                    if version == obsolete_version
            ));
            assert_eq!(std::fs::read(live.join("old")).expect("old"), b"old");
            assert_eq!(std::fs::read(prepared.join("new")).expect("new"), b"new");
            assert_eq!(
                std::fs::read(&journal_path).expect("unchanged journal"),
                serde_json::to_vec(&journal).expect("original bytes")
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_recovery_rolls_back_after_live_moves_to_scratch() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir_all(live.join("private")).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/key"), b"secret").expect("carried file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let mut journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([7; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: vec![PathBuf::from("private")],
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        };
        write_journal(&journal_path, &journal).expect("carrying journal");
        carry(&live, &prepared, &journal.carried).expect("carry exclusion");
        journal.phase = NativeExchangePhase::Exchanging;
        write_journal(&journal_path, &journal).expect("exchange journal");
        let scratch = scratch(&live).expect("scratch path");
        durable_rename(
            &live,
            &scratch,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )
        .expect("move live to scratch");

        let outcome = recover_native_exchange(&journal_path).expect("roll back exchange");
        assert!(!outcome.published);
        assert!(outcome.displaced.is_none());
        assert_eq!(std::fs::read(live.join("old")).expect("old"), b"old");
        assert_eq!(
            std::fs::read(live.join("private/key")).expect("carried"),
            b"secret"
        );
        assert_eq!(std::fs::read(prepared.join("new")).expect("new"), b"new");
        assert!(!scratch.exists());
        assert!(!journal_path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_recovery_finishes_after_replacement_moves_to_live() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir_all(live.join("private")).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/key"), b"secret").expect("carried file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal_path = temporary.path().join("exchange.json");
        let mut journal = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([8; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: vec![PathBuf::from("private")],
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Carrying,
        };
        write_journal(&journal_path, &journal).expect("carrying journal");
        carry(&live, &prepared, &journal.carried).expect("carry exclusion");
        journal.phase = NativeExchangePhase::Exchanging;
        write_journal(&journal_path, &journal).expect("exchange journal");
        let scratch = scratch(&live).expect("scratch path");
        durable_rename(
            &live,
            &scratch,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )
        .expect("move live to scratch");
        durable_rename(
            &prepared,
            &live,
            acyclic_native_runtime::RenameMode::NoReplace, None,
        )
        .expect("publish replacement");

        let outcome = recover_native_exchange(&journal_path).expect("finish exchange");
        assert!(outcome.published);
        assert_eq!(outcome.displaced.as_deref(), Some(prepared.as_path()));
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(live.join("private/key")).expect("carried"),
            b"secret"
        );
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!scratch.exists());
        assert!(!journal_path.exists());
    }

    #[test]
    fn exchange_publishes_whole_tree_and_carries_nested_exclusion() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir_all(live.join("private/nested")).expect("live tree");
        std::fs::create_dir_all(&prepared).expect("prepared tree");
        std::fs::write(live.join("old"), b"old").expect("old file");
        std::fs::write(live.join("private/nested/key"), b"secret").expect("excluded file");
        std::fs::write(prepared.join("new"), b"new").expect("new file");
        let journal = temporary.path().join("exchange.json");
        let outcome = publish_native_exchange(
            &journal,
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([2; 16]),
            vec![PathBuf::from("private/nested")],
        )
        .expect("publish");
        assert!(outcome.published);
        assert_eq!(std::fs::read(live.join("new")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(live.join("private/nested/key")).expect("excluded"),
            b"secret"
        );
        assert_eq!(std::fs::read(prepared.join("old")).expect("old"), b"old");
        assert!(!journal.exists());
    }

    #[test]
    fn stale_completed_journal_does_not_suppress_the_next_publication() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let live = temporary.path().join("live");
        let prepared = temporary.path().join("prepared");
        std::fs::create_dir(&live).expect("live tree");
        std::fs::create_dir(&prepared).expect("prepared tree");
        std::fs::write(live.join("first"), b"first").expect("first live file");
        std::fs::write(prepared.join("second"), b"second").expect("first prepared file");
        let journal_path = temporary.path().join("exchange.json");
        let stale = NativeExchangeJournal {
            version: NATIVE_EXCHANGE_JOURNAL_VERSION,
            operation: crate::IdempotencyKey::from_bytes([3; 16]),
            recovery: Vec::new(),
            live: live.clone(),
            prepared: prepared.clone(),
            carried: Vec::new(),
            roots: [
                root_identity(&live).expect("live identity"),
                root_identity(&prepared).expect("prepared identity"),
            ],
            phase: NativeExchangePhase::Exchanging,
        };
        write_journal(&journal_path, &stale).expect("stale journal");
        exchange(&live, &prepared).expect("completed first exchange");

        let recovered = recover_native_exchange(&journal_path).expect("recover completed exchange");
        assert!(recovered.published);
        remove_entry(&prepared).expect("remove displaced tree");
        std::fs::create_dir(&prepared).expect("next prepared tree");
        std::fs::write(prepared.join("third"), b"third").expect("next prepared file");

        let outcome = publish_native_exchange(
            &journal_path,
            &live,
            &prepared,
            crate::IdempotencyKey::from_bytes([4; 16]),
            Vec::new(),
        )
        .expect("publish after stale completed journal");
        assert!(outcome.published);
        assert_eq!(std::fs::read(live.join("third")).expect("third"), b"third");
        assert_eq!(
            std::fs::read(prepared.join("second")).expect("second"),
            b"second"
        );
        assert!(!journal_path.exists());
    }

    #[test]
    fn entry_exchange_preserves_both_complete_entries() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let left = temporary.path().join("left");
        let right = temporary.path().join("right");
        std::fs::create_dir(&left).expect("left tree");
        std::fs::create_dir(&right).expect("right tree");
        std::fs::write(left.join("value"), b"left").expect("left value");
        std::fs::write(right.join("value"), b"right").expect("right value");
        exchange_native_entries(&left, &right).expect("exchange entries");
        assert_eq!(std::fs::read(left.join("value")).expect("left"), b"right");
        assert_eq!(std::fs::read(right.join("value")).expect("right"), b"left");
    }
}
