use super::*;
use crate::demand::{DemandError, SourceDemandReadWork};
use crate::native_host::{self as host};
use crate::{NativeRootIdentity, WorkCounters, WorkError};
use std::io::{Read, Write};
use std::os::windows::ffi::{OsStrExt, OsStringExt};

pub(super) const ENTRY_WITNESS: &[u8; 32] = b"acyclic-entry-exchange-v2\0\0\0\0\0\0\0";

fn service_path(live: &Path, suffix: &str) -> Result<PathBuf, NativeExchangeError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let mut name = std::ffi::OsString::from(".");
    name.push(live.file_name().ok_or(NativeExchangeError::InvalidLayout)?);
    name.push(suffix);
    Ok(parent.join(name))
}
pub(super) fn scratch(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    service_path(live, ".acyclic-exchange")
}
pub(super) fn entry_exchange_witness(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    service_path(live, ".acyclic-exchange-witness")
}

pub(super) fn with_service_name<W: SourceDemandReadWork, T>(
    name: &Path,
    suffix: &str,
    work: &mut W,
    use_name: impl FnOnce(&Path, &mut W) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    let bytes = name
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(suffix.len())
        .and_then(|n| n.checked_add(1))
        .ok_or(WorkError::Overflow)?;
    let mut allocation = work.claim(bytes as u64)?;
    let result = (|| {
        let copied = WorkCounters {
            bytes_copied: bytes as u64,
            ..WorkCounters::UNCHARGED
        };
        work.admit(copied)?;
        let mut name_buffer = std::ffi::OsString::new();
        name_buffer
            .try_reserve_exact(bytes)
            .map_err(std::io::Error::other)?;
        name_buffer.push(".");
        name_buffer.push(name);
        name_buffer.push(suffix);
        work.reconcile_allocation(&mut allocation, name_buffer.capacity() as u64)?;
        work.record(copied)?;
        use_name(Path::new(&name_buffer), work)
    })();
    work.release(allocation)?;
    result
}

fn rename<W: SourceDemandReadWork>(
    from: &cap_std::fs::Dir,
    from_name: &Path,
    to: &cap_std::fs::Dir,
    to_name: &Path,
    identity: NativeRootIdentity,
    work: &mut W,
) -> Result<(), DemandError> {
    host::with_wide_path(from_name, work, |name, work| host::with_wide_path(to_name, work, |target, work| host::rename_held(from, name, to, target, acyclic_native_runtime::RenameMode::NoReplace, Some(identity), work)))?;
    host::sync_directory_held(from, work)?;
    host::sync_directory_held(to, work)
}
fn remove_witness<W: SourceDemandReadWork>(
    parent: &cap_std::fs::Dir,
    name: &Path,
    identity: NativeRootIdentity,
    work: &mut W,
) -> Result<(), DemandError> {
    if host::entry_identity(parent, name, work)? != Some(identity) {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    host::with_wide_path(name, work, |name, work| {
        host::remove_windows_leaf(parent, name, false, Some(identity), work)
    })?;
    host::sync_directory_held(parent, work)
}

pub(super) fn create_entry_witness<W: SourceDemandReadWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    witness: &Path,
    roots: [NativeRootIdentity; 2],
    work: &mut W,
) -> Result<NativeRootIdentity, DemandError> {
    let mut file = host::with_wide_path(witness, work, |name, work| {
        host::open_windows_at(left, name, ::windows::Win32::Storage::FileSystem::FILE_GENERIC_READ | ::windows::Win32::Storage::FileSystem::FILE_GENERIC_WRITE | ::windows::Win32::Storage::FileSystem::DELETE, ::windows::Wdk::Storage::FileSystem::FILE_CREATE, ::windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE | ::windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT | ::windows::Wdk::Storage::FileSystem::FILE_DELETE_ON_CLOSE, work)
    })?;
    write_all(&mut file, ENTRY_WITNESS, work)?;
    for identity in [
        host::directory_identity(left, work)?,
        host::directory_identity(right, work)?,
        roots[0],
        roots[1],
    ] {
        let encoded = WorkCounters {
            bytes_encoded: 16,
            ..WorkCounters::UNCHARGED
        };
        work.admit(encoded)?;
        let bytes = identity.to_bytes();
        work.record(encoded)?;
        write_all(&mut file, &bytes, work)?;
    }
    for (parent, name) in [(left, left_name), (right, right_name)] {
        let owned = host::canonical_directory_wide(parent, work)?;
        let result = write_path(
            &mut file,
            owned.0.len() as u64,
            owned.0.iter().copied(),
            work,
        );
        drop(owned.0);
        work.release(owned.1)?;
        result?;
        let mut length = 0_u64;
        for _ in name.as_os_str().encode_wide() {
            let item = WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            };
            work.admit(item)?;
            work.record(item)?;
            length = length.checked_add(1).ok_or(WorkError::Overflow)?;
        }
        write_path(&mut file, length, name.as_os_str().encode_wide(), work)?;
    }
    work.attempt_io(
        WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        },
        || file.sync_all(),
    )?;
    host::keep_windows_created_file(&file, work)?;
    host::sync_directory_held(left, work)?;
    work.attempt_io(
        WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        },
        || NativeRootIdentity::from_file(&file),
    )
}

pub(super) fn exchange_windows_held<W: SourceDemandReadWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), DemandError> {
    with_service_name(left_name, ".acyclic-exchange", work, |scratch, work| {
        with_service_name(
            left_name,
            ".acyclic-exchange-witness",
            work,
            |witness, work| {
                if host::entry_identity(left, witness, work)?.is_some()
                    && recover_entry_held(
                        left,
                        left_name,
                        scratch,
                        witness,
                        Some((right, right_name)),
                        work,
                    )?
                {
                    return Ok(());
                }
                if host::entry_identity(left, scratch, work)?.is_some() {
                    return Err(NativeExchangeError::IncompatibleJournal.into());
                }
                let roots = [
                    host::entry_identity(left, left_name, work)?
                        .ok_or(NativeExchangeError::InvalidLayout)?,
                    host::entry_identity(right, right_name, work)?
                        .ok_or(NativeExchangeError::InvalidLayout)?,
                ];
                let witness_identity =
                    create_entry_witness(left, left_name, right, right_name, witness, roots, work)?;
                rename(left, left_name, left, scratch, roots[0], work)?;
                if host::entry_identity(right, right_name, work)? != Some(roots[1]) {
                    return Err(NativeExchangeError::IncompatibleJournal.into());
                }
                rename(right, right_name, left, left_name, roots[1], work)?;
                if host::entry_identity(left, scratch, work)? != Some(roots[0]) {
                    return Err(NativeExchangeError::IncompatibleJournal.into());
                }
                rename(left, scratch, right, right_name, roots[0], work)?;
                remove_witness(left, witness, witness_identity, work)
            },
        )
    })
}

pub(super) fn recover_entry_held<W: SourceDemandReadWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    scratch: &Path,
    witness: &Path,
    requested_right: Option<(&cap_std::fs::Dir, &Path)>,
    work: &mut W,
) -> Result<bool, DemandError> {
    let mut file = host::open_leaf(left, witness, &host::NativeOpenOptions::Read, work)?;
    let (witness_identity, length) = host::file_identity_and_length(&file, work)?;
    let published = with_witness(&mut file, length, work, |mut input, file, work| {
        let magic = take(&mut input, 16).map_err(std::io::Error::from)?;
        if !magic
            .iter()
            .zip(ENTRY_WITNESS.chunks_exact(2))
            .all(|(word, bytes)| *word == u16::from_le_bytes([bytes[0], bytes[1]]))
        {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        let parents = [read_identity(&mut input)?, read_identity(&mut input)?];
        let roots = [read_identity(&mut input)?, read_identity(&mut input)?];
        if host::directory_identity(left, work)? != parents[0] {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        let names = [
            take_path(&mut input)?,
            take_path(&mut input)?,
            take_path(&mut input)?,
            take_path(&mut input)?,
        ];
        if !input.is_empty() {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        with_path(names[0], work, |path, work| {
            if !path.is_absolute()
                || host::directory_identity(&host::open_root_directory(path, work)?.0, work)?
                    != parents[0]
            {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            Ok(())
        })?;
        with_path(names[1], work, |path, _| {
            if path != left_name {
                Err(NativeExchangeError::IncompatibleJournal.into())
            } else {
                Ok(())
            }
        })?;
        let right = with_path(names[2], work, |path, work| {
            if !path.is_absolute() {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            let parent = host::open_root_directory(path, work)?.0;
            if host::directory_identity(&parent, work)? != parents[1] {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            Ok(parent)
        })?;
        with_path(names[3], work, |right_name, work| {
            super::entry::validate_leaf(right_name)?;
            if let Some((parent, name)) = requested_right {
                if name != right_name || host::directory_identity(parent, work)? != parents[1] {
                    return Err(NativeExchangeError::IncompatibleJournal.into());
                }
            }
            if host::file_identity_and_length(file, work)? != (witness_identity, length) {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            let current = (
                host::entry_identity(left, left_name, work)?,
                host::entry_identity(&right, right_name, work)?,
                host::entry_identity(left, scratch, work)?,
            );
            match current {
                (Some(original), Some(replacement), None) if [original, replacement] == roots => {
                    Ok(false)
                }
                (None, Some(replacement), Some(original)) if [original, replacement] == roots => {
                    rename(left, scratch, left, left_name, roots[0], work)?;
                    Ok(false)
                }
                (Some(replacement), None, Some(original)) if [original, replacement] == roots => {
                    rename(left, scratch, &right, right_name, roots[0], work)?;
                    Ok(true)
                }
                (Some(replacement), Some(original), None) if [original, replacement] == roots => {
                    Ok(true)
                }
                _ => Err(NativeExchangeError::IncompatibleJournal.into()),
            }
        })
    })?;
    drop(file);
    remove_witness(left, witness, witness_identity, work)?;
    Ok(published)
}

fn take_path<'a>(input: &mut &'a [u16]) -> Result<&'a [u16], DemandError> {
    if input.len() < 4 {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let header = take(input, 4).map_err(std::io::Error::from)?;
    let length = u64::from(header[0])
        | (u64::from(header[1]) << 16)
        | (u64::from(header[2]) << 32)
        | (u64::from(header[3]) << 48);
    let bytes = length.checked_mul(2).ok_or(WorkError::Overflow)?;
    if bytes
        > (input.len() as u64)
            .checked_mul(2)
            .ok_or(WorkError::Overflow)?
    {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    take(
        input,
        usize::try_from(length).map_err(|_| WorkError::Overflow)?,
    )
    .map_err(|error| std::io::Error::from(error).into())
}

fn with_path<W: SourceDemandReadWork, T>(
    units: &[u16],
    work: &mut W,
    decode: impl FnOnce(&Path, &mut W) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    let maximum = (units.len() as u64)
        .checked_mul(3)
        .ok_or(WorkError::Overflow)?;
    let mut allocation = work.reserve_opaque_decode(maximum)?;
    let result = (|| {
        work.admit(WorkCounters {
            bytes_encoded: maximum,
            ..WorkCounters::UNCHARGED
        })?;
        let path = PathBuf::from(std::ffi::OsString::from_wide(units));
        let reconciled = work.reconcile_allocation(&mut allocation, path.capacity() as u64);
        let recorded = work.record(WorkCounters {
            bytes_encoded: path.as_os_str().as_encoded_bytes().len() as u64,
            ..WorkCounters::UNCHARGED
        });
        reconciled?;
        recorded?;
        decode(&path, work)
    })();
    work.release(allocation)?;
    result
}

fn with_witness<W: SourceDemandReadWork, T>(
    file: &mut std::fs::File,
    length: u64,
    work: &mut W,
    decode: impl FnOnce(&[u16], &std::fs::File, &mut W) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    if length % 2 != 0 {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let length = usize::try_from(length / 2).map_err(|_| WorkError::Overflow)?;
    let (mut buffer, allocation) = host::buffer::<u16, W>(length, work)?;
    let result = (|| {
        buffer.resize(length, 0);
        read_exact(file, utf16_bytes_mut(&mut buffer), work)?;
        #[cfg(target_endian = "big")]
        for word in &mut buffer {
            *word = u16::from_le(*word);
        }
        decode(&buffer, file, work)
    })();
    drop(buffer);
    work.release(allocation)?;
    result
}

fn take<'a>(input: &mut &'a [u16], length: usize) -> Result<&'a [u16], std::io::ErrorKind> {
    let (value, rest) = input
        .split_at_checked(length)
        .ok_or(std::io::ErrorKind::UnexpectedEof)?;
    *input = rest;
    Ok(value)
}

fn write_all<W: SourceDemandReadWork>(
    file: &mut std::fs::File,
    mut bytes: &[u8],
    work: &mut W,
) -> Result<(), DemandError> {
    while !bytes.is_empty() {
        work.admit(WorkCounters {
            backend_write_operations: 1,
            output_bytes: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        match work.attempt_io(
            WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            },
            || file.write(bytes),
        ) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
            Ok(n) => {
                work.record(WorkCounters {
                    output_bytes: n as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                bytes = bytes
                    .get(n..)
                    .ok_or_else(|| std::io::Error::other("invalid native write length"))?;
            }
            Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::Interrupted) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
fn read_exact<W: SourceDemandReadWork>(
    file: &mut std::fs::File,
    mut bytes: &mut [u8],
    work: &mut W,
) -> Result<(), DemandError> {
    while !bytes.is_empty() {
        work.admit(WorkCounters {
            backend_read_operations: 1,
            source_bytes_read: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        match work.attempt_io(
            WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            },
            || file.read(bytes),
        ) {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()),
            Ok(n) => {
                work.record(WorkCounters {
                    source_bytes_read: n as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                bytes = bytes
                    .get_mut(n..)
                    .ok_or_else(|| std::io::Error::other("invalid native read length"))?;
            }
            Err(error) if W::io_error_kind(&error) == Some(std::io::ErrorKind::Interrupted) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
fn read_identity(input: &mut &[u16]) -> Result<NativeRootIdentity, std::io::Error> {
    let words = take(input, 8).map_err(std::io::Error::from)?;
    let mut bytes = [0; 16];
    for (bytes, word) in bytes.chunks_exact_mut(2).zip(words) {
        bytes.copy_from_slice(&word.to_le_bytes());
    }
    Ok(NativeRootIdentity::from_bytes(bytes))
}
fn write_path<W: SourceDemandReadWork>(
    file: &mut std::fs::File,
    length: u64,
    units: impl Iterator<Item = u16>,
    work: &mut W,
) -> Result<(), DemandError> {
    let header = WorkCounters {
        bytes_encoded: 8,
        ..WorkCounters::UNCHARGED
    };
    work.admit(header)?;
    let length = length.to_le_bytes();
    work.record(header)?;
    write_all(file, &length, work)?;
    let encoded = WorkCounters {
        bytes_encoded: 2,
        ..WorkCounters::UNCHARGED
    };
    let mut buffer = [0; 512];
    let mut used = 0;
    for unit in units {
        work.admit(encoded)?;
        buffer[used..used + 2].copy_from_slice(&unit.to_le_bytes());
        used += 2;
        work.record(encoded)?;
        if used == buffer.len() {
            write_all(file, &buffer, work)?;
            used = 0;
        }
    }
    write_all(file, &buffer[..used], work)
}

#[allow(
    unsafe_code,
    reason = "initialized u16 storage permits an exclusive byte view"
)]
fn utf16_bytes_mut(units: &mut [u16]) -> &mut [u8] {
    // SAFETY: u16 has no padding or invalid bit patterns; the borrow is exclusive.
    unsafe { std::slice::from_raw_parts_mut(units.as_mut_ptr().cast(), units.len() * 2) }
}
