use crate::kernel::NameEncoding;
use crate::model::FilesystemProfile;
use std::ffi::OsStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HostNameEncodingError;

#[cfg(not(target_arch = "wasm32"))]
use crate::demand::{DemandError, DemandWork};
#[cfg(not(target_arch = "wasm32"))]
use crate::{CancellationToken, WorkBudget, WorkCounters};

#[cfg(not(target_arch = "wasm32"))]
fn name_buffer(length: usize, maximum_bytes: u32, work: &mut DemandWork) -> Result<Vec<u8>, DemandError> {
    work.check()?;
    if length > maximum_bytes as usize { return Err(DemandError::InvalidRequest); }
    let prior_peak = work.work.peak_allocation_bytes;
    work.allocate(length as u64)?;
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(length).is_err() {
        work.release(length as u64)?;
        work.work.peak_allocation_bytes = prior_peak;
        return Err(DemandError::Io(std::io::ErrorKind::OutOfMemory.into()));
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
fn copy_bytes_measured(source: &[u8], maximum_bytes: u32, work: &mut DemandWork) -> Result<Vec<u8>, DemandError> {
    let delta = WorkCounters { bytes_copied: source.len() as u64, ..WorkCounters::UNCHARGED };
    work.work.admit(&delta, &work.budget)?;
    let mut bytes = name_buffer(source.len(), maximum_bytes, work)?;
    bytes.extend_from_slice(source);
    work.record(delta)?;
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
fn utf16_bytes_measured<I: Iterator<Item = u16> + Clone>(
    units: I, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<Vec<u8>, DemandError> {
    let mut length = 0_usize;
    for _ in units.clone() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        length = length.checked_add(2).ok_or(crate::WorkError::Overflow)?;
        if length > maximum_bytes as usize { return Err(DemandError::InvalidRequest); }
    }
    let mut bytes = name_buffer(length, maximum_bytes, work)?;
    let result = (|| {
        for unit in units {
            let delta = WorkCounters { bytes_encoded: 2, items_examined: 1, ..WorkCounters::UNCHARGED };
            work.check()?;
            work.work.admit(&delta, &work.budget)?;
            bytes.extend_from_slice(&unit.to_le_bytes());
            work.record(delta)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        drop(bytes);
        work.release(length as u64)?;
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn host_name_bytes(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32,
) -> Result<(NameEncoding, Vec<u8>), HostNameEncodingError> {
    host_name_bytes_measured(
        name, profile, maximum_bytes,
        &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
    ).map_err(|_| HostNameEncodingError)
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
pub(crate) fn host_name_bytes_measured(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<(NameEncoding, Vec<u8>), DemandError> {
    use std::os::unix::ffi::OsStrExt as _;
    work.check()?;
    let raw = name.as_bytes();
    match profile {
        FilesystemProfile::Posix => Ok((NameEncoding::PosixBytes, copy_bytes_measured(raw, maximum_bytes, work)?)),
        FilesystemProfile::Windows => {
            let text = std::str::from_utf8(raw).map_err(|_| DemandError::InvalidRequest)?;
            Ok((NameEncoding::WindowsUtf16Le, utf16_bytes_measured(text.encode_utf16(), maximum_bytes, work)?))
        }
        FilesystemProfile::Portable | FilesystemProfile::Browser => {
            std::str::from_utf8(raw).map_err(|_| DemandError::InvalidRequest)?;
            Ok((NameEncoding::Utf8, copy_bytes_measured(raw, maximum_bytes, work)?))
        }
    }
}

#[cfg(windows)]
pub(crate) fn host_name_bytes_measured(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<(NameEncoding, Vec<u8>), DemandError> {
    use std::os::windows::ffi::OsStrExt as _;
    work.check()?;
    match profile {
        FilesystemProfile::Windows => Ok((NameEncoding::WindowsUtf16Le, utf16_bytes_measured(name.encode_wide(), maximum_bytes, work)?)),
        FilesystemProfile::Posix | FilesystemProfile::Portable | FilesystemProfile::Browser => {
            let text = name.to_str().ok_or(DemandError::InvalidRequest)?;
            let encoding = if profile == FilesystemProfile::Posix { NameEncoding::PosixBytes } else { NameEncoding::Utf8 };
            Ok((encoding, copy_bytes_measured(text.as_bytes(), maximum_bytes, work)?))
        }
    }
}

#[cfg(windows)]
pub(crate) fn host_wide_name_bytes_measured(
    name: &[u16], profile: FilesystemProfile, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<(NameEncoding, Vec<u8>), DemandError> {
    if profile == FilesystemProfile::Windows {
        return Ok((NameEncoding::WindowsUtf16Le, utf16_bytes_measured(name.iter().copied(), maximum_bytes, work)?));
    }
    let mut length = 0_usize;
    for character in char::decode_utf16(name.iter().copied()) {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        length = length.checked_add(character.map_err(|_| DemandError::InvalidRequest)?.len_utf8())
            .ok_or(crate::WorkError::Overflow)?;
        if length > maximum_bytes as usize { return Err(DemandError::InvalidRequest); }
    }
    let mut bytes = name_buffer(length, maximum_bytes, work)?;
    let result = (|| {
        for character in char::decode_utf16(name.iter().copied()) {
            work.check()?;
            let character = character.map_err(|_| DemandError::InvalidRequest)?;
            let mut encoded = [0; 4];
            let encoded = character.encode_utf8(&mut encoded).as_bytes();
            let delta = WorkCounters { bytes_encoded: encoded.len() as u64, items_examined: 1, ..WorkCounters::UNCHARGED };
            work.work.admit(&delta, &work.budget)?;
            bytes.extend_from_slice(encoded);
            work.record(delta)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        drop(bytes);
        work.release(length as u64)?;
        return Err(error);
    }
    let encoding = if profile == FilesystemProfile::Posix { NameEncoding::PosixBytes } else { NameEncoding::Utf8 };
    Ok((encoding, bytes))
}

#[cfg(all(not(any(unix, windows)), not(target_arch = "wasm32")))]
pub(crate) fn host_name_bytes_measured(
    name: &OsStr, _profile: FilesystemProfile, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<(NameEncoding, Vec<u8>), DemandError> {
    Ok((NameEncoding::Utf8, copy_bytes_measured(
        name.to_str().ok_or(DemandError::InvalidRequest)?.as_bytes(), maximum_bytes, work,
    )?))
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn host_name_bytes(
    name: &OsStr, _profile: FilesystemProfile, maximum_bytes: u32,
) -> Result<(NameEncoding, Vec<u8>), HostNameEncodingError> {
    let source = name.to_str().ok_or(HostNameEncodingError)?.as_bytes();
    if source.len() > maximum_bytes as usize { return Err(HostNameEncodingError); }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(source.len()).map_err(|_| HostNameEncodingError)?;
    bytes.extend_from_slice(source);
    Ok((NameEncoding::Utf8, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_rejects_a_component_before_allocating_beyond_its_bound() {
        assert!(host_name_bytes(OsStr::new("ab"), FilesystemProfile::Portable, 1).is_err());
        assert!(host_name_bytes(OsStr::new("ab"), FilesystemProfile::Windows, 3).is_err());
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn finite_name_budget() -> WorkBudget {
        WorkBudget {
            bytes_copied: 1_024,
            bytes_encoded: 1_024,
            items_examined: 1_024,
            allocation_operations: 16,
            peak_allocation_bytes: 1_024,
            ..WorkBudget::default()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_name_zero_and_exact_budgets_preserve_real_allocations()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let mut zero = DemandWork::new(WorkBudget::default(), &cancellation);
        assert!(matches!(host_name_bytes_measured(
            OsStr::new("native"), FilesystemProfile::Portable, 255, &mut zero,
        ), Err(DemandError::Work(_))));
        assert_eq!(zero.work, WorkCounters::default());
        let mut measured = DemandWork::new(finite_name_budget(), &cancellation);
        let (_, bytes) = host_name_bytes_measured(
            OsStr::new("native"), FilesystemProfile::Portable, 255, &mut measured,
        )?;
        assert_eq!(bytes, b"native");
        let exact_budget = measured.work;
        drop(bytes);
        let mut exact = DemandWork::new(exact_budget, &cancellation);
        let (_, bytes) = host_name_bytes_measured(
            OsStr::new("native"), FilesystemProfile::Portable, 255, &mut exact,
        )?;
        assert_eq!(bytes, b"native");
        assert_eq!(exact.work, exact_budget);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_name_cancel_and_short_unit_budget_keep_actual_prefix() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = DemandWork::new(finite_name_budget(), &cancellation);
        assert!(matches!(host_name_bytes_measured(
            OsStr::new("native"), FilesystemProfile::Portable, 255, &mut cancelled,
        ), Err(DemandError::Cancelled)));
        assert_eq!(cancelled.work, WorkCounters::default());
        let mut short = finite_name_budget();
        short.items_examined = 1;
        let mut measured = DemandWork::new(short, &CancellationToken::new());
        assert!(matches!(host_name_bytes_measured(
            OsStr::new("ab"), FilesystemProfile::Windows, 255, &mut measured,
        ), Err(DemandError::Work(_))));
        assert_eq!(measured.work.items_examined, 1);
        assert_eq!(measured.work.allocation_operations, 0);
        assert_eq!(measured.work.peak_allocation_bytes, 0);
    }
}
