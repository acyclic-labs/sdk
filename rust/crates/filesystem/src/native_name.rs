use crate::kernel::NameEncoding;
use crate::model::FilesystemProfile;
use std::ffi::OsStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HostNameEncodingError;

#[cfg(not(target_arch = "wasm32"))]
use crate::demand::{DemandError, DemandWork, NativeSourceWork, SourceDemandReadWork};
#[cfg(not(target_arch = "wasm32"))]
use crate::{CancellationToken, WorkBudget, WorkCounters};

#[cfg(not(target_arch = "wasm32"))]
enum NameWork<'a, W: SourceDemandReadWork = DemandWork> {
    Owned {
        allocations: &'a mut crate::kernel::allocation::AllocationLedger,
        work: &'a mut WorkCounters,
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    },
    Original {
        work: &'a crate::NativeMaterializationWork,
        allocation: Option<crate::materializer::NativeAllocation>,
        source: Option<NativeSourceWork<'a>>,
    },
    Source(&'a mut W),
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn original_name_error(error: crate::NativeTreeMaterializationError) -> DemandError {
    match error {
        crate::NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
        crate::NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
        crate::NativeTreeMaterializationError::OriginalWorkPoisoned => DemandError::OriginalWorkPoisoned,
        crate::NativeTreeMaterializationError::AllocationFailed(error) => DemandError::Allocation(error),
        crate::NativeTreeMaterializationError::Io(error) => DemandError::Io(error),
        _ => DemandError::InvalidRequest,
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<W: SourceDemandReadWork> NameWork<'_, W> {
    fn check(&self) -> Result<(), DemandError> {
        match self {
            Self::Owned { cancellation, .. } => cancellation.check().map_err(|_| DemandError::Cancelled),
            Self::Source(work) => work.check(),
            Self::Original { work, source, .. } => {
                if let Some(source) = source { crate::native_host::NativeHostWork::check(source)?; }
                work.admit(WorkCounters::UNCHARGED).map_err(original_name_error)
            }
        }
    }

    fn allocate(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.check()?;
        match self {
            Self::Owned { allocations, work, budget, .. } => allocations
                .claim_bytes(bytes, u64::from(bytes != 0), work, *budget)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest)),
            Self::Source(work) => work.allocate(bytes),
            Self::Original { work, allocation, .. } => {
                *allocation = Some(work.claim_allocation(bytes, u64::from(bytes != 0)).map_err(original_name_error)?);
                Ok(())
            }
        }
    }

    fn reconcile(&mut self, claimed: u64, actual: u64) -> Result<(), DemandError> {
        match self {
            Self::Owned { allocations, work, budget, .. } => {
                if actual > claimed {
                    allocations.record_owned_bytes(actual - claimed, work)
                        .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                } else if actual < claimed {
                    allocations.release(claimed - actual)
                        .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                }
                work.verify(*budget)?;
                Ok(())
            }
            Self::Source(work) => {
                let mut bytes = claimed;
                work.reconcile_allocation(&mut bytes, actual)
            }
            Self::Original { allocation, .. } => allocation.as_mut()
                .ok_or(DemandError::InvalidRequest)?.reconcile_capacity(actual).map_err(original_name_error),
        }
    }

    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        match self {
            Self::Owned { allocations, .. } => allocations.release(bytes)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest)),
            Self::Source(work) => work.release(bytes),
            Self::Original { allocation, .. } => {
                let result = allocation.as_mut().ok_or(DemandError::InvalidRequest)?
                    .release(bytes).map_err(original_name_error);
                if result.is_ok() { *allocation = None; }
                result
            }
        }
    }

    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        match self {
            Self::Owned { work, budget, .. } => work.admit(&delta, budget).map_err(Into::into),
            Self::Source(work) => work.admit(delta),
            Self::Original { work, .. } => work.admit(delta).map_err(original_name_error),
        }
    }

    fn reserve_copy(&mut self, bytes: u64) -> Result<Option<crate::materializer::NativeCopyReservation>, DemandError> {
        match self {
            Self::Owned { .. } | Self::Source(_) => {
                self.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED })?;
                Ok(None)
            }
            Self::Original { work, .. } => work.reserve_copy(bytes).map(Some).map_err(original_name_error),
        }
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.check()?;
        match self {
            Self::Owned { work, budget, .. } => work.charge(&delta, budget).map_err(Into::into),
            Self::Source(work) => work.charge(delta),
            Self::Original { work, .. } => work.charge(delta).map_err(original_name_error),
        }
    }

    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        match self {
            Self::Owned { work, budget, .. } => {
                work.try_add_assign(&delta)?;
                work.verify(*budget)?;
                Ok(())
            }
            Self::Source(work) => work.record(delta),
            Self::Original { work, .. } => work.record_admitted(delta).map_err(original_name_error),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn host_name_bytes_measured(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<(NameEncoding, Vec<u8>), DemandError> {
    work.check()?;
    let (allocations, work, budget, cancellation) = work.allocation_parts();
    host_name_bytes_with_accounting(name, profile, maximum_bytes, &mut NameWork::<DemandWork>::Owned {
        allocations, work, budget, cancellation,
    })
}

#[cfg(windows)]
pub(crate) fn host_wide_name_bytes_measured(
    name: &[u16], profile: FilesystemProfile, maximum_bytes: u32, work: &mut DemandWork,
) -> Result<(NameEncoding, Vec<u8>), DemandError> {
    work.check()?;
    let (allocations, work, budget, cancellation) = work.allocation_parts();
    host_wide_name_bytes_with_accounting(name, profile, maximum_bytes, &mut NameWork::<DemandWork>::Owned {
        allocations, work, budget, cancellation,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn host_name_bytes_with_native_work(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32,
    original: &crate::NativeMaterializationWork,
) -> Result<((NameEncoding, Vec<u8>), crate::materializer::NativeAllocation), crate::NativeTreeMaterializationError> {
    let mut accounting: NameWork<'_> = NameWork::Original { work: original, allocation: None, source: None };
    let name = host_name_bytes_with_accounting(name, profile, maximum_bytes, &mut accounting)
        .map_err(name_native_error)?;
    let NameWork::Original { allocation, .. } = accounting else { unreachable!() };
    Ok((name, allocation.ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?))
}

#[cfg(windows)]
pub(crate) fn host_wide_name_bytes_with_native_work(
    name: &[u16], profile: FilesystemProfile, maximum_bytes: u32,
    original: &crate::NativeMaterializationWork,
) -> Result<((NameEncoding, Vec<u8>), crate::materializer::NativeAllocation), crate::NativeTreeMaterializationError> {
    let mut accounting: NameWork<'_> = NameWork::Original { work: original, allocation: None, source: None };
    let name = host_wide_name_bytes_with_accounting(name, profile, maximum_bytes, &mut accounting)
        .map_err(name_native_error)?;
    let NameWork::Original { allocation, .. } = accounting else { unreachable!() };
    Ok((name, allocation.ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?))
}

/// Same name producer with its original source request's private stop fence.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn host_name_bytes_with_source_work<W: SourceDemandReadWork>(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32, work: &mut W,
) -> Result<((NameEncoding, Vec<u8>), Option<crate::NativeAllocation>), DemandError> {
    if !work.is_native_original() {
        return host_name_bytes_with_accounting(
            name, profile, maximum_bytes, &mut NameWork::Source(work),
        ).map(|name| (name, None));
    }
    let source = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?;
    let original = source.materialization_work()?;
    let mut accounting: NameWork<'_> = NameWork::Original { work: &original, allocation: None, source: Some(source) };
    let name = host_name_bytes_with_accounting(name, profile, maximum_bytes, &mut accounting)?;
    crate::native_host::NativeHostWork::check(&source)?;
    let NameWork::Original { allocation, .. } = accounting else { unreachable!() };
    Ok((name, Some(allocation.ok_or(DemandError::InvalidRequest)?)))
}

#[cfg(windows)]
pub(crate) fn host_wide_name_bytes_with_source_work<W: SourceDemandReadWork>(
    name: &[u16], profile: FilesystemProfile, maximum_bytes: u32, work: &mut W,
) -> Result<((NameEncoding, Vec<u8>), Option<crate::NativeAllocation>), DemandError> {
    if !work.is_native_original() {
        return host_wide_name_bytes_with_accounting(
            name, profile, maximum_bytes, &mut NameWork::Source(work),
        ).map(|name| (name, None));
    }
    let source = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?;
    let original = source.materialization_work()?;
    let mut accounting: NameWork<'_> = NameWork::Original { work: &original, allocation: None, source: Some(source) };
    let name = host_wide_name_bytes_with_accounting(name, profile, maximum_bytes, &mut accounting)?;
    crate::native_host::NativeHostWork::check(&source)?;
    let NameWork::Original { allocation, .. } = accounting else { unreachable!() };
    Ok((name, Some(allocation.ok_or(DemandError::InvalidRequest)?)))
}

#[cfg(not(target_arch = "wasm32"))]
fn name_native_error(error: DemandError) -> crate::NativeTreeMaterializationError {
    match error {
        DemandError::InvalidRequest => crate::NativeTreeMaterializationError::InvalidLayout,
        error => crate::materializer::native_materialization_work_error(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn name_buffer<W: SourceDemandReadWork>(
    length: usize, maximum_bytes: u32, work: &mut NameWork<'_, W>,
) -> Result<Vec<u8>, DemandError> {
    work.check()?;
    if length > maximum_bytes as usize {
        return Err(DemandError::InvalidRequest);
    }
    work.allocate(length as u64)?;
    let mut bytes = Vec::new();
    if let Err(error) = bytes.try_reserve_exact(length) {
        let actual = bytes.capacity() as u64;
        let _ = work.reconcile(length as u64, actual);
        drop(bytes);
        let _ = work.release(actual);
        return Err(DemandError::Allocation(error));
    }
    let actual = bytes.capacity() as u64;
    if let Err(error) = work.reconcile(length as u64, actual) {
        drop(bytes);
        let _ = work.release(actual);
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
fn copy_bytes_measured<W: SourceDemandReadWork>(
    source: &[u8], maximum_bytes: u32, work: &mut NameWork<'_, W>,
) -> Result<Vec<u8>, DemandError> {
    let delta = WorkCounters { bytes_copied: source.len() as u64, ..WorkCounters::UNCHARGED };
    let reservation = work.reserve_copy(source.len() as u64)?;
    let mut bytes = name_buffer(source.len(), maximum_bytes, work)?;
    bytes.extend_from_slice(source);
    let recorded = match reservation {
        Some(reservation) => reservation.record(source.len() as u64).map_err(original_name_error),
        None => work.record(delta),
    };
    if let Err(error) = recorded {
        let retained = bytes.capacity() as u64;
        drop(bytes);
        let _ = work.release(retained);
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
fn utf16_bytes_measured<I: Iterator<Item = u16> + Clone, W: SourceDemandReadWork>(
    units: I, maximum_bytes: u32, work: &mut NameWork<'_, W>,
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
            work.admit(delta)?;
            bytes.extend_from_slice(&unit.to_le_bytes());
            work.record(delta)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        let retained = bytes.capacity() as u64;
        drop(bytes);
        let _ = work.release(retained);
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
fn host_name_bytes_with_accounting<W: SourceDemandReadWork>(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32, work: &mut NameWork<'_, W>,
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
fn host_name_bytes_with_accounting<W: SourceDemandReadWork>(
    name: &OsStr, profile: FilesystemProfile, maximum_bytes: u32, work: &mut NameWork<'_, W>,
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
fn host_wide_name_bytes_with_accounting<W: SourceDemandReadWork>(
    name: &[u16], profile: FilesystemProfile, maximum_bytes: u32, work: &mut NameWork<'_, W>,
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
            let delta = WorkCounters { bytes_encoded: character.len_utf8() as u64, items_examined: 1, ..WorkCounters::UNCHARGED };
            work.admit(delta)?;
            let mut encoded = [0; 4];
            let encoded = character.encode_utf8(&mut encoded).as_bytes();
            bytes.extend_from_slice(encoded);
            work.record(delta)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        let retained = bytes.capacity() as u64;
        drop(bytes);
        let _ = work.release(retained);
        return Err(error);
    }
    let encoding = if profile == FilesystemProfile::Posix { NameEncoding::PosixBytes } else { NameEncoding::Utf8 };
    Ok((encoding, bytes))
}

#[cfg(all(not(any(unix, windows)), not(target_arch = "wasm32")))]
fn host_name_bytes_with_accounting<W: SourceDemandReadWork>(
    name: &OsStr, _profile: FilesystemProfile, maximum_bytes: u32, work: &mut NameWork<'_, W>,
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
    fn native_name_uses_original_allocation_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { peak_allocation_bytes: 64 * 1024, ..finite_name_budget() },
            &cancellation,
        ).map_err(|(error, _work)| error)?;
        original.admit(WorkCounters { items_examined: 3, ..WorkCounters::UNCHARGED })?;
        let baseline = original.live_allocation_bytes();
        let prior = original.receipt();
        let ((encoding, bytes), custody) = host_name_bytes_with_native_work(
            OsStr::new("native"), FilesystemProfile::Portable, 255, &original,
        )?;
        assert_eq!(encoding, NameEncoding::Utf8);
        assert_eq!(bytes, b"native");
        assert!(original.shares_ledger(custody.work()));
        assert_eq!(custody.bytes(), bytes.capacity() as u64);
        assert_eq!(original.receipt().items_examined, prior.items_examined);
        assert_eq!(original.receipt().allocation_operations, prior.allocation_operations + 1);
        assert_eq!(
            original.live_allocation_bytes(),
            baseline + bytes.capacity() as u64,
        );
        drop(bytes);
        drop(custody);
        assert_eq!(
            original.live_allocation_bytes(),
            baseline,
        );
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

    #[cfg(all(unix, not(target_arch = "wasm32")))]
    #[test]
    fn original_name_preserves_non_utf8_bytes_and_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStrExt as _;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { peak_allocation_bytes: 64 * 1024, ..finite_name_budget() },
            &cancellation,
        ).map_err(|(error, _)| error)?;
        let raw = [b'x', 0xff];
        let ((encoding, bytes), allocation) = host_name_bytes_with_native_work(
            OsStr::from_bytes(&raw), FilesystemProfile::Posix, 255, &original,
        )?;
        assert_eq!(encoding, NameEncoding::PosixBytes);
        assert_eq!(bytes, raw);
        assert_eq!(allocation.bytes(), bytes.capacity() as u64);
        drop(bytes);
        drop(allocation);
        assert!(matches!(host_name_bytes_with_native_work(
            OsStr::from_bytes(&raw), FilesystemProfile::Portable, 255, &original,
        ), Err(crate::NativeTreeMaterializationError::InvalidLayout)));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn original_utf16_quota_keeps_completed_prefix_and_retires_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { bytes_encoded: 2, peak_allocation_bytes: 64 * 1024, ..finite_name_budget() },
            &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let prior = original.receipt();
        assert!(matches!(host_name_bytes_with_native_work(
            OsStr::new("ab"), FilesystemProfile::Windows, 255, &original,
        ), Err(crate::NativeTreeMaterializationError::Work(_))));
        let completed = original.receipt();
        assert_eq!(completed.bytes_encoded, prior.bytes_encoded + 2);
        assert_eq!(completed.items_examined, prior.items_examined + 3);
        assert_eq!(completed.allocation_operations, prior.allocation_operations + 1);
        assert_eq!(original.live_allocation_bytes(), baseline);
        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(host_name_bytes_with_native_work(
            OsStr::new("c"), FilesystemProfile::Portable, 255, &original,
        ), Err(crate::NativeTreeMaterializationError::Cancelled)));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn original_wide_name_preserves_unpaired_surrogates()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget { peak_allocation_bytes: 64 * 1024, ..finite_name_budget() },
            &cancellation,
        ).map_err(|(error, _)| error)?;
        let ((encoding, bytes), allocation) = host_wide_name_bytes_with_native_work(
            &[0xd800, 0x0061], FilesystemProfile::Windows, 255, &original,
        )?;
        assert_eq!(encoding, NameEncoding::WindowsUtf16Le);
        assert_eq!(bytes, [0, 0xd8, 0x61, 0]);
        assert_eq!(allocation.bytes(), bytes.capacity() as u64);
        drop(bytes);
        drop(allocation);
        assert!(matches!(host_wide_name_bytes_with_native_work(
            &[0xd800], FilesystemProfile::Portable, 255, &original,
        ), Err(crate::NativeTreeMaterializationError::InvalidLayout)));
        Ok(())
    }
}
