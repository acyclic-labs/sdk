use super::super::adapter::NativeDemandCarrier;
use super::*;
use crate::demand::NativeMountAllocationCustody;

pub(super) fn vector<T>(
    count: usize,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(Vec<T>, Option<Arc<NativeMountAllocationCustody>>), MountSourceError> {
    carrier
        .with_work(|work| work.check())
        .map_err(MountSourceError::from)?;
    if count == 0 {
        return Ok((Vec::new(), None));
    }
    let custody = carrier.new_custody()?;
    let (value, bytes) = carrier
        .with_work(|work| crate::native_host::buffer::<T, _>(count, work))
        .map_err(MountSourceError::from)?;
    if let Err(error) = carrier.with_work(|work| {
        custody
            .retain(bytes, work)
            .map_err(crate::DemandError::from)
    }) {
        drop(value);
        carrier
            .with_work(|work| work.release(bytes))
            .map_err(MountSourceError::from)?;
        return Err(MountSourceError::from(error));
    }
    Ok((value, Some(custody)))
}

pub(super) fn charge(
    carrier: &mut NativeDemandCarrier<'_>,
    delta: crate::WorkCounters,
) -> Result<(), MountSourceError> {
    carrier
        .with_work(|work| work.charge(delta))
        .map_err(MountSourceError::from)
}

pub(super) fn map_bytes(capacity: usize) -> Result<u64, MountSourceError> {
    if capacity == 0 {
        return Ok(0);
    }
    // Fund payload and control storage before asking HashMap to allocate.
    (capacity as u64)
        .checked_mul(2 * std::mem::size_of::<(u64, u64)>() as u64 + 2)
        .and_then(|n| n.checked_add(32))
        .ok_or(crate::WorkError::Overflow.into())
}

#[path = "positions.rs"]
mod positions;
pub(super) use positions::{ChangePositions, record, unchanged_since};

pub(super) fn adopt_backing(
    owner: Option<&NativeMountAllocationCustody>,
    bytes: u64,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<u64, MountSourceError> {
    if owner
        .map(|owner| carrier.with_work(|work| owner.is_admitted(work)))
        .transpose()
        .map_err(MountSourceError::from)?
        .is_some_and(|admitted| !admitted)
    {
        carrier
            .with_work(|work| work.claim_allocation(bytes, 0, crate::WorkCounters::UNCHARGED))
            .map_err(MountSourceError::from)?;
        Ok(bytes)
    } else {
        Ok(0)
    }
}

pub(super) fn release_backing(
    bytes: u64,
    carrier: &mut NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    if bytes == 0 {
        Ok(())
    } else {
        carrier
            .with_work(|work| work.release(bytes))
            .map_err(MountSourceError::from)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
pub(in crate::native_mount) fn same_work(
    left: &crate::NativeMaterializationWork,
    right: &crate::NativeMaterializationWork,
) -> bool {
    left.shares_allocation_context(right)
        && left.capture_origin.as_ref().map(Weak::as_ptr)
            == right.capture_origin.as_ref().map(Weak::as_ptr)
        && left.invocation.as_ref().map(Arc::as_ptr) == right.invocation.as_ref().map(Arc::as_ptr)
        && left.execution.as_ref().map(Arc::as_ptr) == right.execution.as_ref().map(Arc::as_ptr)
}

pub(in crate::native_mount) fn copy_work(bytes: u64) -> crate::WorkCounters {
    crate::WorkCounters {
        bytes_copied: bytes,
        ..crate::WorkCounters::UNCHARGED
    }
}

pub(in crate::native_mount) fn scan_work(items: u64) -> crate::WorkCounters {
    crate::WorkCounters {
        items_examined: items,
        ..crate::WorkCounters::UNCHARGED
    }
}
