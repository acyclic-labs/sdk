use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn lowercase_unicode_with_work(text: &str, work: &mut DemandWork) -> Result<Vec<u8>, DemandError> {
    work.reconcile_cancellation_registry()?;
    work.check()?;
    let bytes = crate::kernel::types::lowercase_unicode(text, &mut work.allocations, &mut work.work, work.budget, Some(&work.cancellation))?;
    if let Err(error) = work.check() {
        let retained = bytes.capacity() as u64;
        drop(bytes);
        work.release(retained)?;
        return Err(error);
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) fn clone_namespace<W: SourceDemandReadWork>(path: &NamespacePath, work: &mut W) -> Result<NamespacePath, DemandError> {
    use crate::kernel::{NamespacePathStep as Step, ReadAllocationStep as Allocation};
    work.check()?;
    let cancellation = work.with_original_leaf(|work| work.cancellation().clone());
    let mut owned_bytes = 0_u64;
    let mut pending = WorkCounters::UNCHARGED;
    let cloned = path.clone_with_steps(&cancellation, &mut |step| {
        let (delta, admitting) = match step {
            Step::Allocation(Allocation::Allocate { bytes, operations }) => {
                work.claim_allocation(bytes, operations)?;
                owned_bytes = owned_bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                return Ok(());
            }
            Step::Allocation(Allocation::Capacity { admitted_bytes, actual_bytes }) => {
                let mut allocation = admitted_bytes;
                let completed = work.reconcile_allocation(&mut allocation, actual_bytes);
                owned_bytes = owned_bytes.checked_sub(admitted_bytes)
                    .and_then(|owned| owned.checked_add(allocation)).ok_or(crate::WorkError::Overflow)?;
                return completed;
            }
            Step::Allocation(Allocation::ReserveFailed(error)) => return Err(DemandError::Allocation(error)),
            Step::Allocation(Allocation::BeforeCopy(bytes) | Allocation::Copied(bytes)) =>
                (WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED }, matches!(step, Step::Allocation(Allocation::BeforeCopy(_)))),
            Step::Allocation(Allocation::BeforeHash(bytes) | Allocation::Hashed(bytes)) =>
                (WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED }, matches!(step, Step::Allocation(Allocation::BeforeHash(_)))),
            Step::Allocation(Allocation::BeforeItem | Allocation::Item) =>
                (WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }, matches!(step, Step::Allocation(Allocation::BeforeItem))),
            Step::Admit(delta) | Step::Performed(delta) => (*delta, matches!(step, Step::Admit(_))),
            Step::Charge(delta) => return work.charge(*delta),
        };
        if admitting {
            work.check()?;
            let reserved = pending.checked_add(delta)?;
            work.with_original_leaf(|work| work.reserve_native_work(delta))?;
            pending = reserved;
            Ok(())
        } else {
            pending = delta.remaining(pending)?;
            work.with_original_leaf(|work| work.record_native_work(delta))
        }
    });
    let refunded = work.with_original_leaf(|work| work.refund_native_work(pending));
    match cloned {
        Ok(cloned) => match refunded.and_then(|()| work.check()) {
            Ok(()) => Ok(cloned),
            Err(error) => { drop(cloned); let _cleanup = work.release(owned_bytes); Err(error) }
        },
        Err(error) => { let _cleanup = work.release(owned_bytes); Err(error) }
    }
}

#[cfg(all(feature = "native-watch", unix))]
pub(in crate::demand) fn host_component(name: &crate::kernel::LogicalName, work: &mut DemandWork) -> Result<std::ffi::OsString, DemandError> {
    use crate::kernel::NameEncoding;
    use std::os::unix::ffi::OsStringExt;
    if name.encoding() != NameEncoding::WindowsUtf16Le {
        work.allocate_for(name.as_bytes().len() as u64, WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default() })?;
        return Ok(std::ffi::OsString::from_vec(name.as_bytes().to_vec()));
    }
    let units = || name.as_bytes().chunks_exact(2).map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]));
    let length = char::decode_utf16(units()).try_fold(0usize, |length, value| {
        value.map(|value| length + value.len_utf8()).map_err(|_| DemandError::InvalidRequest)
    })?;
    work.allocate_for(length as u64, WorkCounters { bytes_encoded: length as u64, ..WorkCounters::default() })?;
    let mut text = String::with_capacity(length);
    for value in char::decode_utf16(units()) {
        text.push(value.map_err(|_| DemandError::InvalidRequest)?);
    }
    Ok(text.into())
}

#[cfg(all(feature = "native-watch", windows))]
pub(crate) fn native_wide_path(
    path: &NamespacePath, limits: crate::model::VolumeLimits, work: &mut DemandWork,
) -> Result<Vec<u16>, DemandError> {
    use crate::kernel::NameEncoding;
    if path.depth() > usize::from(limits.maximum_path_depth) || path.encoded_bytes() > limits.maximum_path_bytes {
        return Err(DemandError::InvalidRequest);
    }
    let mut units = path.depth().saturating_sub(1);
    for name in path.components() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        if name.as_bytes().len() > limits.maximum_component_bytes as usize { return Err(DemandError::InvalidRequest); }
        let length = if name.encoding() == NameEncoding::WindowsUtf16Le {
            name.as_bytes().len() / 2
        } else {
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| DemandError::InvalidRequest)?;
            if text.contains(['/', '\\']) { return Err(DemandError::InvalidRequest); }
            text.encode_utf16().count()
        };
        units = units.checked_add(length).ok_or(crate::WorkError::Overflow)?;
    }
    let bytes = units.checked_mul(std::mem::size_of::<u16>()).ok_or(crate::WorkError::Overflow)?;
    if bytes > isize::MAX as usize { return Err(crate::WorkError::Overflow.into()); }
    work.allocate(bytes as u64)?;
    let mut relative = Vec::with_capacity(units);
    for name in path.components() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        if !relative.is_empty() {
            work.charge(WorkCounters { bytes_encoded: 2, ..WorkCounters::default() })?;
            relative.push(u16::from(b'\\'));
        }
        if name.encoding() == NameEncoding::WindowsUtf16Le {
            work.charge(WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default() })?;
            relative.extend(name.as_bytes().chunks_exact(2).map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]])));
        } else {
            let text = std::str::from_utf8(name.as_bytes()).map_err(|_| DemandError::InvalidRequest)?;
            work.charge(WorkCounters { bytes_encoded: (text.encode_utf16().count() * 2) as u64, ..WorkCounters::default() })?;
            relative.extend(text.encode_utf16());
        }
    }
    Ok(relative)
}
