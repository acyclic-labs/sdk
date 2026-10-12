use super::*;
use crate::wire_codec::CodecQuota;
use std::mem::size_of;

pub(super) fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| diagnostic(&None, Invalid, "scheduler clone ownership overflows"))
}
pub(super) fn slots<T>(count: usize) -> Result<u64> {
    count
        .checked_mul(size_of::<T>())
        .map(|n| n as u64)
        .ok_or_else(|| diagnostic(&None, Invalid, "scheduler clone ownership overflows"))
}
pub(super) fn resources_bytes(values: &BTreeMap<String, u64>) -> Result<u64> {
    values
        .keys()
        .try_fold(slots::<(String, u64)>(values.len())?, |bytes, key| {
            add(bytes, key.capacity() as u64)
        })
}
pub(super) fn copy_text(source: &str, quota: &mut CodecQuota<'_, '_>) -> Result<String> {
    observe(quota.work())?;
    crate::core::copy_text(source, quota)
}
pub(super) fn claim_slots<T>(count: usize, quota: &mut CodecQuota<'_, '_>) -> Result<()> {
    let bytes = count.checked_mul(size_of::<T>()).ok_or_else(|| {
        diagnostic(
            quota.work(),
            Invalid,
            "scheduler collection ownership overflows",
        )
    })? as u64;
    if let Some(io) = quota.work().as_deref_mut() {
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    Ok(())
}
pub(super) fn copy_checkpoint(
    source: &CheckpointRef,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<CheckpointRef> {
    observe(quota.work())?;
    let copied = source.clone_with_work(quota.work())?;
    quota.adopt(copied.logical_owned_bytes()?)?;
    Ok(copied)
}
macro_rules! closed_clone {
    ($kind:ty) => {
        impl $kind {
            pub(crate) fn clone_with_work(
                &self,
                work: &mut Option<&mut dyn HistoryIo>,
            ) -> Result<Self> {
                if work.is_none() {
                    return Ok(self.clone());
                }
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let value = self.copy_fields(&mut quota)?;
                quota.transfer();
                Ok(value)
            }
        }
    };
}
pub(super) use closed_clone;

pub(super) fn push<T>(
    values: &mut Vec<T>,
    value: T,
    held: &mut u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if values.len() == values.capacity() {
        let target = values
            .capacity()
            .checked_mul(2)
            .unwrap_or(usize::MAX)
            .max(4);
        let bytes = target
            .checked_mul(size_of::<T>())
            .ok_or_else(|| diagnostic(work, Invalid, "scheduler vector slots overflow"))?
            as u64;
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.admit_copy((values.len() * size_of::<T>()) as u64)?;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
        }
        if let Err(cause) = values.try_reserve_exact(target - values.len()) {
            if let Some(io) = work.as_deref_mut() {
                let error = io.allocation_failure(cause);
                let _ = io.release_owned(bytes);
                return Err(error);
            }
            return Err(Error::Storage(cause.to_string()));
        }
        let old = *held;
        *held = (values.capacity() * size_of::<T>()) as u64;
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(old)?;
            io.copy_bytes((values.len() * size_of::<T>()) as u64)?;
        }
    }
    values.push(value);
    Ok(())
}
