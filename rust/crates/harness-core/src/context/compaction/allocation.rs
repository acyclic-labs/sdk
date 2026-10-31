use super::*;

pub(super) fn quota_vector<T>(
    capacity: usize,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<Vec<T>> {
    let mut values = Vec::new();
    reserve_vector(&mut values, capacity, quota.work())?;
    quota.adopt((values.capacity() * std::mem::size_of::<T>()) as u64)?;
    Ok(values)
}

pub(super) fn retire_vector<T>(
    values: Vec<T>,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<()> {
    let bytes = (values.capacity() * std::mem::size_of::<T>()) as u64;
    drop(values);
    quota.relinquish(bytes)
}

pub(crate) fn reserve_vector<T>(
    values: &mut Vec<T>,
    capacity: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let Some(io) = work.as_deref_mut() else {
        values.reserve(capacity.saturating_sub(values.len()));
        return Ok(());
    };
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))?
        as u64;
    let old = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let copied = (values.len() * std::mem::size_of::<T>()) as u64;
    io.admit_copy(copied)?;
    io.admit_allocation(bytes)?;
    let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
    quota.observer()?.copy_bytes(copied)?;
    if let Err(cause) = values.try_reserve_exact(capacity - values.len()) {
        return Err(quota.observer()?.allocation_failure(cause));
    }
    let actual = (values.capacity() * std::mem::size_of::<T>()) as u64;
    let reconciled = (|| {
        if actual > bytes {
            let io = quota.observer()?;
            io.admit_owned(actual - bytes)?;
            io.retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
        quota.observer()?.release_owned(old)?;
        Ok(())
    })();
    if let Err(error) = reconciled {
        drop(std::mem::take(values));
        return Err(error);
    }
    quota.transfer();
    Ok(())
}
