use super::*;

pub(super) fn conversation_buffer_bytes<T>(
    capacity: usize,
) -> std::result::Result<u64, &'static str> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or("conversation buffer capacity overflows")
}

pub(super) fn reserve_conversation_buffer<T>(
    values: &mut Vec<T>,
    target: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] owner: &mut Option<
        acyclic_fs::NativeAllocation,
    >,
) -> Result<u64> {
    if target <= values.capacity() {
        return Ok(0);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work
        .as_deref_mut()
        .filter(|io| io.original_native_work().is_some())
    {
        let bytes = conversation_buffer_bytes::<T>(target).map_err(|reason| {
            conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason)
        })?;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        let mut incoming = io.take_native_custody(bytes)?;
        if let Err(cause) = values.try_reserve_exact(target - values.len()) {
            drop(incoming);
            return Err(io.allocation_failure(cause));
        }
        let actual = conversation_buffer_bytes::<T>(values.capacity()).map_err(|reason| {
            conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason)
        })?;
        let result = incoming
            .reconcile_capacity(actual)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            });
        *owner = Some(incoming);
        result?;
        return Ok(0);
    }
    let old = values.capacity();
    if let Some(io) = work.as_deref_mut() {
        let requested = conversation_buffer_bytes::<T>(target - old).map_err(|reason| {
            conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason)
        })?;
        io.admit_allocation(requested)?;
        values
            .try_reserve_exact(target - values.len())
            .map_err(|cause| io.allocation_failure(cause))?;
        let actual = conversation_buffer_bytes::<T>(values.capacity() - old).map_err(|reason| {
            conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason)
        })?;
        io.retain_owned(actual)?;
        Ok(actual)
    } else {
        values.reserve_exact(target - values.len());
        Ok(0)
    }
}

pub(super) fn grow_conversation_buffer<T>(
    values: &mut Vec<T>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] owner: &mut Option<
        acyclic_fs::NativeAllocation,
    >,
) -> Result<u64> {
    if values.len() < values.capacity() {
        return Ok(0);
    }
    let target = values
        .capacity()
        .checked_mul(2)
        .map(|count| count.max(4))
        .ok_or_else(|| {
            conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "conversation buffer capacity overflows",
            )
        })?;
    reserve_conversation_buffer(
        values,
        target,
        work,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        owner,
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn adopt_existing_buffer<T>(
    owner: &mut Option<acyclic_fs::NativeAllocation>,
    capacity: usize,
    io: &mut dyn crate::store::history::HistoryIo,
) -> Result<()> {
    if owner.is_none() && capacity != 0 {
        let bytes = conversation_buffer_bytes::<T>(capacity).map_err(|reason| {
            conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason)
        })?;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        *owner = Some(io.take_native_custody(bytes)?);
    }
    Ok(())
}

impl NativeConversationCustody {
    pub(super) fn retain_index(
        &mut self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work
            .as_deref_mut()
            .filter(|io| io.original_native_work().is_some())
        {
            grow_conversation_buffer(
                &mut self.indexes,
                &mut Some(&mut *io),
                &mut self.index_buffer,
            )?;
            let bytes = INDEX_ROW_BYTES as u64;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            self.indexes.push(io.take_native_custody(bytes)?);
            return Ok(0);
        }
        if let Some(io) = work.as_deref_mut() {
            let bytes = INDEX_ROW_BYTES as u64;
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            return Ok(bytes);
        }
        Ok(0)
    }
}
