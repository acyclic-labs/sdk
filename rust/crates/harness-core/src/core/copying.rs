//! Funded copies of main reducer records.
use super::*;
/// Copies text into the supplied quota.
pub(crate) fn copy_text(source: &str, quota: &mut CodecQuota<'_, '_>) -> Result<String> {
    let text = match quota.work().as_deref_mut() {
        Some(work) => work.clone_text(source)?,
        None => return Ok(source.to_owned()),
    };
    quota.adopt(text.capacity() as u64)?;
    Ok(text)
}
/// Copies immutable text through the same funded text-copy boundary.
pub(crate) fn copy_boxed_text(source: &str, quota: &mut CodecQuota<'_, '_>) -> Result<Box<str>> {
    let text = copy_text(source, quota)?;
    let spare = text.capacity() - text.len();
    let text = text.into_boxed_str();
    quota.relinquish(spare as u64)?;
    Ok(text)
}
pub(crate) fn copy_authority(
    source: &Authority,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Authority> {
    let authority = match quota.work().as_deref_mut() {
        Some(work) => work.clone_authority(source)?,
        None => return Ok(source.clone()),
    };
    quota.adopt(authority.id.capacity() as u64)?;
    Ok(authority)
}
/// Copies the canonical three provider identity fields through the same quota.
pub(crate) fn copy_provider(
    source: &crate::resources::ProviderRef,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<crate::resources::ProviderRef> {
    if quota.work().is_none() {
        return Ok(source.clone());
    }
    let namespace = copy_text(source.namespace(), quota)?;
    let family = copy_text(source.family(), quota)?;
    let version = copy_text(source.version(), quota)?;
    crate::resources::ProviderRef::new(namespace, family, version, quota.work())
}

#[cfg(feature = "filesystem")]
pub(crate) fn copy_resource(
    source: &crate::resources::ResourceRef,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<crate::resources::ResourceRef> {
    crate::resources::ResourceRef::new(
        source.kind(),
        copy_provider(source.provider(), quota)?,
        copy_copy_vector(source.key(), quota)?,
        source
            .version()
            .map(|value| copy_text(value, quota))
            .transpose()?,
        quota.work(),
    )
}

pub(crate) fn copy_vector<T>(
    source: &[T],
    quota: &mut CodecQuota<'_, '_>,
    mut copy: impl FnMut(&T, &mut CodecQuota<'_, '_>) -> Result<T>,
) -> Result<Vec<T>> {
    let mut values = reserve_copy_vector(source.len(), quota)?;
    for value in source {
        values.push(copy(value, quota)?);
    }
    Ok(values)
}
pub(crate) fn reserve_copy_vector<T>(
    count: usize,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Vec<T>> {
    if quota.work().is_none() {
        return Ok(Vec::with_capacity(count));
    }
    let bytes = owned_buffer::<T>(count)?;
    if bytes != 0 {
        let io = quota.observer()?;
        if is_native(io) {
            io.admit_allocation(bytes)?;
        }
        quota.replace(owned_sum([quota.retained(), bytes])?)?;
    }
    let mut values = Vec::new();
    if bytes != 0 {
        if let Err(cause) = values.try_reserve_exact(count) {
            return Err(quota.observer()?.allocation_failure(cause));
        }
        let actual = owned_buffer::<T>(values.capacity())?;
        quota.replace(owned_sum([quota.retained(), actual - bytes])?)?;
    }
    Ok(values)
}
pub(crate) fn copy_copy_vector<T: Copy>(
    source: &[T],
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Vec<T>> {
    if quota.work().is_none() {
        return Ok(source.to_vec());
    }
    let bytes = owned_buffer::<T>(source.len())?;
    quota.observer()?.admit_copy(bytes)?;
    let mut values = reserve_copy_vector(source.len(), quota)?;
    quota.observer()?.copy_bytes(bytes)?;
    values.extend_from_slice(source);
    Ok(values)
}
pub(crate) fn copy<T: Clone + Serialize>(
    source: &T,
    quota: &mut CodecQuota<'_, '_>,
    owned: impl FnOnce(&T, &mut Option<&mut dyn HistoryIo>) -> Result<u64>,
) -> Result<T> {
    if quota.work().is_none() {
        return Ok(source.clone());
    }
    inspect_metadata(source, quota.work())?;
    let bytes = owned(source, quota.work())?;
    quota.observer()?.admit_copy(bytes)?;
    quota.replace(owned_sum([quota.retained(), bytes])?)?;
    quota.observer()?.copy_bytes(bytes)?;
    Ok(source.clone())
}

impl<'a, 'w> CodecQuota<'a, 'w> {
    pub(crate) fn produce<T>(
        work: &'a mut Option<&'w mut dyn HistoryIo>,
        copy: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<(T, u64)> {
        let mut quota = Self::new(work, 0)?;
        let value = copy(&mut quota)?;
        let retained = quota.retained();
        quota.transfer();
        Ok((value, retained))
    }
}

pub(crate) fn copy_boxed<T>(
    source: &T,
    quota: &mut CodecQuota<'_, '_>,
    copy: impl FnOnce(&T, &mut CodecQuota<'_, '_>) -> Result<T>,
) -> Result<Box<T>> {
    retain_entry::<T>(quota)?;
    Ok(Box::new(copy(source, quota)?))
}
/// Logical inline map/set payload, never an estimate of opaque allocator nodes.
pub(in crate::core) fn retain_entry<T>(quota: &mut CodecQuota<'_, '_>) -> Result<()> {
    if quota.work().is_some() {
        quota.replace(owned_sum([
            quota.retained(),
            std::mem::size_of::<T>() as u64,
        ])?)?;
    }
    Ok(())
}

pub(in crate::core) fn is_native(io: &dyn HistoryIo) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        io.original_native_work().is_some()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = io;
        false
    }
}
