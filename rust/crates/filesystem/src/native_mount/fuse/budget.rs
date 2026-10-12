use super::{FuseRequestError, MountPath, NativeMountError};
use crate::{NativeAllocation, NativeMaterializationWork, WorkCounters, WorkError};
use crate::demand::{NativeMountAllocationCustody, NativeSourceWork};
use std::sync::Arc;

pub(super) struct FuseOwned<T> {
    value: Option<T>,
    legacy: Option<(std::sync::Arc<NativeMountAllocationCustody>, u64)>,
    allocation: Option<NativeAllocation>,
}

impl<T> FuseOwned<T> {
    pub(super) fn empty(value: T) -> Self {
        Self { value: Some(value), legacy: None, allocation: None }
    }

    pub(super) fn native(value: T, allocation: NativeAllocation) -> Self {
        Self { value: Some(value), legacy: None, allocation: Some(allocation) }
    }

    pub(super) fn map<U>(mut self, map: impl FnOnce(T) -> U) -> FuseOwned<U> {
        FuseOwned { value: Some(map(self.value.take().expect("owned FUSE payload"))),
            legacy: self.legacy.take(), allocation: self.allocation.take() }
    }

    fn absorb(&mut self, mut allocation: NativeAllocation) -> Result<(), crate::NativeTreeMaterializationError> {
        match &mut self.allocation {
            Some(owned) => owned.absorb(&mut allocation),
            None => { self.allocation = Some(allocation); Ok(()) }
        }
    }

    pub(super) fn claim(&mut self, bytes: u64, work: &NativeMaterializationWork)
        -> Result<(), crate::NativeTreeMaterializationError> {
        work.admit(WorkCounters::UNCHARGED)?;
        if bytes == 0 { return Ok(()); }
        self.absorb(work.claim(bytes, 1, WorkCounters::UNCHARGED)?)
    }

    pub(super) fn split_claim(&mut self, bytes: u64) -> FuseOwned<()> {
        if let Some(allocation) = &mut self.allocation {
            return FuseOwned::native((), allocation.split_off(bytes).expect("admitted payload"));
        }
        let (custody, retained) = self.legacy.as_mut().expect("admitted payload");
        *retained = retained.checked_sub(bytes).expect("admitted payload");
        FuseOwned { value: Some(()), legacy: Some((custody.clone(), bytes)), allocation: None }
    }

    pub(super) fn adopt(value: T, bytes: u64, work: &NativeMaterializationWork)
        -> Result<Self, crate::native_mount::NativeMountStartupAdmissionError> {
        if bytes == 0 {
            work.admit(WorkCounters::UNCHARGED)?;
            return Ok(Self::empty(value));
        }
        let admitted = (|| {
            let custody = NativeMountAllocationCustody::new(&mut NativeSourceWork::from_native_materialization_work(work))?;
            work.source_leaf(|work| custody.retain_admitted(bytes, work))??;
            Ok::<_, crate::native_mount::NativeMountStartupAdmissionError>(custody)
        })();
        match admitted {
            Ok(custody) => Ok(Self { value: Some(value), legacy: Some((custody, bytes)), allocation: None }),
            Err(error) => {
                drop(value);
                work.source_leaf(|work| work.release(bytes))??;
                Err(error)
            }
        }
    }
}

impl<T> std::ops::Deref for FuseOwned<T> {
    type Target = T;
    fn deref(&self) -> &T { self.value.as_ref().expect("owned FUSE payload") }
}
impl<T> std::ops::DerefMut for FuseOwned<T> {
    fn deref_mut(&mut self) -> &mut T { self.value.as_mut().expect("owned FUSE payload") }
}
impl<T> Drop for FuseOwned<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        if let Some((custody, bytes)) = self.legacy.take() { custody.retire(bytes).expect("dropped FUSE payload"); }
    }
}
impl<T: std::hash::Hash> std::hash::Hash for FuseOwned<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { (**self).hash(state); }
}
impl<T: PartialEq> PartialEq for FuseOwned<T> {
    fn eq(&self, other: &Self) -> bool { **self == **other }
}
impl<T: Eq> Eq for FuseOwned<T> {}
impl<T: std::fmt::Debug> std::fmt::Debug for FuseOwned<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { (**self).fmt(formatter) }
}
impl<T: Default> Default for FuseOwned<T> {
    fn default() -> Self { Self::empty(T::default()) }
}
impl<T> IntoIterator for FuseOwned<Vec<T>> {
    type Item = T;
    type IntoIter = FuseOwned<std::vec::IntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter { self.map(Vec::into_iter) }
}
impl<T> Iterator for FuseOwned<std::vec::IntoIter<T>> {
    type Item = T;
    fn next(&mut self) -> Option<T> { self.value.as_mut().expect("owned FUSE iterator").next() }
}
impl std::borrow::Borrow<MountPath> for FuseOwned<MountPath> {
    fn borrow(&self) -> &MountPath { self }
}
impl std::borrow::Borrow<[u8]> for FuseOwned<Vec<u8>> {
    fn borrow(&self) -> &[u8] { self }
}

pub(super) fn fuse_item(work: &NativeMaterializationWork) -> Result<(), FuseRequestError> {
    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(Into::into)
}
pub(super) fn fuse_copy_admit(bytes: u64, work: &NativeMaterializationWork) -> Result<(), FuseRequestError> {
    work.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED }).map_err(Into::into)
}
pub(super) fn fuse_copy_record(bytes: u64, work: &NativeMaterializationWork) -> Result<(), FuseRequestError> {
    work.record_admitted(WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED }).map_err(Into::into)
}
pub(super) fn fuse_owned_source_chunk(chunk: crate::kernel::OwnedSourceChunk, work: &NativeMaterializationWork)
    -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, FuseRequestError> {
    if chunk.is_intrinsic() { return Ok(FuseOwned::empty(chunk)); }
    let bytes = chunk.logical_owned_bytes;
    FuseOwned::adopt(chunk, bytes, work).map_err(Into::into)
}

pub(super) fn fuse_vec<T>(capacity: usize, work: &NativeMaterializationWork) -> Result<FuseOwned<Vec<T>>, FuseRequestError> {
    let bytes = capacity.checked_mul(std::mem::size_of::<T>()).ok_or(WorkError::Overflow)? as u64;
    let mut owned = FuseOwned::empty(Vec::new());
    owned.claim(bytes, work)?;
    if bytes != 0 { owned.try_reserve_exact(capacity).map_err(NativeMountError::Allocation)?; }
    let actual = owned.capacity().checked_mul(std::mem::size_of::<T>()).ok_or(WorkError::Overflow)? as u64;
    let extra = actual.checked_sub(bytes).ok_or(WorkError::Overflow)?;
    if extra != 0 {
        let (allocation, recorded) = work.record_completed_allocation(extra, None);
        owned.absorb(allocation)?;
        recorded?;
    }
    Ok(owned)
}

pub(super) fn fuse_reserve<T>(owned: &mut FuseOwned<Vec<T>>, additional: usize, work: &NativeMaterializationWork)
    -> Result<(), FuseRequestError> {
    let required = owned.len().checked_add(additional).ok_or(WorkError::Overflow)?;
    if required <= owned.capacity() { return Ok(()); }
    let capacity = required.max(owned.capacity().checked_mul(2).ok_or(WorkError::Overflow)?);
    let copied = owned.len().checked_mul(std::mem::size_of::<T>()).ok_or(WorkError::Overflow)? as u64;
    fuse_copy_admit(copied, work)?;
    let mut replacement = fuse_vec(capacity, work)?;
    fuse_copy_admit(copied, work)?;
    replacement.append(owned);
    drop(std::mem::replace(owned, replacement));
    fuse_copy_record(copied, work)
}

pub(super) fn fuse_copy_bytes(bytes: &[u8], work: &NativeMaterializationWork) -> Result<FuseOwned<Vec<u8>>, FuseRequestError> {
    fuse_copy_admit(bytes.len() as u64, work)?;
    let mut copied = fuse_vec(bytes.len(), work)?;
    fuse_copy_admit(bytes.len() as u64, work)?;
    copied.extend_from_slice(bytes);
    fuse_copy_record(bytes.len() as u64, work)?;
    Ok(copied)
}

fn path<'a>(capacity: usize, components: impl Iterator<Item = &'a [u8]>, work: &NativeMaterializationWork)
    -> Result<FuseOwned<MountPath>, FuseRequestError> {
    let mut path = fuse_vec::<Vec<u8>>(capacity, work)?;
    for component in components {
        fuse_item(work)?;
        let copied_bytes = (component.len() as u64).checked_add(std::mem::size_of::<Vec<u8>>() as u64).ok_or(WorkError::Overflow)?;
        fuse_copy_admit(copied_bytes, work)?;
        let mut copied = fuse_copy_bytes(component, work)?;
        if let Some(allocation) = copied.allocation.take() { path.absorb(allocation)?; }
        let copied = copied.value.take().expect("owned native path component");
        path.push(copied);
        fuse_copy_record(std::mem::size_of::<Vec<u8>>() as u64, work)?;
    }
    Ok(path.map(|components| MountPath { components }))
}

pub(super) fn fuse_clone_path(value: &MountPath, work: &NativeMaterializationWork) -> Result<FuseOwned<MountPath>, FuseRequestError> {
    path(value.components().len(), value.components().iter().map(Vec::as_slice), work)
}
pub(super) fn fuse_child_path(parent: &MountPath, name: &[u8], work: &NativeMaterializationWork) -> Result<FuseOwned<MountPath>, FuseRequestError> {
    path(parent.components().len().checked_add(1).ok_or(WorkError::Overflow)?,
        parent.components().iter().map(Vec::as_slice).chain(std::iter::once(name)), work)
}
pub(super) fn fuse_parent(value: &MountPath, work: &NativeMaterializationWork) -> Result<Option<FuseOwned<MountPath>>, FuseRequestError> {
    let Some((_, parent)) = value.components().split_last() else { return Ok(None); };
    path(parent.len(), parent.iter().map(Vec::as_slice), work).map(Some)
}
pub(super) fn fuse_has_prefix(value: &MountPath, prefix: &MountPath, work: &NativeMaterializationWork) -> Result<bool, FuseRequestError> {
    if value.components().len() < prefix.components().len() { return Ok(false); }
    for (component, expected) in value.components().iter().zip(prefix.components()) {
        fuse_item(work)?;
        if component != expected { return Ok(false); }
    }
    Ok(true)
}
pub(super) fn fuse_replace_prefix(value: &MountPath, source: &MountPath, destination: &MountPath, work: &NativeMaterializationWork)
    -> Result<Option<FuseOwned<MountPath>>, FuseRequestError> {
    if !fuse_has_prefix(value, source, work)? { return Ok(None); }
    let suffix = &value.components()[source.components().len()..];
    path(destination.components().len().checked_add(suffix.len()).ok_or(WorkError::Overflow)?,
        destination.components().iter().map(Vec::as_slice).chain(suffix.iter().map(Vec::as_slice)), work).map(Some)
}
pub(super) fn fuse_initialized<T>(value: T, bytes: u64, work: &NativeMaterializationWork) -> Result<FuseOwned<T>, FuseRequestError> {
    let mut owner = FuseOwned::empty(value);
    owner.claim(bytes, work)?;
    Ok(owner)
}
pub(super) fn fuse_arc_clone<T: ?Sized>(value: &Arc<T>, work: &NativeMaterializationWork)
    -> Result<Arc<T>, crate::native_mount::NativeMountStartupAdmissionError> {
    let counters = WorkCounters { bytes_copied: std::mem::size_of::<Arc<T>>() as u64, ..WorkCounters::UNCHARGED };
    work.admit(counters)?;
    let cloned = Arc::clone(value);
    work.record_admitted(counters)?;
    Ok(cloned)
}
