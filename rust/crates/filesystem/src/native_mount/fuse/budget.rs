use super::{MountPath, NativeMountError};
use crate::demand::NativeMountAllocationCustody;
use crate::materializer::NativeResourceCredit::{Copy, Items};
use crate::{NativeMaterializationWork, WorkCounters, WorkError};

pub(super) struct FuseOwned<T> {
    value: T,
    custody: Option<std::sync::Arc<NativeMountAllocationCustody>>,
}

impl<T> FuseOwned<T> {
    pub(super) fn empty(value: T) -> Self {
        Self {
            value,
            custody: None,
        }
    }

    pub(super) fn map<U>(self, map: impl FnOnce(T) -> U) -> FuseOwned<U> {
        FuseOwned {
            value: map(self.value),
            custody: self.custody,
        }
    }

    pub(super) fn claim(
        &mut self,
        bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(), crate::native_mount::NativeMountStartupAdmissionError> {
        work.with_original_leaf(|original| original.check())?;
        if bytes == 0 {
            return Ok(());
        }
        if self.custody.is_none() {
            self.custody = Some(NativeMountAllocationCustody::new(&mut work.clone())?);
        }
        let custody = self
            .custody
            .as_ref()
            .ok_or(crate::DemandError::InvalidRequest)?;
        work.with_original_leaf(|original| custody.claim(bytes, original))
            .map_err(Into::into)
    }

    pub(super) fn adopt(
        value: T,
        bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<Self, crate::native_mount::NativeMountStartupAdmissionError> {
        if bytes == 0 {
            work.admit(WorkCounters::UNCHARGED)?;
            return Ok(Self::empty(value));
        }
        let admitted = (|| {
            let custody = NativeMountAllocationCustody::new(&mut work.clone())?;
            work.with_original_leaf(|work| {
                work.check()?;
                custody.retain(bytes, work)?;
                Ok::<_, crate::DemandError>(())
            })?;
            Ok::<_, crate::DemandError>(custody)
        })();
        match admitted {
            Ok(custody) => Ok(Self {
                value,
                custody: Some(custody),
            }),
            Err(error) => {
                drop(value);
                work.with_original_leaf(|work| work.release(bytes))?;
                Err(error.into())
            }
        }
    }
}

impl<T> std::ops::Deref for FuseOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> std::ops::DerefMut for FuseOwned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}
impl<T: std::hash::Hash> std::hash::Hash for FuseOwned<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}
impl<T: PartialEq> PartialEq for FuseOwned<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for FuseOwned<T> {}
impl<T: std::fmt::Debug> std::fmt::Debug for FuseOwned<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(formatter)
    }
}
impl<T: Default> Default for FuseOwned<T> {
    fn default() -> Self {
        Self::empty(T::default())
    }
}
impl std::borrow::Borrow<MountPath> for FuseOwned<MountPath> {
    fn borrow(&self) -> &MountPath {
        self
    }
}
impl std::borrow::Borrow<[u8]> for FuseOwned<Vec<u8>> {
    fn borrow(&self) -> &[u8] {
        self
    }
}

pub(super) fn fuse_owned_source_chunk(
    chunk: crate::kernel::OwnedSourceChunk,
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<crate::kernel::OwnedSourceChunk>, NativeMountError> {
    if chunk.intrinsic {
        return Ok(FuseOwned::empty(chunk));
    }
    let bytes = chunk.logical_owned_bytes;
    FuseOwned::adopt(chunk, bytes, work).map_err(Into::into)
}

pub(super) fn fuse_vec<T>(
    capacity: usize,
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<Vec<T>>, NativeMountError> {
    let (values, bytes) = work
        .with_original_leaf(|work| crate::native_host::buffer(capacity, work))
        .map_err(crate::materializer::native_materialization_work_error)?;
    FuseOwned::adopt(values, bytes, work).map_err(Into::into)
}

pub(super) fn fuse_reserve<T>(
    owned: &mut FuseOwned<Vec<T>>,
    additional: usize,
    work: &NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let required = owned
        .len()
        .checked_add(additional)
        .ok_or(WorkError::Overflow)?;
    if required <= owned.capacity() {
        return Ok(());
    }
    let capacity = required.max(owned.capacity().checked_mul(2).ok_or(WorkError::Overflow)?);
    let copied = owned
        .len()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(WorkError::Overflow)? as u64;
    work.admit(Copy.delta(copied))?;
    let mut replacement = fuse_vec(capacity, work)?;
    work.admit(Copy.delta(copied))?;
    replacement.append(owned);
    drop(std::mem::replace(owned, replacement));
    work.record_admitted(Copy.delta(copied)).map_err(Into::into)
}

pub(super) fn fuse_copy_bytes(
    bytes: &[u8],
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<Vec<u8>>, NativeMountError> {
    work.admit(Copy.delta(bytes.len() as u64))?;
    let mut copied = fuse_vec(bytes.len(), work)?;
    copied.extend_from_slice(bytes);
    work.record_admitted(Copy.delta(bytes.len() as u64))?;
    Ok(copied)
}

pub(super) fn fuse_source_chunk(
    bytes: &[u8],
    work: &NativeMaterializationWork,
) -> Result<crate::kernel::OwnedSourceChunk, NativeMountError> {
    let copied = fuse_copy_bytes(bytes, work)?;
    let logical_owned_bytes = copied.capacity() as u64;
    let owned = acyclic_stream::OwnedBytes::from_vec_with_guard(
        copied.value,
        copied.custody,
        work.runtime_meter(),
    )
    .map_err(|failure| match failure.into_error() {
        acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
            NativeMountError::from(work.runtime_error(error))
        }
        acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
            unreachable!("initialized vector")
        }
    })?;
    Ok(crate::kernel::OwnedSourceChunk::intrinsic(
        owned,
        logical_owned_bytes,
    ))
}

pub(super) fn fuse_path<'a>(
    capacity: usize,
    components: impl Iterator<Item = &'a [u8]>,
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<MountPath>, NativeMountError> {
    let mut path = fuse_vec::<Vec<u8>>(capacity, work)?;
    for component in components {
        work.charge(Items.delta(1))?;
        let copied_bytes = (component.len() as u64)
            .checked_add(std::mem::size_of::<Vec<u8>>() as u64)
            .ok_or(WorkError::Overflow)?;
        work.admit(Copy.delta(copied_bytes))?;
        let (mut copied, bytes) = work
            .with_original_leaf(|work| crate::native_host::buffer::<u8, _>(component.len(), work))
            .map_err(crate::materializer::native_materialization_work_error)?;
        let retained = work.with_original_leaf(|work| {
            path.custody
                .as_ref()
                .ok_or(crate::DemandError::InvalidRequest)?
                .retain(bytes, work)
                .map_err(crate::DemandError::from)
        });
        if let Err(error) = retained {
            drop(copied);
            work.with_original_leaf(|work| work.release(bytes))?;
            return Err(error.into());
        }
        copied.extend_from_slice(component);
        work.record_admitted(Copy.delta(component.len() as u64))?;
        path.push(copied);
        work.record_admitted(Copy.delta(std::mem::size_of::<Vec<u8>>() as u64))?;
    }
    Ok(path.map(|components| MountPath { components }))
}

pub(super) fn fuse_clone_path(
    value: &MountPath,
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<MountPath>, NativeMountError> {
    fuse_path(
        value.components().len(),
        value.components().iter().map(Vec::as_slice),
        work,
    )
}
pub(super) fn fuse_child_path(
    parent: &MountPath,
    name: &[u8],
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<MountPath>, NativeMountError> {
    fuse_path(
        parent
            .components()
            .len()
            .checked_add(1)
            .ok_or(WorkError::Overflow)?,
        parent
            .components()
            .iter()
            .map(Vec::as_slice)
            .chain(std::iter::once(name)),
        work,
    )
}
pub(super) fn fuse_parent(
    value: &MountPath,
    work: &NativeMaterializationWork,
) -> Result<Option<FuseOwned<MountPath>>, NativeMountError> {
    let Some((_, parent)) = value.components().split_last() else {
        return Ok(None);
    };
    fuse_path(parent.len(), parent.iter().map(Vec::as_slice), work).map(Some)
}
pub(super) fn has_prefix(path: &MountPath, prefix: &MountPath) -> bool {
    path.components().starts_with(prefix.components())
}
pub(super) fn fuse_replace_prefix(
    value: &MountPath,
    source: &MountPath,
    destination: &MountPath,
    work: &NativeMaterializationWork,
) -> Result<Option<FuseOwned<MountPath>>, NativeMountError> {
    if !has_prefix(value, source) {
        return Ok(None);
    }
    fuse_rebase_path(value, source.components().len(), destination, work).map(Some)
}

pub(super) fn fuse_rebase_path(
    value: &MountPath,
    depth: usize,
    destination: &MountPath,
    work: &NativeMaterializationWork,
) -> Result<FuseOwned<MountPath>, NativeMountError> {
    let suffix = value.components().get(depth..).ok_or(libc::ESTALE)?;
    fuse_path(
        destination
            .components()
            .len()
            .checked_add(suffix.len())
            .ok_or(WorkError::Overflow)?,
        destination
            .components()
            .iter()
            .map(Vec::as_slice)
            .chain(suffix.iter().map(Vec::as_slice)),
        work,
    )
}
