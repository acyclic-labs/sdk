use super::*;
use crate::kernel::allocation::retained::Retained;
pub(super) type GetRequest = (acyclic_objects::Digest, ByteRange, u64);
pub(super) type PutRequest = (acyclic_stream::SealedBytes, Vec<acyclic_objects::Digest>);
pub(super) type RetentionRequest = (
    Retained<acyclic_objects::RootId>,
    Retained<Vec<acyclic_objects::Digest>>,
);

/// Each value retires before its original funding, including on cancellation.
pub(super) fn retention(
    epoch: u64,
    objects: &[ObjectId],
    work: &mut KernelAccounting<'_>,
) -> Result<RetentionRequest, crate::FsError> {
    let root = Retained::<Vec<u8>>::allocate(super::Epoch::ROOT_BYTES, work)?;
    let root = Retained {
        value: super::Epoch::root_from(epoch, root.value),
        allocation: root.allocation,
    };
    let mut references = Retained::<Vec<acyclic_objects::Digest>>::allocate(objects.len(), work)?;
    references.value.extend(objects.iter().copied().map(blob));
    Ok((root, references))
}

pub(super) fn children(
    id: ObjectId,
    plaintext: &[u8],
    work: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<Vec<acyclic_objects::Digest>>, crate::FsError> {
    if id.kind == crate::ObjectKind::BlobChunk {
        return Ok(crate::kernel::Owned::new(Vec::new(), guard(work)?, 0));
    }
    let mut references = crate::kernel::Owned::allocate(plaintext.len() / 33, work)?;
    crate::kernel::visit_object_references(
        id,
        plaintext,
        crate::kernel::DecodeLimits::for_object_bytes(plaintext.len()),
        work,
        |child| {
            if id.storage.is_some() && child.storage.is_none() {
                return Err(ObjectStoreError::Corrupt.into());
            }
            if references.value.len() == references.value.capacity() {
                return Err(ObjectStoreError::Corrupt.into());
            }
            references.value.push(blob(child));
            Ok(())
        },
    )?;
    Ok(references)
}

/// Original custody for a request whose data owns no heap storage.
pub(super) fn guard(
    work: &mut KernelAccounting<'_>,
) -> Result<KernelOwnedAllocation, crate::FsError> {
    work.claim_owned(0, 0)
}
pub(super) fn release(
    work: &mut KernelAccounting<'_>,
    mut allocation: KernelOwnedAllocation,
    bytes: u64,
) -> Result<(), crate::FsError> {
    work.release_owned(&mut allocation, bytes)
}
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn packet<T>(
    owned: crate::kernel::Owned<T>,
    work: &crate::NativeMaterializationWork,
) -> Result<acyclic_objects::ObjectsNativeRequest<T>, crate::FsError> {
    match owned.allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            acyclic_objects::ObjectsNativeRequest::new(
                owned.value,
                allocation,
                &work.runtime_meter(),
            )
            .map_err(|error| native_error(error, work).into())
        }
        _ => Err(ObjectStoreError::Corrupt.into()),
    }
}
pub(super) fn record_write(
    work: &mut KernelAccounting<'_>,
    bytes: u64,
) -> Result<(), crate::FsError> {
    let delta = WorkCounters {
        object_bytes_written: bytes,
        ..WorkCounters::UNCHARGED
    };
    match work {
        KernelAccounting::Standalone { work, .. } => {
            *work = work.checked_add(delta)?;
            Ok(())
        }
        KernelAccounting::BorrowedStandalone { work, .. } => {
            **work = work.checked_add(delta)?;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => {
            work.record_admitted(delta).map_err(Into::into)
        }
    }
}
