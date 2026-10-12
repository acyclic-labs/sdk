//! Immutable checkpoint and two-parent merge-generation construction.

use super::{DecodeLimits, GenerationRoot, encode_generation_root};
#[cfg(not(target_arch = "wasm32"))]
use super::allocation::KernelOwnedAllocation;
use super::persistent_point::Owned;
use crate::cancellation::CancellationToken;
use crate::foundation::{GenerationId, usize_to_u64};
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStoreError,
    HashedObject,
};
use std::mem::size_of;
use thiserror::Error;

/// Inputs for constructing a candidate immutable generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointRequest {
    /// Existing authenticated generation used as first parent.
    pub base: ObjectId,
    /// Candidate authenticated path-independent file-table root.
    pub file_table: ObjectId,
    /// Optional distinct second parent for a three-way merge result.
    pub merge_parent: Option<ObjectId>,
}

/// Checkpoint construction failure retaining all spent work and orphan writes.
pub type CheckpointFailure = OperationFailure<CheckpointError>;

/// Constructs an unpublished checkpoint or merge generation.
pub async fn build_checkpoint<S: crate::async_storage::AsyncObjectStore>(
    store: &S,
    request: CheckpointRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<ObjectId>, CheckpointFailure> {
    let span = crate::obs::span!(
        DEBUG,
        "acyclic.fs.kernel.checkpoint",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    let result = crate::obs::in_span(&span, async {
        build_checkpoint_with_accounting(store, request, limits, &mut accounting, cancellation)
            .await.map(|value| OperationReceipt { value, work: accounting.spent() })
            .map_err(|error| OperationFailure::new(
                checkpoint_error(error), accounting.spent(),
            ))
    })
    .await;
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

fn checkpoint_error(error: crate::FsError) -> CheckpointError {
    match error {
        crate::FsError::Checkpoint(error) => error,
        crate::FsError::Work(error) => error.into(),
        crate::FsError::RootReadWorkMerge { error, .. } => error.into(),
        crate::FsError::Decode(error) => error.into(),
        crate::FsError::Object(error) => error.into(),
        crate::FsError::Cancelled(_) => ObjectStoreError::Cancelled.into(),
        _ => unreachable!("standalone checkpoint returns only checkpoint, codec, storage, cancellation, or work errors"),
    }
}

async fn checkpoint_parent<S: crate::AsyncObjectStore>(
    store: &S, object: ObjectId, limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Owned<GenerationRoot>, crate::FsError> {
    let (root, allocation) = super::generation::read_generation_root_with_limits_and_accounting(
        store, object, limits, accounting, cancellation,
    ).await?;
    let bytes = usize_to_u64(root.parents.capacity() * size_of::<GenerationId>());
    Ok(Owned::new(root, allocation, bytes))
}

pub(crate) async fn build_checkpoint_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, request: CheckpointRequest, limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, crate::FsError> {
    validate_request(request)?;
    accounting.check()?;
    let base = checkpoint_parent(store, request.base, limits, accounting, cancellation).await?;
    let result = async {
        if let Some(parent_id) = request.merge_parent {
            let parent = checkpoint_parent(store, parent_id, limits, accounting, cancellation).await?;
            let valid = validate_merge_parents(&base.value, &parent.value);
            parent.retire(accounting)?;
            valid?;
        }
        if request.merge_parent.is_none() && request.file_table == base.value.file_table {
            return Ok(request.base);
        }
        let mut parents = Owned::<Vec<GenerationId>>::allocate(1 + usize::from(request.merge_parent.is_some()), accounting)?;
        parents.value.push(GenerationId::new(request.base.digest));
        if let Some(parent) = request.merge_parent { parents.value.push(GenerationId::new(parent.digest)); }
        let Owned { value: parents, allocation, bytes } = parents;
        let root = Owned::new(GenerationRoot {
            volume_id: base.value.volume_id, root_file_id: base.value.root_file_id,
            file_table: request.file_table, parents,
            required_features: base.value.continuation_features(),
        }, allocation, bytes);
        let encoded = super::codec::encode_owned(accounting, |observe, reserve| {
            encode_generation_root(&root.value, observe, reserve)
        });
        root.retire(accounting)?;
        let (value, allocation) = encoded?;
        let bytes = usize_to_u64(value.capacity());
        let mut encoded = Owned::new(value, allocation, bytes);
        let written = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let KernelOwnedAllocation::NativeOriginal(guard) = &mut encoded.allocation else {
                    return Err(WorkError::Overflow.into());
                };
                let hashed = HashedObject::from_native_vec(
                    ObjectKind::GenerationRoot, &mut encoded.value, guard, original, cancellation,
                ).map_err(checkpoint_storage_error)?;
                let object = hashed.object_id();
                store.put_hashed_with_native_work(hashed, original, cancellation).await
                    .map_err(checkpoint_storage_error)?;
                return Ok(object);
            }
            let hashing = WorkCounters {
                bytes_hashed: usize_to_u64(encoded.value.len()).checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                    .ok_or(WorkError::Overflow)?, ..WorkCounters::UNCHARGED
            };
            accounting.admit(hashing)?;
            let hashed = HashedObject::new(ObjectKind::GenerationRoot, std::mem::take(&mut encoded.value).into());
            accounting.record_completed(hashing)?;
            let object = hashed.object_id();
            let receipt = store.put_hashed(hashed, accounting.remaining()?, cancellation).await;
            let spent = match &receipt { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
            super::file_read::complete_standalone_backend(accounting, spent, 0)?;
            receipt.map_err(|failure| checkpoint_storage_error(failure.error))?;
            Ok(object)
        }.await;
        encoded.retire(accounting)?;
        written
    }.await;
    base.retire(accounting)?;
    result
}

fn checkpoint_storage_error(error: ObjectStoreError) -> crate::FsError {
    match error {
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => error.into(),
    }
}

fn validate_request(request: CheckpointRequest) -> Result<(), CheckpointError> {
    if request.base.kind != ObjectKind::GenerationRoot
        || request
            .merge_parent
            .is_some_and(|parent| parent.kind != ObjectKind::GenerationRoot)
    {
        return Err(CheckpointError::WrongGenerationKind);
    }
    if request.file_table.kind != ObjectKind::FileTablePage {
        return Err(CheckpointError::WrongFileTableKind);
    }
    if request.merge_parent == Some(request.base) {
        return Err(CheckpointError::DuplicateParent);
    }
    Ok(())
}

fn validate_merge_parents(
    base: &GenerationRoot,
    parent: &GenerationRoot,
) -> Result<(), CheckpointError> {
    if parent.volume_id != base.volume_id {
        return Err(CheckpointError::CrossVolumeMerge);
    }
    if parent.root_file_id != base.root_file_id {
        return Err(CheckpointError::RootIdentityMismatch);
    }
    Ok(())
}

/// Immutable checkpoint construction failures.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum CheckpointError {
    /// Base or merge parent is not a generation-root object.
    #[error("checkpoint parent has the wrong object kind")]
    WrongGenerationKind,
    /// Candidate file table has the wrong object kind.
    #[error("checkpoint file table has the wrong object kind")]
    WrongFileTableKind,
    /// Merge repeats its base as second parent.
    #[error("checkpoint merge parents are duplicated")]
    DuplicateParent,
    /// Merge parent belongs to another volume.
    #[error("checkpoint cannot merge generations from different volumes")]
    CrossVolumeMerge,
    /// Merge parents disagree on the stable volume-root file identity.
    #[error("checkpoint merge parents have different root identities")]
    RootIdentityMismatch,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical generation encoding/decoding failed.
    #[error(transparent)]
    Decode(#[from] super::CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/checkpoint.rs"]
mod tests;
