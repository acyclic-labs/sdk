//! Immutable checkpoint and two-parent merge-generation construction.

use super::{DecodeLimits, GenerationRoot, decode_generation_root, encode_generation_root};
use crate::cancellation::CancellationToken;
use crate::foundation::GenerationId;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStore, ObjectStoreError,
    object_digest,
};
use bytes::Bytes;
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

/// Candidate checkpoint construction evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointReceipt {
    /// Candidate generation-root object.
    pub root: ObjectId,
    /// Content-addressed generation identity.
    pub generation_id: GenerationId,
    /// Whether the unchanged single-parent request reused its base exactly.
    pub reused_base: bool,
    /// Exact reads, encoding, hashing, and immutable writes.
    pub work: WorkCounters,
}

/// Checkpoint construction failure retaining all spent work and orphan writes.
pub type CheckpointFailure = OperationFailure<CheckpointError>;

/// Constructs an unpublished checkpoint or merge generation.
///
/// The volume and root-file identity come only from authenticated parent roots.
/// An unchanged single-parent checkpoint reuses the base generation without a
/// write. A merge parent must be distinct and belong to the same volume with the
/// same root identity. Authority publication and complete closure proof remain
/// separate, mandatory operations.
///
/// # Errors
///
/// Rejects wrong object classes before backend work, malformed/missing parents,
/// cross-volume/root merges, duplicate parents, storage failures, and work
/// outside the admitted budget.
pub fn build_checkpoint<S: ObjectStore>(
    store: &S,
    request: CheckpointRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<CheckpointReceipt, CheckpointFailure> {
    validate_request(request)?;
    let (base, mut work) = read_root(store, request.base, limits, budget, WorkCounters::default())?;
    let merge = match request.merge_parent {
        Some(parent_id) => {
            let (parent, next) = read_root(store, parent_id, limits, budget, work)?;
            work = next;
            validate_merge_parents(&base, &parent, work)?;
            Some(parent_id)
        }
        None => None,
    };
    if merge.is_none() && request.file_table == base.file_table {
        return Ok(reused_checkpoint(request.base, work));
    }
    let (object, encoded, prospective) = encode_checkpoint(request, &base, merge, work, budget)?;
    let remaining = prospective
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let receipt = store
        .put(object, Bytes::from(encoded), remaining)
        .map_err(|failure| failure.map_with_prior_work(prospective, Into::into))?;
    work = prospective
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), prospective))?;
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(CheckpointReceipt {
        root: object,
        generation_id: GenerationId::new(object.digest),
        reused_base: false,
        work,
    })
}

/// Asynchronously constructs an unpublished checkpoint or merge generation.
///
/// This is the nonblocking execution surface for browser and remote stores;
/// it preserves the same validation, no-op reuse, canonical encoding, and
/// exact work receipts as [`build_checkpoint`].
///
/// # Errors
///
/// Returns the same typed failures as [`build_checkpoint`], including
/// cancellation from the asynchronous object boundary.
pub async fn build_checkpoint_async<S: crate::async_storage::AsyncObjectStore>(
    store: &S,
    request: CheckpointRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<CheckpointReceipt, CheckpointFailure> {
    validate_request(request)?;
    let (base, mut work) = read_root_async(
        store,
        request.base,
        limits,
        budget,
        WorkCounters::default(),
        cancellation,
    )
    .await?;
    let merge = match request.merge_parent {
        Some(parent_id) => {
            let (parent, next) =
                read_root_async(store, parent_id, limits, budget, work, cancellation).await?;
            work = next;
            validate_merge_parents(&base, &parent, work)?;
            Some(parent_id)
        }
        None => None,
    };
    if merge.is_none() && request.file_table == base.file_table {
        return Ok(reused_checkpoint(request.base, work));
    }
    let (object, encoded, prospective) = encode_checkpoint(request, &base, merge, work, budget)?;
    let remaining = prospective
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let receipt = crate::async_storage::AsyncObjectStore::put(
        store,
        object,
        Bytes::from(encoded),
        remaining,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(prospective, Into::into))?;
    work = prospective
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), prospective))?;
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(CheckpointReceipt {
        root: object,
        generation_id: GenerationId::new(object.digest),
        reused_base: false,
        work,
    })
}

fn validate_request(request: CheckpointRequest) -> Result<(), CheckpointFailure> {
    if request.base.kind != ObjectKind::GenerationRoot
        || request
            .merge_parent
            .is_some_and(|parent| parent.kind != ObjectKind::GenerationRoot)
    {
        return Err(OperationFailure::before_work(
            CheckpointError::WrongGenerationKind,
        ));
    }
    if request.file_table.kind != ObjectKind::FileTablePage {
        return Err(OperationFailure::before_work(
            CheckpointError::WrongFileTableKind,
        ));
    }
    if request.merge_parent == Some(request.base) {
        return Err(OperationFailure::before_work(
            CheckpointError::DuplicateParent,
        ));
    }
    Ok(())
}

fn validate_merge_parents(
    base: &GenerationRoot,
    parent: &GenerationRoot,
    work: WorkCounters,
) -> Result<(), CheckpointFailure> {
    if parent.volume_id != base.volume_id {
        return Err(OperationFailure::new(
            CheckpointError::CrossVolumeMerge,
            work,
        ));
    }
    if parent.root_file_id != base.root_file_id {
        return Err(OperationFailure::new(
            CheckpointError::RootIdentityMismatch,
            work,
        ));
    }
    Ok(())
}

fn reused_checkpoint(root: ObjectId, work: WorkCounters) -> CheckpointReceipt {
    CheckpointReceipt {
        root,
        generation_id: GenerationId::new(root.digest),
        reused_base: true,
        work,
    }
}

fn encode_checkpoint(
    request: CheckpointRequest,
    base: &GenerationRoot,
    merge: Option<ObjectId>,
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<(ObjectId, Vec<u8>, WorkCounters), CheckpointFailure> {
    let parent_count = 1_usize + usize::from(merge.is_some());
    let mut parents = Vec::with_capacity(parent_count);
    parents.push(GenerationId::new(request.base.digest));
    if let Some(parent) = merge {
        parents.push(GenerationId::new(parent.digest));
    }
    let root = GenerationRoot {
        volume_id: base.volume_id,
        root_file_id: base.root_file_id,
        file_table: request.file_table,
        parents,
        required_features: base.required_features,
    };
    let encoded =
        encode_generation_root(&root).map_err(|error| OperationFailure::new(error.into(), work))?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &encoded),
    };
    let encoded_capacity = u64::try_from(encoded.capacity()).unwrap_or(u64::MAX);
    let parent_capacity = u64::try_from(root.parents.capacity())
        .unwrap_or(u64::MAX)
        .checked_mul(u64::try_from(size_of::<GenerationId>()).unwrap_or(u64::MAX))
        .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?;
    let peak_allocation_bytes = encoded_capacity
        .checked_add(parent_capacity)
        .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?;
    let encoded_length = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    let prospective = work
        .checked_add(WorkCounters {
            bytes_encoded: encoded_length,
            bytes_hashed: encoded_length.saturating_add(OBJECT_DIGEST_ENVELOPE_BYTES),
            allocation_operations: 2,
            peak_allocation_bytes,
            ..WorkCounters::default()
        })
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok((object, encoded, prospective))
}

fn read_root<S: ObjectStore>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    budget: WorkBudget,
    work: WorkCounters,
) -> Result<(GenerationRoot, WorkCounters), CheckpointFailure> {
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let receipt = store
        .read(object, limits.maximum_object_bytes, remaining)
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    let combined = work
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let root = decode_generation_root(&receipt.value, limits)
        .map_err(|error| OperationFailure::new(error.into(), combined))?;
    Ok((root, combined))
}

async fn read_root_async<S: crate::async_storage::AsyncObjectStore>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    budget: WorkBudget,
    work: WorkCounters,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, WorkCounters), CheckpointFailure> {
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let receipt = crate::async_storage::AsyncObjectStore::read(
        store,
        object,
        limits.maximum_object_bytes,
        remaining,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    let combined = work
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let root = decode_generation_root(&receipt.value.bytes, limits)
        .map_err(|error| OperationFailure::new(error.into(), combined))?;
    Ok((root, combined))
}

/// Immutable checkpoint construction failures.
#[derive(Debug, Error)]
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
