//! Immutable checkpoint and two-parent merge-generation construction.

use super::{DecodeLimits, GenerationRoot, decode_generation_root, encode_generation_root};
use crate::cancellation::CancellationToken;
use crate::foundation::{GenerationId, usize_to_u64};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStore, ObjectStoreError,
    object_digest,
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
        .put(object, encoded.into(), remaining)
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
pub async fn build_checkpoint_async<S: crate::async_storage::AsyncObjectStore>(
    store: &S,
    request: CheckpointRequest,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<CheckpointReceipt, CheckpointFailure> {
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
            .await.map_err(|error| OperationFailure::new(
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
        crate::FsError::Decode(error) => error.into(),
        crate::FsError::Object(error) => error.into(),
        crate::FsError::Cancelled(_) => ObjectStoreError::Cancelled.into(),
        _ => unreachable!("standalone checkpoint returns only checkpoint, codec, storage, cancellation, or work errors"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn build_checkpoint_with_native_work<S: crate::AsyncObjectStore>(
    store: &S,
    request: CheckpointRequest,
    limits: DecodeLimits,
    original: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
) -> Result<CheckpointReceipt, crate::FsError> {
    let mut accounting = super::allocation::KernelAccounting::native(original, cancellation);
    build_checkpoint_with_accounting(store, request, limits, &mut accounting, cancellation).await
}

async fn checkpoint_parent<S: crate::AsyncObjectStore>(
    store: &S, object: ObjectId, limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(GenerationRoot, super::allocation::KernelOwnedAllocation), crate::FsError> {
    #[cfg(not(target_arch = "wasm32"))]
    if accounting.native_work().is_some() {
        return super::generation::read_generation_root_with_limits_and_accounting(
            store, object, limits, accounting, cancellation,
        ).await;
    }
    let result = read_root_async(
        store, object, limits, accounting.remaining()?, WorkCounters::default(), cancellation,
    ).await;
    let spent = match &result { Ok((_, work)) => *work, Err(error) => *error.work };
    accounting.charge(spent)?;
    result.map(|(root, _)| (root, super::allocation::KernelOwnedAllocation::Standalone))
        .map_err(|failure| failure.error.into())
}

pub(crate) async fn build_checkpoint_with_accounting<S: crate::AsyncObjectStore>(
    store: &S, request: CheckpointRequest, limits: DecodeLimits,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<CheckpointReceipt, crate::FsError> {
    validate_request(request).map_err(|failure| crate::FsError::from(failure.error))?;
    accounting.check()?;
    let base = checkpoint_parent(store, request.base, limits, accounting, cancellation).await?;
    let result = async {
        if let Some(parent_id) = request.merge_parent {
            let parent = checkpoint_parent(store, parent_id, limits, accounting, cancellation).await?;
            let valid = validate_merge_parents(&base.0, &parent.0, accounting.spent())
                .map_err(|failure| crate::FsError::from(failure.error));
            drop(parent);
            valid?;
        }
        if request.merge_parent.is_none() && request.file_table == base.0.file_table {
            return Ok(reused_checkpoint(request.base, accounting.spent()));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let count = 1 + usize::from(request.merge_parent.is_some());
            let requested = usize_to_u64(count * size_of::<GenerationId>());
            let allocation = accounting.claim_owned(requested, 1)?;
            // Tuple destruction always frees the vector before its paid guard.
            let mut parents = (Vec::new(), allocation);
            if parents.0.try_reserve_exact(count).is_err() {
                return Err(crate::FsError::HandleAllocationFailed);
            }
            let actual = usize_to_u64(parents.0.capacity() * size_of::<GenerationId>());
            accounting.reconcile_owned(&mut parents.1, requested, actual)?;
            accounting.check()?;
            parents.0.push(GenerationId::new(request.base.digest));
            if let Some(parent) = request.merge_parent {
                parents.0.push(GenerationId::new(parent.digest));
            }
            let root = (GenerationRoot {
                volume_id: base.0.volume_id, root_file_id: base.0.root_file_id,
                file_table: request.file_table, parents: parents.0,
                required_features: base.0.continuation_features(),
            }, parents.1);
            let mut encoded = crate::kernel::codec::encode_owned(accounting, |observe, _reserve| { cancellation.check()?; super::generation::encode_generation_root(&root.0, observe, crate::FsError::EncodedAllocationFailed) })?;
            drop(root);
            let guard = match &mut encoded.1 {
                super::allocation::KernelOwnedAllocation::NativeOriginal(guard) => guard,
                _ => unreachable!("native encoding retains original custody"),
            };
            let hashed = crate::storage::HashedObject::from_native_vec(
                ObjectKind::GenerationRoot, &mut encoded.0, guard, original, cancellation,
            ).map_err(native_checkpoint_storage_error)?;
            let object = hashed.object_id();
            store.put_hashed_with_native_work(hashed, original, cancellation).await
                .map_err(native_checkpoint_storage_error)?;
            return Ok(CheckpointReceipt {
                root: object, generation_id: GenerationId::new(object.digest),
                reused_base: false, work: accounting.spent(),
            });
        }
        let (object, encoded, prospective) = encode_checkpoint(
            request, &base.0, request.merge_parent, WorkCounters::default(), accounting.remaining()?,
        ).map_err(|failure| crate::FsError::from(failure.error))?;
        accounting.charge(prospective)?;
        let receipt = store.put(object, encoded.into(), accounting.remaining()?, cancellation).await;
        let spent = match &receipt { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        accounting.charge(spent)?;
        receipt.map_err(|failure| crate::FsError::from(failure.error))?;
        Ok(CheckpointReceipt {
            root: object, generation_id: GenerationId::new(object.digest),
            reused_base: false, work: accounting.spent(),
        })
    }.await;
    drop(base);
    result
}

#[cfg(not(target_arch = "wasm32"))]
fn native_checkpoint_storage_error(error: ObjectStoreError) -> crate::FsError {
    match error {
        ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => error.into(),
    }
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
        required_features: base.continuation_features(),
    };
    let encoded =
        encode_generation_root(&root, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed).map_err(|error| OperationFailure::new(error.into(), work))?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &encoded),
    };
    let encoded_capacity = usize_to_u64(encoded.capacity());
    let parent_capacity = usize_to_u64(root.parents.capacity())
        .checked_mul(usize_to_u64(size_of::<GenerationId>()))
        .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?;
    let peak_allocation_bytes = encoded_capacity
        .checked_add(parent_capacity)
        .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?;
    let encoded_length = usize_to_u64(encoded.len());
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
    let root = decode_generation_root(&receipt.value, limits, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))
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
    let root = decode_generation_root(&receipt.value.bytes, limits, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))
        .map_err(|error| OperationFailure::new(error.into(), combined))?;
    Ok((root, combined))
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
