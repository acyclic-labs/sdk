//! Resumable bounded generation transfer over the asynchronous object boundary.

use super::{
    ClosureError, ClosureLimits, GenerationExportManifest, GenerationExportManifestError,
    prove_generation_closure_async, validate_generation_export_manifest,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::{CancellationError, CancellationToken};
use crate::foundation::VolumeId;
use crate::model::VolumeConfig;
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use crate::storage::{ObjectId, ObjectRead, ObjectReadRequest, ObjectStoreError};
use bytes::Bytes;
use std::mem::size_of;
use thiserror::Error;

/// Stable next-object position in a canonical generation manifest.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct TransferCursor(u64);

impl TransferCursor {
    /// Initial transfer position.
    pub const START: Self = Self(0);

    /// Constructs one manifest-relative object position.
    #[must_use]
    pub const fn new(next_object: u64) -> Self {
        Self(next_object)
    }

    /// Returns the manifest-relative next-object position.
    #[must_use]
    pub const fn next_object(self) -> u64 {
        self.0
    }
}

/// One ordered bounded export page. Object identities are the corresponding
/// manifest slice beginning at `first_object`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationTransferBatch {
    /// First manifest object represented by `objects`.
    pub first_object: TransferCursor,
    /// Cursor for the next request, absent at the canonical end.
    pub next: Option<TransferCursor>,
    /// Ordered immutable bodies with explicit retention evidence.
    pub objects: Vec<ObjectRead>,
}

/// Transfer validation, storage, closure, cancellation, and work failures.
#[derive(Debug, Error)]
pub enum GenerationTransferError {
    /// Operation was cancelled before further transfer work.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// Manifest bytes or fields are noncanonical or out of bounds.
    #[error(transparent)]
    Manifest(#[from] GenerationExportManifestError),
    /// Imported or exported closure authentication failed.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Immutable object storage failed.
    #[error(transparent)]
    Object(#[from] ObjectStoreError),
    /// Authenticated closure facts do not exactly match the manifest.
    #[error("generation transfer manifest does not match its authenticated closure")]
    ManifestMismatch,
    /// Cursor is beyond the canonical manifest end.
    #[error("generation transfer cursor is outside the manifest")]
    InvalidCursor,
    /// A nonterminal transfer page must request at least one object.
    #[error("generation transfer batch bound must be positive")]
    EmptyBatch,
    /// A transfer page exceeds its caller or manifest object bound.
    #[error("generation transfer batch exceeds its object bound")]
    TooManyObjects,
    /// Request-vector allocation failed after bounded admission.
    #[error("generation transfer request allocation failed")]
    AllocationFailed,
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// Receipt-bearing generation transfer result.
pub type GenerationTransferResult<T> = MeasuredResult<OperationReceipt<T>, GenerationTransferError>;

/// Authenticates a generation closure and builds its canonical transfer manifest.
///
/// # Errors
///
/// Returns typed cancellation, storage, closure, identity, configuration, or
/// bounded-work failures.
pub async fn build_generation_export_manifest_async<S: AsyncObjectStore>(
    store: &S,
    volume_id: VolumeId,
    config: VolumeConfig,
    generation_root: ObjectId,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> GenerationTransferResult<GenerationExportManifest> {
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    config
        .validate()
        .map_err(GenerationExportManifestError::from)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let proof = prove_generation_closure_async(
        store,
        generation_root,
        closure_limits,
        budget,
        cancellation,
    )
    .await
    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let manifest = GenerationExportManifest {
        volume_id,
        config,
        generation_root,
        generation_id: proof.generation_id,
        objects: proof.objects,
        file_count: proof.file_count,
    };
    validate_generation_export_manifest(&manifest, config.limits.maximum_objects_per_generation)
        .map_err(|error| OperationFailure::new(error.into(), proof.work))?;
    if proof.root.volume_id != volume_id {
        return Err(OperationFailure::new(
            GenerationTransferError::ManifestMismatch,
            proof.work,
        ));
    }
    Ok(OperationReceipt {
        value: manifest,
        work: proof.work,
    })
}

/// Reauthenticates a completely imported generation against its manifest.
///
/// # Errors
///
/// Returns before backend work for malformed manifests, otherwise preserving
/// exact closure work on missing, corrupt, foreign, or mismatched objects.
pub async fn authenticate_generation_export_manifest_async<S: AsyncObjectStore>(
    store: &S,
    manifest: &GenerationExportManifest,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> GenerationTransferResult<()> {
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    validate_generation_export_manifest(
        manifest,
        manifest.config.limits.maximum_objects_per_generation,
    )
    .map_err(|error| OperationFailure::before_work(error.into()))?;
    let proof = prove_generation_closure_async(
        store,
        manifest.generation_root,
        closure_limits,
        budget,
        cancellation,
    )
    .await
    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    if proof.root.volume_id != manifest.volume_id
        || proof.generation_id != manifest.generation_id
        || proof.objects != manifest.objects
        || proof.file_count != manifest.file_count
    {
        return Err(OperationFailure::new(
            GenerationTransferError::ManifestMismatch,
            proof.work,
        ));
    }
    Ok(OperationReceipt {
        value: (),
        work: proof.work,
    })
}

/// Reads one resumable manifest page using the backend's ordered batch primitive.
///
/// The request vector is bounded and accounted before allocation. A terminal
/// cursor returns an empty terminal page without backend work.
///
/// # Errors
///
/// Rejects malformed manifests, invalid cursors/bounds, cancellation,
/// allocation, storage, and work-budget failures without losing spent work.
pub async fn export_generation_batch_async<S: AsyncObjectStore + ?Sized>(
    store: &S,
    manifest: &GenerationExportManifest,
    cursor: TransferCursor,
    maximum_objects: u32,
    maximum_object_bytes: u64,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> GenerationTransferResult<GenerationTransferBatch> {
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    validate_generation_export_manifest(
        manifest,
        manifest.config.limits.maximum_objects_per_generation,
    )
    .map_err(|error| OperationFailure::before_work(error.into()))?;
    if maximum_objects == 0 || maximum_object_bytes == 0 {
        return Err(OperationFailure::before_work(
            GenerationTransferError::EmptyBatch,
        ));
    }
    let object_count = u64::try_from(manifest.objects.len()).unwrap_or(u64::MAX);
    if cursor.0 > object_count {
        return Err(OperationFailure::before_work(
            GenerationTransferError::InvalidCursor,
        ));
    }
    if cursor.0 == object_count {
        return Ok(OperationReceipt {
            value: GenerationTransferBatch {
                first_object: cursor,
                next: None,
                objects: Vec::new(),
            },
            work: WorkCounters::default(),
        });
    }
    let remaining_objects = object_count - cursor.0;
    let count = remaining_objects.min(u64::from(maximum_objects));
    let start = usize::try_from(cursor.0)
        .map_err(|_| OperationFailure::before_work(GenerationTransferError::InvalidCursor))?;
    let count_usize = usize::try_from(count)
        .map_err(|_| OperationFailure::before_work(GenerationTransferError::TooManyObjects))?;
    let request_bytes = count
        .checked_mul(u64::try_from(size_of::<ObjectReadRequest>()).unwrap_or(u64::MAX))
        .ok_or_else(|| {
            OperationFailure::before_work(GenerationTransferError::Work(WorkError::Overflow))
        })?;
    let mut work = WorkCounters {
        bytes_copied: request_bytes,
        allocation_operations: 1,
        peak_allocation_bytes: request_bytes,
        ..WorkCounters::default()
    };
    work.verify(budget)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let mut requests = Vec::new();
    requests
        .try_reserve_exact(count_usize)
        .map_err(|_| OperationFailure::new(GenerationTransferError::AllocationFailed, work))?;
    let admitted_object_bytes =
        maximum_object_bytes.min(manifest.config.limits.maximum_object_bytes);
    for object_id in &manifest.objects[start..start + count_usize] {
        requests.push(ObjectReadRequest {
            object_id: *object_id,
            maximum_bytes: admitted_object_bytes,
        });
    }
    let nested = store
        .read_many(
            &requests,
            work.remaining(budget).map_err(|error| {
                OperationFailure::new(GenerationTransferError::Work(error), work)
            })?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, GenerationTransferError::from))?;
    let nested_peak = request_bytes.saturating_add(nested.work.peak_allocation_bytes);
    work = work
        .checked_add(nested.work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(nested_peak);
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let next_index = cursor.0 + count;
    Ok(OperationReceipt {
        value: GenerationTransferBatch {
            first_object: cursor,
            next: (next_index < object_count).then_some(TransferCursor(next_index)),
            objects: nested.value,
        },
        work,
    })
}

/// Idempotently imports one manifest-aligned page of immutable object bodies.
///
/// A failure may leave a valid immutable prefix, so callers retry the same
/// cursor and bodies. Canonical object identity makes that retry exactly safe.
///
/// # Errors
///
/// Rejects malformed manifests, cursor/body bounds, cancellation, storage, or
/// exact-work failures while preserving work spent on an imported prefix.
pub async fn import_generation_batch_async<S: AsyncObjectStore + ?Sized>(
    store: &S,
    manifest: &GenerationExportManifest,
    cursor: TransferCursor,
    bodies: &[Bytes],
    maximum_objects: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> GenerationTransferResult<TransferCursor> {
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    validate_generation_export_manifest(
        manifest,
        manifest.config.limits.maximum_objects_per_generation,
    )
    .map_err(|error| OperationFailure::before_work(error.into()))?;
    if maximum_objects == 0 {
        return Err(OperationFailure::before_work(
            GenerationTransferError::EmptyBatch,
        ));
    }
    let object_count = u64::try_from(manifest.objects.len()).unwrap_or(u64::MAX);
    let body_count = u64::try_from(bodies.len()).unwrap_or(u64::MAX);
    if cursor.0 > object_count {
        return Err(OperationFailure::before_work(
            GenerationTransferError::InvalidCursor,
        ));
    }
    if cursor.0 == object_count && bodies.is_empty() {
        return Ok(OperationReceipt {
            value: cursor,
            work: WorkCounters::default(),
        });
    }
    if bodies.is_empty() {
        return Err(OperationFailure::before_work(
            GenerationTransferError::EmptyBatch,
        ));
    }
    if body_count > u64::from(maximum_objects)
        || cursor
            .0
            .checked_add(body_count)
            .is_none_or(|end| end > object_count)
    {
        return Err(OperationFailure::before_work(
            GenerationTransferError::TooManyObjects,
        ));
    }
    let start = usize::try_from(cursor.0)
        .map_err(|_| OperationFailure::before_work(GenerationTransferError::InvalidCursor))?;
    let mut work = WorkCounters::default();
    for (offset, body) in bodies.iter().enumerate() {
        cancellation
            .check()
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let object_id = manifest.objects[start + offset];
        let receipt = store
            .put(
                object_id,
                body.clone(),
                work.remaining(budget).map_err(|error| {
                    OperationFailure::new(GenerationTransferError::Work(error), work)
                })?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(work, GenerationTransferError::from))?;
        work = work
            .checked_add(receipt.work)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
    }
    Ok(OperationReceipt {
        value: TransferCursor(cursor.0 + body_count),
        work,
    })
}

#[cfg(test)]
#[path = "tests/transfer.rs"]
mod tests;
