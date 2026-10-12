//! Bounded authenticated named-attribute lookup.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::attribute_mutation::AttributeFormat;
use super::{persistent_batch, persistent_point};
use super::{
    AttributeEntry, AttributeName, CanonicalDecodeError, DecodeLimits,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectStoreError};
use thiserror::Error;

/// Looks up named attributes while reading each distinct frontier once.
pub async fn lookup_attributes<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    names: &[AttributeName],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<Vec<Option<AttributeEntry>>>, AttributeLookupFailure> {
    persistent_batch::lookup_async::<S, AttributeFormat>(
        store,
        root,
        names,
        maximum_queries,
        limits,
        budget,
        cancellation,
    )
    .await
    .map_err(map_batch_failure)
}

fn map_batch_failure(failure: persistent_batch::Failure) -> AttributeLookupFailure {
    OperationFailure::new(map_batch_error(failure.error), *failure.work)
}

fn map_batch_error(error: persistent_batch::Error) -> AttributeLookupError {
    match error {
        persistent_batch::Error::Cancelled => AttributeLookupError::Cancelled,
        persistent_batch::Error::Empty => AttributeLookupError::EmptyBatch,
        persistent_batch::Error::TooManyQueries => AttributeLookupError::TooManyQueries,
        persistent_batch::Error::WrongRootKind => AttributeLookupError::WrongRootKind,
        persistent_batch::Error::InvalidLimits => AttributeLookupError::InvalidLimits,
        persistent_batch::Error::HeightExceeded => AttributeLookupError::HeightExceeded,
        persistent_batch::Error::CycleOrAlias => AttributeLookupError::CycleOrAlias,
        persistent_batch::Error::ChildBoundsMismatch => AttributeLookupError::ChildBoundsMismatch,
        persistent_batch::Error::InvalidRouting => AttributeLookupError::InvalidRouting,
        persistent_batch::Error::AllocationFailed => AttributeLookupError::AllocationFailed,
        persistent_batch::Error::Storage(ObjectStoreError::Cancelled) => {
            AttributeLookupError::Cancelled
        }
        persistent_batch::Error::Storage(error) => error.into(),
        persistent_batch::Error::Decode(error) => error.into(),
        persistent_batch::Error::Work(error) => error.into(),
    }
}

/// Named-attribute lookup failure retaining all exact spent work.
pub type AttributeLookupFailure = OperationFailure<AttributeLookupError>;

/// Looks up one exact named attribute through one authenticated frontier.
pub async fn lookup_attribute<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    name: &AttributeName,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<Option<AttributeEntry>>, AttributeLookupFailure> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    lookup_attribute_async_with_accounting(store, root, name, limits, &mut accounting, cancellation)
        .await
        .map(|(value, _)| OperationReceipt { value, work: accounting.spent() })
        .map_err(|error| failed(project_scalar_error(error), accounting.spent()))
}

pub(crate) async fn lookup_attribute_async_with_accounting<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    name: &AttributeName,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(Option<AttributeEntry>, KernelOwnedAllocation), crate::FsError> {
    let (value, allocation) = persistent_point::lookup_async_with_accounting::<S, AttributeFormat>(
        store, root, name, limits, accounting, cancellation, None,
    ).await.map_err(|error| match error {
        crate::FsError::FileRecord(error) => map_batch_error(super::file_table::unmap_batch_error(error)).into(),
        crate::FsError::PendingMutationAllocationFailed => AttributeLookupError::AllocationFailed.into(),
        error => error,
    })?;
    let bytes = value.as_ref().map_or(0, |entry| usize_to_u64(entry.name.retained_bytes()));
    let owned = persistent_point::Owned::new(value, allocation, bytes);
    if let Err(error) = accounting.charge(WorkCounters {
        items_returned: u64::from(owned.value.is_some()), ..WorkCounters::UNCHARGED
    }) {
        owned.retire(accounting)?;
        return Err(error);
    }
    let persistent_point::Owned { value, allocation, .. } = owned;
    Ok((value, allocation))
}

fn project_scalar_error(error: crate::FsError) -> AttributeLookupError {
    match error {
        crate::FsError::AttributeLookup(error) => error,
        crate::FsError::Cancelled(_) => AttributeLookupError::Cancelled,
        crate::FsError::Work(error) => AttributeLookupError::Work(error),
        crate::FsError::Object(error) => map_storage(error),
        crate::FsError::Decode(error) => AttributeLookupError::Decode(error),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => AttributeLookupError::Storage(ObjectStoreError::Native(error)),
        _ => unreachable!("scalar attribute leaves return only typed attribute, object, decode, work or cancellation errors"),
    }
}

fn map_storage(error: ObjectStoreError) -> AttributeLookupError {
    match error {
        ObjectStoreError::Cancelled => AttributeLookupError::Cancelled,
        error => AttributeLookupError::Storage(error),
    }
}

fn failed(error: AttributeLookupError, work: WorkCounters) -> AttributeLookupFailure {
    OperationFailure::new(error, work)
}

/// Authenticated named-attribute lookup failures.
#[derive(Debug, Error)]
pub enum AttributeLookupError {
    /// Cancellation occurred before a storage boundary.
    #[error("attribute lookup was cancelled")]
    Cancelled,
    /// Batch lookup requires at least one name.
    #[error("attribute lookup batch is empty")]
    EmptyBatch,
    /// Batch lookup exceeds its explicit query limit.
    #[error("attribute lookup batch exceeds its admitted query bound")]
    TooManyQueries,
    /// Root is not an attribute page.
    #[error("attribute lookup root is not an attribute page")]
    WrongRootKind,
    /// Traversal/page limits are invalid.
    #[error("attribute lookup limits are invalid")]
    InvalidLimits,
    /// Traversal exceeded the admitted height.
    #[error("attribute tree exceeds its admitted height")]
    HeightExceeded,
    /// Authenticated graph cycles or aliases.
    #[error("attribute tree contains a cycle or alias")]
    CycleOrAlias,
    /// A parent route does not match child bounds.
    #[error("attribute child bounds mismatch")]
    ChildBoundsMismatch,
    /// No child can route the requested name.
    #[error("attribute tree routing is invalid")]
    InvalidRouting,
    /// Bounded scratch allocation failed.
    #[error("attribute lookup scratch allocation failed")]
    AllocationFailed,
    /// Immutable object storage failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical attribute page failed decoding.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Work accounting overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/attribute_access.rs"]
mod tests;
