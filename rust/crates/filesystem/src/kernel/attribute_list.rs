//! Bounded authenticated named-attribute pagination.

use super::attribute_mutation::AttributeFormat;
use super::persistent_pagination;
use super::{AttributeEntry, AttributeName, CanonicalDecodeError, DecodeLimits, DirectoryReadError, DirectoryReadFailure};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{ObjectId, ObjectStoreError};
use std::ops::Bound;
use thiserror::Error;

/// One bounded ordered named-attribute page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeListing {
    /// Entries strictly after the supplied exact cursor.
    pub entries: Vec<AttributeEntry>,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
    /// Exact unvisited metadata successor exposed by authenticated traversal.
    pub next_residency: Option<ResidencyHint>,
    /// Exact backend, traversal, copy, and logical-allocation work.
    pub work: WorkCounters,
}

/// Lists one bounded attribute page through the shared persistent-tree cursor.
pub async fn list_attributes<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    after: Option<&AttributeName>,
    maximum_entries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AttributeListing, AttributeListFailure> {
    persistent_pagination::paginate::<S, AttributeFormat>(
        store,
        root,
        after.map_or(Bound::Unbounded, Bound::Excluded),
        maximum_entries,
        limits,
        budget,
        cancellation,
        None,
    )
    .await
    .map(to_listing)
    .map_err(map_failure)
}

pub(crate) async fn list_attributes_async_with_accounting<S: AsyncObjectStore>(
    store: &S, root: ObjectId, after: Option<&AttributeName>, maximum_entries: u32,
    limits: DecodeLimits, accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken, data_custody: Option<&super::ProofDataCustody>,
) -> Result<(AttributeListing, super::allocation::KernelOwnedAllocation), crate::FsError> {
    persistent_pagination::paginate_with_accounting::<S, AttributeFormat>(
        store, root, after.map_or(Bound::Unbounded, Bound::Excluded), maximum_entries, limits, accounting, cancellation, data_custody,
    ).await.map(|(receipt, allocation)| (to_listing(receipt), allocation))
        .map_err(|error| match error {
            crate::FsError::Directory(error) => crate::FsError::AttributeList(map_error(error)),
            error => error,
        })
}

fn to_listing(receipt: persistent_pagination::Receipt<AttributeEntry>) -> AttributeListing {
    AttributeListing {
        entries: receipt.values,
        has_more: receipt.has_more,
        next_residency: receipt.next_request.map(|request| ResidencyHint {
            request,
            reason: ResidencyReason::MetadataSuccessor,
        }),
        work: receipt.work,
    }
}

/// Named-attribute pagination failure retaining exact spent work.
pub type AttributeListFailure = OperationFailure<AttributeListError>;

fn map_failure(failure: DirectoryReadFailure) -> AttributeListFailure {
    OperationFailure::new(map_error(failure.error), *failure.work)
}

fn map_error(error: DirectoryReadError) -> AttributeListError {
    match error {
        DirectoryReadError::Cancelled => AttributeListError::Cancelled,
        DirectoryReadError::ZeroLimit => AttributeListError::ZeroLimit,
        DirectoryReadError::LimitOverflow => AttributeListError::LimitOverflow,
        DirectoryReadError::WrongRootKind => AttributeListError::WrongRootKind,
        DirectoryReadError::InvalidLimits => AttributeListError::InvalidLimits,
        DirectoryReadError::HeightExceeded => AttributeListError::HeightExceeded,
        DirectoryReadError::Cycle => AttributeListError::CycleOrAlias,
        DirectoryReadError::ChildBoundsMismatch => {
            AttributeListError::ChildBoundsMismatch
        }
        DirectoryReadError::TraversalState => AttributeListError::TraversalState,
        DirectoryReadError::AllocationFailed => AttributeListError::AllocationFailed,
        DirectoryReadError::Storage(error) => error.into(),
        DirectoryReadError::Decode(error) => error.into(),
        DirectoryReadError::Work(error) => error.into(),
    }
}

/// Authenticated bounded named-attribute pagination failures.
#[derive(Debug, Error)]
pub enum AttributeListError {
    /// Cooperative cancellation occurred before the next backend boundary.
    #[error("attribute listing was cancelled")]
    Cancelled,
    /// Page bound must be positive.
    #[error("attribute page limit must be non-zero")]
    ZeroLimit,
    /// Page-bound allocation overflowed this target.
    #[error("attribute page limit cannot be represented")]
    LimitOverflow,
    /// Root is not an attribute page.
    #[error("attribute root is not an attribute page")]
    WrongRootKind,
    /// Decode/traversal limits are internally inconsistent.
    #[error("attribute listing limits are invalid")]
    InvalidLimits,
    /// Traversal exceeded the admitted height.
    #[error("attribute tree exceeds its admitted height")]
    HeightExceeded,
    /// Child graph references an ancestor or aliases a traversed page.
    #[error("attribute tree contains a cycle or alias")]
    CycleOrAlias,
    /// Parent routing bounds disagree with a child page.
    #[error("attribute child bounds do not match its page")]
    ChildBoundsMismatch,
    /// The private traversal reached an impossible state.
    #[error("attribute listing transition state is invalid")]
    TraversalState,
    /// A bounded scratch allocation could not be represented or reserved.
    #[error("attribute listing allocation failed")]
    AllocationFailed,
    /// Immutable object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical attribute page failed decoding.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/attribute_list.rs"]
mod tests;
