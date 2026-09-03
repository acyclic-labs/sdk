//! Bounded authenticated named-attribute pagination.

use super::attribute_mutation::AttributeFormat;
use super::persistent_pagination;
use super::{AttributeEntry, AttributeName, CanonicalDecodeError, DecodeLimits};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{ObjectId, ObjectStoreError};
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
///
/// # Errors
///
/// Fails closed on malformed routing, cycles, invalid bounds, storage failure,
/// or work outside the admitted budget.
pub fn list_attributes<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    after: Option<&AttributeName>,
    maximum_entries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<AttributeListing, AttributeListFailure> {
    persistent_pagination::paginate::<S, AttributeFormat>(
        store,
        root,
        after,
        maximum_entries,
        limits,
        budget,
    )
    .map(to_listing)
    .map_err(map_failure)
}

/// Asynchronously executes the same cursor machine as [`list_attributes`].
///
/// # Errors
///
/// Returns the same typed failures, including cancellation before every
/// backend boundary.
pub async fn list_attributes_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    after: Option<&AttributeName>,
    maximum_entries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AttributeListing, AttributeListFailure> {
    persistent_pagination::paginate_async::<S, AttributeFormat>(
        store,
        root,
        after,
        maximum_entries,
        limits,
        budget,
        cancellation,
    )
    .await
    .map(to_listing)
    .map_err(map_failure)
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

fn map_failure(failure: persistent_pagination::Failure) -> AttributeListFailure {
    OperationFailure::new(map_error(failure.error), *failure.work)
}

fn map_error(error: persistent_pagination::Error) -> AttributeListError {
    match error {
        persistent_pagination::Error::Cancelled => AttributeListError::Cancelled,
        persistent_pagination::Error::ZeroLimit => AttributeListError::ZeroLimit,
        persistent_pagination::Error::LimitOverflow => AttributeListError::LimitOverflow,
        persistent_pagination::Error::WrongRootKind => AttributeListError::WrongRootKind,
        persistent_pagination::Error::InvalidLimits => AttributeListError::InvalidLimits,
        persistent_pagination::Error::HeightExceeded => AttributeListError::HeightExceeded,
        persistent_pagination::Error::CycleOrAlias => AttributeListError::CycleOrAlias,
        persistent_pagination::Error::ChildBoundsMismatch => {
            AttributeListError::ChildBoundsMismatch
        }
        persistent_pagination::Error::TraversalState => AttributeListError::TraversalState,
        persistent_pagination::Error::AllocationFailed => AttributeListError::AllocationFailed,
        persistent_pagination::Error::Storage(error) => error.into(),
        persistent_pagination::Error::Decode(error) => error.into(),
        persistent_pagination::Error::Work(error) => error.into(),
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
