//! Bounded authenticated directory pagination without prefix enumeration.

use super::persistent_pagination;
use super::tree_mutation::TreeFormat;
use super::{CanonicalDecodeError, DecodeLimits, LogicalName, TreeEntry};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::{ObjectId, ObjectStoreError};
use thiserror::Error;

/// One authenticated bounded directory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryPage {
    /// Entries strictly after the supplied cursor.
    pub entries: Vec<TreeEntry>,
    /// Whether at least one additional entry exists.
    pub has_more: bool,
    /// Exact unvisited successor object, when the authenticated traversal
    /// exposed one without widening the requested listing.
    pub next_residency: Option<ResidencyHint>,
    /// Exact backend, traversal, copy, and logical-allocation work.
    pub work: WorkCounters,
}

/// Returns one bounded ordered directory page and an exact continuation fact.
///
/// The shared persistent-tree paginator reads only the cursor frontier and
/// enough successors to prove `maximum_entries + 1`.
///
/// # Errors
///
/// Fails closed on invalid bounds, malformed routing, cycles, storage failure,
/// or work outside the admitted budget.
pub fn list_tree_entries<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<DirectoryPage, DirectoryReadFailure> {
    persistent_pagination::paginate::<S, TreeFormat>(
        store,
        root,
        after,
        maximum_entries,
        limits,
        budget,
    )
    .map(to_page)
    .map_err(map_failure)
}

/// Asynchronously executes the same cursor machine as [`list_tree_entries`].
///
/// # Errors
///
/// Returns the same typed failures, including cancellation before every
/// backend boundary.
pub async fn list_tree_entries_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<DirectoryPage, DirectoryReadFailure> {
    persistent_pagination::paginate_async::<S, TreeFormat>(
        store,
        root,
        after,
        maximum_entries,
        limits,
        budget,
        cancellation,
    )
    .await
    .map(to_page)
    .map_err(map_failure)
}

fn to_page(receipt: persistent_pagination::Receipt<TreeEntry>) -> DirectoryPage {
    DirectoryPage {
        entries: receipt.values,
        has_more: receipt.has_more,
        next_residency: receipt.next_request.map(|request| ResidencyHint {
            request,
            reason: ResidencyReason::DirectorySuccessor,
        }),
        work: receipt.work,
    }
}

/// Bounded directory-read failure retaining exact spent work.
pub type DirectoryReadFailure = OperationFailure<DirectoryReadError>;

fn map_failure(failure: persistent_pagination::Failure) -> DirectoryReadFailure {
    OperationFailure::new(map_error(failure.error), *failure.work)
}

fn map_error(error: persistent_pagination::Error) -> DirectoryReadError {
    match error {
        persistent_pagination::Error::Cancelled => DirectoryReadError::Cancelled,
        persistent_pagination::Error::ZeroLimit => DirectoryReadError::ZeroLimit,
        persistent_pagination::Error::LimitOverflow => DirectoryReadError::LimitOverflow,
        persistent_pagination::Error::WrongRootKind => DirectoryReadError::WrongRootKind,
        persistent_pagination::Error::InvalidLimits => DirectoryReadError::InvalidLimits,
        persistent_pagination::Error::HeightExceeded => DirectoryReadError::HeightExceeded,
        persistent_pagination::Error::CycleOrAlias => DirectoryReadError::Cycle,
        persistent_pagination::Error::ChildBoundsMismatch => {
            DirectoryReadError::ChildBoundsMismatch
        }
        persistent_pagination::Error::TraversalState => DirectoryReadError::TraversalState,
        persistent_pagination::Error::AllocationFailed => DirectoryReadError::AllocationFailed,
        persistent_pagination::Error::Storage(error) => error.into(),
        persistent_pagination::Error::Decode(error) => error.into(),
        persistent_pagination::Error::Work(error) => error.into(),
    }
}

/// Authenticated bounded directory-read failures.
#[derive(Debug, Error)]
pub enum DirectoryReadError {
    /// Cooperative cancellation occurred before the next storage boundary.
    #[error("directory listing was cancelled")]
    Cancelled,
    /// The private driver attempted an impossible transition.
    #[error("directory listing transition state is invalid")]
    TraversalState,
    /// Page bound must be positive.
    #[error("directory page limit must be non-zero")]
    ZeroLimit,
    /// Page-bound allocation overflowed this target.
    #[error("directory page limit cannot be represented")]
    LimitOverflow,
    /// Root is not a tree page.
    #[error("directory root is not a tree page")]
    WrongRootKind,
    /// Decode/traversal limits are internally inconsistent.
    #[error("directory listing limits are invalid")]
    InvalidLimits,
    /// Traversal exceeded the admitted height.
    #[error("directory tree exceeds its admitted height")]
    HeightExceeded,
    /// Child graph references an ancestor or aliases a traversed page.
    #[error("directory tree contains a cycle or alias")]
    Cycle,
    /// Parent routing bounds disagree with a child page.
    #[error("directory child bounds do not match its page")]
    ChildBoundsMismatch,
    /// A bounded scratch allocation could not be represented or reserved.
    #[error("directory listing allocation failed")]
    AllocationFailed,
    /// Immutable object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical tree page failed decoding.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/list.rs"]
mod tests;
