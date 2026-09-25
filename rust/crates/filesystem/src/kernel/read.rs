//! Sparse authenticated reads with exact implementation-work receipts.

use super::persistent_batch;
use super::persistent_point;
use super::tree_mutation::TreeFormat;
use super::{CanonicalDecodeError, DecodeLimits, LogicalName, TreeEntry};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectStoreError};
use thiserror::Error;

/// Exact lookup result and work evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeLookup {
    /// Matching entry, or explicit authenticated absence.
    pub entry: Option<TreeEntry>,
    /// Exact kernel-visible work performed.
    pub work: WorkCounters,
}

/// Original-order results from one shared authenticated frontier batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeBatchLookup {
    /// One explicit present/absent result for every requested name.
    pub entries: Vec<Option<TreeEntry>>,
    /// Exact shared traversal, backend, copy, and allocation work.
    pub work: WorkCounters,
}

/// Looks up an arbitrary name batch while reading each distinct tree frontier once.
///
/// Input order and duplicate requests are preserved in the returned vector.
/// Internally, borrowed keys are deterministically ordered and queries sharing
/// ancestors or leaves are routed together.
///
/// # Errors
///
/// Rejects empty/oversized batches, malformed routing, cycles, invalid limits,
/// backend failures, and work outside the admitted budget.
pub fn lookup_tree_entries<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    names: &[LogicalName],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<TreeBatchLookup, TreeReadFailure> {
    persistent_batch::lookup::<S, TreeFormat>(store, root, names, maximum_queries, limits, budget)
        .map(to_batch)
        .map_err(map_batch_failure)
}

/// Asynchronously executes the same shared-frontier batch as [`lookup_tree_entries`].
///
/// # Errors
///
/// Returns the same failures, including cancellation before every backend read.
pub async fn lookup_tree_entries_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    names: &[LogicalName],
    maximum_queries: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<TreeBatchLookup, TreeReadFailure> {
    persistent_batch::lookup_async::<S, TreeFormat>(
        store,
        root,
        names,
        maximum_queries,
        limits,
        budget,
        cancellation,
    )
    .await
    .map(to_batch)
    .map_err(map_batch_failure)
}

fn to_batch(receipt: persistent_batch::Receipt<TreeEntry>) -> TreeBatchLookup {
    TreeBatchLookup {
        entries: receipt.values,
        work: receipt.work,
    }
}

fn map_batch_failure(failure: persistent_batch::Failure) -> TreeReadFailure {
    OperationFailure::new(map_batch_error(failure.error), *failure.work)
}

fn map_batch_error(error: persistent_batch::Error) -> TreeReadError {
    match error {
        persistent_batch::Error::Cancelled => TreeReadError::Cancelled,
        persistent_batch::Error::Empty => TreeReadError::EmptyBatch,
        persistent_batch::Error::TooManyQueries => TreeReadError::TooManyQueries,
        persistent_batch::Error::WrongRootKind => TreeReadError::WrongRootKind,
        persistent_batch::Error::InvalidLimits => TreeReadError::InvalidHeightLimit,
        persistent_batch::Error::HeightExceeded => TreeReadError::HeightExceeded,
        persistent_batch::Error::CycleOrAlias => TreeReadError::Cycle,
        persistent_batch::Error::ChildBoundsMismatch => TreeReadError::ChildBoundsMismatch,
        persistent_batch::Error::InvalidRouting => TreeReadError::InvalidRouting,
        persistent_batch::Error::AllocationFailed => TreeReadError::AllocationFailed,
        persistent_batch::Error::Storage(error) => error.into(),
        persistent_batch::Error::Decode(error) => error.into(),
        persistent_batch::Error::Work(error) => error.into(),
    }
}

/// Looks up one exact name through an authenticated directory B+tree.
///
/// # Errors
///
/// Fails on wrong object classes, corrupt pages, cycles, excessive height,
/// backend failures, or a work budget that would be exceeded. The function
/// never enumerates unrelated leaf entries or reads file bodies.
pub fn lookup_tree_entry<S: crate::ImmediateObjectStore>(
    store: &S,
    root: ObjectId,
    name: &LogicalName,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<TreeLookup, TreeReadFailure> {
    crate::async_storage::poll_immediate(lookup_tree_entry_async(
        store,
        root,
        name,
        limits,
        budget,
        &CancellationToken::new(),
    ))
}

/// Asynchronously looks up one exact name, sharing every page another
/// reader of `store` already decoded.
///
/// # Errors
///
/// Returns the same typed semantic, storage, decode, work, and cancellation
/// failures as [`lookup_tree_entry`].
pub async fn lookup_tree_entry_async<S: AsyncObjectStore>(
    store: &S,
    root: ObjectId,
    name: &LogicalName,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<TreeLookup, TreeReadFailure> {
    persistent_point::lookup_async::<S, TreeFormat>(store, root, name, limits, budget, cancellation)
        .await
        .map(|receipt| TreeLookup {
            entry: receipt.value,
            work: receipt.work,
        })
        .map_err(map_batch_failure)
}

/// Sparse authenticated tree failure retaining exact spent work.
pub type TreeReadFailure = OperationFailure<TreeReadError>;

/// Sparse authenticated tree-read failures.
#[derive(Debug, Error)]
pub enum TreeReadError {
    /// Cooperative cancellation occurred before the next storage boundary.
    #[error("tree lookup was cancelled")]
    Cancelled,
    /// Batch lookup requires at least one requested name.
    #[error("tree lookup batch is empty")]
    EmptyBatch,
    /// Batch lookup exceeds its explicit query limit.
    #[error("tree lookup batch exceeds its admitted query bound")]
    TooManyQueries,
    /// Root object is not an authenticated tree page.
    #[error("tree lookup root is not a tree page")]
    WrongRootKind,
    /// Page-height bound must be positive.
    #[error("tree page height limit must be non-zero")]
    InvalidHeightLimit,
    /// A child graph referenced an ancestor.
    #[error("tree page graph contains a cycle")]
    Cycle,
    /// Traversal did not reach a leaf within its hard height bound.
    #[error("tree page height exceeds its admitted bound")]
    HeightExceeded,
    /// Internal routing state was structurally impossible.
    #[error("tree page routing invariant failed")]
    InvalidRouting,
    /// Parent routing bounds do not match the authenticated child frontier.
    #[error("tree child bounds do not match its page")]
    ChildBoundsMismatch,
    /// A bounded batch scratch allocation failed.
    #[error("tree lookup allocation failed")]
    AllocationFailed,
    /// Stored page exceeds the decode/work bound.
    #[error("tree page has {observed} bytes; maximum is {maximum}")]
    PageTooLarge {
        /// Observed canonical bytes.
        observed: u64,
        /// Admitted maximum.
        maximum: u64,
    },
    /// Immutable-object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page failed decoding or semantic validation.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact implementation work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/read.rs"]
mod tests;
