//! Sparse authenticated reads with exact implementation-work receipts.

use super::frontier;
use super::persistent_batch;
use super::tree_mutation::TreeFormat;
use super::{
    CanonicalDecodeError, DecodeLimits, LogicalName, TreeEntry, TreePage, decode_tree_page,
};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectKind, ObjectRead, ObjectReceipt, ObjectStore, ObjectStoreError,
};
use std::collections::HashSet;
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
pub fn lookup_tree_entry<S: ObjectStore>(
    store: &S,
    root: ObjectId,
    name: &LogicalName,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<TreeLookup, TreeReadFailure> {
    let mut machine = LookupMachine::new(root, name, limits, budget)?;
    frontier::drive_sync(store, &mut machine)
}

/// Asynchronous tree lookup driven by the same semantic machine as the native
/// synchronous fast path.
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
    let mut machine = LookupMachine::new(root, name, limits, budget)?;
    frontier::drive_async(store, &mut machine, cancellation).await
}

struct LookupMachine<'a> {
    name: &'a LogicalName,
    limits: DecodeLimits,
    budget: WorkBudget,
    page: ObjectId,
    expected_lower: Option<LogicalName>,
    expected_upper: Option<LogicalName>,
    visited: HashSet<ObjectId>,
    work: WorkCounters,
}

impl<'a> LookupMachine<'a> {
    fn new(
        root: ObjectId,
        name: &'a LogicalName,
        limits: DecodeLimits,
        budget: WorkBudget,
    ) -> Result<Self, TreeReadFailure> {
        if root.kind != ObjectKind::TreePage {
            return Err(tree_failed(
                TreeReadError::WrongRootKind,
                WorkCounters::default(),
            ));
        }
        if !limits.page_limits_valid(1) {
            return Err(tree_failed(
                TreeReadError::InvalidHeightLimit,
                WorkCounters::default(),
            ));
        }

        Ok(Self {
            name,
            limits,
            budget,
            page: root,
            expected_lower: None,
            expected_upper: None,
            visited: HashSet::new(),
            work: WorkCounters::default(),
        })
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, TreeReadFailure> {
        if self.visited.len() >= usize::from(self.limits.maximum_page_height) {
            return Err(tree_failed(TreeReadError::HeightExceeded, self.work));
        }
        if !self.visited.insert(self.page) {
            return Err(tree_failed(TreeReadError::Cycle, self.work));
        }
        let semantic = WorkCounters {
            page_reads: 1,
            ..WorkCounters::default()
        };
        let prospective = self
            .work
            .checked_add(semantic)
            .map_err(|error| tree_failed(TreeReadError::Work(error), self.work))?;
        let remaining = prospective
            .remaining(self.budget)
            .map_err(|error| tree_failed(TreeReadError::Work(error), self.work))?;
        Ok(frontier::ReadRequest {
            page: self.page,
            maximum_bytes: self.limits.maximum_page_object_bytes(),
            remaining,
            prospective,
        })
    }

    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<TreeLookup>, TreeReadFailure> {
        self.work = prospective
            .checked_add(receipt.work)
            .map_err(|error| tree_failed(TreeReadError::Work(error), prospective))?;
        self.work
            .verify(self.budget)
            .map_err(|error| tree_failed(TreeReadError::Work(error), self.work))?;

        match decode_tree_page(&receipt.value, self.limits)
            .map_err(|error| tree_failed(TreeReadError::Decode(error), self.work))?
        {
            TreePage::Leaf(entries) => {
                validate_leaf_bounds(
                    &entries,
                    self.expected_lower.as_ref(),
                    self.expected_upper.as_ref(),
                )
                .map_err(|error| tree_failed(error, self.work))?;
                let entry = entries
                    .binary_search_by(|entry| entry.name.cmp(self.name))
                    .ok()
                    .and_then(|index| entries.get(index).cloned());
                Ok(Some(TreeLookup {
                    entry,
                    work: self.work,
                }))
            }
            TreePage::Internal(children) => {
                validate_internal_bounds(
                    &children,
                    self.expected_lower.as_ref(),
                    self.expected_upper.as_ref(),
                )
                .map_err(|error| tree_failed(error, self.work))?;
                let child_index = children.partition_point(|child| child.first_name <= *self.name);
                let selected = child_index.saturating_sub(1);
                let child = children
                    .get(selected)
                    .ok_or_else(|| tree_failed(TreeReadError::InvalidRouting, self.work))?
                    .clone();
                self.expected_lower = Some(child.first_name);
                self.expected_upper = children
                    .get(selected + 1)
                    .map(|next| next.first_name.clone())
                    .or(self.expected_upper.take());
                self.page = child.page;
                Ok(None)
            }
        }
    }
}

impl frontier::Machine for LookupMachine<'_> {
    type Output = TreeLookup;
    type Failure = TreeReadFailure;

    fn complete(&mut self) -> Result<Option<Self::Output>, Self::Failure> {
        Ok(None)
    }

    fn prepare_read(&mut self) -> Result<frontier::ReadRequest, Self::Failure> {
        LookupMachine::prepare_read(self)
    }

    fn accept(
        &mut self,
        prospective: WorkCounters,
        receipt: &ObjectReceipt<ObjectRead>,
    ) -> Result<Option<Self::Output>, Self::Failure> {
        LookupMachine::accept(self, prospective, receipt)
    }

    fn storage_failure(
        &self,
        prospective: WorkCounters,
        failure: crate::storage::ObjectFailure,
    ) -> Self::Failure {
        match prospective.checked_add(*failure.work) {
            Ok(spent) => tree_failed(TreeReadError::Storage(failure.error), spent),
            Err(error) => tree_failed(TreeReadError::Work(error), prospective),
        }
    }

    fn cancelled(&self) -> Self::Failure {
        tree_failed(TreeReadError::Cancelled, self.work)
    }
}

/// Sparse authenticated tree failure retaining exact spent work.
pub type TreeReadFailure = OperationFailure<TreeReadError>;

fn tree_failed(error: TreeReadError, work: WorkCounters) -> TreeReadFailure {
    OperationFailure::new(error, work)
}

fn validate_leaf_bounds(
    entries: &[TreeEntry],
    lower: Option<&LogicalName>,
    upper: Option<&LogicalName>,
) -> Result<(), TreeReadError> {
    if let Some(lower) = lower
        && entries.first().map(|entry| &entry.name) != Some(lower)
    {
        return Err(TreeReadError::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && entries.last().is_some_and(|entry| entry.name >= *upper)
    {
        return Err(TreeReadError::ChildBoundsMismatch);
    }
    Ok(())
}

fn validate_internal_bounds(
    children: &[super::TreeChild],
    lower: Option<&LogicalName>,
    upper: Option<&LogicalName>,
) -> Result<(), TreeReadError> {
    if let Some(lower) = lower
        && children.first().map(|child| &child.first_name) != Some(lower)
    {
        return Err(TreeReadError::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && children
            .last()
            .is_some_and(|child| child.first_name >= *upper)
    {
        return Err(TreeReadError::ChildBoundsMismatch);
    }
    Ok(())
}

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
