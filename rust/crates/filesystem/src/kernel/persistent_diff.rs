//! Bounded Merkle-aware value diffs for persistent authenticated B+trees.

use super::allocation::AllocationLedger;
use super::file_table_mutation::FileTableFormat;
use super::persistent_btree::{Format, Page};
use super::persistent_io::{self, OwnedPage, PageLease};
use super::tree_mutation::TreeFormat;
use super::{CanonicalDecodeError, DecodeLimits};
use super::{FileRecord, LogicalName, TreeEntry};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectStoreError};
use std::collections::BTreeMap;
use thiserror::Error;

type ChangeMap<F> =
    BTreeMap<<F as Format>::Key, (Option<<F as Format>::Value>, Option<<F as Format>::Value>)>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValueChange<K, V> {
    pub(crate) key: K,
    pub(crate) before: Option<V>,
    pub(crate) after: Option<V>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Diff<VK, V> {
    pub(crate) changes: Vec<ValueChange<VK, V>>,
    pub(crate) truncated: bool,
    pub(crate) work: WorkCounters,
}

#[derive(Debug, Error)]
pub(crate) enum DiffError {
    #[error("persistent diff roots have the wrong object kind")]
    WrongRootKind,
    #[error("persistent diff requires a positive change bound")]
    InvalidLimit,
    #[error("persistent diff allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    #[error(transparent)]
    Work(#[from] WorkError),
    #[error("persistent diff was cancelled")]
    Cancelled,
}

enum Task {
    Compare(ObjectId, ObjectId),
    Before(ObjectId),
    After(ObjectId),
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn diff_async<S, F>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Diff<F::Key, F::Value>, OperationFailure<DiffError>>
where
    S: AsyncObjectStore,
    F: Format,
{
    if before.is_some_and(|value| value.kind != F::kind())
        || after.is_some_and(|value| value.kind != F::kind())
    {
        return Err(OperationFailure::before_work(DiffError::WrongRootKind));
    }
    if maximum_changes == 0 {
        return Err(OperationFailure::before_work(DiffError::InvalidLimit));
    }
    let maximum = usize::try_from(maximum_changes).unwrap_or(usize::MAX);
    let mut work = WorkCounters::default();
    let mut tasks = match (before, after) {
        (Some(before), Some(after)) if before == after => {
            return Ok(Diff {
                changes: Vec::new(),
                truncated: false,
                work,
            });
        }
        (Some(before), Some(after)) => vec![Task::Compare(before, after)],
        (Some(before), None) => vec![Task::Before(before)],
        (None, Some(after)) => vec![Task::After(after)],
        (None, None) => {
            return Ok(Diff {
                changes: Vec::new(),
                truncated: false,
                work,
            });
        }
    };
    let mut values = ChangeMap::<F>::new();
    let mut truncated = false;
    let mut allocations = AllocationLedger::default();

    while let Some(task) = tasks.pop() {
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(DiffError::Cancelled, work))?;
        if values.len() > maximum {
            truncated = true;
            break;
        }
        match task {
            Task::Compare(left, right) if left == right => {}
            Task::Compare(left, right) => {
                let left = read_diff_page::<S, F>(
                    store,
                    left,
                    limits,
                    budget,
                    cancellation,
                    &mut allocations,
                    &mut work,
                )
                .await?;
                let right = read_diff_page::<S, F>(
                    store,
                    right,
                    limits,
                    budget,
                    cancellation,
                    &mut allocations,
                    &mut work,
                )
                .await?;
                if let (Some(left_children), Some(right_children)) =
                    (internal_children(&left), internal_children(&right))
                {
                    align_children::<F>(left_children, right_children, &mut tasks);
                    release_owned_page(left, &mut allocations, work)?;
                    release_owned_page(right, &mut allocations, work)?;
                } else {
                    push_owned_page(
                        left,
                        true,
                        &mut tasks,
                        &mut values,
                        maximum,
                        &mut truncated,
                        budget,
                        &mut allocations,
                        &mut work,
                    )?;
                    push_owned_page(
                        right,
                        false,
                        &mut tasks,
                        &mut values,
                        maximum,
                        &mut truncated,
                        budget,
                        &mut allocations,
                        &mut work,
                    )?;
                }
            }
            Task::Before(page) => {
                let page = read_diff_page::<S, F>(
                    store,
                    page,
                    limits,
                    budget,
                    cancellation,
                    &mut allocations,
                    &mut work,
                )
                .await?;
                push_owned_page(
                    page,
                    true,
                    &mut tasks,
                    &mut values,
                    maximum,
                    &mut truncated,
                    budget,
                    &mut allocations,
                    &mut work,
                )?;
            }
            Task::After(page) => {
                let page = read_diff_page::<S, F>(
                    store,
                    page,
                    limits,
                    budget,
                    cancellation,
                    &mut allocations,
                    &mut work,
                )
                .await?;
                push_owned_page(
                    page,
                    false,
                    &mut tasks,
                    &mut values,
                    maximum,
                    &mut truncated,
                    budget,
                    &mut allocations,
                    &mut work,
                )?;
            }
        }
        if truncated {
            break;
        }
    }

    let mut changes = Vec::new();
    changes
        .try_reserve(values.len().min(maximum))
        .map_err(|_| OperationFailure::new(DiffError::AllocationFailed, work))?;
    for (key, (before, after)) in values {
        if before != after {
            if changes.len() == maximum {
                truncated = true;
                break;
            }
            changes.push(ValueChange { key, before, after });
        }
    }
    Ok(Diff {
        changes,
        truncated,
        work,
    })
}

fn align_children<F: Format>(
    left: &[super::persistent_btree::Child<F::Key>],
    right: &[super::persistent_btree::Child<F::Key>],
    tasks: &mut Vec<Task>,
) {
    let mut common = Vec::new();
    let mut next_right = 0;
    for (left_index, left_child) in left.iter().enumerate() {
        if let Some(relative) = right[next_right..]
            .iter()
            .position(|right_child| right_child.page == left_child.page)
        {
            let right_index = next_right + relative;
            common.push((left_index, right_index));
            next_right = right_index + 1;
        }
    }
    let mut left_start = 0;
    let mut right_start = 0;
    common.push((left.len(), right.len()));
    for (left_end, right_end) in common {
        push_unmatched::<F>(
            &left[left_start..left_end],
            &right[right_start..right_end],
            tasks,
        );
        left_start = left_end.saturating_add(1);
        right_start = right_end.saturating_add(1);
    }
}

fn push_unmatched<F: Format>(
    left: &[super::persistent_btree::Child<F::Key>],
    right: &[super::persistent_btree::Child<F::Key>],
    tasks: &mut Vec<Task>,
) {
    if left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.first == right.first)
    {
        tasks.extend(
            left.iter()
                .zip(right)
                .rev()
                .map(|(left, right)| Task::Compare(left.page, right.page)),
        );
    } else {
        tasks.extend(right.iter().rev().map(|child| Task::After(child.page)));
        tasks.extend(left.iter().rev().map(|child| Task::Before(child.page)));
    }
}

fn internal_children<F: Format>(
    page: &OwnedPage<F>,
) -> Option<&[super::persistent_btree::Child<F::Key>]> {
    match &page.page {
        PageLease::Owned(Page::Internal(children)) => Some(children),
        PageLease::Owned(Page::Leaf(_)) => None,
        PageLease::Shared { page, .. } => match page.as_ref() {
            Page::Internal(children) => Some(children),
            Page::Leaf(_) => None,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn push_owned_page<F: Format>(
    page: OwnedPage<F>,
    before: bool,
    tasks: &mut Vec<Task>,
    values: &mut ChangeMap<F>,
    maximum: usize,
    truncated: &mut bool,
    budget: WorkBudget,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<(), OperationFailure<DiffError>> {
    if let Some(children) = internal_children(&page) {
        tasks.extend(children.iter().rev().map(|child| {
            if before {
                Task::Before(child.page)
            } else {
                Task::After(child.page)
            }
        }));
        release_owned_page(page, allocations, *work)?;
        return Ok(());
    }
    let (page, logical_bytes) = page
        .into_owned(budget, allocations, work)
        .map_err(|error| OperationFailure::new(map_page_error(error), *work))?;
    match page {
        Page::Leaf(page) => insert_leaf::<F>(values, page, before, maximum, truncated),
        Page::Internal(children) => {
            tasks.extend(children.into_iter().rev().map(|child| {
                if before {
                    Task::Before(child.page)
                } else {
                    Task::After(child.page)
                }
            }));
        }
    }
    release_page_bytes(logical_bytes, allocations, *work)
}

fn insert_leaf<F: Format>(
    values: &mut ChangeMap<F>,
    page: Vec<F::Value>,
    before: bool,
    maximum: usize,
    truncated: &mut bool,
) {
    for value in page {
        if values.len() > maximum {
            *truncated = true;
            break;
        }
        let key = F::key(&value).clone();
        let entry = values.entry(key).or_insert((None, None));
        if before {
            entry.0 = Some(value);
        } else {
            entry.1 = Some(value);
        }
    }
}

async fn read_diff_page<S, F>(
    store: &S,
    page: ObjectId,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    allocations: &mut AllocationLedger,
    work: &mut WorkCounters,
) -> Result<OwnedPage<F>, OperationFailure<DiffError>>
where
    S: AsyncObjectStore,
    F: Format,
{
    persistent_io::read_page::<S, F>(store, page, limits, budget, cancellation, allocations, work)
        .await
        .map_err(|error| OperationFailure::new(map_page_error(error), *work))
}

fn map_page_error(error: persistent_io::Error) -> DiffError {
    match error {
        persistent_io::Error::AllocationFailed | persistent_io::Error::Allocation(_) => {
            DiffError::AllocationFailed
        }
        persistent_io::Error::Storage(error) => DiffError::Storage(error),
        persistent_io::Error::Decode(error) => DiffError::Decode(error),
        persistent_io::Error::Work(error) => DiffError::Work(error),
    }
}

fn release_page_bytes(
    logical_bytes: u64,
    allocations: &mut AllocationLedger,
    work: WorkCounters,
) -> Result<(), OperationFailure<DiffError>> {
    allocations
        .release(logical_bytes)
        .map_err(|_| OperationFailure::new(DiffError::AllocationFailed, work))
}

fn release_owned_page<F: Format>(
    page: OwnedPage<F>,
    allocations: &mut AllocationLedger,
    work: WorkCounters,
) -> Result<(), OperationFailure<DiffError>> {
    let logical_bytes = page.logical_bytes;
    drop(page);
    release_page_bytes(logical_bytes, allocations, work)
}

pub(crate) async fn diff_file_records_async<S: AsyncObjectStore>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Diff<crate::FileId, FileRecord>, OperationFailure<DiffError>> {
    diff_async::<S, FileTableFormat>(
        store,
        before,
        after,
        maximum_changes,
        limits,
        budget,
        cancellation,
    )
    .await
}

pub(crate) async fn diff_tree_entries_async<S: AsyncObjectStore>(
    store: &S,
    before: Option<ObjectId>,
    after: Option<ObjectId>,
    maximum_changes: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Diff<LogicalName, TreeEntry>, OperationFailure<DiffError>> {
    diff_async::<S, TreeFormat>(
        store,
        before,
        after,
        maximum_changes,
        limits,
        budget,
        cancellation,
    )
    .await
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/persistent_diff.rs"]
mod tests;
