//! Bounded Merkle-aware value diffs for persistent authenticated B+trees.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::file_table_mutation::FileTableFormat;
use super::persistent_btree::{Format, Page};
use super::persistent_io::{self, OwnedPage, PageLease};
use super::persistent_point::{Owned, absorb, retire_allocation, split_allocation};
use super::tree_mutation::TreeFormat;
use super::{CanonicalDecodeError, DecodeLimits, FileRecord, LogicalName, TreeEntry};
use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectStoreError};
use crate::FsError;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValueChange<K, V> {
    pub(crate) key: K,
    pub(crate) before: Option<V>,
    pub(crate) after: Option<V>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Diff<K, V> {
    pub(crate) changes: Vec<ValueChange<K, V>>,
    pub(crate) truncated: bool,
    pub(crate) work: WorkCounters,
    pub(crate) retained_allocation_bytes: u64,
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

#[derive(Clone, Copy)]
enum Task {
    Compare(ObjectId, ObjectId),
    Before(ObjectId),
    After(ObjectId),
}

type Changes<F> = Owned<Vec<ValueChange<<F as Format>::Key, <F as Format>::Value>>>;

pub(crate) async fn diff_async<S: AsyncObjectStore, F: Format>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Diff<F::Key, F::Value>, OperationFailure<DiffError>> {
    // Preserve the standalone error distinction used by facade/merge projections.
    if before.is_some_and(|id| id.kind != F::kind()) || after.is_some_and(|id| id.kind != F::kind()) {
        return Err(OperationFailure::before_work(DiffError::WrongRootKind));
    }
    if maximum_changes == 0 {
        return Err(OperationFailure::before_work(DiffError::InvalidLimit));
    }
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = diff_with_accounting::<S, F>(store, before, after, maximum_changes, limits, &mut accounting, cancellation).await;
    let work = accounting.spent();
    result.map(|(mut diff, _)| { diff.work = work; diff })
        .map_err(|error| OperationFailure::new(project_failure(error), work))
}

async fn diff_with_accounting<S: AsyncObjectStore, F: Format>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(Diff<F::Key, F::Value>, KernelOwnedAllocation), FsError> {
    if before.is_some_and(|id| id.kind != F::kind()) || after.is_some_and(|id| id.kind != F::kind()) || maximum_changes == 0 {
        return Err(FsError::InvalidDiff);
    }
    accounting.check()?;
    let maximum = usize::try_from(maximum_changes).unwrap_or(usize::MAX);
    let initial = match (before, after) {
        (Some(left), Some(right)) if left != right => Some(Task::Compare(left, right)),
        (Some(left), None) => Some(Task::Before(left)),
        (None, Some(right)) => Some(Task::After(right)),
        _ => None,
    };
    let Some(initial) = initial else {
        let allocation = accounting.claim_owned(0, 0)?;
        return Ok((Diff {
            changes: Vec::new(), truncated: false, work: WorkCounters::UNCHARGED,
            retained_allocation_bytes: 0,
        }, allocation));
    };
    let mut tasks = Owned::<Vec<Task>>::allocate(0, accounting).map_err(allocation_error)?;
    let mut values = match Changes::<F>::allocate(0, accounting) {
        Ok(values) => values,
        Err(error) => { tasks.retire(accounting)?; return Err(allocation_error(error)); }
    };
    let result = async {
        push_task(&mut tasks, initial, accounting)?;
        let mut truncated = false;
        while let Some(task) = tasks.value.pop() {
            cancellation.check()?;
            accounting.check()?;
            if values.value.len() > maximum { truncated = true; break; }
            match task {
                Task::Compare(left, right) if left == right => {}
                Task::Compare(left, right) => {
                    // Both cold pages enter the same read_many frontier, rather
                    // than serially borrowing the native invocation across I/O.
                    let mut batch = persistent_io::read_pages_with_accounting::<S, F, _>(
                        store, [left, right].into_iter(), limits, accounting, cancellation, None,
                    ).await?;
                    let left = match batch.next_with_accounting(limits, accounting, cancellation) {
                        Ok((page, guard)) => { let bytes = page.logical_bytes; Owned::new(page, guard, bytes) }
                        Err(error) => { batch.discard_with_accounting(accounting, cancellation)?; return Err(error); }
                    };
                    let right = match batch.next_with_accounting(limits, accounting, cancellation) {
                        Ok((page, guard)) => { let bytes = page.logical_bytes; Owned::new(page, guard, bytes) }
                        Err(error) => { left.retire(accounting)?; batch.discard_with_accounting(accounting, cancellation)?; return Err(error); }
                    };
                    if let Err(error) = batch.finish_with_accounting(accounting, cancellation) {
                        left.retire(accounting)?; right.retire(accounting)?; return Err(error);
                    }
                    if let (Some(left_children), Some(right_children)) = (internal_children(&left.value), internal_children(&right.value)) {
                        let aligned = align_children::<F>(left_children, right_children, &mut tasks, accounting);
                        left.retire(accounting)?;
                        right.retire(accounting)?;
                        aligned?;
                    } else {
                        let pushed = push_page::<F>(left, true, &mut tasks, &mut values, maximum, &mut truncated, limits, accounting);
                        if let Err(error) = pushed { right.retire(accounting)?; return Err(error); }
                        push_page::<F>(right, false, &mut tasks, &mut values, maximum, &mut truncated, limits, accounting)?;
                    }
                }
                Task::Before(id) | Task::After(id) => {
                    let before = matches!(task, Task::Before(_));
                    let (page, guard) = persistent_io::read_page_with_accounting::<S, F>(store, id, limits, accounting, cancellation, None).await?;
                    let bytes = page.logical_bytes;
                    push_page::<F>(Owned::new(page, guard, bytes), before, &mut tasks, &mut values, maximum, &mut truncated, limits, accounting)?;
                }
            }
            if truncated { break; }
        }
        let mut index = 0;
        while index < values.value.len() {
            let change = values.value.get(index).ok_or(FsError::DiffAllocationFailed)?;
            if change.before == change.after {
                remove_change::<F>(&mut values, index, accounting)?;
            } else { index += 1; }
        }
        while values.value.len() > maximum {
            truncated = true;
            let index = values.value.len() - 1;
            remove_change::<F>(&mut values, index, accounting)?;
        }
        Ok(truncated)
    }.await;
    tasks.retire(accounting)?;
    match result {
        Err(error) => { values.retire(accounting)?; Err(allocation_error(error)) }
        Ok(truncated) => {
            let Owned { value: changes, allocation, bytes } = values;
            Ok((Diff { changes, truncated, work: accounting.spent(), retained_allocation_bytes: bytes }, allocation))
        }
    }
}

fn push_task(tasks: &mut Owned<Vec<Task>>, task: Task, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    tasks.ensure_for_push(usize::MAX, accounting).map_err(allocation_error)?;
    tasks.value.push(task);
    Ok(())
}

fn align_children<F: Format>(
    left: &[super::persistent_btree::Child<F::Key>], right: &[super::persistent_btree::Child<F::Key>],
    tasks: &mut Owned<Vec<Task>>, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let mut left_start = 0;
    let mut right_start = 0;
    for (left_index, left_child) in left.iter().enumerate() {
        if let Some(relative) = right.get(right_start..).and_then(|children| children.iter().position(|child| child.page == left_child.page)) {
            let right_index = right_start + relative;
            push_unmatched::<F>(left.get(left_start..left_index).ok_or(FsError::InvalidDiff)?, right.get(right_start..right_index).ok_or(FsError::InvalidDiff)?, tasks, accounting)?;
            left_start = left_index + 1;
            right_start = right_index + 1;
        }
    }
    push_unmatched::<F>(left.get(left_start..).ok_or(FsError::InvalidDiff)?, right.get(right_start..).ok_or(FsError::InvalidDiff)?, tasks, accounting)
}

fn push_unmatched<F: Format>(
    left: &[super::persistent_btree::Child<F::Key>], right: &[super::persistent_btree::Child<F::Key>],
    tasks: &mut Owned<Vec<Task>>, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if left.len() == right.len() && left.iter().zip(right).all(|(left, right)| left.first == right.first) {
        for (left, right) in left.iter().zip(right).rev() { push_task(tasks, Task::Compare(left.page, right.page), accounting)?; }
    } else {
        for child in right.iter().rev() { push_task(tasks, Task::After(child.page), accounting)?; }
        for child in left.iter().rev() { push_task(tasks, Task::Before(child.page), accounting)?; }
    }
    Ok(())
}

fn page_view<F: Format>(page: &OwnedPage<F>) -> &Page<F> {
    match &page.page { PageLease::Owned(page) => page, PageLease::Shared { page, .. } => page.as_ref() }
}

fn internal_children<F: Format>(page: &OwnedPage<F>) -> Option<&[super::persistent_btree::Child<F::Key>]> {
    match page_view(page) { Page::Internal(children) => Some(children), Page::Leaf(_) => None }
}

#[allow(clippy::too_many_arguments, reason = "one page updates the guarded frontier and bounded sorted changes")]
fn push_page<F: Format>(
    mut page: Owned<OwnedPage<F>>, before: bool, tasks: &mut Owned<Vec<Task>>, values: &mut Changes<F>,
    maximum: usize, truncated: &mut bool, limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let result = (|| {
        if let Some(children) = internal_children(&page.value) {
            for child in children.iter().rev() {
                push_task(tasks, if before { Task::Before(child.page) } else { Task::After(child.page) }, accounting)?;
            }
            return Ok(());
        }
        match &mut page.value.page {
            PageLease::Owned(Page::Leaf(entries)) => {
                // Move owned leaf payloads; split their original guard instead
                // of copying names or adopting them after the allocation.
                for value in std::mem::take(entries) {
                    if values.value.len() > maximum { *truncated = true; break; }
                    let bytes = F::value_owned_bytes(&value);
                    let guard = split_allocation(&mut page.allocation, bytes)?;
                    page.bytes = page.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
                    insert_value::<F>(values, Owned::new(value, guard, bytes), before, maximum, limits, accounting)?;
                }
            }
            PageLease::Shared { page, .. } => {
                if let Page::Leaf(entries) = page.as_ref() {
                    for value in entries {
                        if values.value.len() > maximum { *truncated = true; break; }
                        let (value, guard) = persistent_io::clone_value_with_accounting::<F>(value, limits.maximum_name_bytes, accounting)?;
                        let bytes = F::value_owned_bytes(&value);
                        insert_value::<F>(values, Owned::new(value, guard, bytes), before, maximum, limits, accounting)?;
                    }
                }
            }
            PageLease::Owned(Page::Internal(_)) => return Err(FsError::InvalidDiff),
        }
        Ok(())
    })();
    page.retire(accounting)?;
    result
}

fn insert_value<F: Format>(
    values: &mut Changes<F>, value: Owned<F::Value>, before: bool, maximum: usize,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let prepared = (|| {
        let position = super::search::binary_search_with_accounting(&values.value, accounting, |entry| entry.key.cmp(F::key(&value.value)))?;
        if let Err(index) = position {
            values.ensure_for_push(maximum.saturating_add(1), accounting).map_err(allocation_error)?;
            let (key, guard) = persistent_io::clone_key_with_accounting::<F>(F::key(&value.value), limits.maximum_name_bytes, accounting)?;
            let bytes = F::key_owned_bytes(&key);
            absorb(&mut values.allocation, guard)?;
            values.bytes = values.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            values.value.insert(index, ValueChange { key, before: None, after: None });
        }
        Ok(match position { Ok(index) | Err(index) => index })
    })();
    let index = match prepared { Ok(index) => index, Err(error) => { value.retire(accounting)?; return Err(error); } };
    let slot = values.value.get_mut(index).ok_or(FsError::DiffAllocationFailed)?;
    let destination = if before { &mut slot.before } else { &mut slot.after };
    if let Some(old) = destination.take() {
        let bytes = F::value_owned_bytes(&old);
        let guard = split_allocation(&mut values.allocation, bytes)?;
        values.bytes = values.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
        drop(old);
        retire_allocation(guard, bytes, accounting)?;
    }
    let Owned { value, allocation, bytes } = value;
    absorb(&mut values.allocation, allocation)?;
    values.bytes = values.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
    *destination = Some(value);
    Ok(())
}

fn remove_change<F: Format>(values: &mut Changes<F>, index: usize, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let change = values.value.remove(index);
    let bytes = F::key_owned_bytes(&change.key)
        .checked_add(change.before.as_ref().map_or(0, F::value_owned_bytes)).ok_or(WorkError::Overflow)?
        .checked_add(change.after.as_ref().map_or(0, F::value_owned_bytes)).ok_or(WorkError::Overflow)?;
    let allocation = split_allocation(&mut values.allocation, bytes)?;
    values.bytes = values.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
    drop(change);
    retire_allocation(allocation, bytes, accounting)
}

fn allocation_error(error: FsError) -> FsError {
    match error { FsError::Decode(CanonicalDecodeError::AllocationFailed) => FsError::DiffAllocationFailed, other => other }
}

fn project_failure(error: FsError) -> DiffError {
    match error {
        FsError::InvalidDiff => DiffError::InvalidLimit,
        FsError::DiffAllocationFailed | FsError::Decode(CanonicalDecodeError::AllocationFailed) => DiffError::AllocationFailed,
        FsError::Object(error) => DiffError::Storage(error),
        FsError::Decode(error) => DiffError::Decode(error),
        FsError::Work(error) => DiffError::Work(error),
        FsError::Cancelled(_) => DiffError::Cancelled,
        _ => unreachable!("canonical persistent diff only produces its typed failures"),
    }
}

pub(crate) async fn diff_file_records_with_accounting<S: AsyncObjectStore>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(Diff<crate::FileId, FileRecord>, KernelOwnedAllocation), FsError> {
    diff_with_accounting::<S, FileTableFormat>(store, before, after, maximum_changes, limits, accounting, cancellation).await
}

pub(crate) async fn diff_tree_entries_with_accounting<S: AsyncObjectStore>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
) -> Result<(Diff<LogicalName, TreeEntry>, KernelOwnedAllocation), FsError> {
    diff_with_accounting::<S, TreeFormat>(store, before, after, maximum_changes, limits, accounting, cancellation).await
}

pub(crate) async fn diff_file_records_async<S: AsyncObjectStore>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Diff<crate::FileId, FileRecord>, OperationFailure<DiffError>> {
    diff_async::<S, FileTableFormat>(store, before, after, maximum_changes, limits, budget, cancellation).await
}

pub(crate) async fn diff_tree_entries_async<S: AsyncObjectStore>(
    store: &S, before: Option<ObjectId>, after: Option<ObjectId>, maximum_changes: u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<Diff<LogicalName, TreeEntry>, OperationFailure<DiffError>> {
    diff_async::<S, TreeFormat>(store, before, after, maximum_changes, limits, budget, cancellation).await
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/persistent_diff.rs"]
mod tests;
