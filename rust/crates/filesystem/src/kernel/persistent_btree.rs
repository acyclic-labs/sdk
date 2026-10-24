//! Shared immutable path-copy mechanics for authenticated B+tree formats. Original native claims are structural; sorting and producers run outside root loans.

use super::allocation::{
    AllocationError, KernelAccounting, KernelOwnedAllocation, VisitedObjectSet,
};
use super::codec::DecodedPageShape;
use super::persistent_io;
use super::persistent_point::{Owned, retire_allocation, split_allocation};
use super::search::binary_search_with_accounting;
use super::{CanonicalDecodeError, DecodeLimits};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationReceipt, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use std::marker::PhantomData;
use std::ops::Range;
use thiserror::Error;

pub(crate) trait Format: Send + Sync + 'static {
    type Key: Clone + Eq + Ord + Send + Sync + 'static;
    type Value: Clone + Eq + Send + Sync + 'static;

    fn kind() -> ObjectKind;
    fn key(value: &Self::Value) -> &Self::Key;
    fn decode<E: From<CanonicalDecodeError>>(
        bytes: &[u8],
        limits: DecodeLimits,
        observe: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Page<Self>, E>
    where
        Self: Sized;
    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError>;
    fn page_encoded_length(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<usize, CanonicalDecodeError>
    where
        Self: Sized;
    fn leaf_item_encoded_length(value: &Self::Value) -> Result<usize, CanonicalDecodeError>;
    fn internal_item_encoded_length(key: &Self::Key) -> Result<usize, CanonicalDecodeError>;
    fn key_nested_bytes(key: &Self::Key) -> u64;
    fn value_nested_bytes(value: &Self::Value) -> u64;
    fn key_owned_bytes(key: &Self::Key) -> u64;
    fn value_owned_bytes(value: &Self::Value) -> u64;
    fn try_clone_key<E: From<CanonicalDecodeError>>(
        key: &Self::Key,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Key, E>;
    fn try_clone_value<E: From<CanonicalDecodeError>>(
        value: &Self::Value,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Value, E>;
    fn encode_with_accounting(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError>
    where
        Self: Sized;
}

/// One authenticated child page and its exact lower-bound key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Child<K> {
    /// Exact first key represented by the child subtree.
    pub first: K,
    /// Authenticated child page.
    pub page: ObjectId,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Page<F: Format> {
    Leaf(Vec<F::Value>),
    Internal(Vec<Child<F::Key>>),
}

impl<F: Format> Clone for Page<F> {
    fn clone(&self) -> Self {
        match self {
            Self::Leaf(values) => Self::Leaf(values.clone()),
            Self::Internal(children) => Self::Internal(children.clone()),
        }
    }
}

pub(crate) enum PageRef<'a, F: Format> {
    Leaf(entries::Entries<'a, F::Value>),
    Internal(entries::Entries<'a, Child<F::Key>>),
}

pub(crate) trait Mutation<F: Format> {
    type Error: std::error::Error;

    fn key(&self) -> &F::Key;
    fn changes_cardinality(&self) -> bool;
    fn replacement<'a>(
        &'a self,
        current: Option<&F::Value>,
    ) -> Result<Option<&'a F::Value>, Self::Error>;
    fn map_error(error: Error<Self::Error>) -> crate::FsError;
}

#[derive(Debug, Error)]
pub(crate) enum Error<E: std::error::Error> {
    #[error("persistent B+tree mutation batch is empty")]
    Empty,
    #[error("persistent B+tree mutation count exceeds its bound")]
    TooManyMutations,
    #[error("persistent B+tree root has the wrong object kind")]
    WrongRootKind,
    #[error("persistent B+tree limits are invalid")]
    InvalidLimits,
    #[error("one persistent B+tree item cannot fit in an admitted page")]
    PageItemTooLarge,
    #[error("persistent B+tree exceeds its height bound")]
    HeightExceeded,
    #[error("persistent B+tree contains a cycle or alias")]
    CycleOrAlias,
    #[error("persistent B+tree child bounds mismatch")]
    ChildBoundsMismatch,
    #[error("persistent B+tree mutation violated its physical-plan contract")]
    MutationContract,
    #[error("persistent B+tree scratch allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    Allocation(#[from] AllocationError),
    #[error("persistent B+tree semantic mutation failed: {0}")]
    Semantic(E),
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    #[error(transparent)]
    Work(#[from] WorkError),
}

struct NodeRequest<K> {
    page: ObjectId,
    lower: Owned<Option<K>>,
    upper: Owned<Option<K>>,
    mutations: Range<usize>,
    height: u16,
}

struct InternalFrame<F: Format> {
    original: ObjectId,
    entries: internal::InternalEntries<F>,
    inherited_upper: Owned<Option<F::Key>>,
    next_child: usize,
    mutation_cursor: usize,
    mutation_end: usize,
    height: u16,
}

#[allow(
    clippy::large_enum_variant,
    reason = "The traversal frame stays on the stack instead of adding an unadmitted allocation."
)]
enum EnteredNode<F: Format> {
    Complete(Owned<Vec<Child<F::Key>>>),
    Internal(InternalFrame<F>),
}

struct Context<'a, 'accounting, 'work, S, F, M>
where
    F: Format,
    M: Mutation<F>,
{
    store: &'a S,
    limits: DecodeLimits,
    accounting: &'accounting mut KernelAccounting<'work>,
    visited: Owned<VisitedObjectSet>,
    maximum_seen_height: u16,
    cancellation: &'a CancellationToken,
    marker: PhantomData<(F, M)>,
}

pub(crate) async fn apply_with_accounting<S, F, M>(
    store: &S,
    root: ObjectId,
    mutations: &[M],
    maximum_mutations: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<ObjectId>, crate::FsError>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    accounting.check()?;
    validate::<F, M>(root, mutations, maximum_mutations, limits).map_err(M::map_error)?;
    let copied = WorkCounters {
        bytes_copied: usize_to_u64(
            mutations
                .len()
                .checked_mul(std::mem::size_of::<(usize, &M)>())
                .ok_or(WorkError::Overflow)?,
        ),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let mut ordered = Owned::allocate(mutations.len(), accounting)?;
    ordered.value.extend(mutations.iter().enumerate());
    accounting.record_completed(copied)?;
    sort_indexed::<F, M>(&mut ordered.value, accounting)?;
    let maximum_visited = mutations
        .len()
        .checked_mul(usize::from(limits.maximum_page_height))
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| M::map_error(Error::InvalidLimits))?
        .min(
            usize::try_from(limits.maximum_visited_pages)
                .map_err(|_| M::map_error(Error::InvalidLimits))?,
        );
    let visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting)?;
    let mut context = Context::<S, F, M> {
        store,
        limits,
        accounting,
        visited,
        maximum_seen_height: 0,
        cancellation,
        marker: PhantomData,
    };
    let summaries = context.rewrite(root, &ordered.value).await?;
    let root = context.finish_root(summaries).await?;
    context.visited.retire(context.accounting)?;
    ordered.retire(context.accounting)?;
    Ok(OperationReceipt {
        value: root,
        work: context.accounting.spent(),
    })
}

fn sort_indexed<F, M>(
    mutations: &mut [(usize, &M)],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError>
where
    F: Format,
    M: Mutation<F>,
{
    let count = mutations.len();
    if count < 2 {
        return Ok(());
    }
    let scan_bound = usize_to_u64(count - 1);
    accounting.admit(WorkCounters {
        items_examined: scan_bound,
        ..WorkCounters::default()
    })?;
    let mut scan_comparisons = 0_u64;
    let mut ordered = true;
    for adjacent in mutations.windows(2) {
        scan_comparisons = scan_comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
        if let [left, right] = adjacent
            && indexed_order::<F, M>(left, right).is_gt()
        {
            ordered = false;
            break;
        }
    }
    accounting.record_completed(WorkCounters {
        items_examined: scan_comparisons,
        ..WorkCounters::default()
    })?;
    if ordered {
        return Ok(());
    }

    let levels = usize::BITS - count.leading_zeros();
    let maximum_comparisons = usize_to_u64(count)
        .checked_mul(u64::from(levels))
        .and_then(|value| value.checked_mul(3))
        .ok_or(WorkError::Overflow)?;
    accounting.admit(WorkCounters {
        items_examined: maximum_comparisons,
        ..WorkCounters::default()
    })?;

    let mut comparisons = 0_u64;
    for start in (0..count / 2).rev() {
        sift_down::<F, M>(mutations, start, count, &mut comparisons)?;
    }
    for end in (1..count).rev() {
        mutations.swap(0, end);
        sift_down::<F, M>(mutations, 0, end, &mut comparisons)?;
    }
    accounting.record_completed(WorkCounters {
        items_examined: comparisons,
        ..WorkCounters::default()
    })?;
    Ok(())
}

fn sift_down<F, M>(
    mutations: &mut [(usize, &M)],
    mut root: usize,
    end: usize,
    comparisons: &mut u64,
) -> Result<(), WorkError>
where
    F: Format,
    M: Mutation<F>,
{
    loop {
        let left = root.checked_mul(2).and_then(|value| value.checked_add(1));
        let Some(left) = left.filter(|left| *left < end) else {
            return Ok(());
        };
        let right = left + 1;
        let mut greater = left;
        // `left < end` was just established by the `filter` above, and callers
        // maintain `end <= mutations.len()` (it is either the original slice
        // length or a shrinking heap boundary passed in by `sort_indexed`), so
        // `left < mutations.len()`. `right` is only indexed inside the
        // `right < end` guard, so it too is `< mutations.len()`.
        #[allow(
            clippy::indexing_slicing,
            reason = "left < end and (guarded) right < end, with end <= mutations.len() maintained by callers"
        )]
        if right < end {
            *comparisons = comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
            if indexed_order::<F, M>(&mutations[left], &mutations[right]).is_lt() {
                greater = right;
            }
        }
        *comparisons = comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
        // `root` is either the initial caller-supplied index (`< end` by
        // construction in `sort_indexed`) or a previous `greater`, and
        // `greater` is one of `left`/`right`, both shown `< end <=
        // mutations.len()` above.
        #[allow(
            clippy::indexing_slicing,
            reason = "root and greater are both < end <= mutations.len(), established above and by sort_indexed's callers"
        )]
        if !indexed_order::<F, M>(&mutations[root], &mutations[greater]).is_lt() {
            return Ok(());
        }
        mutations.swap(root, greater);
        root = greater;
    }
}

fn indexed_order<F, M>(left: &(usize, &M), right: &(usize, &M)) -> std::cmp::Ordering
where
    F: Format,
    M: Mutation<F>,
{
    left.1
        .key()
        .cmp(right.1.key())
        .then_with(|| left.0.cmp(&right.0))
}

fn validate<F, M>(
    root: ObjectId,
    mutations: &[M],
    maximum_mutations: u32,
    limits: DecodeLimits,
) -> Result<(), Error<M::Error>>
where
    F: Format,
    M: Mutation<F>,
{
    let error = if root.kind != F::kind() {
        Some(Error::WrongRootKind)
    } else if mutations.is_empty() {
        Some(Error::Empty)
    } else if maximum_mutations == 0
        || u32::try_from(mutations.len()).unwrap_or(u32::MAX) > maximum_mutations
    {
        Some(Error::TooManyMutations)
    } else if !limits.page_limits_valid(2) {
        Some(Error::InvalidLimits)
    } else {
        None
    };
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

impl<S, F, M> Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    fn error(error: Error<M::Error>) -> crate::FsError {
        M::map_error(error)
    }

    fn empty<T>(&mut self, value: T) -> Result<Owned<T>, crate::FsError> {
        let allocation = self.accounting.claim_owned(0, 0)?;
        Ok(Owned::new(value, allocation, 0))
    }

    fn clone_key(&mut self, key: &F::Key) -> Result<Owned<F::Key>, crate::FsError> {
        let (value, allocation) = persistent_io::clone_key::<F>(
            key,
            self.limits.maximum_name_bytes,
            self.accounting,
            None,
            0,
        )?;
        let bytes = F::key_owned_bytes(&value);
        Ok(Owned::new(value, allocation, bytes))
    }

    fn clone_bound(
        &mut self,
        key: Option<&F::Key>,
    ) -> Result<Owned<Option<F::Key>>, crate::FsError> {
        match key {
            Some(key) => {
                let Owned {
                    value,
                    allocation,
                    bytes,
                } = self.clone_key(key)?;
                Ok(Owned::new(Some(value), allocation, bytes))
            }
            None => self.empty(None),
        }
    }

    fn clone_value(&mut self, value: &F::Value) -> Result<Owned<Option<F::Value>>, crate::FsError> {
        let (value, allocation) = persistent_io::clone_value::<F>(
            value,
            self.limits.maximum_name_bytes,
            self.accounting,
            None,
            0,
        )?;
        let bytes = F::value_owned_bytes(&value);
        Ok(Owned::new(Some(value), allocation, bytes))
    }

    fn charge_items(&mut self, count: u64) -> Result<(), crate::FsError> {
        self.accounting.charge(WorkCounters {
            items_examined: count,
            ..WorkCounters::UNCHARGED
        })
    }

    async fn rewrite(
        &mut self,
        root: ObjectId,
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let mut stack = Owned::<Vec<InternalFrame<F>>>::allocate(0, self.accounting)?;
        let mut request = NodeRequest {
            page: root,
            lower: self.empty(None)?,
            upper: self.empty(None)?,
            mutations: 0..mutations.len(),
            height: 1,
        };
        'traverse: loop {
            let mut result = match self.enter_node(request, mutations).await? {
                EnteredNode::Complete(result) => result,
                EnteredNode::Internal(mut frame) => {
                    if let Some(next) = self.advance_frame(&mut frame, mutations)? {
                        stack.ensure_for_push(
                            usize::from(self.limits.maximum_page_height),
                            self.accounting,
                        )?;
                        stack.value.push(frame);
                        request = next;
                        continue;
                    }
                    self.finish_frame(frame).await?
                }
            };
            loop {
                let Some(mut frame) = stack.value.pop() else {
                    stack.retire(self.accounting)?;
                    return Ok(result);
                };
                frame.entries.append(result, self.accounting)?;
                if let Some(next) = self.advance_frame(&mut frame, mutations)? {
                    stack.ensure_for_push(
                        usize::from(self.limits.maximum_page_height),
                        self.accounting,
                    )?;
                    stack.value.push(frame);
                    request = next;
                    continue 'traverse;
                }
                result = self.finish_frame(frame).await?;
            }
        }
    }
}

fn storage_error(error: ObjectStoreError) -> crate::FsError {
    match error {
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => error.into(),
    }
}

fn push_summary<K>(
    output: &mut Owned<Vec<Child<K>>>,
    first: Owned<K>,
    page: ObjectId,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    output.ensure_for_push(usize::MAX, accounting)?;
    let Owned {
        value,
        mut allocation,
        bytes,
    } = first;
    output.value.push(Child { first: value, page });
    output.allocation.absorb(&mut allocation)?;
    output.bytes = output.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
    Ok(())
}

fn push_value<V>(
    output: &mut Owned<Vec<V>>,
    mut value: Owned<Option<V>>,
) -> Result<(), crate::FsError> {
    let entry = value
        .value
        .take()
        .ok_or(CanonicalDecodeError::AllocationFailed)?;
    output.value.push(entry);
    output.allocation.absorb(&mut value.allocation)?;
    output.bytes = output
        .bytes
        .checked_add(value.bytes)
        .ok_or(WorkError::Overflow)?;
    Ok(())
}

fn append_owned<T>(
    output: &mut Owned<Vec<T>>,
    mut source: Owned<Vec<T>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let buffer_bytes = usize_to_u64(
        source
            .value
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(WorkError::Overflow)?,
    );
    while !source.value.is_empty() {
        output.ensure_for_push(usize::MAX, accounting)?;
        let spare = output.value.capacity() - output.value.len();
        let count = spare.min(source.value.len());
        output.value.extend(source.value.drain(..count));
    }
    let buffer_allocation = split_allocation(&mut source.allocation, buffer_bytes)?;
    drop(source.value);
    retire_allocation(buffer_allocation, buffer_bytes, accounting)?;
    output.allocation.absorb(&mut source.allocation)?;
    output.bytes = output
        .bytes
        .checked_add(
            source
                .bytes
                .checked_sub(buffer_bytes)
                .ok_or(WorkError::Overflow)?,
        )
        .ok_or(WorkError::Overflow)?;
    Ok(())
}

/// One authenticated page written, as charged before the backend's work.
pub(crate) fn within_bounds<K: Ord>(
    first: Option<&K>,
    last: Option<&K>,
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
    (lower.is_none() || first == lower)
        && !matches!((last, upper), (Some(last), Some(upper)) if last >= upper)
}

pub(crate) fn leaf_within<'a, F: Format>(
    values: impl Into<entries::Entries<'a, F::Value>>,
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> bool {
    let values = values.into();
    within_bounds(
        values.first().map(F::key),
        values.last().map(F::key),
        lower,
        upper,
    )
}

pub(crate) fn children_within<'a, K: Ord + 'a>(
    children: impl Into<entries::Entries<'a, Child<K>>>,
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
    let children = children.into();
    let (first, last) = (children.first(), children.last());
    within_bounds(
        first.map(|child| &child.first),
        last.map(|child| &child.first),
        lower,
        upper,
    )
}

#[cfg(test)]
#[path = "tests/persistent_btree.rs"]
mod tests;

mod enter;
pub(crate) mod entries;
mod internal;
mod leaf;
mod pages;
pub(crate) mod shared;
mod shared_leaf;
#[cfg(test)]
use pages::PageChunks;
#[cfg(test)]
pub(crate) use pages::TARGET_PAGE_BYTES;
