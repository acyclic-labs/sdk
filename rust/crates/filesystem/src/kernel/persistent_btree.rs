//! Shared immutable path-copy mechanics for authenticated B+tree formats.
//! Original native claims are structural; sorting and producers run outside root loans.

use super::allocation::{AllocationError, KernelAccounting, KernelOwnedAllocation, VisitedObjectSet};
use super::codec::DecodedPageShape;
use super::persistent_io::{self, PageLease};
use super::persistent_point::{Owned, absorb, retire_allocation};
use super::search::counted_binary_search;
use super::{CanonicalDecodeError, DecodeLimits};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{WorkCounters, WorkError};
use crate::storage::{
    HashedObject, OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStoreError,
};
use std::marker::PhantomData;
use std::ops::Range;
use thiserror::Error;

pub(crate) trait Format: Send + Sync + 'static {
    type Key: Clone + Eq + Ord + Send + Sync + 'static;
    type Value: Clone + Eq + Send + Sync + 'static;

    fn kind() -> ObjectKind;
    fn key(value: &Self::Value) -> &Self::Key;
    fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<Page<Self>, CanonicalDecodeError>
    where
        Self: Sized;
    /// Observes the canonical parser's actual owned-allocation and item boundaries.
    fn decode_with_steps<E: From<CanonicalDecodeError>>(
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
    /// Actual capacity of this one moved key/value's owned name buffer.
    fn key_owned_bytes(key: &Self::Key) -> u64;
    fn value_owned_bytes(value: &Self::Value) -> u64;
    fn try_clone_key(
        key: &Self::Key,
        maximum_bytes: u32,
    ) -> Result<Self::Key, CanonicalDecodeError>;
    fn try_clone_value(
        value: &Self::Value,
        maximum_bytes: u32,
    ) -> Result<Self::Value, CanonicalDecodeError>;
    fn try_clone_key_with_steps<E: From<CanonicalDecodeError>>(
        key: &Self::Key,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Key, E>;
    fn try_clone_value_with_steps<E: From<CanonicalDecodeError>>(
        value: &Self::Value,
        maximum_bytes: u32,
        steps: &mut impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
    ) -> Result<Self::Value, E>;
    fn encode_with_accounting(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<u8>, KernelOwnedAllocation), crate::FsError>
    where Self: Sized;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Child<K> {
    pub(crate) first: K,
    pub(crate) page: ObjectId,
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
    Leaf(&'a [F::Value]),
    Internal(&'a [Child<F::Key>]),
}

pub(crate) trait Mutation<F: Format>: Clone {
    type Error: std::error::Error;

    fn key(&self) -> &F::Key;
    fn changes_cardinality(&self) -> bool;
    /// Checks semantic preconditions without copying the current value.
    fn replacement<'a>(&'a self, current: Option<&F::Value>) -> Result<Option<&'a F::Value>, Self::Error>;
    fn map_error(error: Error<Self::Error>) -> crate::FsError;
    fn apply_current(&self, current: &mut Option<F::Value>) -> Result<(), Self::Error> {
        let replacement = self.replacement(current.as_ref())?;
        *current = replacement.cloned();
        Ok(())
    }
}

#[derive(Clone)]
struct IndexedMutation<'a, M> {
    ordinal: usize,
    mutation: &'a M,
}

impl<F, M> Mutation<F> for IndexedMutation<'_, M>
where F: Format, M: Mutation<F> {
    type Error = M::Error;
    fn key(&self) -> &F::Key { self.mutation.key() }
    fn changes_cardinality(&self) -> bool { self.mutation.changes_cardinality() }
    fn replacement<'a>(&'a self, current: Option<&F::Value>) -> Result<Option<&'a F::Value>, Self::Error> {
        self.mutation.replacement(current)
    }
    fn map_error(error: Error<Self::Error>) -> crate::FsError { M::map_error(error) }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Receipt {
    pub(crate) root: ObjectId,
    pub(crate) work: WorkCounters,
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

type Summary<K> = Child<K>;

struct NodeRequest<K> {
    page: ObjectId,
    lower: Owned<Option<K>>,
    upper: Owned<Option<K>>,
    mutations: Range<usize>,
    height: u16,
}

struct InternalFrame<F: Format> {
    original: ObjectId,
    children: Owned<Vec<Child<F::Key>>>,
    inherited_upper: Owned<Option<F::Key>>,
    next_child: usize,
    mutation_cursor: usize,
    mutation_end: usize,
    height: u16,
    rewritten: Owned<Vec<Summary<F::Key>>>,
}

enum EnteredNode<F: Format> {
    Complete(Owned<Vec<Summary<F::Key>>>),
    Internal(InternalFrame<F>),
}

struct Context<'a, 'accounting, 'work, S, F, M>
where F: Format, M: Mutation<F> {
    store: &'a S,
    limits: DecodeLimits,
    accounting: &'accounting mut KernelAccounting<'work>,
    visited: Owned<VisitedObjectSet>,
    maximum_seen_height: u16,
    cancellation: &'a CancellationToken,
    marker: PhantomData<(F, M)>,
}

/// The ordinary and original-native callers drive this one path-copy body.
/// Inputs remain borrowed under their caller's custody; only indices are sorted.
pub(crate) async fn apply_with_accounting<S, F, M>(
    store: &S,
    root: ObjectId,
    mutations: &[M],
    maximum_mutations: u32,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Receipt, crate::FsError>
where S: crate::AsyncObjectStore, F: Format, M: Mutation<F> {
    accounting.check()?;
    validate::<F, M>(root, mutations, maximum_mutations, limits).map_err(M::map_error)?;
    let copied = WorkCounters {
        bytes_copied: usize_to_u64(mutations.len().checked_mul(std::mem::size_of::<IndexedMutation<'_, M>>()).ok_or(WorkError::Overflow)?),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let mut ordered = Owned::allocate(mutations.len(), accounting)?;
    ordered.value.extend(mutations.iter().enumerate().map(|(ordinal, mutation)| IndexedMutation { ordinal, mutation }));
    accounting.record_completed(copied)?;
    sort_indexed::<F, M>(&mut ordered.value, accounting)?;
    let maximum_visited = mutations.len()
        .checked_mul(usize::from(limits.maximum_page_height))
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| M::map_error(Error::InvalidLimits))?
        .min(usize::try_from(limits.maximum_visited_pages).map_err(|_| M::map_error(Error::InvalidLimits))?);
    let visited = VisitedObjectSet::new_with_accounting(maximum_visited, accounting)?;
    let mut context = Context::<S, F, IndexedMutation<'_, M>> {
        store, limits, accounting, visited,
        maximum_seen_height: 0, cancellation, marker: PhantomData,
    };
    let summaries = context.rewrite(root, &ordered.value).await?;
    let root = context.finish_root(summaries).await?;
    context.visited.retire(context.accounting)?;
    ordered.retire(context.accounting)?;
    Ok(Receipt { root, work: context.accounting.spent() })
}

fn sort_indexed<F, M>(
    mutations: &mut [IndexedMutation<M>],
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
    mutations: &mut [IndexedMutation<M>],
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

fn indexed_order<F, M>(left: &IndexedMutation<M>, right: &IndexedMutation<M>) -> std::cmp::Ordering
where
    F: Format,
    M: Mutation<F>,
{
    left.mutation
        .key()
        .cmp(right.mutation.key())
        .then_with(|| left.ordinal.cmp(&right.ordinal))
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
where S: crate::AsyncObjectStore, F: Format, M: Mutation<F> {
    fn error(error: Error<M::Error>) -> crate::FsError { M::map_error(error) }

    fn empty<T>(&mut self, value: T) -> Result<Owned<T>, crate::FsError> {
        let allocation = self.accounting.claim_owned(0, 0)?;
        Ok(Owned::new(value, allocation, 0))
    }

    fn clone_key(&mut self, key: &F::Key) -> Result<Owned<F::Key>, crate::FsError> {
        let (value, allocation) = persistent_io::clone_key_with_accounting::<F>(
            key, self.limits.maximum_name_bytes, self.accounting,
        )?;
        let bytes = F::key_owned_bytes(&value);
        Ok(Owned::new(value, allocation, bytes))
    }

    fn clone_bound(&mut self, key: Option<&F::Key>) -> Result<Owned<Option<F::Key>>, crate::FsError> {
        match key {
            Some(key) => {
                let Owned { value, allocation, bytes } = self.clone_key(key)?;
                Ok(Owned::new(Some(value), allocation, bytes))
            }
            None => self.empty(None),
        }
    }

    fn clone_value(&mut self, value: &F::Value) -> Result<Owned<Option<F::Value>>, crate::FsError> {
        let (value, allocation) = persistent_io::clone_value_with_accounting::<F>(
            value, self.limits.maximum_name_bytes, self.accounting,
        )?;
        let bytes = F::value_owned_bytes(&value);
        Ok(Owned::new(Some(value), allocation, bytes))
    }

    fn charge_items(&mut self, count: u64) -> Result<(), crate::FsError> {
        self.accounting.charge(WorkCounters { items_examined: count, ..WorkCounters::UNCHARGED })
    }

    async fn rewrite(&mut self, root: ObjectId, mutations: &[M]) -> Result<Owned<Vec<Summary<F::Key>>>, crate::FsError> {
        let mut stack = Owned::<Vec<InternalFrame<F>>>::allocate(0, self.accounting)?;
        let mut request = NodeRequest {
            page: root, lower: self.empty(None)?, upper: self.empty(None)?,
            mutations: 0..mutations.len(), height: 1,
        };
        'traverse: loop {
            let mut result = match self.enter_node(request, mutations).await? {
                EnteredNode::Complete(result) => result,
                EnteredNode::Internal(mut frame) => {
                    if let Some(next) = self.advance_frame(&mut frame, mutations)? {
                        stack.ensure_for_push(usize::from(self.limits.maximum_page_height), self.accounting)?;
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
                append_owned(&mut frame.rewritten, result, self.accounting)?;
                if let Some(next) = self.advance_frame(&mut frame, mutations)? {
                    stack.ensure_for_push(usize::from(self.limits.maximum_page_height), self.accounting)?;
                    stack.value.push(frame);
                    request = next;
                    continue 'traverse;
                }
                result = self.finish_frame(frame).await?;
            }
        }
    }

    async fn enter_node(&mut self, request: NodeRequest<F::Key>, mutations: &[M]) -> Result<EnteredNode<F>, crate::FsError> {
        if request.height > self.limits.maximum_page_height { return Err(Self::error(Error::HeightExceeded)); }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let inserted = VisitedObjectSet::insert_with_accounting(&mut self.visited, request.page, self.accounting)?;
        if !inserted.inserted { return Err(Self::error(Error::CycleOrAlias)); }
        let decoded = self.read_page(request.page).await?;
        let Owned { value: page, allocation, bytes } = decoded;
        match page {
            Page::Leaf(entries) => {
                let entries = Owned::new(entries, allocation, bytes);
                if !leaf_within::<F>(&entries.value, request.lower.value.as_ref(), request.upper.value.as_ref()) {
                    return Err(Self::error(Error::ChildBoundsMismatch));
                }
                request.lower.retire(self.accounting)?;
                request.upper.retire(self.accounting)?;
                let selected = mutations.get(request.mutations).ok_or_else(|| Self::error(Error::MutationContract))?;
                Ok(EnteredNode::Complete(self.rewrite_leaf(request.page, entries, selected).await?))
            }
            Page::Internal(children) => {
                let children = Owned::new(children, allocation, bytes);
                if !children_within(&children.value, request.lower.value.as_ref(), request.upper.value.as_ref()) {
                    return Err(Self::error(Error::ChildBoundsMismatch));
                }
                request.lower.retire(self.accounting)?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page, children, inherited_upper: request.upper,
                    next_child: 0, mutation_cursor: request.mutations.start, mutation_end: request.mutations.end,
                    height: request.height, rewritten: Owned::allocate(0, self.accounting)?,
                }))
            }
        }
    }

    fn advance_frame(&mut self, frame: &mut InternalFrame<F>, mutations: &[M]) -> Result<Option<NodeRequest<F::Key>>, crate::FsError> {
        while let Some(child) = frame.children.value.get(frame.next_child) {
            let index = frame.next_child;
            frame.next_child += 1;
            let selected_start = frame.mutation_cursor;
            if let Some(next) = frame.children.value.get(index + 1) {
                while frame.mutation_cursor < frame.mutation_end {
                    self.charge_items(1)?;
                    let mutation = mutations.get(frame.mutation_cursor).ok_or_else(|| Self::error(Error::MutationContract))?;
                    if mutation.key() >= &next.first { break; }
                    frame.mutation_cursor += 1;
                }
            } else {
                frame.mutation_cursor = frame.mutation_end;
            }
            self.charge_items(1)?;
            if selected_start == frame.mutation_cursor {
                let first = self.clone_key(&child.first)?;
                push_summary(&mut frame.rewritten, first, child.page, self.accounting)?;
                continue;
            }
            let lower = self.clone_bound(Some(&child.first))?;
            let upper = self.clone_bound(frame.children.value.get(index + 1).map(|next| &next.first).or(frame.inherited_upper.value.as_ref()))?;
            return Ok(Some(NodeRequest {
                page: child.page, lower, upper, mutations: selected_start..frame.mutation_cursor,
                height: frame.height.checked_add(1).ok_or_else(|| Self::error(Error::HeightExceeded))?,
            }));
        }
        Ok(None)
    }

    async fn finish_frame(&mut self, frame: InternalFrame<F>) -> Result<Owned<Vec<Summary<F::Key>>>, crate::FsError> {
        let result = if frame.rewritten.value.is_empty() {
            frame.rewritten
        } else if unchanged(&frame.rewritten.value, &frame.children.value) {
            let mut result = Owned::allocate(1, self.accounting)?;
            let first = frame.children.value.first().ok_or_else(|| Self::error(Error::MutationContract))?;
            let first = self.clone_key(&first.first)?;
            push_summary(&mut result, first, frame.original, self.accounting)?;
            frame.rewritten.retire(self.accounting)?;
            result
        } else if frame.height == 1 && frame.rewritten.value.len() == 1 {
            frame.rewritten
        } else {
            let result = self.write_internal_chunks(&frame.rewritten.value).await?;
            frame.rewritten.retire(self.accounting)?;
            result
        };
        frame.children.retire(self.accounting)?;
        frame.inherited_upper.retire(self.accounting)?;
        Ok(result)
    }

    async fn rewrite_leaf(&mut self, original: ObjectId, mut entries: Owned<Vec<F::Value>>, mutations: &[M]) -> Result<Owned<Vec<Summary<F::Key>>>, crate::FsError> {
        let changed;
        if mutations.iter().all(|mutation| !mutation.changes_cardinality()) {
            changed = self.apply_point_mutations(&mut entries, mutations)?;
        } else {
            let rewritten = self.apply_structural_mutations(&entries.value, mutations)?;
            changed = rewritten.value != entries.value;
            entries.retire(self.accounting)?;
            entries = rewritten;
        }
        let result = if entries.value.is_empty() {
            Owned::allocate(0, self.accounting)?
        } else if !changed {
            let mut result = Owned::allocate(1, self.accounting)?;
            let first = self.clone_key(F::key(entries.value.first().ok_or_else(|| Self::error(Error::MutationContract))?))?;
            push_summary(&mut result, first, original, self.accounting)?;
            result
        } else {
            self.write_leaf_chunks(&entries.value).await?
        };
        entries.retire(self.accounting)?;
        Ok(result)
    }

    fn apply_mutation(&mut self, mutation: &M, current: &mut Owned<Option<F::Value>>) -> Result<(), crate::FsError> {
        let next = mutation.replacement(current.value.as_ref()).map_err(|error| Self::error(Error::Semantic(error)))?;
        let replacement = match next { Some(next) => self.clone_value(next)?, None => self.empty(None)? };
        let prior = std::mem::replace(current, replacement);
        prior.retire(self.accounting)
    }

    fn apply_point_mutations(&mut self, entries: &mut Owned<Vec<F::Value>>, mutations: &[M]) -> Result<bool, crate::FsError> {
        let mut changed = false;
        for mutation in mutations {
            let (found, comparisons) = search::<F>(&entries.value, mutation.key());
            self.charge_items(comparisons)?;
            let prior = found.ok().and_then(|index| entries.value.get(index));
            let next = mutation.replacement(prior).map_err(|error| Self::error(Error::Semantic(error)))?;
            if prior.is_some() != next.is_some() { return Err(Self::error(Error::MutationContract)); }
            if prior == next { continue; }
            changed = true;
            if let (Ok(index), Some(next)) = (found, next) {
                let mut current = self.clone_value(next)?;
                let replacement = current.value.take().ok_or_else(|| Self::error(Error::MutationContract))?;
                let slot = entries.value.get_mut(index).ok_or_else(|| Self::error(Error::MutationContract))?;
                let old_bytes = F::value_owned_bytes(slot);
                let old_allocation = split_guard(&mut entries.allocation, old_bytes)?;
                drop(std::mem::replace(slot, replacement));
                retire_allocation(old_allocation, old_bytes, self.accounting)?;
                absorb(&mut entries.allocation, current.allocation)?;
                entries.bytes = entries.bytes.checked_sub(old_bytes).and_then(|bytes| bytes.checked_add(current.bytes)).ok_or(WorkError::Overflow)?;
            }
        }
        Ok(changed)
    }

    fn apply_structural_mutations(&mut self, entries: &[F::Value], mutations: &[M]) -> Result<Owned<Vec<F::Value>>, crate::FsError> {
        let capacity = entries.len().checked_add(mutations.len()).ok_or_else(|| Self::error(Error::AllocationFailed))?;
        let mut output = Owned::allocate(capacity, self.accounting)?;
        let mut entry_index = 0;
        let mut mutation_index = 0;
        while entry_index < entries.len() || mutation_index < mutations.len() {
            let Some(mutation) = mutations.get(mutation_index) else {
                self.charge_items(1)?;
                let entry = entries.get(entry_index).ok_or_else(|| Self::error(Error::MutationContract))?;
                let cloned = self.clone_value(entry)?;
                push_value(&mut output, cloned)?;
                entry_index += 1;
                continue;
            };
            let entry = entries.get(entry_index);
            let ordering = entry.map(|entry| F::key(entry).cmp(mutation.key()));
            self.charge_items(u64::from(ordering.is_some()))?;
            if matches!(ordering, Some(std::cmp::Ordering::Less)) {
                let cloned = self.clone_value(entry.ok_or_else(|| Self::error(Error::MutationContract))?)?;
                push_value(&mut output, cloned)?;
                entry_index += 1;
                continue;
            }
            let mut current = if matches!(ordering, Some(std::cmp::Ordering::Equal)) {
                entry_index += 1;
                self.clone_value(entry.ok_or_else(|| Self::error(Error::MutationContract))?)?
            } else { self.empty(None)? };
            let key = mutation.key();
            while let Some(mutation) = mutations.get(mutation_index).filter(|mutation| mutation.key() == key) {
                self.charge_items(1)?;
                self.apply_mutation(mutation, &mut current)?;
                mutation_index += 1;
            }
            if current.value.is_some() { push_value(&mut output, current)?; }
            else { current.retire(self.accounting)?; }
        }
        Ok(output)
    }

    async fn write_leaf_chunks(&mut self, entries: &[F::Value]) -> Result<Owned<Vec<Summary<F::Key>>>, crate::FsError> {
        let (mut chunks, examined) = PageChunks::new(entries, self.limits, F::leaf_item_encoded_length).map_err(Self::error)?;
        self.charge_items(examined)?;
        let mut result = Owned::allocate(0, self.accounting)?;
        while let Some((chunk, examined)) = chunks.next().map_err(Self::error)? {
            self.charge_items(examined)?;
            let first = self.clone_key(F::key(chunk.first().ok_or_else(|| Self::error(Error::MutationContract))?))?;
            result.ensure_for_push(entries.len(), self.accounting)?;
            let page = self.write_page(&PageRef::<F>::Leaf(chunk)).await?;
            push_summary(&mut result, first, page, self.accounting)?;
        }
        Ok(result)
    }

    async fn write_internal_chunks(&mut self, children: &[Summary<F::Key>]) -> Result<Owned<Vec<Summary<F::Key>>>, crate::FsError> {
        let child_length = |child: &Summary<F::Key>| F::internal_item_encoded_length(&child.first);
        let (mut counted, examined) = PageChunks::new(children, self.limits, child_length).map_err(Self::error)?;
        self.charge_items(examined)?;
        let mut pages = 0;
        while let Some((_, examined)) = counted.next().map_err(Self::error)? {
            self.charge_items(examined)?;
            pages += 1;
        }
        if children.len() > 1 && pages >= children.len() { return Err(Self::error(Error::PageItemTooLarge)); }
        let (mut chunks, examined) = PageChunks::new(children, self.limits, child_length).map_err(Self::error)?;
        self.charge_items(examined)?;
        let mut result = Owned::allocate(pages, self.accounting)?;
        while let Some((chunk, examined)) = chunks.next().map_err(Self::error)? {
            self.charge_items(examined)?;
            let first = self.clone_key(&chunk.first().ok_or_else(|| Self::error(Error::MutationContract))?.first)?;
            let page = self.write_page(&PageRef::<F>::Internal(chunk)).await?;
            push_summary(&mut result, first, page, self.accounting)?;
        }
        Ok(result)
    }

    async fn finish_root(&mut self, mut summaries: Owned<Vec<Summary<F::Key>>>) -> Result<ObjectId, crate::FsError> {
        if summaries.value.is_empty() {
            summaries.retire(self.accounting)?;
            return self.write_page(&PageRef::<F>::Leaf(&[])).await;
        }
        let mut height = self.maximum_seen_height;
        while summaries.value.len() > 1 {
            height = height.checked_add(1).ok_or_else(|| Self::error(Error::HeightExceeded))?;
            if height > self.limits.maximum_page_height { return Err(Self::error(Error::HeightExceeded)); }
            let next = self.write_internal_chunks(&summaries.value).await?;
            summaries.retire(self.accounting)?;
            summaries = next;
        }
        let root = summaries.value.first().ok_or_else(|| Self::error(Error::MutationContract))?.page;
        summaries.retire(self.accounting)?;
        Ok(root)
    }

    async fn read_page(&mut self, page: ObjectId) -> Result<Owned<Page<F>>, crate::FsError> {
        let mut pages = persistent_io::read_pages_with_accounting::<S, F, _>(
            self.store, std::iter::once(page), self.limits, self.accounting, self.cancellation, None,
        ).await?;
        let (decoded, allocation) = pages.next_with_accounting(self.limits, self.accounting, self.cancellation)?;
        let bytes = decoded.logical_bytes;
        match decoded.page {
            PageLease::Owned(page) => Ok(Owned::new(page, allocation, bytes)),
            PageLease::Shared { page, .. } => {
                let result = match page.as_ref() {
                    Page::Leaf(entries) => {
                        let mut output = Owned::allocate(entries.len(), self.accounting)?;
                        for entry in entries { let cloned = self.clone_value(entry)?; push_value(&mut output, cloned)?; }
                        let Owned { value, allocation, bytes } = output;
                        Owned::new(Page::Leaf(value), allocation, bytes)
                    }
                    Page::Internal(children) => {
                        let mut output = Owned::allocate(children.len(), self.accounting)?;
                        for child in children {
                            let first = self.clone_key(&child.first)?;
                            push_summary(&mut output, first, child.page, self.accounting)?;
                        }
                        let Owned { value, allocation, bytes } = output;
                        Owned::new(Page::Internal(value), allocation, bytes)
                    }
                };
                drop(page);
                retire_allocation(allocation, bytes, self.accounting)?;
                Ok(result)
            }
        }
    }

    async fn write_page(&mut self, page: &PageRef<'_, F>) -> Result<ObjectId, crate::FsError> {
        self.accounting.check()?;
        self.accounting.admit(PAGE_WRITE)?;
        let encoded_length = F::page_encoded_length(page, self.limits.maximum_page_items)?;
        if usize_to_u64(encoded_length) > self.limits.maximum_page_object_bytes() { return Err(Self::error(Error::PageItemTooLarge)); }
        let (encoded, allocation) = F::encode_with_accounting(page, self.limits.maximum_page_items, self.accounting)?;
        let capacity = usize_to_u64(encoded.capacity());
        let mut encoded = Owned::new(encoded, allocation, capacity);
        if encoded.value.len() != encoded_length { return Err(Self::error(Error::MutationContract)); }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.accounting.native_work() {
            let KernelOwnedAllocation::NativeOriginal(allocation) = &mut encoded.allocation else {
                return Err(Self::error(Error::MutationContract));
            };
            let hashed = HashedObject::from_native_vec(F::kind(), &mut encoded.value, allocation, work, self.cancellation)
                .map_err(storage_error)?;
            let object = hashed.object_id();
            self.accounting.charge(PAGE_WRITE)?;
            crate::AsyncObjectStore::put_hashed_with_native_work(self.store, hashed, work, self.cancellation)
                .await.map_err(storage_error)?;
            drop(encoded);
            return Ok(object);
        }
        let hashing = WorkCounters {
            bytes_hashed: usize_to_u64(encoded.value.len()).checked_add(OBJECT_DIGEST_ENVELOPE_BYTES).ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        self.accounting.admit(hashing)?;
        let hashed = HashedObject::new(F::kind(), std::mem::take(&mut encoded.value).into());
        let object = hashed.object_id();
        self.accounting.charge(hashing)?;
        self.accounting.charge(PAGE_WRITE)?;
        let remaining = self.accounting.remaining()?;
        match crate::AsyncObjectStore::put_hashed(self.store, hashed, remaining, self.cancellation).await {
            Ok(receipt) => self.accounting.charge(receipt.work)?,
            Err(failure) => {
                self.accounting.charge(*failure.work)?;
                encoded.retire(self.accounting)?;
                return Err(storage_error(failure.error));
            }
        }
        encoded.retire(self.accounting)?;
        Ok(object)
    }
}

fn storage_error(error: ObjectStoreError) -> crate::FsError {
    match error {
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => error.into(),
    }
}

fn split_guard(allocation: &mut KernelOwnedAllocation, bytes: u64) -> Result<KernelOwnedAllocation, crate::FsError> {
    match allocation {
        KernelOwnedAllocation::Standalone => Ok(KernelOwnedAllocation::Standalone),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => Ok(KernelOwnedAllocation::NativeOriginal(allocation.split_off(bytes)?)),
    }
}

fn push_summary<K>(output: &mut Owned<Vec<Child<K>>>, first: Owned<K>, page: ObjectId, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    output.ensure_for_push(usize::MAX, accounting)?;
    let Owned { value, allocation, bytes } = first;
    output.value.push(Child { first: value, page });
    absorb(&mut output.allocation, allocation)?;
    output.bytes = output.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
    Ok(())
}

fn push_value<V>(output: &mut Owned<Vec<V>>, mut value: Owned<Option<V>>) -> Result<(), crate::FsError> {
    let entry = value.value.take().ok_or(CanonicalDecodeError::AllocationFailed)?;
    output.value.push(entry);
    absorb(&mut output.allocation, value.allocation)?;
    output.bytes = output.bytes.checked_add(value.bytes).ok_or(WorkError::Overflow)?;
    Ok(())
}

fn append_owned<T>(output: &mut Owned<Vec<T>>, mut source: Owned<Vec<T>>, accounting: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    let buffer_bytes = usize_to_u64(source.value.capacity().checked_mul(std::mem::size_of::<T>()).ok_or(WorkError::Overflow)?);
    while !source.value.is_empty() {
        output.ensure_for_push(usize::MAX, accounting)?;
        let spare = output.value.capacity() - output.value.len();
        let count = spare.min(source.value.len());
        output.value.extend(source.value.drain(..count));
    }
    let buffer_allocation = split_guard(&mut source.allocation, buffer_bytes)?;
    drop(source.value);
    retire_allocation(buffer_allocation, buffer_bytes, accounting)?;
    absorb(&mut output.allocation, source.allocation)?;
    output.bytes = output.bytes.checked_add(source.bytes.checked_sub(buffer_bytes).ok_or(WorkError::Overflow)?).ok_or(WorkError::Overflow)?;
    Ok(())
}

/// One authenticated page written, as charged before the backend's work.
const PAGE_WRITE: WorkCounters = WorkCounters {
    page_writes: 1,
    ..WorkCounters::UNCHARGED
};


/// Soft encoded size of every rewritten page.
///
/// Readers admit any page within [`DecodeLimits`]. Writers instead split each
/// rewritten run into balanced pages near this size, so a point mutation
/// re-encodes, re-hashes, and stages a bounded page however many items the
/// tree holds, and a split leaves room on both sides for later inserts.
pub(crate) const TARGET_PAGE_BYTES: usize = 2 * 1024;
const PAGE_HEADER_BYTES: usize = 8 + 2 + 1 + 4;

/// Balanced page boundaries over one ordered run of page items.
///
/// Each page is cut once it reaches an equal share of the remaining bytes,
/// recomputed per page, and never above the hard item and byte limits. A page
/// closes on the soft target only after holding two items, so internal levels
/// always shrink unless a single item alone reaches the hard limits.
struct PageChunks<'a, T, L> {
    items: &'a [T],
    encoded_length: L,
    start: usize,
    remaining_bytes: usize,
    maximum_items: usize,
    maximum_bytes: usize,
}

impl<'a, T, L> PageChunks<'a, T, L>
where
    L: Fn(&T) -> Result<usize, CanonicalDecodeError>,
{
    fn new<E: std::error::Error>(
        items: &'a [T],
        limits: DecodeLimits,
        encoded_length: L,
    ) -> Result<(Self, u64), Error<E>> {
        let mut remaining_bytes = 0_usize;
        for item in items {
            remaining_bytes = remaining_bytes
                .checked_add(encoded_length(item)?)
                .ok_or(Error::PageItemTooLarge)?;
        }
        Ok((
            Self {
                items,
                encoded_length,
                start: 0,
                remaining_bytes,
                maximum_items: usize::try_from(limits.maximum_page_items)
                    .map_err(|_| Error::InvalidLimits)?,
                maximum_bytes: usize::try_from(limits.maximum_page_bytes)
                    .map_err(|_| Error::InvalidLimits)?,
            },
            usize_to_u64(items.len()),
        ))
    }

    /// Returns the next non-empty page and the items examined to cut it.
    fn next<E: std::error::Error>(&mut self) -> Result<Option<(&'a [T], u64)>, Error<E>> {
        let items = self.items;
        let Some(rest) = items.get(self.start..).filter(|rest| !rest.is_empty()) else {
            return Ok(None);
        };
        let pages = self
            .remaining_bytes
            .div_ceil(TARGET_PAGE_BYTES)
            .max(rest.len().div_ceil(self.maximum_items.max(1)))
            .max(1);
        let share = self.remaining_bytes.div_ceil(pages);
        let mut count = 0_usize;
        let mut payload = 0_usize;
        for item in rest {
            if count == self.maximum_items || (count >= 2 && payload >= share) {
                break;
            }
            let item_bytes = (self.encoded_length)(item)?;
            let next = payload
                .checked_add(item_bytes)
                .ok_or(Error::PageItemTooLarge)?;
            if next.saturating_add(PAGE_HEADER_BYTES) > self.maximum_bytes {
                break;
            }
            payload = next;
            count += 1;
        }
        let page = rest
            .get(..count)
            .filter(|page| !page.is_empty())
            .ok_or(Error::PageItemTooLarge)?;
        self.start += count;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(payload);
        Ok(Some((page, usize_to_u64(count))))
    }
}

fn search<F: Format>(entries: &[F::Value], key: &F::Key) -> (Result<usize, usize>, u64) {
    counted_binary_search(entries, |entry| F::key(entry).cmp(key))
}

fn unchanged<K: Eq>(rewritten: &[Summary<K>], children: &[Child<K>]) -> bool {
    rewritten.len() == children.len()
        && rewritten
            .iter()
            .zip(children)
            .all(|(left, right)| left.first == right.first && left.page == right.page)
}

/// Whether a page whose first and last keys are `first` and `last` matches its
/// parent's routing: it starts exactly at `lower` and stays below `upper`.
pub(crate) fn within_bounds<K: Ord>(
    first: Option<&K>,
    last: Option<&K>,
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
    (lower.is_none() || first == lower)
        && !matches!((last, upper), (Some(last), Some(upper)) if last >= upper)
}

/// [`within_bounds`] for a leaf's values.
pub(crate) fn leaf_within<F: Format>(
    values: &[F::Value],
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> bool {
    within_bounds(
        values.first().map(F::key),
        values.last().map(F::key),
        lower,
        upper,
    )
}

/// [`within_bounds`] for an internal page's routing keys.
pub(crate) fn children_within<K: Ord>(
    children: &[Child<K>],
    lower: Option<&K>,
    upper: Option<&K>,
) -> bool {
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
