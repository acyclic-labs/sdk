//! Shared immutable path-copy mechanics for authenticated B+tree formats.

use super::allocation::{AllocationError, AllocationLedger, VisitedObjectSet};
use super::codec::DecodedPageShape;
use super::persistent_io::{self, OwnedPage};
use super::{CanonicalDecodeError, DecodeLimits};
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    OBJECT_DIGEST_ENVELOPE_BYTES, ObjectId, ObjectKind, ObjectStoreError, object_digest,
};
use bytes::Bytes;
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
    fn decode_shape(
        bytes: &[u8],
        limits: DecodeLimits,
    ) -> Result<DecodedPageShape, CanonicalDecodeError>;
    fn encode(
        page: &PageRef<'_, Self>,
        maximum_items: u32,
    ) -> Result<Vec<u8>, CanonicalDecodeError>
    where
        Self: Sized;
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
    fn try_clone_key(
        key: &Self::Key,
        maximum_bytes: u32,
    ) -> Result<Self::Key, CanonicalDecodeError>;
    fn try_clone_value(
        value: &Self::Value,
        maximum_bytes: u32,
    ) -> Result<Self::Value, CanonicalDecodeError>;
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
    fn apply_current(&self, current: &mut Option<F::Value>) -> Result<(), Self::Error>;
}

#[derive(Clone)]
struct IndexedMutation<M> {
    ordinal: usize,
    mutation: M,
}

impl<F, M> Mutation<F> for IndexedMutation<M>
where
    F: Format,
    M: Mutation<F>,
{
    type Error = M::Error;

    fn key(&self) -> &F::Key {
        self.mutation.key()
    }

    fn changes_cardinality(&self) -> bool {
        self.mutation.changes_cardinality()
    }

    fn apply_current(&self, current: &mut Option<F::Value>) -> Result<(), Self::Error> {
        self.mutation.apply_current(current)
    }
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

struct OwnedValues<F: Format> {
    values: Vec<F::Value>,
    logical_bytes: u64,
}

struct NodeRequest<K> {
    page: ObjectId,
    lower: Option<K>,
    upper: Option<K>,
    mutations: Range<usize>,
    height: u16,
}

struct InternalFrame<F: Format> {
    original: ObjectId,
    children: Vec<Child<F::Key>>,
    inherited_upper: Option<F::Key>,
    next_child: usize,
    mutation_cursor: usize,
    mutation_end: usize,
    height: u16,
    rewritten: Vec<Summary<F::Key>>,
    logical_bytes: u64,
}

enum EnteredNode<F: Format> {
    Complete(Vec<Summary<F::Key>>),
    Internal(InternalFrame<F>),
}

struct Context<'a, S, F, M>
where
    F: Format,
    M: Mutation<F>,
{
    store: &'a S,
    limits: DecodeLimits,
    budget: WorkBudget,
    work: WorkCounters,
    allocations: AllocationLedger,
    visited: VisitedObjectSet,
    maximum_seen_height: u16,
    cancellation: &'a CancellationToken,
    marker: PhantomData<(F, M)>,
}

pub(crate) fn apply<S, F, M>(
    store: &S,
    root: ObjectId,
    mutations: Vec<M>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
) -> Result<Receipt, OperationFailure<Error<M::Error>>>
where
    S: crate::ImmediateObjectStore,
    F: Format,
    M: Mutation<F>,
{
    let cancellation = CancellationToken::new();
    crate::async_storage::poll_immediate(apply_async::<S, F, M>(
        store,
        root,
        mutations,
        maximum_mutations,
        limits,
        budget,
        &cancellation,
    ))
}

pub(crate) async fn apply_async<S, F, M>(
    store: &S,
    root: ObjectId,
    mutations: Vec<M>,
    maximum_mutations: u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Receipt, OperationFailure<Error<M::Error>>>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    validate::<F, M>(root, &mutations, maximum_mutations, limits)?;
    let mut allocations = AllocationLedger::default();
    let mut initial_work = WorkCounters::default();
    let ordered_allocation = allocations
        .claim_elements::<IndexedMutation<M>>(mutations.len(), &mut initial_work, budget)
        .map_err(|error| OperationFailure::new(Error::Allocation(error), initial_work))?;
    let mut ordered = Vec::new();
    if ordered.try_reserve_exact(mutations.len()).is_err() {
        return Err(OperationFailure::new(Error::AllocationFailed, initial_work));
    }
    ordered.extend(
        mutations
            .into_iter()
            .enumerate()
            .map(|(ordinal, mutation)| IndexedMutation { ordinal, mutation }),
    );
    sort_indexed::<F, M>(&mut ordered, &mut initial_work, budget)
        .map_err(|error| OperationFailure::new(Error::Work(error), initial_work))?;
    let maximum_visited = ordered
        .len()
        .checked_mul(usize::from(limits.maximum_page_height))
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| OperationFailure::before_work(Error::InvalidLimits))?
        .min(
            usize::try_from(limits.maximum_visited_pages)
                .map_err(|_| OperationFailure::before_work(Error::InvalidLimits))?,
        );
    let visited =
        VisitedObjectSet::new(maximum_visited, &mut allocations, &mut initial_work, budget)
            .map_err(|error| OperationFailure::new(Error::Allocation(error), initial_work))?;
    let mut context = Context::<S, F, IndexedMutation<M>> {
        store,
        limits,
        budget,
        work: initial_work,
        allocations,
        visited,
        maximum_seen_height: 0,
        cancellation,
        marker: PhantomData,
    };
    let summaries = context
        .rewrite(root, &ordered)
        .await
        .map_err(|error| OperationFailure::new(error, context.work))?;
    let new_root = context
        .finish_root(summaries)
        .await
        .map_err(|error| OperationFailure::new(error, context.work))?;
    context
        .visited
        .release(&mut context.allocations)
        .map_err(|error| OperationFailure::new(Error::Allocation(error), context.work))?;
    context
        .allocations
        .release(ordered_allocation)
        .map_err(|error| OperationFailure::new(Error::Allocation(error), context.work))?;
    Ok(Receipt {
        root: new_root,
        work: context.work,
    })
}

fn sort_indexed<F, M>(
    mutations: &mut [IndexedMutation<M>],
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), WorkError>
where
    F: Format,
    M: Mutation<F>,
{
    let count = mutations.len();
    if count < 2 {
        return Ok(());
    }
    let scan_bound = crate::foundation::usize_to_u64(count - 1);
    work.checked_add(WorkCounters {
        items_examined: scan_bound,
        ..WorkCounters::default()
    })?
    .verify(budget)?;
    let mut scan_comparisons = 0_u64;
    let mut ordered = true;
    for adjacent in mutations.windows(2) {
        scan_comparisons = scan_comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
        if indexed_order::<F, M>(&adjacent[0], &adjacent[1]).is_gt() {
            ordered = false;
            break;
        }
    }
    *work = work.checked_add(WorkCounters {
        items_examined: scan_comparisons,
        ..WorkCounters::default()
    })?;
    if ordered {
        return Ok(());
    }

    let levels = usize::BITS - count.leading_zeros();
    let maximum_comparisons = u64::try_from(count)
        .ok()
        .and_then(|value| value.checked_mul(u64::from(levels)))
        .and_then(|value| value.checked_mul(3))
        .ok_or(WorkError::Overflow)?;
    work.checked_add(WorkCounters {
        items_examined: maximum_comparisons,
        ..WorkCounters::default()
    })?
    .verify(budget)?;

    let mut comparisons = 0_u64;
    for start in (0..count / 2).rev() {
        sift_down::<F, M>(mutations, start, count, &mut comparisons)?;
    }
    for end in (1..count).rev() {
        mutations.swap(0, end);
        sift_down::<F, M>(mutations, 0, end, &mut comparisons)?;
    }
    *work = work.checked_add(WorkCounters {
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
        if right < end {
            *comparisons = comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
            if indexed_order::<F, M>(&mutations[left], &mutations[right]).is_lt() {
                greater = right;
            }
        }
        *comparisons = comparisons.checked_add(1).ok_or(WorkError::Overflow)?;
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
) -> Result<(), OperationFailure<Error<M::Error>>>
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
        Some(error) => Err(OperationFailure::before_work(error)),
        None => Ok(()),
    }
}

impl<S, F, M> Context<'_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    async fn rewrite(
        &mut self,
        root: ObjectId,
        mutations: &[M],
    ) -> Result<Vec<Summary<F::Key>>, Error<M::Error>> {
        let mut stack = Vec::new();
        let mut request = NodeRequest {
            page: root,
            lower: None,
            upper: None,
            mutations: 0..mutations.len(),
            height: 1,
        };
        'traverse: loop {
            let entered = self.enter_node(request, mutations).await?;
            let mut result = match entered {
                EnteredNode::Complete(result) => result,
                EnteredNode::Internal(mut frame) => {
                    if let Some(next) = self.advance_frame(&mut frame, mutations)? {
                        stack.push(frame);
                        request = next;
                        continue;
                    }
                    self.finish_frame(frame).await?
                }
            };
            loop {
                let Some(mut frame) = stack.pop() else {
                    return Ok(result);
                };
                frame.rewritten.extend(result);
                if let Some(next) = self.advance_frame(&mut frame, mutations)? {
                    stack.push(frame);
                    request = next;
                    continue 'traverse;
                }
                result = self.finish_frame(frame).await?;
            }
        }
    }

    async fn enter_node(
        &mut self,
        request: NodeRequest<F::Key>,
        mutations: &[M],
    ) -> Result<EnteredNode<F>, Error<M::Error>> {
        if request.height > self.limits.maximum_page_height {
            return Err(Error::HeightExceeded);
        }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let visited = self.visited.insert(
            request.page,
            &mut self.allocations,
            &mut self.work,
            self.budget,
        )?;
        if !visited.inserted {
            return Err(Error::CycleOrAlias);
        }
        let decoded = self.read_page(request.page).await?;
        let (page, decoded_bytes) = decoded
            .into_owned(self.budget, &mut self.allocations, &mut self.work)
            .map_err(map_io)?;
        match page {
            Page::Leaf(entries) => {
                validate_leaf::<F, M>(&entries, request.lower.as_ref(), request.upper.as_ref())?;
                let result = self
                    .rewrite_leaf(request.page, entries, &mutations[request.mutations])
                    .await;
                self.allocations.release(decoded_bytes)?;
                Ok(EnteredNode::Complete(result?))
            }
            Page::Internal(children) => {
                validate_children::<F, M>(
                    &children,
                    request.lower.as_ref(),
                    request.upper.as_ref(),
                )?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page,
                    children,
                    inherited_upper: request.upper,
                    next_child: 0,
                    mutation_cursor: request.mutations.start,
                    mutation_end: request.mutations.end,
                    height: request.height,
                    rewritten: Vec::new(),
                    logical_bytes: decoded_bytes,
                }))
            }
        }
    }

    fn advance_frame(
        &mut self,
        frame: &mut InternalFrame<F>,
        mutations: &[M],
    ) -> Result<Option<NodeRequest<F::Key>>, Error<M::Error>> {
        while frame.next_child < frame.children.len() {
            let index = frame.next_child;
            frame.next_child += 1;
            let child = &frame.children[index];
            let child_upper = frame
                .children
                .get(index + 1)
                .map(|next| next.first.clone())
                .or_else(|| frame.inherited_upper.clone());
            let selected_start = frame.mutation_cursor;
            if let Some(next) = frame.children.get(index + 1) {
                while frame.mutation_cursor < frame.mutation_end {
                    self.charge_items(1)?;
                    if mutations[frame.mutation_cursor].key() >= &next.first {
                        break;
                    }
                    frame.mutation_cursor += 1;
                }
            } else {
                frame.mutation_cursor = frame.mutation_end;
            }
            self.charge_items(1)?;
            if selected_start == frame.mutation_cursor {
                frame.rewritten.push(Summary {
                    first: child.first.clone(),
                    page: child.page,
                });
                continue;
            }
            return Ok(Some(NodeRequest {
                page: child.page,
                lower: Some(child.first.clone()),
                upper: child_upper,
                mutations: selected_start..frame.mutation_cursor,
                height: frame.height.checked_add(1).ok_or(Error::HeightExceeded)?,
            }));
        }
        Ok(None)
    }

    async fn finish_frame(
        &mut self,
        frame: InternalFrame<F>,
    ) -> Result<Vec<Summary<F::Key>>, Error<M::Error>> {
        let result = if frame.rewritten.is_empty() {
            Ok(Vec::new())
        } else if unchanged(&frame.rewritten, &frame.children) {
            Ok(vec![Summary {
                first: frame.children[0].first.clone(),
                page: frame.original,
            }])
        } else if frame.height == 1 && frame.rewritten.len() == 1 {
            Ok(frame.rewritten)
        } else {
            self.write_internal_chunks(&frame.rewritten).await
        };
        self.allocations.release(frame.logical_bytes)?;
        result
    }

    async fn rewrite_leaf(
        &mut self,
        original: ObjectId,
        mut entries: Vec<F::Value>,
        mutations: &[M],
    ) -> Result<Vec<Summary<F::Key>>, Error<M::Error>> {
        let mut structural_allocation = 0_u64;
        let changed = if mutations
            .iter()
            .all(|mutation| !mutation.changes_cardinality())
        {
            self.apply_point_mutations(&mut entries, mutations)?
        } else {
            let original = entries;
            let rewritten = self.apply_structural_mutations(&original, mutations)?;
            entries = rewritten.values;
            structural_allocation = rewritten.logical_bytes;
            entries != original
        };
        let result = if entries.is_empty() {
            Ok(Vec::new())
        } else if !changed {
            Ok(vec![Summary {
                first: F::key(&entries[0]).clone(),
                page: original,
            }])
        } else {
            self.write_leaf_chunks(&entries).await
        };
        self.allocations.release(structural_allocation)?;
        result
    }

    fn apply_point_mutations(
        &mut self,
        entries: &mut [F::Value],
        mutations: &[M],
    ) -> Result<bool, Error<M::Error>> {
        let mut changed = false;
        for mutation in mutations {
            let (found, comparisons) = search::<F>(entries, mutation.key());
            self.charge_items(comparisons)?;
            let index = found.ok();
            let prior = index.map(|index| entries[index].clone());
            let mut current = prior.clone();
            mutation
                .apply_current(&mut current)
                .map_err(Error::Semantic)?;
            if prior.is_some() != current.is_some() {
                return Err(Error::MutationContract);
            }
            if let (Some(index), Some(replacement)) = (index, current) {
                changed |= prior.as_ref() != Some(&replacement);
                entries[index] = replacement;
            }
        }
        Ok(changed)
    }

    fn apply_structural_mutations(
        &mut self,
        entries: &[F::Value],
        mutations: &[M],
    ) -> Result<OwnedValues<F>, Error<M::Error>> {
        let capacity = entries
            .len()
            .checked_add(mutations.len())
            .ok_or(Error::AllocationFailed)?;
        let logical_bytes =
            self.allocations
                .claim_elements::<F::Value>(capacity, &mut self.work, self.budget)?;
        let mut output = Vec::new();
        if output.try_reserve_exact(capacity).is_err() {
            self.allocations.release(logical_bytes)?;
            return Err(Error::AllocationFailed);
        }

        let mut entry_index = 0_usize;
        let mut mutation_index = 0_usize;
        while entry_index < entries.len() || mutation_index < mutations.len() {
            if mutation_index == mutations.len() {
                self.charge_items(1)?;
                output.push(entries[entry_index].clone());
                entry_index += 1;
                continue;
            }
            let mutation_key = mutations[mutation_index].key();
            let ordering = entries
                .get(entry_index)
                .map(|entry| F::key(entry).cmp(mutation_key));
            self.charge_items(u64::from(ordering.is_some()))?;
            if matches!(ordering, Some(std::cmp::Ordering::Less)) {
                output.push(entries[entry_index].clone());
                entry_index += 1;
                continue;
            }
            let mut current = if matches!(ordering, Some(std::cmp::Ordering::Equal)) {
                let value = entries[entry_index].clone();
                entry_index += 1;
                Some(value)
            } else {
                None
            };
            let key = mutation_key.clone();
            while mutation_index < mutations.len() && mutations[mutation_index].key() == &key {
                self.charge_items(1)?;
                mutations[mutation_index]
                    .apply_current(&mut current)
                    .map_err(Error::Semantic)?;
                mutation_index += 1;
            }
            if let Some(value) = current {
                output.push(value);
            }
        }
        Ok(OwnedValues {
            values: output,
            logical_bytes,
        })
    }

    fn charge_items(&mut self, count: u64) -> Result<(), Error<M::Error>> {
        let prospective = self.work.checked_add(WorkCounters {
            items_examined: count,
            ..WorkCounters::default()
        })?;
        prospective.verify(self.budget)?;
        self.work = prospective;
        Ok(())
    }

    async fn write_leaf_chunks(
        &mut self,
        entries: &[F::Value],
    ) -> Result<Vec<Summary<F::Key>>, Error<M::Error>> {
        let mut result = Vec::new();
        let mut start = 0_usize;
        while start < entries.len() {
            let (end, examined) =
                page_chunk_end(entries, start, self.limits, F::leaf_item_encoded_length)?;
            self.charge_items(examined)?;
            let chunk = &entries[start..end];
            result.push(Summary {
                first: F::key(&chunk[0]).clone(),
                page: self.write_page(&PageRef::<F>::Leaf(chunk)).await?,
            });
            start = end;
        }
        Ok(result)
    }

    async fn write_internal_chunks(
        &mut self,
        children: &[Summary<F::Key>],
    ) -> Result<Vec<Summary<F::Key>>, Error<M::Error>> {
        let mut cursor = 0_usize;
        let mut chunks = 0_usize;
        while cursor < children.len() {
            let (end, examined) = page_chunk_end(children, cursor, self.limits, |child| {
                F::internal_item_encoded_length(&child.first)
            })?;
            self.charge_items(examined)?;
            chunks += 1;
            cursor = end;
        }
        if children.len() > 1 && chunks >= children.len() {
            return Err(Error::PageItemTooLarge);
        }
        let mut result = Vec::new();
        let mut start = 0_usize;
        while start < children.len() {
            let (end, examined) = page_chunk_end(children, start, self.limits, |child| {
                F::internal_item_encoded_length(&child.first)
            })?;
            self.charge_items(examined)?;
            let chunk = &children[start..end];
            result.push(Summary {
                first: chunk[0].first.clone(),
                page: self.write_page(&PageRef::<F>::Internal(chunk)).await?,
            });
            start = end;
        }
        Ok(result)
    }

    async fn finish_root(
        &mut self,
        mut summaries: Vec<Summary<F::Key>>,
    ) -> Result<ObjectId, Error<M::Error>> {
        if summaries.is_empty() {
            return self.write_page(&PageRef::<F>::Leaf(&[])).await;
        }
        let mut height = self.maximum_seen_height;
        while summaries.len() > 1 {
            height = height.checked_add(1).ok_or(Error::HeightExceeded)?;
            if height > self.limits.maximum_page_height {
                return Err(Error::HeightExceeded);
            }
            summaries = self.write_internal_chunks(&summaries).await?;
        }
        Ok(summaries[0].page)
    }

    async fn read_page(&mut self, page: ObjectId) -> Result<OwnedPage<F>, Error<M::Error>> {
        persistent_io::read_page_mutable::<S, F>(
            self.store,
            page,
            self.limits,
            self.budget,
            self.cancellation,
            &mut self.allocations,
            &mut self.work,
        )
        .await
        .map_err(map_io)
    }

    async fn write_page(&mut self, page: &PageRef<'_, F>) -> Result<ObjectId, Error<M::Error>> {
        let encoded_length = F::page_encoded_length(page, self.limits.maximum_page_items)?;
        let encoded_bytes = crate::foundation::usize_to_u64(encoded_length);
        if encoded_bytes > self.limits.maximum_page_object_bytes() {
            return Err(Error::PageItemTooLarge);
        }
        let allocation =
            self.allocations
                .claim_elements::<u8>(encoded_length, &mut self.work, self.budget)?;
        let encoded_work = self.work.checked_add(WorkCounters {
            bytes_encoded: encoded_bytes,
            ..WorkCounters::default()
        })?;
        if let Err(error) = encoded_work.verify(self.budget) {
            self.allocations.release(allocation)?;
            return Err(Error::Work(error));
        }
        let encoded = F::encode(page, self.limits.maximum_page_items)?;
        self.work = encoded_work;
        if encoded.len() != encoded_length {
            self.allocations.release(allocation)?;
            return Err(Error::MutationContract);
        }
        let hashed_work = self.work.checked_add(WorkCounters {
            bytes_hashed: encoded_bytes
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::default()
        })?;
        if let Err(error) = hashed_work.verify(self.budget) {
            self.allocations.release(allocation)?;
            return Err(Error::Work(error));
        }
        let object = ObjectId {
            kind: F::kind(),
            digest: object_digest(F::kind(), &encoded),
        };
        self.work = hashed_work;
        let prospective = self.work.checked_add(WorkCounters {
            page_writes: 1,
            ..WorkCounters::default()
        })?;
        let remaining = prospective.remaining(self.budget)?;
        let receipt = match crate::AsyncObjectStore::put(
            self.store,
            object,
            Bytes::from(encoded),
            remaining,
            self.cancellation,
        )
        .await
        {
            Ok(receipt) => receipt,
            Err(failure) => {
                self.allocations.release(allocation)?;
                return match prospective.checked_add(*failure.work) {
                    Ok(spent) => {
                        self.work = spent;
                        Err(Error::Storage(failure.error))
                    }
                    Err(error) => {
                        self.work = prospective;
                        Err(Error::Work(error))
                    }
                };
            }
        };
        self.work = prospective.checked_add(receipt.work)?;
        self.work.verify(self.budget)?;
        self.allocations.release(allocation)?;
        Ok(object)
    }
}

fn map_io<E: std::error::Error>(error: persistent_io::Error) -> Error<E> {
    match error {
        persistent_io::Error::AllocationFailed => Error::AllocationFailed,
        persistent_io::Error::Allocation(error) => Error::Allocation(error),
        persistent_io::Error::Storage(error) => Error::Storage(error),
        persistent_io::Error::Decode(error) => Error::Decode(error),
        persistent_io::Error::Work(error) => Error::Work(error),
    }
}

fn page_chunk_end<T, E: std::error::Error>(
    items: &[T],
    start: usize,
    limits: DecodeLimits,
    encoded_length: impl Fn(&T) -> Result<usize, CanonicalDecodeError>,
) -> Result<(usize, u64), Error<E>> {
    const PAGE_HEADER_BYTES: usize = 8 + 2 + 1 + 4;
    let maximum_items =
        usize::try_from(limits.maximum_page_items).map_err(|_| Error::InvalidLimits)?;
    let maximum_bytes =
        usize::try_from(limits.maximum_page_bytes).map_err(|_| Error::InvalidLimits)?;
    let mut end = start;
    let mut bytes = PAGE_HEADER_BYTES;
    let mut examined = 0_u64;
    while end < items.len() && end - start < maximum_items {
        examined = examined.saturating_add(1);
        let item_bytes = encoded_length(&items[end])?;
        let next = bytes
            .checked_add(item_bytes)
            .ok_or(Error::PageItemTooLarge)?;
        if next > maximum_bytes {
            if end == start {
                return Err(Error::PageItemTooLarge);
            }
            break;
        }
        bytes = next;
        end += 1;
    }
    if end == start {
        return Err(Error::PageItemTooLarge);
    }
    Ok((end, examined))
}

fn search<F: Format>(entries: &[F::Value], key: &F::Key) -> (Result<usize, usize>, u64) {
    let mut left = 0;
    let mut right = entries.len();
    let mut comparisons = 0_u64;
    while left < right {
        comparisons = comparisons.saturating_add(1);
        let middle = left + (right - left) / 2;
        match F::key(&entries[middle]).cmp(key) {
            std::cmp::Ordering::Less => left = middle + 1,
            std::cmp::Ordering::Greater => right = middle,
            std::cmp::Ordering::Equal => return (Ok(middle), comparisons),
        }
    }
    (Err(left), comparisons)
}

fn unchanged<K: Eq>(rewritten: &[Summary<K>], children: &[Child<K>]) -> bool {
    rewritten.len() == children.len()
        && rewritten
            .iter()
            .zip(children)
            .all(|(left, right)| left.first == right.first && left.page == right.page)
}

fn validate_leaf<F, M>(
    entries: &[F::Value],
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> Result<(), Error<M::Error>>
where
    F: Format,
    M: Mutation<F>,
{
    if lower.is_some() && entries.first().map(F::key) != lower {
        return Err(Error::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && entries.last().is_some_and(|entry| F::key(entry) >= upper)
    {
        return Err(Error::ChildBoundsMismatch);
    }
    Ok(())
}

fn validate_children<F, M>(
    children: &[Child<F::Key>],
    lower: Option<&F::Key>,
    upper: Option<&F::Key>,
) -> Result<(), Error<M::Error>>
where
    F: Format,
    M: Mutation<F>,
{
    if lower.is_some() && children.first().map(|child| &child.first) != lower {
        return Err(Error::ChildBoundsMismatch);
    }
    if let Some(upper) = upper
        && children.last().is_some_and(|child| &child.first >= upper)
    {
        return Err(Error::ChildBoundsMismatch);
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/persistent_btree.rs"]
mod tests;
