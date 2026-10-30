//! Shared immutable path-copy mechanics for authenticated B+tree formats. Original native claims are structural; sorting and producers run outside root loans.

use super::allocation::{
    AllocationError, KernelAccounting, KernelOwnedAllocation, VisitedObjectSet,
};
use super::codec::DecodedPageShape;
use super::persistent_io::{self, PageLease};
use super::persistent_point::{Owned, retire_allocation, split_allocation};
use super::search::binary_search_with_accounting;
use super::{CanonicalDecodeError, DecodeLimits};
use crate::cancellation::CancellationToken;
use crate::foundation::usize_to_u64;
use crate::performance::{OperationReceipt, WorkCounters, WorkError};
use crate::storage::{HashedObject, ObjectId, ObjectKind, ObjectStoreError};
use acyclic_collections::page::rewrite::{self, Entered};
use acyclic_collections::page::{merge, publish, selection};
use std::marker::PhantomData;
use std::ops::Range;

mod support;
pub use support::Child;
#[cfg(test)]
use support::PageChunks;
#[cfg(test)]
pub(crate) use support::TARGET_PAGE_BYTES;
pub(crate) use support::{Error, Format, Mutation, Page, PageRef, children_within, leaf_within};
use support::{
    PAGE_HEADER_BYTES, PAGE_WRITE, append_owned, push_summary, push_value, sort_indexed,
    storage_error, validate,
};

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
    selection: selection::Cursor,
    height: u16,
    rewritten: Owned<Vec<Child<F::Key>>>,
}

type EnteredNode<F> =
    Entered<InternalFrame<F>, Owned<Vec<Child<<F as acyclic_collections::page::Format>::Key>>>>;

impl<'mutation, S, F, M> rewrite::Work<[(usize, &'mutation M)]> for Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F> + 'mutation,
{
    type Request = NodeRequest<F::Key>;
    type Frame = InternalFrame<F>;
    type Output = Owned<Vec<Child<F::Key>>>;
    type Stack = Owned<Vec<InternalFrame<F>>>;
    type Error = crate::FsError;

    fn advance(
        &mut self,
        frame: &mut Self::Frame,
        mutations: &[(usize, &'mutation M)],
    ) -> Result<Option<Self::Request>, Self::Error> {
        self.advance_frame(frame, mutations)
    }

    fn push(&mut self, stack: &mut Self::Stack, frame: Self::Frame) -> Result<(), Self::Error> {
        stack.ensure_for_push(
            usize::from(self.limits.maximum_page_height),
            self.accounting,
        )?;
        stack.value.push(frame);
        Ok(())
    }

    fn pop(&mut self, stack: &mut Self::Stack) -> Option<Self::Frame> {
        stack.value.pop()
    }

    fn append(&mut self, frame: &mut Self::Frame, output: Self::Output) -> Result<(), Self::Error> {
        append_owned(&mut frame.rewritten, output, self.accounting)
    }
}

impl<'mutation, S, F, M> rewrite::Io<[(usize, &'mutation M)]> for Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F> + 'mutation,
{
    fn enter(
        &mut self,
        request: NodeRequest<F::Key>,
        mutations: &[(usize, &'mutation M)],
    ) -> impl Future<Output = Result<EnteredNode<F>, crate::FsError>> {
        self.enter_node(request, mutations)
    }

    fn finish(
        &mut self,
        frame: InternalFrame<F>,
    ) -> impl Future<Output = Result<Owned<Vec<Child<F::Key>>>, crate::FsError>> {
        self.finish_frame(frame)
    }

    fn retire_stack(&mut self, stack: Owned<Vec<InternalFrame<F>>>) -> Result<(), crate::FsError> {
        stack.retire(self.accounting)
    }
}

impl<S, F, M> merge::Work<F, M> for Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    type Current = Owned<Option<F::Value>>;
    type Output = Owned<Vec<F::Value>>;
    type Error = crate::FsError;

    fn mutation_key(mutation: &M) -> &F::Key {
        mutation.key()
    }

    fn current(&mut self, value: Option<&F::Value>) -> Result<Self::Current, Self::Error> {
        match value {
            Some(value) => self.clone_value(value),
            None => self.empty(None),
        }
    }

    fn apply(&mut self, mutation: &M, current: &mut Self::Current) -> Result<(), Self::Error> {
        self.apply_mutation(mutation, current)
    }

    fn append(
        &mut self,
        output: &mut Self::Output,
        current: Self::Current,
    ) -> Result<(), Self::Error> {
        if current.value.is_some() {
            push_value(output, current)
        } else {
            current.retire(self.accounting)
        }
    }

    fn charge_items(&mut self, count: u64) -> Result<(), Self::Error> {
        Context::charge_items(self, count)
    }

    fn invalid_input() -> Self::Error {
        Self::error(Error::MutationContract)
    }
}

impl<S, F, M> publish::Work<F> for Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    type First = Owned<F::Key>;
    type Output = Owned<Vec<Child<F::Key>>>;
    type Error = crate::FsError;

    fn limits(&self) -> Result<acyclic_collections::page::PackingLimits, Self::Error> {
        Ok(acyclic_collections::page::PackingLimits {
            maximum_items: usize::try_from(self.limits.maximum_page_items)
                .map_err(|_| Self::error(Error::InvalidLimits))?,
            maximum_bytes: usize::try_from(self.limits.maximum_page_bytes)
                .map_err(|_| Self::error(Error::InvalidLimits))?,
        })
    }

    fn leaf_length(value: &F::Value) -> Result<usize, Self::Error> {
        F::leaf_item_encoded_length(value).map_err(|error| Self::error(Error::Decode(error)))
    }

    fn child_length(child: &Child<F::Key>) -> Result<usize, Self::Error> {
        F::internal_item_encoded_length(&child.first, child.page)
            .map_err(|error| Self::error(Error::Decode(error)))
    }

    fn packing_error(error: acyclic_collections::page::PackingError<Self::Error>) -> Self::Error {
        match error {
            acyclic_collections::page::PackingError::InvalidLimits => {
                Self::error(Error::InvalidLimits)
            }
            acyclic_collections::page::PackingError::ItemTooLarge => {
                Self::error(Error::PageItemTooLarge)
            }
            acyclic_collections::page::PackingError::Encode(error) => error,
        }
    }

    fn invalid_input() -> Self::Error {
        Self::error(Error::MutationContract)
    }

    fn nonreducing() -> Self::Error {
        Self::error(Error::PageItemTooLarge)
    }

    fn next_height(&self, height: u16) -> Result<u16, Self::Error> {
        let height = height
            .checked_add(1)
            .filter(|height| *height <= self.limits.maximum_page_height)
            .ok_or_else(|| Self::error(Error::HeightExceeded))?;
        Ok(height)
    }

    fn charge_items(&mut self, count: u64) -> Result<(), Self::Error> {
        Context::charge_items(self, count)
    }

    fn allocate(&mut self, capacity: usize) -> Result<Self::Output, Self::Error> {
        Owned::allocate(capacity, self.accounting)
    }

    fn reserve(&mut self, output: &mut Self::Output, maximum: usize) -> Result<(), Self::Error> {
        output.ensure_for_push(maximum, self.accounting)
    }

    fn first(&mut self, key: &F::Key) -> Result<Self::First, Self::Error> {
        self.clone_key(key)
    }

    fn append(
        &mut self,
        output: &mut Self::Output,
        first: Self::First,
        page: ObjectId,
    ) -> Result<(), Self::Error> {
        push_summary(output, first, page, self.accounting)
    }

    fn children(output: &Self::Output) -> &[Child<F::Key>] {
        &output.value
    }

    fn retire(&mut self, output: Self::Output) -> Result<(), Self::Error> {
        output.retire(self.accounting)
    }

    fn write(
        &mut self,
        page: &PageRef<'_, F>,
    ) -> impl Future<Output = Result<ObjectId, Self::Error>> {
        self.write_page(page)
    }
}

struct Context<'a, 'accounting, 'work, S, F, M>
where
    F: Format,
    M: Mutation<F>,
{
    store: &'a S,
    limits: DecodeLimits,
    accounting: &'accounting mut KernelAccounting<'work>,
    visited: Option<Owned<VisitedObjectSet>>,
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
    apply_optional_root_with_accounting::<S, F, M>(
        store,
        Some(root),
        mutations,
        maximum_mutations,
        limits,
        accounting,
        cancellation,
    )
    .await
}

pub(crate) async fn apply_optional_root_with_accounting<S, F, M>(
    store: &S,
    root: Option<ObjectId>,
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
    let visited = if root.is_some() {
        Some(VisitedObjectSet::new_with_accounting(
            maximum_visited,
            accounting,
        )?)
    } else {
        None
    };
    let mut context = Context::<S, F, M> {
        store,
        limits,
        accounting,
        visited,
        maximum_seen_height: 0,
        cancellation,
        marker: PhantomData,
    };
    let summaries = match root {
        Some(root) => context.rewrite(root, &ordered.value).await?,
        None => {
            context.maximum_seen_height = 1;
            let values = context.apply_structural_mutations(&[], &ordered.value)?;
            let result = context.write_leaf_chunks(&values.value).await;
            values.retire(context.accounting)?;
            result?
        }
    };
    let root = context.finish_root(summaries).await?;
    if let Some(visited) = context.visited.take() {
        visited.retire(context.accounting)?;
    }
    ordered.retire(context.accounting)?;
    Ok(OperationReceipt {
        value: root,
        work: context.accounting.spent(),
    })
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
        let stack = Owned::<Vec<InternalFrame<F>>>::allocate(0, self.accounting)?;
        let request = NodeRequest {
            page: root,
            lower: self.empty(None)?,
            upper: self.empty(None)?,
            mutations: 0..mutations.len(),
            height: 1,
        };
        rewrite::run(self, stack, request, mutations).await
    }

    async fn enter_node(
        &mut self,
        request: NodeRequest<F::Key>,
        mutations: &[(usize, &M)],
    ) -> Result<EnteredNode<F>, crate::FsError> {
        if request.height > self.limits.maximum_page_height {
            return Err(Self::error(Error::HeightExceeded));
        }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let inserted = VisitedObjectSet::insert_with_accounting(
            self.visited
                .as_mut()
                .ok_or_else(|| M::map_error(Error::MutationContract))?,
            request.page,
            self.accounting,
        )?;
        if !inserted.inserted {
            return Err(Self::error(Error::CycleOrAlias));
        }
        let decoded = self.read_page(request.page).await?;
        let Owned {
            value: page,
            allocation,
            bytes,
        } = decoded;
        match page {
            Page::Leaf(entries) => {
                let entries = Owned::new(entries, allocation, bytes);
                if !leaf_within::<F>(
                    &entries.value,
                    request.lower.value.as_ref(),
                    request.upper.value.as_ref(),
                ) {
                    return Err(Self::error(Error::ChildBoundsMismatch));
                }
                request.lower.retire(self.accounting)?;
                request.upper.retire(self.accounting)?;
                let selected = mutations
                    .get(request.mutations)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                Ok(EnteredNode::Complete(
                    self.rewrite_leaf(request.page, entries, selected).await?,
                ))
            }
            Page::Internal(children) => {
                let children = Owned::new(children, allocation, bytes);
                if !children_within(
                    &children.value,
                    request.lower.value.as_ref(),
                    request.upper.value.as_ref(),
                ) {
                    return Err(Self::error(Error::ChildBoundsMismatch));
                }
                request.lower.retire(self.accounting)?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page,
                    children,
                    inherited_upper: request.upper,
                    selection: selection::Cursor::new(request.mutations),
                    height: request.height,
                    rewritten: Owned::allocate(0, self.accounting)?,
                }))
            }
        }
    }

    fn advance_frame(
        &mut self,
        frame: &mut InternalFrame<F>,
        mutations: &[(usize, &M)],
    ) -> Result<Option<NodeRequest<F::Key>>, crate::FsError> {
        while let Some(selected) = frame.selection.select(
            &frame.children.value,
            |index| {
                mutations
                    .get(index)
                    .map(|(_, mutation)| mutation.key())
                    .ok_or_else(|| Self::error(Error::MutationContract))
            },
            || self.charge_items(1),
        )? {
            let child = selected.child;
            if selected.mutations.is_empty() {
                let first = self.clone_key(&child.first)?;
                push_summary(&mut frame.rewritten, first, child.page, self.accounting)?;
                continue;
            }
            let lower = self.clone_bound(Some(&child.first))?;
            let upper =
                self.clone_bound(selected.upper.or(frame.inherited_upper.value.as_ref()))?;
            return Ok(Some(NodeRequest {
                page: child.page,
                lower,
                upper,
                mutations: selected.mutations,
                height: frame
                    .height
                    .checked_add(1)
                    .ok_or_else(|| Self::error(Error::HeightExceeded))?,
            }));
        }
        Ok(None)
    }

    async fn finish_frame(
        &mut self,
        frame: InternalFrame<F>,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let result = if frame.rewritten.value.is_empty() {
            frame.rewritten
        } else if frame.rewritten.value == frame.children.value {
            let mut result = Owned::allocate(1, self.accounting)?;
            let first = frame
                .children
                .value
                .first()
                .ok_or_else(|| Self::error(Error::MutationContract))?;
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

    async fn rewrite_leaf(
        &mut self,
        original: ObjectId,
        mut entries: Owned<Vec<F::Value>>,
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let changed;
        if mutations
            .iter()
            .all(|(_, mutation)| !mutation.changes_cardinality())
        {
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
            let first = self.clone_key(F::key(
                entries
                    .value
                    .first()
                    .ok_or_else(|| Self::error(Error::MutationContract))?,
            ))?;
            push_summary(&mut result, first, original, self.accounting)?;
            result
        } else {
            self.write_leaf_chunks(&entries.value).await?
        };
        entries.retire(self.accounting)?;
        Ok(result)
    }

    fn apply_mutation(
        &mut self,
        mutation: &M,
        current: &mut Owned<Option<F::Value>>,
    ) -> Result<(), crate::FsError> {
        let next = mutation
            .replacement(current.value.as_ref())
            .map_err(|error| Self::error(Error::Semantic(error)))?;
        let replacement = match next {
            Some(next) => self.clone_value(next)?,
            None => self.empty(None)?,
        };
        let prior = std::mem::replace(current, replacement);
        prior.retire(self.accounting)
    }

    fn apply_point_mutations(
        &mut self,
        entries: &mut Owned<Vec<F::Value>>,
        mutations: &[(usize, &M)],
    ) -> Result<bool, crate::FsError> {
        let mut changed = false;
        for (_, mutation) in mutations {
            let found = binary_search_with_accounting(&entries.value, self.accounting, |entry| {
                F::key(entry).cmp(mutation.key())
            })?;
            let prior = found.ok().and_then(|index| entries.value.get(index));
            let next = mutation
                .replacement(prior)
                .map_err(|error| Self::error(Error::Semantic(error)))?;
            if prior.is_some() != next.is_some() {
                return Err(Self::error(Error::MutationContract));
            }
            if prior == next {
                continue;
            }
            changed = true;
            if let (Ok(index), Some(next)) = (found, next) {
                let mut current = self.clone_value(next)?;
                let replacement = current
                    .value
                    .take()
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let slot = entries
                    .value
                    .get_mut(index)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let old_bytes = F::value_owned_bytes(slot);
                let old_allocation = split_allocation(&mut entries.allocation, old_bytes)?;
                drop(std::mem::replace(slot, replacement));
                retire_allocation(old_allocation, old_bytes, self.accounting)?;
                entries.allocation.absorb(&mut current.allocation)?;
                entries.bytes = entries
                    .bytes
                    .checked_sub(old_bytes)
                    .and_then(|bytes| bytes.checked_add(current.bytes))
                    .ok_or(WorkError::Overflow)?;
            }
        }
        Ok(changed)
    }

    fn apply_structural_mutations(
        &mut self,
        entries: &[F::Value],
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<F::Value>>, crate::FsError> {
        let capacity = entries
            .len()
            .checked_add(mutations.len())
            .ok_or_else(|| Self::error(Error::AllocationFailed))?;
        let mut output = Owned::allocate(capacity, self.accounting)?;
        merge::apply::<F, M, _>(self, entries, mutations, &mut output)?;
        Ok(output)
    }

    async fn write_leaf_chunks(
        &mut self,
        entries: &[F::Value],
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        publish::write::<F, publish::Leaf, _, PAGE_HEADER_BYTES, { support::TARGET_PAGE_BYTES }>(
            self, entries,
        )
        .await
    }

    async fn write_internal_chunks(
        &mut self,
        children: &[Child<F::Key>],
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        publish::write::<F, publish::Internal, _, PAGE_HEADER_BYTES, { support::TARGET_PAGE_BYTES }>(
            self, children,
        )
        .await
    }

    async fn finish_root(
        &mut self,
        summaries: Owned<Vec<Child<F::Key>>>,
    ) -> Result<ObjectId, crate::FsError> {
        let height = self.maximum_seen_height;
        publish::root::<F, _, PAGE_HEADER_BYTES, { support::TARGET_PAGE_BYTES }>(
            self, summaries, height,
        )
        .await
    }

    async fn read_page(&mut self, page: ObjectId) -> Result<Owned<Page<F>>, crate::FsError> {
        let (decoded, allocation) = persistent_io::read_page_with_accounting::<S, F>(
            self.store,
            page,
            self.limits,
            self.accounting,
            self.cancellation,
            None,
        )
        .await?;
        let bytes = decoded.logical_bytes;
        match decoded.page {
            PageLease::Owned(page) => Ok(Owned::new(page, allocation, bytes)),
            PageLease::Shared { page, .. } => {
                let result = match page.as_ref() {
                    Page::Leaf(entries) => {
                        let mut output = Owned::allocate(entries.len(), self.accounting)?;
                        for entry in entries {
                            let cloned = self.clone_value(entry)?;
                            push_value(&mut output, cloned)?;
                        }
                        let Owned {
                            value,
                            allocation,
                            bytes,
                        } = output;
                        Owned::new(Page::Leaf(value), allocation, bytes)
                    }
                    Page::Internal(children) => {
                        let mut output = Owned::allocate(children.len(), self.accounting)?;
                        for child in children {
                            let first = self.clone_key(&child.first)?;
                            push_summary(&mut output, first, child.page, self.accounting)?;
                        }
                        let Owned {
                            value,
                            allocation,
                            bytes,
                        } = output;
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
        if usize_to_u64(encoded_length) > self.limits.maximum_page_object_bytes() {
            return Err(Self::error(Error::PageItemTooLarge));
        }
        let (encoded, allocation) =
            F::encode_with_accounting(page, self.limits.maximum_page_items, self.accounting)?;
        let capacity = usize_to_u64(encoded.capacity());
        let mut encoded = Owned::new(encoded, allocation, capacity);
        if encoded.value.len() != encoded_length {
            return Err(Self::error(Error::MutationContract));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.accounting.native_work() {
            let KernelOwnedAllocation::NativeOriginal(allocation) = &mut encoded.allocation else {
                return Err(Self::error(Error::MutationContract));
            };
            let hashed = HashedObject::from_native_vec(
                F::kind(),
                &mut encoded.value,
                allocation,
                work,
                self.cancellation,
            )
            .map_err(storage_error)?;
            let hashed = crate::async_storage::prepare_object(
                self.store,
                hashed,
                self.accounting,
                self.cancellation,
            )
            .await?;
            let object = hashed.object_id();
            self.accounting.charge(PAGE_WRITE)?;
            {
                let mut work = crate::AuthorityWork::native(work, self.cancellation);
                crate::AsyncObjectStore::put_hashed(self.store, hashed, &mut work).await
            }
            .map_err(storage_error)?;
            drop(encoded);
            return Ok(object);
        }
        let hashing = WorkCounters {
            bytes_hashed: usize_to_u64(encoded.value.len()),
            ..WorkCounters::UNCHARGED
        };
        self.accounting.admit(hashing)?;
        let hashed = HashedObject::new(F::kind(), std::mem::take(&mut encoded.value).into());
        let hashed = crate::async_storage::prepare_object(
            self.store,
            hashed,
            self.accounting,
            self.cancellation,
        )
        .await?;
        let object = hashed.object_id();
        self.accounting.charge(hashing)?;
        self.accounting.charge(PAGE_WRITE)?;
        let remaining = self.accounting.remaining()?;
        let res = {
            let mut work = crate::AuthorityWork::new(remaining, self.cancellation);
            let result = crate::AsyncObjectStore::put_hashed(self.store, hashed, &mut work).await;
            work.receipt(result)
        };
        match res {
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

#[cfg(test)]
#[path = "tests/persistent_btree.rs"]
mod tests;
