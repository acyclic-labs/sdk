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
pub(crate) use support::{
    Error, Format, Mutation, Page, PageRef, children_within, leaf_within, stable_hash,
};
use support::{
    PAGE_HEADER_BYTES, PAGE_WRITE, append_owned, push_summary, push_value, reaches, sort_indexed,
    storage_error, validate,
};

#[cfg(test)]
thread_local! {
    /// Negative control: close each rewritten page where it ends instead of
    /// carrying it right, as page-local chunking did.
    pub(crate) static LOCAL_REWRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn local_rewrite() -> bool {
    #[cfg(test)]
    return LOCAL_REWRITE.with(std::cell::Cell::get);
    #[cfg(not(test))]
    false
}

struct NodeRequest<K> {
    page: ObjectId,
    lower: Owned<Option<K>>,
    upper: Owned<Option<K>>,
    mutations: Range<usize>,
    height: u16,
    leftmost: bool,
    rightmost: bool,
    revisit_tail: bool,
}

struct InternalFrame<F: Format> {
    original: ObjectId,
    children: Owned<Vec<Child<F::Key>>>,
    inherited_upper: Owned<Option<F::Key>>,
    selection: selection::Cursor,
    height: u16,
    leftmost: bool,
    rightmost: bool,
    revisit_tail: bool,
    // The rewrite began with summaries carried in from the left.
    carried: bool,
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
    type LeafCarry = Owned<Vec<F::Value>>;
    type Output = Owned<Vec<Child<F::Key>>>;
    type Error = crate::FsError;

    fn limits(&self) -> Result<acyclic_collections::page::PackingLimits, Self::Error> {
        Ok(acyclic_collections::page::PackingLimits {
            maximum_items: usize::try_from(self.limits.maximum_page_items)
                .map_err(|_| Self::error(Error::InvalidLimits))?,
            maximum_object_bytes: usize::try_from(self.limits.maximum_object_bytes)
                .unwrap_or(usize::MAX),
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

    fn key_hash(key: &F::Key) -> u64 {
        F::key_hash(key)
    }

    fn carry_leaf(&mut self, entries: &[F::Value]) -> Result<Self::LeafCarry, Self::Error> {
        let mut open = Owned::allocate(entries.len(), self.accounting)?;
        for entry in entries {
            let cloned = self.clone_value(entry)?;
            push_value(&mut open, cloned)?;
        }
        Ok(open)
    }

    fn carry_children(&mut self, children: &[Child<F::Key>]) -> Result<Self::Output, Self::Error> {
        let mut open = Owned::allocate(children.len(), self.accounting)?;
        for child in children {
            let first = self.clone_key(&child.first)?;
            push_summary(&mut open, first, child.page, self.accounting)?;
        }
        Ok(open)
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
    expected_start: Option<&'a F::Key>,
    expected_end: Option<&'a F::Key>,
    // Unfinished items, or the predecessor tail of a changed first key,
    // carried into the next page: leaf values and summaries by depth.
    carry_values: Owned<Vec<F::Value>>,
    carries: Owned<Vec<Option<Owned<Vec<Child<F::Key>>>>>>,
    leaf_depth: Option<u16>,
    // Same-width point replacements cannot change cuts and keep their exact
    // original sparse/no-op paths.
    boundary_changes: bool,
    // Level of the summaries the root returns, when it is not leaves'.
    top_level: Option<u16>,
    // The rightmost summary's page has no left neighbour yet at the levels
    // from this one up: those single-child pages are written only once a
    // level has a second page, so the root is never a single-child page.
    unwrapped_from: Option<u16>,
    // The page this operation wrote last, which the root needs no reread of.
    last_written: Option<ObjectId>,
    cancellation: &'a CancellationToken,
    marker: PhantomData<(F, M)>,
}

pub(crate) async fn apply_with_accounting<S, F, M>(
    store: &S,
    (root, expected_start, expected_end): (ObjectId, Option<&F::Key>, Option<&F::Key>),
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
        (Some(root), expected_start, expected_end),
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
    (root, expected_start, expected_end): (Option<ObjectId>, Option<&F::Key>, Option<&F::Key>),
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
    // Carry may visit right neighbours beyond the selected mutation paths.
    let maximum_visited = usize::try_from(limits.maximum_visited_pages)
        .map_err(|_| M::map_error(Error::InvalidLimits))?;
    let visited = if root.is_some() {
        Some(VisitedObjectSet::new_with_accounting(
            maximum_visited,
            accounting,
        )?)
    } else {
        None
    };
    let carry_values = Owned::allocate(0, accounting)?;
    let depths = usize::from(limits.maximum_page_height) + 2;
    let mut carries = Owned::allocate(depths, accounting)?;
    carries.value.resize_with(depths, || None);
    let mut boundary_changes = false;
    for (_, mutation) in &ordered.value {
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        boundary_changes |= mutation
            .changes_boundary()
            .map_err(|error| M::map_error(Error::Decode(error)))?;
    }
    let mut context = Context::<S, F, M> {
        store,
        limits,
        accounting,
        visited,
        expected_start,
        expected_end,
        carry_values,
        carries,
        leaf_depth: None,
        boundary_changes,
        top_level: None,
        unwrapped_from: None,
        last_written: None,
        cancellation,
        marker: PhantomData,
    };
    let summaries = match root {
        Some(root) => context.rewrite(root, &ordered.value).await?,
        None => {
            context.leaf_depth = Some(0);
            let values = context.apply_structural_mutations(&[], &ordered.value)?;
            let result = context.write_leaf_chunks(&values.value, true, false).await;
            values.retire(context.accounting)?;
            let (summaries, pending) = result?;
            if pending.is_some() {
                return Err(M::map_error(Error::MutationContract));
            }
            summaries
        }
    };
    let root = context.finish_root(root, summaries).await?;
    // The rightmost page of every level closes, so nothing stays carried.
    if !context.carry_values.value.is_empty() || context.carries.value.iter().any(Option::is_some) {
        return Err(M::map_error(Error::MutationContract));
    }
    context.carry_values.retire(context.accounting)?;
    context.carries.retire(context.accounting)?;
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
            leftmost: true,
            rightmost: true,
            revisit_tail: false,
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
        // A nonempty root must retain the caller's original first key before
        // a child supplies its own otherwise self-consistent routing bound.
        // Empty roots remain valid inputs for the first insertion.
        if request.height == 1
            && let Some(expected) = self.expected_start
        {
            self.charge_items(1)?;
            let first = match &decoded.value {
                Page::Leaf(entries) => entries.first().map(F::key),
                Page::Internal(children) => children.first().map(|child| &child.first),
            };
            if first.is_some_and(|first| first != expected) {
                return Err(Self::error(Error::ChildBoundsMismatch));
            }
        }
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
                // A balanced tree holds every leaf at one depth.
                if *self.leaf_depth.get_or_insert(request.height) != request.height {
                    return Err(Self::error(Error::MutationContract));
                }
                let selected = mutations
                    .get(request.mutations)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let result = self
                    .rewrite_leaf(
                        request.page,
                        entries,
                        selected,
                        (request.lower.value.as_ref(), request.upper.value.as_ref()),
                        request.rightmost,
                        request.revisit_tail,
                    )
                    .await?;
                request.lower.retire(self.accounting)?;
                request.upper.retire(self.accounting)?;
                Ok(EnteredNode::Complete(result))
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
                let rewritten = match self.take_carry(request.height)? {
                    Some(carried) => carried,
                    None => Owned::allocate(0, self.accounting)?,
                };
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page,
                    children,
                    inherited_upper: request.upper,
                    selection: selection::Cursor::new(request.mutations),
                    height: request.height,
                    leftmost: request.leftmost,
                    rightmost: request.rightmost,
                    revisit_tail: request.revisit_tail,
                    carried: !rewritten.value.is_empty(),
                    rewritten,
                }))
            }
        }
    }

    fn advance_frame(
        &mut self,
        frame: &mut InternalFrame<F>,
        mutations: &[(usize, &M)],
    ) -> Result<Option<NodeRequest<F::Key>>, crate::FsError> {
        let remaining = frame.selection.remaining();
        let removed = remaining
            .end
            .checked_sub(1)
            .filter(|last| *last >= remaining.start)
            .and_then(|last| mutations.get(last))
            .and_then(|(_, mutation)| mutation.removes_from());
        while let Some(selected) = frame.selection.select(
            &frame.children.value,
            |index| {
                mutations
                    .get(index)
                    .map(|(_, mutation)| mutation.key())
                    .ok_or_else(|| Self::error(Error::MutationContract))
            },
            |index, upper| {
                mutations
                    .get(index)
                    .map(|(_, mutation)| reaches(mutation.last(), upper))
                    .ok_or_else(|| Self::error(Error::MutationContract))
            },
            removed,
            || self.charge_items(1),
        )? {
            let child = selected.child;
            let child_depth = frame
                .height
                .checked_add(1)
                .ok_or_else(|| Self::error(Error::HeightExceeded))?;
            let revisit_tail = (self.boundary_changes && selected.following_first_changes)
                || (frame.revisit_tail && selected.index + 1 == frame.children.value.len());
            if selected.mutations.is_empty()
                && !revisit_tail
                && !self.carrying_at_or_below(child_depth)
            {
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
                revisit_tail,
                height: child_depth,
                leftmost: frame.leftmost && selected.index == 0,
                rightmost: frame.rightmost
                    && selected
                        .upper
                        .is_none_or(|upper| removed.is_some_and(|removed| upper >= removed)),
            }));
        }
        Ok(None)
    }

    /// Whether a page carried in from the left waits at `depth` or deeper.
    fn carrying_at_or_below(&self, depth: u16) -> bool {
        !self.carry_values.value.is_empty()
            || self
                .carries
                .value
                .iter()
                .skip(usize::from(depth))
                .any(Option::is_some)
    }

    fn take_carry(
        &mut self,
        depth: u16,
    ) -> Result<Option<Owned<Vec<Child<F::Key>>>>, crate::FsError> {
        Ok(self
            .carries
            .value
            .get_mut(usize::from(depth))
            .ok_or_else(|| Self::error(Error::HeightExceeded))?
            .take())
    }

    /// The level of the page an internal node at `depth` forms; leaves are 0.
    fn level_at(&self, depth: u16) -> Result<u16, crate::FsError> {
        self.leaf_depth
            .and_then(|leaf| leaf.checked_sub(depth))
            .ok_or_else(|| Self::error(Error::MutationContract))
    }

    async fn finish_frame(
        &mut self,
        frame: InternalFrame<F>,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let sole = frame.leftmost && frame.rightmost;
        let unchanged =
            !frame.revisit_tail && !frame.carried && frame.rewritten.value == frame.children.value;
        let mut rewritten = frame.rewritten;
        if rewritten.value.len() > 1 && self.unwrapped_from.is_some() {
            let level = self.level_at(frame.height)?;
            self.wrap_unwrapped(&mut rewritten, level).await?;
        }
        let result = if rewritten.value.is_empty() {
            rewritten
        } else if unchanged {
            let mut result = Owned::allocate(1, self.accounting)?;
            let first = frame
                .children
                .value
                .first()
                .ok_or_else(|| Self::error(Error::MutationContract))?;
            let first = self.clone_key(&first.first)?;
            push_summary(&mut result, first, frame.original, self.accounting)?;
            rewritten.retire(self.accounting)?;
            if sole && self.top_level.is_none() && self.leaf_depth.is_some() {
                self.top_level = Some(self.level_at(frame.height)?);
            }
            result
        } else if sole {
            // The only page at its level: the root builds every level above
            // these summaries, so a shrunk tree keeps no single-child pages.
            if self.top_level.is_none() {
                let level = self.level_at(frame.height)?;
                self.top_level = Some(
                    level
                        .checked_sub(1)
                        .ok_or_else(|| Self::error(Error::MutationContract))?,
                );
            }
            rewritten
        } else if frame.rightmost && rewritten.value.len() == 1 {
            // A last page holding one child: whether it is the root's only
            // child is known only once some level gains a second page.
            if self.unwrapped_from.is_none() {
                self.unwrapped_from = Some(self.level_at(frame.height)?);
            }
            rewritten
        } else {
            let level = self.level_at(frame.height)?;
            let (result, open) = self
                .write_internal_chunks(&rewritten.value, level, frame.rightmost, frame.revisit_tail)
                .await?;
            if let Some(open) = open {
                let slot = self
                    .carries
                    .value
                    .get_mut(usize::from(frame.height))
                    .ok_or_else(|| Self::error(Error::HeightExceeded))?;
                *slot = Some(open);
            }
            rewritten.retire(self.accounting)?;
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
        (lower, upper): (Option<&F::Key>, Option<&F::Key>),
        rightmost: bool,
        revisit_tail: bool,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let mut changed;
        let mut join_next = false;
        // A ranged mutation reaching the next leaf may join items across the
        // boundary, so this leaf's items carry right into it, closed or not.
        if M::RANGED {
            let empty = Owned::allocate(0, self.accounting)?;
            let carried = std::mem::replace(&mut self.carry_values, empty);
            let rewritten = self.apply_range_mutations(
                &carried.value,
                &entries.value,
                mutations,
                lower,
                upper,
            )?;
            if !rightmost {
                if let Some(upper) = upper {
                    for (_, mutation) in mutations {
                        self.charge_items(1)?;
                        join_next |= reaches(mutation.last(), upper);
                    }
                }
                self.charge_items(1)?;
                join_next |= rewritten.value.last() != entries.value.last();
            }
            changed = !carried.value.is_empty() || rewritten.value != entries.value;
            carried.retire(self.accounting)?;
            entries.retire(self.accounting)?;
            entries = rewritten;
        } else if mutations
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
        if !self.carry_values.value.is_empty() {
            let empty = Owned::allocate(0, self.accounting)?;
            let mut carried = std::mem::replace(&mut self.carry_values, empty);
            append_owned(&mut carried, entries, self.accounting)?;
            entries = carried;
            changed = true;
        }
        let result = if entries.value.is_empty() {
            Owned::allocate(0, self.accounting)?
        } else if !changed && !revisit_tail && !join_next {
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
            let (result, open) = self
                .write_leaf_chunks(&entries.value, rightmost, revisit_tail || join_next)
                .await?;
            if let Some(open) = open {
                std::mem::replace(&mut self.carry_values, open).retire(self.accounting)?;
            }
            result
        };
        entries.retire(self.accounting)?;
        Ok(result)
    }

    fn apply_range_mutations(
        &mut self,
        carried: &[F::Value],
        entries: &[F::Value],
        mutations: &[(usize, &M)],
        lower: Option<&F::Key>,
        upper: Option<&F::Key>,
    ) -> Result<Owned<Vec<F::Value>>, crate::FsError> {
        let capacity = mutations
            .len()
            .checked_mul(3)
            .and_then(|room| room.checked_add(carried.len()))
            .and_then(|room| room.checked_add(entries.len()))
            .ok_or_else(|| Self::error(Error::AllocationFailed))?;
        let mut output = Owned::allocate(capacity, self.accounting)?;
        let room = output.value.capacity();
        M::rewrite_range(
            carried,
            entries,
            mutations,
            (lower, upper, self.expected_end),
            &mut output.value,
            self.accounting,
        )?;
        // The rewrite stays inside its admitted room.
        if output.value.capacity() != room {
            return Err(Self::error(Error::MutationContract));
        }
        Ok(output)
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

    /// Publishes completed chunks. The exact predecessor's final chunk can
    /// remain owned even when closed, to join a changed following first key.
    async fn write_leaf_chunks(
        &mut self,
        entries: &[F::Value],
        close: bool,
        hold_last: bool,
    ) -> Result<(Owned<Vec<Child<F::Key>>>, Option<Owned<Vec<F::Value>>>), crate::FsError> {
        publish::write::<F, publish::Leaf, _, PAGE_HEADER_BYTES, { support::TARGET_PAGE_BYTES }>(
            self,
            entries,
            0,
            close || local_rewrite(),
            hold_last && !local_rewrite(),
        )
        .await
    }

    /// Writes closed child pages and retains only the unfinished right tail.
    async fn write_internal_chunks(
        &mut self,
        children: &[Child<F::Key>],
        level: u16,
        close: bool,
        hold_last: bool,
    ) -> Result<(Owned<Vec<Child<F::Key>>>, Option<Owned<Vec<Child<F::Key>>>>), crate::FsError>
    {
        publish::write::<F, publish::Internal, _, PAGE_HEADER_BYTES, { support::TARGET_PAGE_BYTES }>(
            self, children, level, close || local_rewrite(), hold_last && !local_rewrite(),
        ).await
    }

    /// Builds the levels above the root's summaries until one page remains.
    async fn finish_root(
        &mut self,
        original: Option<ObjectId>,
        mut summaries: Owned<Vec<Child<F::Key>>>,
    ) -> Result<ObjectId, crate::FsError> {
        if summaries.value.is_empty() {
            summaries.retire(self.accounting)?;
            return self.write_page(&PageRef::<F>::Leaf(&[])).await;
        }
        let mut level = self.top_level.unwrap_or(0);
        if summaries.value.len() > 1 {
            self.wrap_unwrapped(&mut summaries, level.saturating_add(1))
                .await?;
        }
        // A lone summary is the root, whatever single-child pages it skipped.
        self.unwrapped_from = None;
        while summaries.value.len() > 1 {
            level = level
                .checked_add(1)
                .ok_or_else(|| Self::error(Error::HeightExceeded))?;
            if level >= self.limits.maximum_page_height {
                return Err(Self::error(Error::HeightExceeded));
            }
            let (next, _) = self
                .write_internal_chunks(&summaries.value, level, true, false)
                .await?;
            // Internal pages take two summaries, so each level shrinks.
            if next.value.len() >= summaries.value.len() {
                next.retire(self.accounting)?;
                summaries.retire(self.accounting)?;
                return Err(Self::error(Error::PageItemTooLarge));
            }
            summaries.retire(self.accounting)?;
            summaries = next;
        }
        let root = summaries
            .value
            .first()
            .ok_or_else(|| Self::error(Error::MutationContract))?
            .page;
        summaries.retire(self.accounting)?;
        if Some(root) == original || Some(root) == self.last_written {
            return Ok(root);
        }
        self.unwrap_exposed_root(root).await
    }

    /// A page this operation left untouched may reach the top once a shrink
    /// removes everything beside it, and it may be a single-child page that
    /// was the rightmost of its level. The root is the first page below it
    /// with other than one child, as building the contents at once gives.
    async fn unwrap_exposed_root(
        &mut self,
        mut root: ObjectId,
    ) -> Result<ObjectId, crate::FsError> {
        for _ in 0..self.limits.maximum_page_height {
            let page = self.read_page(root).await?;
            let only = match &page.value {
                Page::Internal(children) if children.len() == 1 => {
                    children.first().map(|child| child.page)
                }
                _ => None,
            };
            page.retire(self.accounting)?;
            let Some(only) = only else {
                return Ok(root);
            };
            root = only;
        }
        Err(Self::error(Error::HeightExceeded))
    }

    /// Writes the single-child pages the rightmost of `summaries` skipped,
    /// up to just below `level`, now that its level has a second page.
    async fn wrap_unwrapped(
        &mut self,
        summaries: &mut Owned<Vec<Child<F::Key>>>,
        level: u16,
    ) -> Result<(), crate::FsError> {
        let Some(from) = self.unwrapped_from.take() else {
            return Ok(());
        };
        let last = summaries
            .value
            .last_mut()
            .ok_or_else(|| Self::error(Error::MutationContract))?;
        for _ in from..level {
            last.page = self
                .write_page(&PageRef::<F>::Internal(std::slice::from_ref(last)))
                .await?;
        }
        Ok(())
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

    /// Whether this operation read `object`. A rewritten page whose bytes
    /// equal a page already read is that page, already stored: re-chunking
    /// has resynchronised with the old boundaries, so nothing is written.
    fn read_this_operation(&mut self, object: ObjectId) -> Result<bool, crate::FsError> {
        match self.visited.as_ref() {
            Some(visited) => visited
                .value
                .contains_with_accounting(object, self.accounting),
            None => Ok(false),
        }
    }

    async fn write_page(&mut self, page: &PageRef<'_, F>) -> Result<ObjectId, crate::FsError> {
        self.accounting.check()?;
        let encoded_length = F::page_encoded_length(page, self.limits.maximum_page_items)?;
        if usize_to_u64(encoded_length) > self.limits.maximum_object_bytes {
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
            if self.read_this_operation(object)? {
                self.last_written = Some(object);
                return Ok(object);
            }
            self.accounting.charge(PAGE_WRITE)?;
            {
                let mut work = crate::AuthorityWork::native(work, self.cancellation);
                crate::AsyncObjectStore::put_hashed(self.store, hashed, &mut work).await
            }
            .map_err(storage_error)?;
            drop(encoded);
            self.last_written = Some(object);
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
        if self.read_this_operation(object)? {
            encoded.retire(self.accounting)?;
            self.last_written = Some(object);
            return Ok(object);
        }
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
        self.last_written = Some(object);
        Ok(object)
    }
}

#[cfg(test)]
#[path = "tests/persistent_btree.rs"]
mod tests;
