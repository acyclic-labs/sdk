//! Ordered paired traversal. Parent pages and leaf positions survive each yield.
use super::super::persistent_btree::{Child, children_within, leaf_within};
use super::*;
use std::sync::Arc;

enum Node<F: Format> {
    Leaf(std::vec::IntoIter<F::Value>),
    Internal(Vec<Child<F::Key>>),
    Shared(Arc<Page<F>>),
}

struct Frame<F: Format> {
    id: ObjectId,
    node: Owned<Node<F>>,
    position: usize,
    // A key in an already held ancestor, never a separately cloned bound.
    upper: Option<(usize, usize)>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<Arc<crate::NativeAllocation>>,
}

impl<F: Format> Frame<F> {
    fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let Self {
            node,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody,
            ..
        } = self;
        let retired = node.retire(accounting);
        #[cfg(not(target_arch = "wasm32"))]
        drop(_native_custody);
        retired
    }

    fn children(&self) -> Option<&[Child<F::Key>]> {
        match &self.node.value {
            Node::Internal(children) => Some(children),
            Node::Shared(page) => match page.as_ref() {
                Page::Internal(children) => Some(children),
                Page::Leaf(_) => None,
            },
            Node::Leaf(_) => None,
        }
    }

    fn value(&self) -> Option<&F::Value> {
        match &self.node.value {
            Node::Leaf(values) => values.as_slice().first(),
            Node::Shared(page) => match page.as_ref() {
                Page::Leaf(values) => values.get(self.position),
                Page::Internal(_) => None,
            },
            Node::Internal(_) => None,
        }
    }

    fn exhausted(&self) -> bool {
        self.children().map_or_else(
            || self.value().is_none(),
            |children| self.position == children.len(),
        )
    }
}

struct Side<F: Format> {
    root: Option<ObjectId>,
    frames: Owned<Vec<Frame<F>>>,
}

enum Head<'a, F: Format> {
    End,
    Page {
        id: ObjectId,
        lower: Option<&'a F::Key>,
        upper: Option<(usize, usize)>,
    },
    Value(&'a F::Value),
}

impl<F: Format> Side<F> {
    fn new(root: Option<ObjectId>, accounting: &mut KernelAccounting<'_>) -> Result<Self, FsError> {
        Ok(Self {
            root,
            frames: Owned::allocate(0, accounting)?,
        })
    }

    fn normalize(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        while self.frames.value.last().is_some_and(Frame::exhausted) {
            let frame = self.frames.value.pop().ok_or(FsError::InvalidDiff)?;
            // Destroy cached/native aliases before releasing their original guard.
            frame.retire(accounting)?;
        }
        Ok(())
    }

    fn head(&self) -> Result<Head<'_, F>, FsError> {
        if let Some(id) = self.root {
            return Ok(Head::Page {
                id,
                lower: None,
                upper: None,
            });
        }
        let Some(frame) = self.frames.value.last() else {
            return Ok(Head::End);
        };
        if let Some(children) = frame.children() {
            let child = children.get(frame.position).ok_or(FsError::InvalidDiff)?;
            let upper = if frame.position + 1 < children.len() {
                Some((self.frames.value.len() - 1, frame.position + 1))
            } else {
                frame.upper
            };
            Ok(Head::Page {
                id: child.page,
                lower: Some(&child.first),
                upper,
            })
        } else {
            Ok(Head::Value(frame.value().ok_or(FsError::InvalidDiff)?))
        }
    }

    fn upper_key(&self, bound: Option<(usize, usize)>) -> Result<Option<&F::Key>, FsError> {
        bound
            .map(|(depth, index)| {
                self.frames
                    .value
                    .get(depth)
                    .and_then(Frame::children)
                    .and_then(|children| children.get(index))
                    .map(|child| &child.first)
                    .ok_or(FsError::InvalidDiff)
            })
            .transpose()
    }

    fn advance_page(&mut self) -> Result<(), FsError> {
        if self.root.take().is_none() {
            let frame = self.frames.value.last_mut().ok_or(FsError::InvalidDiff)?;
            frame.position += 1;
        }
        Ok(())
    }

    fn admit_page(
        &mut self,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        self.frames
            .ensure_for_push(usize::from(limits.maximum_page_height), accounting)
    }

    fn install(
        &mut self,
        page: Owned<OwnedPage<F>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let result = (|| {
            let Head::Page { id, lower, upper } = self.head()? else {
                return Err(FsError::InvalidDiff);
            };
            accounting.charge_items(1)?;
            let upper_key = self.upper_key(upper)?;
            let valid = match page_view(&page.value) {
                Page::Leaf(values) => leaf_within::<F>(values, lower, upper_key),
                Page::Internal(children) => children_within(children, lower, upper_key),
            };
            if !valid {
                return Err(FsError::InvalidDiff);
            }
            Ok((id, upper))
        })();
        let (id, upper) = match result {
            Ok(value) => value,
            Err(error) => {
                page.retire(accounting)?;
                return Err(error);
            }
        };
        self.advance_page()?;
        let Owned {
            value,
            allocation,
            bytes,
        } = page;
        let node = match value.page {
            PageLease::Owned(Page::Leaf(values)) => Node::Leaf(values.into_iter()),
            PageLease::Owned(Page::Internal(children)) => Node::Internal(children),
            PageLease::Shared { page, .. } => Node::Shared(page),
        };
        self.frames.value.push(Frame {
            id,
            node: Owned::new(node, allocation, bytes),
            position: 0,
            upper,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: value.native_custody,
        });
        Ok(())
    }

    fn check_ancestor(
        &self,
        id: ObjectId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        for frame in &self.frames.value {
            accounting.charge_items(1)?;
            if frame.id == id {
                return Err(FsError::InvalidDiff);
            }
        }
        Ok(())
    }

    fn take_value(
        &mut self,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<F::Value>, FsError> {
        let frame = self.frames.value.last_mut().ok_or(FsError::InvalidDiff)?;
        match &mut frame.node.value {
            Node::Leaf(values) => {
                let value = values.as_slice().first().ok_or(FsError::InvalidDiff)?;
                let bytes = F::value_owned_bytes(value);
                let remaining = frame
                    .node
                    .bytes
                    .checked_sub(bytes)
                    .ok_or(WorkError::Overflow)?;
                let allocation = split_allocation(&mut frame.node.allocation, bytes)?;
                frame.node.bytes = remaining;
                let value = values.next().ok_or(FsError::InvalidDiff)?;
                Ok(Owned::new(value, allocation, bytes))
            }
            Node::Shared(page) => {
                let Page::Leaf(values) = page.as_ref() else {
                    return Err(FsError::InvalidDiff);
                };
                let value = values.get(frame.position).ok_or(FsError::InvalidDiff)?;
                let (value, allocation) = persistent_io::clone_value::<F>(
                    value,
                    limits.maximum_name_bytes,
                    accounting,
                    None,
                    0,
                )?;
                frame.position += 1;
                let bytes = F::value_owned_bytes(&value);
                Ok(Owned::new(value, allocation, bytes))
            }
            Node::Internal(_) => Err(FsError::InvalidDiff),
        }
    }

    fn skip_value(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let frame = self.frames.value.last_mut().ok_or(FsError::InvalidDiff)?;
        match &mut frame.node.value {
            Node::Leaf(values) => {
                let value = values.as_slice().first().ok_or(FsError::InvalidDiff)?;
                let bytes = F::value_owned_bytes(value);
                let remaining = frame
                    .node
                    .bytes
                    .checked_sub(bytes)
                    .ok_or(WorkError::Overflow)?;
                let allocation = split_allocation(&mut frame.node.allocation, bytes)?;
                frame.node.bytes = remaining;
                drop(values.next().ok_or(FsError::InvalidDiff)?);
                retire_allocation(allocation, bytes, accounting)
            }
            Node::Shared(_) => {
                frame.position += 1;
                Ok(())
            }
            Node::Internal(_) => Err(FsError::InvalidDiff),
        }
    }

    fn retire(mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let mut failure = None;
        while let Some(frame) = self.frames.value.pop() {
            if let Err(error) = frame.retire(accounting) {
                failure.get_or_insert(error);
            }
        }
        if let Err(error) = self.frames.retire(accounting) {
            failure.get_or_insert(error);
        }
        failure.map_or(Ok(()), Err)
    }
}

/// One immutable pair frontier; no emitted prefix is read or searched again.
pub(crate) struct Cursor<F: Format> {
    before: Side<F>,
    after: Side<F>,
    limits: DecodeLimits,
    pages_read: u64,
}

enum Step {
    End,
    SkipPage,
    Expand(bool, bool),
    SkipValue,
    Emit(bool, bool),
}

impl<F: Format> Cursor<F> {
    pub(crate) fn new(
        before: Option<ObjectId>,
        after: Option<ObjectId>,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        if before.is_some_and(|id| id.kind != F::kind())
            || after.is_some_and(|id| id.kind != F::kind())
        {
            return Err(FsError::InvalidDiff);
        }
        accounting.check()?;
        let before_side = Side::new(if before == after { None } else { before }, accounting)?;
        let after_side = match Side::new(if before == after { None } else { after }, accounting) {
            Ok(side) => side,
            Err(error) => {
                before_side.retire(accounting)?;
                return Err(error);
            }
        };
        Ok(Self {
            before: before_side,
            after: after_side,
            limits,
            pages_read: 0,
        })
    }

    fn step(&self, accounting: &mut KernelAccounting<'_>) -> Result<Step, FsError> {
        let before = self.before.head()?;
        let after = self.after.head()?;
        if matches!((&before, &after), (Head::End, Head::End)) {
            return Ok(Step::End);
        }
        accounting.charge_items(1)?;
        Ok(match (before, after) {
            (Head::End, Head::End) => Step::End,
            (Head::Page { id: left, .. }, Head::Page { id: right, .. }) if left == right => {
                Step::SkipPage
            }
            (Head::Page { lower: left, .. }, Head::Page { lower: right, .. }) => {
                match left.cmp(&right) {
                    std::cmp::Ordering::Less => Step::Expand(true, false),
                    std::cmp::Ordering::Equal => Step::Expand(true, true),
                    std::cmp::Ordering::Greater => Step::Expand(false, true),
                }
            }
            (Head::Page { lower, .. }, Head::Value(value)) => {
                if lower.is_none_or(|key| key <= F::key(value)) {
                    Step::Expand(true, false)
                } else {
                    Step::Emit(false, true)
                }
            }
            (Head::Value(value), Head::Page { lower, .. }) => {
                if lower.is_none_or(|key| key <= F::key(value)) {
                    Step::Expand(false, true)
                } else {
                    Step::Emit(true, false)
                }
            }
            (Head::Page { .. }, Head::End) => Step::Expand(true, false),
            (Head::End, Head::Page { .. }) => Step::Expand(false, true),
            (Head::Value(_), Head::End) => Step::Emit(true, false),
            (Head::End, Head::Value(_)) => Step::Emit(false, true),
            (Head::Value(left), Head::Value(right)) => match F::key(left).cmp(F::key(right)) {
                std::cmp::Ordering::Less => Step::Emit(true, false),
                std::cmp::Ordering::Greater => Step::Emit(false, true),
                std::cmp::Ordering::Equal if left == right => Step::SkipValue,
                std::cmp::Ordering::Equal => Step::Emit(true, true),
            },
        })
    }

    async fn expand<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        before: bool,
        after: bool,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let Head::Page { id: first, .. } = (if before {
            self.before.head()?
        } else {
            self.after.head()?
        }) else {
            return Err(FsError::InvalidDiff);
        };
        let mut ids = [first; 2];
        let mut count = 0;
        for (index, side) in [&mut self.before, &mut self.after].into_iter().enumerate() {
            if (index == 0 && before) || (index == 1 && after) {
                let Head::Page { id, .. } = side.head()? else {
                    return Err(FsError::InvalidDiff);
                };
                side.check_ancestor(id, accounting)?;
                side.admit_page(self.limits, accounting)?;
                ids[count] = id;
                count += 1;
            }
        }
        let pages = self
            .pages_read
            .checked_add(count as u64)
            .ok_or(WorkError::Overflow)?;
        if pages > u64::from(self.limits.maximum_visited_pages) {
            return Err(FsError::InvalidDiff);
        }
        let mut batch = persistent_io::read_pages_with_accounting::<S, F, _>(
            store,
            ids.into_iter().take(count),
            self.limits,
            accounting,
            cancellation,
            None,
        )
        .await?;
        self.pages_read = pages;
        let result = (|| {
            for side in [
                if before { Some(&mut self.before) } else { None },
                if after { Some(&mut self.after) } else { None },
            ]
            .into_iter()
            .flatten()
            {
                let (page, guard) =
                    batch.next_with_accounting(self.limits, accounting, cancellation)?;
                let bytes = page.logical_bytes;
                side.install(Owned::new(page, guard, bytes), accounting)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => batch.finish_with_accounting(accounting, cancellation),
            Err(error) => {
                batch.discard_with_accounting(accounting, cancellation)?;
                Err(error)
            }
        }
    }

    async fn advance<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(bool, bool)>, FsError> {
        loop {
            cancellation.check()?;
            accounting.check()?;
            self.before.normalize(accounting)?;
            self.after.normalize(accounting)?;
            match self.step(accounting)? {
                Step::End => return Ok(None),
                Step::SkipPage => {
                    self.before.advance_page()?;
                    self.after.advance_page()?;
                }
                Step::Expand(before, after) => {
                    self.expand(store, before, after, accounting, cancellation)
                        .await?
                }
                Step::SkipValue => {
                    self.before.skip_value(accounting)?;
                    self.after.skip_value(accounting)?;
                }
                Step::Emit(before, after) => return Ok(Some((before, after))),
            }
        }
    }

    pub(crate) async fn next<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<ValueChange<F::Key, F::Value>>>, FsError> {
        match self.advance(store, accounting, cancellation).await? {
            Some((before, after)) => self.emit(before, after, accounting).map(Some),
            None => Ok(None),
        }
    }

    /// A set has equal keys and values, so a changed edge has exactly one
    /// endpoint. Move it through the pair frontier without a redundant key copy.
    pub(crate) async fn next_set_change<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(usize, Owned<F::Value>)>, FsError>
    where
        F: Format<Key = <F as Format>::Value>,
    {
        let side = match self.advance(store, accounting, cancellation).await? {
            Some((true, false)) => 0,
            Some((false, true)) => 1,
            None => return Ok(None),
            _ => return Err(FsError::InvalidDiff),
        };
        let value = if side == 0 {
            &mut self.before
        } else {
            &mut self.after
        }
        .take_value(self.limits, accounting)?;
        Ok(Some((side, value)))
    }

    fn emit(
        &mut self,
        before: bool,
        after: bool,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<ValueChange<F::Key, F::Value>>, FsError> {
        let head = if before {
            self.before.head()?
        } else {
            self.after.head()?
        };
        let Head::Value(value) = head else {
            return Err(FsError::InvalidDiff);
        };
        let (key, allocation) = persistent_io::clone_key::<F>(
            F::key(value),
            self.limits.maximum_name_bytes,
            accounting,
            None,
            0,
        )?;
        let bytes = F::key_owned_bytes(&key);
        let mut result = Owned::new(
            ValueChange {
                key,
                before: None,
                after: None,
            },
            allocation,
            bytes,
        );
        let prepared = (|| {
            for (selected, side, destination) in [
                (before, &mut self.before, &mut result.value.before),
                (after, &mut self.after, &mut result.value.after),
            ] {
                if selected {
                    let part = side.take_value(self.limits, accounting)?;
                    *destination = Some(part.transfer_to(
                        &mut result.allocation,
                        &mut result.bytes,
                        accounting,
                    )?);
                }
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            result.retire(accounting)?;
            return Err(error);
        }
        Ok(result)
    }

    pub(crate) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let before = self.before.retire(accounting);
        let after = self.after.retire(accounting);
        before.and(after)
    }
}

/// A single immutable endpoint uses the same frontier without copying each
/// value's key into a redundant diff row.
pub(crate) struct Values<F: Format>(Cursor<F>);

impl<F: Format> Values<F> {
    pub(crate) fn new(
        root: ObjectId,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        Cursor::new(None, Some(root), limits, accounting).map(Self)
    }

    pub(crate) async fn next<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Owned<F::Value>>, FsError> {
        match self.0.advance(store, accounting, cancellation).await? {
            Some((false, true)) => self.0.after.take_value(self.0.limits, accounting).map(Some),
            None => Ok(None),
            _ => Err(FsError::InvalidDiff),
        }
    }

    pub(crate) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        self.0.retire(accounting)
    }
}
