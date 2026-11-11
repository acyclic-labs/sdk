use super::*;
use acyclic_collections::page::rewrite;
use std::cmp::Ordering;

struct Request {
    root: Root,
    first: Option<Key>,
    upper: Option<Key>,
    height: u8,
}

enum Output {
    Unchanged(Child<Key, Root>),
    Changed(summary::Summaries),
}

struct Frame {
    original: Child<Key, Root>,
    children: Vec<Child<Key, Root>>,
    output: Vec<Output>,
    upper: Option<Key>,
    next: usize,
    height: u8,
    changed: bool,
}

struct Work<'a, S> {
    storage: &'a S,
    updates: Updates<'a, S>,
    next: Option<Entry>,
    inserted: u64,
}

enum Updates<'a, S> {
    Single(Option<Entry>),
    Paged(Cursor<'a, S>),
}

impl<S: Storage> Updates<'_, S> {
    async fn next(&mut self) -> Result<Option<Entry>, StreamError> {
        match self {
            Self::Single(entry) => Ok(entry.take()),
            Self::Paged(cursor) => cursor.next_entry().await,
        }
    }
}

impl<S: Storage> Work<'_, S> {
    async fn before(&self, upper: Option<&Key>) -> Result<bool, StreamError> {
        let Some(next) = &self.next else {
            return Ok(false);
        };
        Ok(match upper {
            Some(upper) => next.key.compare(self.storage, upper).await? == Ordering::Less,
            None => true,
        })
    }

    async fn leaf(
        &mut self,
        entries: Vec<Entry>,
        original: Child<Key, Root>,
        upper: Option<&Key>,
    ) -> Result<Output, StreamError> {
        let mut input = entries.into_iter();
        let mut previous = input.next();
        let mut pages = build::Pages::new(self.storage, 0).await?;
        let mut changed = false;
        loop {
            let updating = self.before(upper).await?;
            if previous.is_none() && !updating {
                break;
            }
            let order = match (&previous, &self.next, updating) {
                (Some(old), Some(next), true) => next.key.compare(self.storage, &old.key).await?,
                (_, _, false) => Ordering::Greater,
                (None, _, true) => Ordering::Less,
                _ => return Err(StreamError::InvalidResponse),
            };
            let entry = if order == Ordering::Greater {
                let entry = previous.take().ok_or(StreamError::InvalidResponse)?;
                previous = input.next();
                entry
            } else {
                let entry = self.next.take().ok_or(StreamError::InvalidResponse)?;
                self.next = self.updates.next().await?;
                if order == Ordering::Equal {
                    let old = previous.take().ok_or(StreamError::InvalidResponse)?;
                    changed |= old.value != entry.value;
                    previous = input.next();
                } else {
                    self.inserted = self.inserted.checked_add(1).ok_or(StreamError::Capacity)?;
                    changed = true;
                }
                entry
            };
            pages.entry(entry).await?;
        }
        if changed {
            Ok(Output::Changed(pages.finish().await?))
        } else {
            Ok(Output::Unchanged(original))
        }
    }
}

impl<S: Storage> rewrite::Work<()> for Work<'_, S> {
    type Request = Request;
    type Frame = Frame;
    type Output = Output;
    type Stack = Vec<Frame>;
    type Error = StreamError;

    fn advance(&mut self, frame: &mut Frame, _: &()) -> Result<Option<Request>, StreamError> {
        let Some(child) = frame.children.get(frame.next) else {
            return Ok(None);
        };
        let upper = frame
            .children
            .get(frame.next + 1)
            .map(|child| child.first.clone())
            .or_else(|| frame.upper.clone());
        frame.next += 1;
        Ok(Some(Request {
            root: child.page,
            first: Some(child.first.clone()),
            upper,
            height: frame.height - 1,
        }))
    }
    fn push(&mut self, stack: &mut Vec<Frame>, frame: Frame) -> Result<(), StreamError> {
        if stack.len() >= usize::from(MAX_HEIGHT) {
            return Err(StreamError::InvalidResponse);
        }
        stack.try_reserve(1).map_err(|_| StreamError::Capacity)?;
        stack.push(frame);
        Ok(())
    }
    fn pop(&mut self, stack: &mut Vec<Frame>) -> Option<Frame> {
        stack.pop()
    }
    fn append(&mut self, frame: &mut Frame, output: Output) -> Result<(), StreamError> {
        if frame.output.len() >= frame.children.len() {
            return Err(StreamError::InvalidResponse);
        }
        frame.changed |= matches!(output, Output::Changed(_));
        frame.output.push(output);
        Ok(())
    }
}

impl<S: Storage> rewrite::Io<()> for Work<'_, S> {
    async fn enter(
        &mut self,
        request: Request,
        _: &(),
    ) -> Result<rewrite::Entered<Frame, Output>, StreamError> {
        if let Some(first) = &request.first
            && !self.before(request.upper.as_ref()).await?
        {
            return Ok(rewrite::Entered::Complete(Output::Unchanged(Child {
                first: first.clone(),
                page: request.root,
            })));
        }
        let page = codec::read(self.storage, request.root, request.height).await?;
        let first = match &page {
            Page::Leaf(entries) => entries.first().map(|entry| &entry.key),
            Page::Internal(children) => children.first().map(|child| &child.first),
        }
        .ok_or(StreamError::InvalidResponse)?
        .clone();
        if request.first.is_some_and(|expected| expected != first) {
            return Err(StreamError::InvalidResponse);
        }
        let original = Child {
            first,
            page: request.root,
        };
        match page {
            Page::Leaf(entries) => Ok(rewrite::Entered::Complete(
                self.leaf(entries, original, request.upper.as_ref()).await?,
            )),
            Page::Internal(children) => {
                let mut output = Vec::new();
                output
                    .try_reserve_exact(children.len())
                    .map_err(|_| StreamError::Capacity)?;
                Ok(rewrite::Entered::Internal(Frame {
                    original,
                    children,
                    output,
                    upper: request.upper,
                    next: 0,
                    height: request.height,
                    changed: false,
                }))
            }
        }
    }
    async fn finish(&mut self, frame: Frame) -> Result<Output, StreamError> {
        if frame.output.len() != frame.children.len() {
            return Err(StreamError::InvalidResponse);
        }
        if !frame.changed {
            return Ok(Output::Unchanged(frame.original));
        }
        let mut pages = build::Pages::new(self.storage, frame.height).await?;
        for output in frame.output {
            match output {
                Output::Unchanged(child) => pages.child(child).await?,
                Output::Changed(summaries) => {
                    if summaries.data_height() + 1 != frame.height {
                        return Err(StreamError::InvalidResponse);
                    }
                    let mut cursor = summaries.cursor(self.storage).await?;
                    while let Some((first, page)) = cursor.next().await? {
                        pages
                            .child(Child {
                                first: Key::stored(self.storage, first).await?,
                                page,
                            })
                            .await?;
                    }
                }
            }
        }
        Ok(Output::Changed(pages.finish().await?))
    }
    fn retire_stack(&mut self, stack: Vec<Frame>) -> Result<(), StreamError> {
        if !stack.is_empty() {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

impl Index {
    /// Returns a private COW successor through the same bounded batch rewrite.
    /// The enclosing owner retains successful puts until publication/retirement.
    pub async fn set(
        &self,
        storage: &impl Storage,
        key: Field,
        value: Field,
    ) -> Result<Self, StreamError> {
        let entry = Entry {
            key: Key::stored(storage, key).await?,
            value: key::store(storage, value, INLINE_FIELD_BYTES).await?,
        };
        if self.root.is_none() {
            let mut builder = Builder::new(storage);
            builder.append(entry).await?;
            return builder.finish().await;
        }
        self.rewrite(storage, Updates::Single(Some(entry))).await
    }
    /// Builds an unpublished successor from one sorted index of upserts.
    /// Update and original roots remain pinned; this assigns no Stream position.
    /// Each original data page is read at most once on the rewrite path, and
    /// untouched subtrees keep their original references. Output is paged even
    /// when every update falls within one original leaf.
    pub async fn merge(&self, storage: &impl Storage, updates: &Self) -> Result<Self, StreamError> {
        if updates.is_empty() || self == updates {
            return Ok(self.clone());
        }
        if self.root.is_none() {
            return Ok(updates.clone());
        }
        self.rewrite(storage, Updates::Paged(updates.scan(storage, None).await?))
            .await
    }

    async fn rewrite<'a, S: Storage>(
        &self,
        storage: &'a S,
        mut updates: Updates<'a, S>,
    ) -> Result<Self, StreamError> {
        let root = self.root.ok_or(StreamError::InvalidArgument)?;
        let next = updates.next().await?;
        let mut work = Work {
            storage,
            updates,
            next,
            inserted: 0,
        };
        let output = rewrite::run(
            &mut work,
            Vec::new(),
            Request {
                root,
                first: None,
                upper: None,
                height: self.height,
            },
            &(),
        )
        .await?;
        if work.next.is_some() {
            return Err(StreamError::InvalidResponse);
        }
        let Output::Changed(mut summaries) = output else {
            return Ok(self.clone());
        };
        let count = self
            .count
            .checked_add(work.inserted)
            .ok_or(StreamError::Capacity)?;
        let mut height = self.height;
        while summaries.len() > 1 {
            height = height
                .checked_add(1)
                .filter(|height| *height <= MAX_HEIGHT)
                .ok_or(StreamError::Capacity)?;
            let mut pages = build::Pages::new(storage, height).await?;
            let mut cursor = summaries.cursor(storage).await?;
            while let Some((first, page)) = cursor.next().await? {
                pages
                    .child(Child {
                        first: Key::stored(storage, first).await?,
                        page,
                    })
                    .await?;
            }
            summaries = pages.finish().await?;
        }
        let mut cursor = summaries.cursor(storage).await?;
        let (_, root) = cursor.next().await?.ok_or(StreamError::InvalidResponse)?;
        if cursor.next().await?.is_some() {
            return Err(StreamError::InvalidResponse);
        }
        Ok(Self {
            root: Some(root),
            height,
            count,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
