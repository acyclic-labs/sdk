use super::*;
use acyclic_collections::page::{publish, rewrite};

type Children = Vec<Child<Key, Root>>;

struct Request {
    root: Root,
    height: u8,
}
struct Frame {
    original: Root,
    children: Children,
    position: usize,
    selected: bool,
    dirty: bool,
    height: u8,
}
struct Work<'a, S> {
    storage: &'a S,
    writing: u8,
    inserted: bool,
    changed: bool,
}

fn copy<T: Clone>(items: &[T]) -> Result<Vec<T>, StreamError> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(items.len())
        .map_err(|_| StreamError::Capacity)?;
    result.extend(items.iter().cloned());
    Ok(result)
}

impl<S: Storage> Work<'_, S> {
    async fn leaf(&mut self, entries: &[Entry]) -> Result<Children, StreamError> {
        self.writing = 0;
        let (output, pending) = publish::write::<Format, publish::Leaf, _, HEADER, TARGET>(
            self, entries, 0, true, false,
        )
        .await?;
        if pending.is_some() {
            return Err(StreamError::InvalidResponse);
        }
        Ok(output)
    }
    async fn internal(
        &mut self,
        children: &[Child<Key, Root>],
        height: u8,
    ) -> Result<Children, StreamError> {
        self.writing = height;
        let (output, pending) = publish::write::<Format, publish::Internal, _, HEADER, TARGET>(
            self,
            children,
            u16::from(height),
            true,
            false,
        )
        .await?;
        if pending.is_some() {
            return Err(StreamError::InvalidResponse);
        }
        Ok(output)
    }
}

impl<S: Storage> rewrite::Work<Entry> for Work<'_, S> {
    type Request = Request;
    type Frame = Frame;
    type Output = Children;
    type Stack = Vec<Frame>;
    type Error = StreamError;

    fn advance(&mut self, frame: &mut Frame, _: &Entry) -> Result<Option<Request>, StreamError> {
        if frame.selected {
            return Ok(None);
        }
        frame.selected = true;
        Ok(Some(Request {
            root: frame.children[frame.position].page,
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
    fn append(&mut self, frame: &mut Frame, output: Children) -> Result<(), StreamError> {
        let original = &frame.children[frame.position];
        frame.dirty = output.len() != 1 || output.first() != Some(original);
        frame
            .children
            .try_reserve(output.len().saturating_sub(1))
            .map_err(|_| StreamError::Capacity)?;
        drop(
            frame
                .children
                .splice(frame.position..frame.position + 1, output),
        );
        Ok(())
    }
}

impl<S: Storage> rewrite::Io<Entry> for Work<'_, S> {
    async fn enter(
        &mut self,
        request: Request,
        input: &Entry,
    ) -> Result<rewrite::Entered<Frame, Children>, StreamError> {
        match codec::read(self.storage, request.root, request.height).await? {
            Page::Leaf(mut entries) => {
                match search(self.storage, &entries, &input.key, |entry| &entry.key).await? {
                    Ok(position) if entries[position].value == input.value => {
                        return Ok(rewrite::Entered::Complete(vec![Child {
                            first: entries[0].key.clone(),
                            page: request.root,
                        }]));
                    }
                    Ok(position) => entries[position].value = input.value.clone(),
                    Err(position) => {
                        entries.try_reserve(1).map_err(|_| StreamError::Capacity)?;
                        entries.insert(position, input.clone());
                        self.inserted = true;
                    }
                }
                self.changed = true;
                Ok(rewrite::Entered::Complete(self.leaf(&entries).await?))
            }
            Page::Internal(children) => {
                let position = match search(self.storage, &children, &input.key, |child| {
                    &child.first
                })
                .await?
                {
                    Ok(position) => position,
                    Err(position) => position.saturating_sub(1),
                };
                Ok(rewrite::Entered::Internal(Frame {
                    original: request.root,
                    children,
                    position,
                    selected: false,
                    dirty: false,
                    height: request.height,
                }))
            }
        }
    }
    async fn finish(&mut self, frame: Frame) -> Result<Children, StreamError> {
        if !frame.dirty {
            return Ok(vec![Child {
                first: frame.children[0].first.clone(),
                page: frame.original,
            }]);
        }
        self.internal(&frame.children, frame.height).await
    }
    fn retire_stack(&mut self, stack: Vec<Frame>) -> Result<(), StreamError> {
        if !stack.is_empty() {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

impl<S: Storage> publish::Work<Format> for Work<'_, S> {
    type First = Key;
    type LeafCarry = Vec<Entry>;
    type Output = Children;
    type Error = StreamError;

    fn limits(&self) -> Result<page::PackingLimits, StreamError> {
        Ok(page::PackingLimits {
            maximum_items: tree::LEAF_BYTES,
            maximum_bytes: tree::LEAF_BYTES,
            maximum_object_bytes: tree::LEAF_BYTES,
        })
    }
    fn leaf_length(entry: &Entry) -> Result<usize, StreamError> {
        Ok(codec::entry_length(entry))
    }
    fn child_length(child: &Child<Key, Root>) -> Result<usize, StreamError> {
        Ok(codec::child_length(child))
    }
    fn packing_error(error: page::PackingError<StreamError>) -> StreamError {
        match error {
            page::PackingError::Encode(error) => error,
            _ => StreamError::InvalidArgument,
        }
    }
    fn invalid_input() -> StreamError {
        StreamError::InvalidResponse
    }
    fn key_hash(key: &Key) -> u64 {
        key.hash()
    }
    fn carry_leaf(&mut self, entries: &[Entry]) -> Result<Vec<Entry>, StreamError> {
        copy(entries)
    }
    fn carry_children(&mut self, children: &[Child<Key, Root>]) -> Result<Children, StreamError> {
        copy(children)
    }
    fn charge_items(&mut self, _: u64) -> Result<(), StreamError> {
        Ok(())
    }
    fn allocate(&mut self, capacity: usize) -> Result<Children, StreamError> {
        let mut output = Vec::new();
        output
            .try_reserve_exact(capacity)
            .map_err(|_| StreamError::Capacity)?;
        Ok(output)
    }
    fn reserve(&mut self, output: &mut Children, _: usize) -> Result<(), StreamError> {
        output.try_reserve(1).map_err(|_| StreamError::Capacity)
    }
    fn first(&mut self, key: &Key) -> Result<Key, StreamError> {
        Ok(key.clone())
    }
    fn append(&mut self, output: &mut Children, first: Key, page: Root) -> Result<(), StreamError> {
        output.push(Child { first, page });
        Ok(())
    }
    async fn write(&mut self, page: &PageRef<'_, Format>) -> Result<Root, StreamError> {
        let page = match page {
            PageRef::Leaf(entries) => PageRef::Leaf(entries),
            PageRef::Internal(children) => PageRef::Internal(children),
        };
        codec::write_root(self.storage, page, self.writing).await
    }
}

impl Index {
    /// Returns a private COW successor, preserving every original pinned root.
    /// The enclosing owner retains successful puts until publication/retirement.
    pub async fn set(
        &self,
        storage: &impl Storage,
        key: Field,
        value: Field,
    ) -> Result<Self, StreamError> {
        let input = Entry {
            key: Key::stored(storage, key).await?,
            value: key::store(storage, value, INLINE_FIELD_BYTES).await?,
        };
        let Some(root) = self.root else {
            let mut builder = Builder::new(storage);
            builder.append(input).await?;
            return builder.finish().await;
        };
        let mut work = Work {
            storage,
            writing: 0,
            inserted: false,
            changed: false,
        };
        let mut output = rewrite::run(
            &mut work,
            Vec::new(),
            Request {
                root,
                height: self.height,
            },
            &input,
        )
        .await?;
        if !work.changed {
            return Ok(self.clone());
        }
        let count = self
            .count
            .checked_add(u64::from(work.inserted))
            .ok_or(StreamError::Capacity)?;
        let mut height = self.height;
        while output.len() > 1 {
            height = height
                .checked_add(1)
                .filter(|height| *height <= MAX_HEIGHT)
                .ok_or(StreamError::Capacity)?;
            output = work.internal(&output, height).await?;
        }
        let child = output.pop().ok_or(StreamError::InvalidResponse)?;
        Ok(Self {
            root: Some(child.page),
            height,
            count,
        })
    }
}
