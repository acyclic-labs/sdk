use super::*;

struct Frame {
    children: Vec<Child<Key, Root>>,
    next: usize,
    height: u8,
}

/// Ordered fields from one immutable index root. The enclosing retention pin
/// owns page lifetime; this cursor retains only bounded pages on one spine.
pub struct Cursor<'a, S> {
    storage: &'a S,
    root: Option<Root>,
    frames: Vec<Frame>,
    leaf: std::vec::IntoIter<Entry>,
    matched: bool,
    retain_spine: bool,
    interrupted: bool,
}

impl<'a, S: Storage> Cursor<'a, S> {
    pub(super) async fn open(
        storage: &'a S,
        index: &Index,
        lower: Option<&Key>,
        retain_spine: bool,
    ) -> Result<Self, StreamError> {
        let mut frames = Vec::new();
        frames
            .try_reserve_exact(if retain_spine {
                usize::from(index.height)
            } else {
                0
            })
            .map_err(|_| StreamError::Capacity)?;
        let mut cursor = Self {
            storage,
            root: index.root,
            frames,
            leaf: Vec::new().into_iter(),
            matched: false,
            retain_spine,
            interrupted: false,
        };
        if let Some(root) = index.root {
            cursor.matched = cursor.descend(root, index.height, lower).await?;
        }
        Ok(cursor)
    }

    /// Immutable root selected when this cursor opened, independent of later
    /// index construction or authority publication.
    pub const fn root(&self) -> Option<Root> {
        self.root
    }

    pub(super) const fn matched(&self) -> bool {
        self.matched
    }

    async fn descend(
        &mut self,
        mut root: Root,
        mut height: u8,
        lower: Option<&Key>,
    ) -> Result<bool, StreamError> {
        loop {
            match codec::read(self.storage, root, height).await? {
                Page::Leaf(mut entries) => {
                    let (first, matched) = if let Some(lower) = lower {
                        match search(self.storage, &entries, lower, |entry| &entry.key).await? {
                            Ok(position) => (position, true),
                            Err(position) => (position, false),
                        }
                    } else {
                        (0, false)
                    };
                    entries.drain(..first);
                    self.leaf = entries.into_iter();
                    return Ok(matched);
                }
                Page::Internal(children) => {
                    let first = if let Some(lower) = lower {
                        match search(self.storage, &children, lower, |child| &child.first).await? {
                            Ok(position) => position,
                            Err(position) => position.saturating_sub(1),
                        }
                    } else {
                        0
                    };
                    root = children
                        .get(first)
                        .ok_or(StreamError::InvalidResponse)?
                        .page;
                    if self.retain_spine {
                        if self.frames.len() >= usize::from(MAX_HEIGHT) {
                            return Err(StreamError::InvalidResponse);
                        }
                        self.frames.push(Frame {
                            children,
                            next: first + 1,
                            height,
                        });
                    }
                    height = height.checked_sub(1).ok_or(StreamError::InvalidResponse)?;
                }
            }
        }
    }

    /// Returns the next key/value without materializing large fields. A failed
    /// or cancelled step retires this cursor rather than yielding a later prefix.
    pub async fn next(&mut self) -> Result<Option<(Field, Field)>, StreamError> {
        Ok(self
            .next_entry()
            .await?
            .map(|entry| (entry.key.field, entry.value)))
    }

    pub(super) async fn next_entry(&mut self) -> Result<Option<Entry>, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = self.advance().await;
        if result.is_ok() {
            self.interrupted = false;
        }
        result
    }

    async fn advance(&mut self) -> Result<Option<Entry>, StreamError> {
        loop {
            if let Some(entry) = self.leaf.next() {
                return Ok(Some(entry));
            }
            let Some(frame) = self.frames.last_mut() else {
                return Ok(None);
            };
            let Some(child) = frame.children.get(frame.next) else {
                self.frames.pop();
                continue;
            };
            let root = child.page;
            let height = frame
                .height
                .checked_sub(1)
                .ok_or(StreamError::InvalidResponse)?;
            frame.next += 1;
            self.descend(root, height, None).await?;
        }
    }
}
