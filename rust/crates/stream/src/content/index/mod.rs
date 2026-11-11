//! Immutable paged ordered indexes for unpublished plans and pinned readers.
//! Keys and values are byte fields; large fields retain content references.
//! Building an index assigns no Stream position and publishes no authority root.
use super::tree::{self, Root, Storage};
use super::{Field, INLINE_FIELD_BYTES};
use crate::{OwnedBytes, StreamError};
use acyclic_collections::page::search::Search;
use acyclic_collections::page::{self, Child, Page, PageRef};

mod build;
mod checkpoint;
mod codec;
mod cursor;
mod key;
mod sort;
pub use build::Builder;
pub use cursor::Cursor;
use key::Key;
pub use sort::Sorter;

const MAGIC: &[u8] = b"acyclic-ordered-index-v1\0";
const HEADER: usize = MAGIC.len() + 5;
const TARGET: usize = tree::LEAF_BYTES / 4;
const MAX_HEIGHT: u8 = u64::BITS as u8;

#[derive(Clone, Eq, PartialEq)]
struct Entry {
    key: Key,
    value: Field,
}

struct Format;
impl page::Format for Format {
    type Key = Key;
    type Value = Entry;
    type Reference = Root;
    fn key(value: &Entry) -> &Key {
        &value.key
    }
}

/// One complete immutable index. Cloning retains only its root descriptor.
/// The enclosing stage or published reader pin must retain referenced pages.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Index {
    root: Option<Root>,
    height: u8,
    count: u64,
}

impl Index {
    /// Number of complete entries, independent of physical page boundaries.
    pub const fn len(&self) -> u64 {
        self.count
    }
    /// Whether there are no entries.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Immutable content root for retention by the enclosing plan/publication.
    pub const fn root(&self) -> Option<Root> {
        self.root
    }
    /// Looks up one field while loading only the bounded search path.
    pub async fn get(
        &self,
        storage: &impl Storage,
        key: Field,
    ) -> Result<Option<Field>, StreamError> {
        if self.root.is_none() {
            return Ok(None);
        }
        let key = Key::new(storage, key).await?;
        let mut cursor = Cursor::open(storage, self, Some(&key), false).await?;
        if cursor.matched() {
            let entry = cursor
                .next_entry()
                .await?
                .ok_or(StreamError::InvalidResponse)?;
            Ok(Some(entry.value))
        } else {
            Ok(None)
        }
    }
    /// Opens ordered iteration at an inclusive byte-key lower bound. Its root
    /// remains fixed while later indexes or authority decisions are built.
    pub async fn scan<'a, S: Storage>(
        &self,
        storage: &'a S,
        lower: Option<Field>,
    ) -> Result<Cursor<'a, S>, StreamError> {
        if self.root.is_none() {
            return Cursor::open(storage, self, None, true).await;
        }
        let lower = match lower {
            Some(field) => Some(Key::new(storage, field).await?),
            None => None,
        };
        Cursor::open(storage, self, lower.as_ref(), true).await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

async fn search<T>(
    storage: &impl Storage,
    entries: &[T],
    key: &Key,
    field: impl Fn(&T) -> &Key,
) -> Result<Result<usize, usize>, StreamError> {
    let mut search = Search::new(entries.len(), 0).ok_or(StreamError::InvalidResponse)?;
    loop {
        match search {
            Search::Complete(result) => return Ok(result),
            Search::Compare(probe) => {
                let ordering = field(&entries[probe.index()]).compare(storage, key).await?;
                search = probe.resolve(ordering);
            }
        }
    }
}
