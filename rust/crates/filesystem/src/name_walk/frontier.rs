//! Persistent bounded frontier blocks. Only a changed stack suffix is encoded
//! at publication; older checkpoint snapshots share all other blocks.
use super::{Continuation, LazyDirectoryCursor, SavedCursor, SearchError};
use imbl::OrdMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, PoisonError};

const BLOCK_ENTRIES: usize = 32;
type Entry = (String, SavedCursor);
type Block = Arc<Vec<Entry>>;

#[derive(Clone, Default)]
pub(super) struct SavedFrontier {
    blocks: OrdMap<usize, Block>,
    length: usize,
}

impl SavedFrontier {
    pub(super) fn len(&self) -> usize {
        self.length
    }

    pub(super) fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub(super) fn push(&mut self, entry: Entry) {
        let ordinal = self.length / BLOCK_ENTRIES;
        if !self.blocks.contains_key(&ordinal) {
            self.blocks.insert(ordinal, Arc::new(Vec::new()));
        }
        Arc::make_mut(self.blocks.get_mut(&ordinal).expect("inserted block")).push(entry);
        self.length += 1;
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.blocks.values().flat_map(|block| block.iter())
    }

    pub(super) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub(super) fn changes<'a>(
        &'a self,
        previous: Option<&'a Self>,
    ) -> Box<dyn Iterator<Item = (usize, Option<&'a [Entry]>)> + Send + 'a> {
        match previous {
            Some(previous) => Box::new(previous.blocks.diff(&self.blocks).map(
                |change| match change {
                    imbl::ordmap::DiffItem::Add(ordinal, block) => {
                        (*ordinal, Some(block.as_slice()))
                    }
                    imbl::ordmap::DiffItem::Update {
                        new: (ordinal, block),
                        ..
                    } => (*ordinal, Some(block.as_slice())),
                    imbl::ordmap::DiffItem::Remove(ordinal, _) => (*ordinal, None),
                },
            )),
            None => Box::new(
                self.blocks
                    .iter()
                    .map(|(ordinal, block)| (*ordinal, Some(block.as_slice()))),
            ),
        }
    }
}

impl Serialize for SavedFrontier {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for SavedFrontier {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SavedFrontier;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("ordered directory continuations")
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> Result<Self::Value, S::Error> {
                let mut saved = SavedFrontier::default();
                while let Some(entry) = seq.next_element()? {
                    saved.push(entry);
                }
                Ok(saved)
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}

#[derive(Default)]
struct Cache {
    saved: SavedFrontier,
    dirty: Option<usize>,
}

#[derive(Default)]
pub(super) struct Frontier {
    entries: Vec<(String, Continuation)>,
    cache: Mutex<Cache>,
}

impl std::ops::Deref for Frontier {
    type Target = [(String, Continuation)];
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl Frontier {
    pub(super) fn restored(entries: Vec<(String, Continuation)>, saved: SavedFrontier) -> Self {
        Self {
            entries,
            cache: Mutex::new(Cache { saved, dirty: None }),
        }
    }

    fn changed(&mut self, from: usize) {
        let cache = self.cache.get_mut().unwrap_or_else(PoisonError::into_inner);
        cache.dirty = Some(cache.dirty.map_or(from, |old| old.min(from)));
    }

    pub(super) fn push(&mut self, entry: (String, Continuation)) {
        self.changed(self.entries.len());
        self.entries.push(entry);
    }

    pub(super) fn pop(&mut self) -> Option<(String, Continuation)> {
        let entry = self.entries.pop()?;
        self.changed(self.entries.len());
        Some(entry)
    }

    pub(super) fn extend(&mut self, entries: impl IntoIterator<Item = (String, Continuation)>) {
        self.changed(self.entries.len());
        self.entries.extend(entries);
    }

    pub(super) fn sort_by_query(&mut self, start: usize, query: &str) {
        self.changed(start);
        self.entries[start..].sort_by_cached_key(|(path, _)| super::directory_matches(path, query));
    }

    pub(super) fn checkpoint(&self) -> Result<SavedFrontier, SearchError> {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(from) = cache.dirty {
            let first = from / BLOCK_ENTRIES;
            let count = self.entries.len().div_ceil(BLOCK_ENTRIES);
            let old_count = cache.saved.blocks.len();
            for ordinal in count..old_count {
                cache.saved.blocks.remove(&ordinal);
            }
            for ordinal in first..count {
                let start = ordinal * BLOCK_ENTRIES;
                let end = (start + BLOCK_ENTRIES).min(self.entries.len());
                let block = self.entries[start..end]
                    .iter()
                    .map(|(path, cursor)| {
                        let cursor = match cursor {
                            Continuation::Lazy(cursor) => SavedCursor::Lazy(
                                cursor
                                    .as_ref()
                                    .map(LazyDirectoryCursor::checkpoint)
                                    .transpose()?,
                            ),
                            Continuation::Exact(name) => SavedCursor::Exact(
                                name.as_ref()
                                    .map(|name| (name.encoding().tag(), name.as_bytes().to_vec())),
                            ),
                        };
                        Ok((path.clone(), cursor))
                    })
                    .collect::<Result<Vec<_>, SearchError>>()?;
                cache.saved.blocks.insert(ordinal, Arc::new(block));
            }
            cache.saved.length = self.entries.len();
            cache.dirty = None;
        }
        Ok(cache.saved.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_search_frontier_checkpoints_encode_only_changed_blocks() {
        let mut frontier = Frontier::default();
        for ordinal in 0..10_000 {
            frontier.push((format!("/d{ordinal:05}"), Continuation::Exact(None)));
        }
        let original = frontier.checkpoint().unwrap();
        let unchanged = frontier.checkpoint().unwrap();
        assert_eq!(unchanged.changes(Some(&original)).count(), 0);
        for (ordinal, block) in &original.blocks {
            assert!(Arc::ptr_eq(block, unchanged.blocks.get(ordinal).unwrap()));
        }
        frontier.pop();
        frontier.push(("/replacement".into(), Continuation::Exact(None)));
        let changed = frontier.checkpoint().unwrap();
        assert_eq!(changed.changes(Some(&original)).count(), 1);
        let entries = changed
            .changes(Some(&original))
            .map(|(_, block)| block.unwrap().len())
            .sum::<usize>();
        assert!(entries <= BLOCK_ENTRIES);
        // Negative control: serializing the full former stack visits every row.
        assert_eq!(changed.iter().count(), 10_000);
        assert_eq!(original.iter().last().unwrap().0, "/d09999");
        assert_eq!(changed.iter().last().unwrap().0, "/replacement");
        let bytes = serde_json::to_vec(&changed).unwrap();
        let restored: SavedFrontier = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&restored).unwrap());
        while !frontier.is_empty() {
            frontier.pop();
        }
        let empty = frontier.checkpoint().unwrap();
        assert_eq!(empty.changes(Some(&changed)).count(), changed.block_count());
        assert!(empty.iter().next().is_none());
    }
}
