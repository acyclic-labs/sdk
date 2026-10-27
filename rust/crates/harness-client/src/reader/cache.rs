//! The one client cache: immutable committed records keyed by
//! `(stream, position)`, plus the hierarchy pages that list a subtree, under a
//! single least-recently-used byte budget.
//!
//! Committed records never change, so a record entry is never invalidated; a
//! stream only extends. A forked stream's prefix *is* its source's records, so
//! `(child, position < fork_point)` resolves to the source's entry and is
//! never stored twice. Persistence writes this same structure to bytes.
//!
//! An entry the server accepted may still be above its durable watermark, so
//! every cached suffix is provisional: each stream keeps the commit of its
//! highest cached record as an anchor, and a resumed follow checks it
//! (`follow_checked`). A changed anchor means the provisional suffix was
//! replaced, and [`RecordCache::forget`] drops that stream's own entries.

use acyclic_stream::{CommitId, StreamPath};
use bytes::Bytes;
use std::collections::BTreeMap;

/// Bound on fork-alias resolution; equals the deepest admitted agent nesting.
const MAX_ALIAS_DEPTH: usize = 64;
/// Fixed accounting charge per entry: map nodes, key and LRU bookkeeping.
const ENTRY_OVERHEAD: usize = 96;

/// One cache key. Records are immutable; listing pages are hierarchy views
/// that the scheduler revalidates while they are displayed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    /// Committed record at `(stream, sequence)`.
    Record(StreamPath, u64),
    /// `children_page` page `index` of a parent stream.
    Listing(StreamPath, u32),
}

/// A cached `children_page` result. `after` is the exclusive start used to
/// fetch it; `next` is the exclusive start of the following page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listing {
    /// Exclusive start, `None` for the first page.
    pub after: Option<StreamPath>,
    /// Ordered direct children.
    pub children: Vec<StreamPath>,
    /// The server's exclusive start of the next page, if one exists.
    pub next: Option<StreamPath>,
    /// Fetched in this process, as opposed to restored from persistence.
    pub fresh: bool,
    /// The parent's witness tail when this page was requested.
    pub witnessed: Option<u64>,
}

enum Value {
    Record(Bytes),
    Listing(Listing),
}

struct Entry {
    value: Value,
    charge: usize,
    used: u64,
}

/// Per-stream facts learned from committed history. Bounded with the entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Meta {
    /// Highest observed exclusive tail.
    tail: Option<u64>,
    /// Source stream and exclusive inherited prefix end.
    fork: Option<(StreamPath, u64)>,
    /// Commit of the highest cached record, checked when a follow resumes.
    anchor: Option<(u64, CommitId)>,
}

/// Bounded immutable record cache shared by every renderer.
pub struct RecordCache {
    entries: BTreeMap<Key, Entry>,
    lru: BTreeMap<u64, Key>,
    meta: BTreeMap<StreamPath, Meta>,
    bytes: usize,
    clock: u64,
    max_bytes: usize,
    max_streams: usize,
}

impl RecordCache {
    /// An empty cache under one byte budget and a stream-metadata bound.
    #[must_use]
    pub fn new(max_bytes: usize, max_streams: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            lru: BTreeMap::new(),
            meta: BTreeMap::new(),
            bytes: 0,
            clock: 0,
            max_bytes,
            max_streams,
        }
    }

    /// Retained charge, including keys, values and bookkeeping.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes + self.meta.len() * ENTRY_OVERHEAD
    }

    /// Retained entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Monotonic use clock; entries used at or after a mark are "this frame".
    #[must_use]
    pub fn clock(&self) -> u64 {
        self.clock
    }

    /// Shrink or grow the byte budget (memory pressure), evicting at once.
    pub fn set_budget(&mut self, max_bytes: usize) {
        self.max_bytes = max_bytes;
        self.evict(None);
    }

    /// Record that `stream`'s first `at` records are `source`'s.
    pub fn set_fork(&mut self, stream: &StreamPath, source: StreamPath, at: u64) {
        if &source == stream {
            return;
        }
        self.meta_mut(stream).fork = Some((source, at));
    }

    /// The fork origin, if learned.
    #[must_use]
    pub fn fork(&self, stream: &StreamPath) -> Option<&(StreamPath, u64)> {
        self.meta.get(stream).and_then(|meta| meta.fork.as_ref())
    }

    /// Advance the stream's confirmed end; it never moves backwards. Only
    /// a source that saw the end reports it (`tail()`, a projected last
    /// record, a follow, an empty read): a cached record proves only that
    /// the stream reaches it, so reads past it are still issued.
    pub fn observe_tail(&mut self, stream: &StreamPath, tail: u64) {
        let meta = self.meta_mut(stream);
        meta.tail = Some(meta.tail.map_or(tail, |old| old.max(tail)));
    }

    /// Record `commit` as the anchor if `sequence` is the highest cached.
    pub fn note_commit(&mut self, stream: &StreamPath, sequence: u64, commit: CommitId) {
        let meta = self.meta_mut(stream);
        if meta.anchor.is_none_or(|(at, _)| at <= sequence) {
            meta.anchor = Some((sequence, commit));
        }
    }

    /// The commit at `sequence`, if it is the stream's anchor.
    #[must_use]
    pub fn anchor(&self, stream: &StreamPath, sequence: u64) -> Option<CommitId> {
        self.meta
            .get(stream)
            .and_then(|meta| meta.anchor)
            .and_then(|(at, commit)| (at == sequence).then_some(commit))
    }

    /// Drop `stream`'s own records and tail after its provisional suffix was
    /// replaced; an inherited fork prefix belongs to the source and stays.
    pub fn forget(&mut self, stream: &StreamPath) {
        let keys: Vec<Key> = self
            .entries
            .range(Key::Record(stream.clone(), 0)..=Key::Record(stream.clone(), u64::MAX))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.remove(&key);
        }
        if let Some(meta) = self.meta.get_mut(stream) {
            meta.tail = None;
            meta.anchor = None;
        }
    }

    /// Drop a parent's listing pages after `index`: their cursors were cut
    /// at a boundary the refreshed page `index` no longer ends at.
    pub fn drop_listings_after(&mut self, parent: &StreamPath, index: u32) {
        let Some(first) = index.checked_add(1) else {
            return;
        };
        let keys: Vec<Key> = self
            .entries
            .range(Key::Listing(parent.clone(), first)..=Key::Listing(parent.clone(), u32::MAX))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }

    /// Highest observed exclusive tail.
    #[must_use]
    pub fn tail(&self, stream: &StreamPath) -> Option<u64> {
        self.meta.get(stream).and_then(|meta| meta.tail)
    }

    fn meta_mut(&mut self, stream: &StreamPath) -> &mut Meta {
        if !self.meta.contains_key(stream) && self.meta.len() >= self.max_streams {
            // Facts are re-learnable from history; drop one without a fork
            // first, so aliasing (which saves the most bytes) survives longest.
            let victim = self
                .meta
                .iter()
                .find(|(_, meta)| meta.fork.is_none())
                .or_else(|| self.meta.iter().next())
                .map(|(path, _)| path.clone());
            if let Some(victim) = victim {
                self.meta.remove(&victim);
            }
        }
        self.meta.entry(stream.clone()).or_default()
    }

    /// Resolve a record through fork aliases to the key that would hold it.
    fn resolve(&self, stream: &StreamPath, sequence: u64) -> Key {
        let mut path = stream;
        for _ in 0..MAX_ALIAS_DEPTH {
            match self.meta.get(path).and_then(|meta| meta.fork.as_ref()) {
                Some((source, at)) if sequence < *at => path = source,
                _ => break,
            }
        }
        Key::Record(path.clone(), sequence)
    }

    /// Borrow a committed record without touching recency.
    #[must_use]
    pub fn record(&self, stream: &StreamPath, sequence: u64) -> Option<&Bytes> {
        match self.entries.get(&self.resolve(stream, sequence)) {
            Some(Entry {
                value: Value::Record(value),
                ..
            }) => Some(value),
            _ => None,
        }
    }

    /// Whether `[from, to)` of `stream` is entirely cached.
    #[must_use]
    pub fn has_range(&self, stream: &StreamPath, from: u64, to: u64) -> bool {
        (from..to).all(|sequence| self.record(stream, sequence).is_some())
    }

    /// First uncached position in `[from, to)`.
    #[must_use]
    pub fn first_missing(&self, stream: &StreamPath, from: u64, to: u64) -> Option<u64> {
        (from..to).find(|sequence| self.record(stream, *sequence).is_none())
    }

    /// Borrow a listing page without touching recency.
    #[must_use]
    pub fn listing(&self, parent: &StreamPath, index: u32) -> Option<&Listing> {
        match self.entries.get(&Key::Listing(parent.clone(), index)) {
            Some(Entry {
                value: Value::Listing(listing),
                ..
            }) => Some(listing),
            _ => None,
        }
    }

    /// Mark displayed entries most recently used.
    pub fn touch_record(&mut self, stream: &StreamPath, sequence: u64) {
        let key = self.resolve(stream, sequence);
        self.touch(&key);
    }

    /// Mark a displayed listing page most recently used.
    pub fn touch_listing(&mut self, parent: &StreamPath, index: u32) {
        self.touch(&Key::Listing(parent.clone(), index));
    }

    fn touch(&mut self, key: &Key) {
        let clock = self.clock + 1;
        if let Some(entry) = self.entries.get_mut(key) {
            self.lru.remove(&entry.used);
            entry.used = clock;
            self.lru.insert(clock, key.clone());
            self.clock = clock;
        }
    }

    /// Insert one committed record. A position inherited through a fork is
    /// stored once, under its source. `protect` keeps entries used at or
    /// after that mark: a prefetched value that would displace them is not
    /// retained, so prefetch can never evict what is on screen.
    pub fn insert_record(
        &mut self,
        stream: &StreamPath,
        sequence: u64,
        value: Bytes,
        protect: Option<u64>,
    ) -> bool {
        let key = self.resolve(stream, sequence);
        if self.entries.contains_key(&key) {
            // Immutable: an existing entry is already this record.
            self.touch(&key);
            return true;
        }
        let charge = value.len() + path_len(&key) + ENTRY_OVERHEAD;
        self.insert(key, Value::Record(value), charge, protect)
    }

    /// Insert or replace one listing page.
    pub fn insert_listing(
        &mut self,
        parent: &StreamPath,
        index: u32,
        listing: Listing,
        protect: Option<u64>,
    ) -> bool {
        let key = Key::Listing(parent.clone(), index);
        let charge = listing
            .children
            .iter()
            .map(|child| child.as_str().len() + 24)
            .sum::<usize>()
            + listing
                .after
                .as_ref()
                .map_or(0, |after| after.as_str().len())
            + listing.next.as_ref().map_or(0, |next| next.as_str().len())
            + path_len(&key)
            + ENTRY_OVERHEAD;
        self.remove(&key);
        self.insert(key, Value::Listing(listing), charge, protect)
    }

    fn insert(&mut self, key: Key, value: Value, charge: usize, protect: Option<u64>) -> bool {
        if charge > self.max_bytes {
            return false;
        }
        self.clock += 1;
        let used = self.clock;
        self.lru.insert(used, key.clone());
        self.entries.insert(
            key.clone(),
            Entry {
                value,
                charge,
                used,
            },
        );
        self.bytes += charge;
        if !self.evict(protect) {
            self.remove(&key);
            return false;
        }
        true
    }

    fn remove(&mut self, key: &Key) {
        if let Some(entry) = self.entries.remove(key) {
            self.lru.remove(&entry.used);
            self.bytes -= entry.charge;
        }
    }

    /// Evict least recently used entries until within budget. Returns false
    /// when only protected entries remain over budget.
    fn evict(&mut self, protect: Option<u64>) -> bool {
        while self.bytes() > self.max_bytes {
            let Some((&used, key)) = self.lru.iter().next() else {
                return true;
            };
            if protect.is_some_and(|mark| used > mark) {
                return false;
            }
            let key = key.clone();
            self.remove(&key);
        }
        true
    }

    /// Persist the cache, most recently used last, as length-prefixed bytes.
    /// Restoring the same bytes yields the same records and listings.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bytes);
        out.extend_from_slice(MAGIC);
        put_u64(&mut out, self.meta.len() as u64);
        for (path, meta) in &self.meta {
            put_str(&mut out, path.as_str());
            put_u64(&mut out, meta.tail.map_or(0, |tail| tail + 1));
            match &meta.fork {
                Some((source, at)) => {
                    out.push(1);
                    put_str(&mut out, source.as_str());
                    put_u64(&mut out, *at);
                }
                None => out.push(0),
            }
            match &meta.anchor {
                Some((at, commit)) => {
                    out.push(1);
                    put_u64(&mut out, *at);
                    out.extend_from_slice(commit.as_bytes());
                }
                None => out.push(0),
            }
        }
        put_u64(&mut out, self.lru.len() as u64);
        for key in self.lru.values() {
            let Some(entry) = self.entries.get(key) else {
                continue;
            };
            match (key, &entry.value) {
                (Key::Record(path, sequence), Value::Record(value)) => {
                    out.push(0);
                    put_str(&mut out, path.as_str());
                    put_u64(&mut out, *sequence);
                    put_bytes(&mut out, value);
                }
                (Key::Listing(path, index), Value::Listing(listing)) => {
                    out.push(1);
                    put_str(&mut out, path.as_str());
                    put_u64(&mut out, u64::from(*index));
                    put_str(
                        &mut out,
                        listing.after.as_ref().map_or("", |after| after.as_str()),
                    );
                    put_str(
                        &mut out,
                        listing.next.as_ref().map_or("", |next| next.as_str()),
                    );
                    put_u64(&mut out, listing.children.len() as u64);
                    for child in &listing.children {
                        put_str(&mut out, child.as_str());
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Restore persisted bytes into this (empty or not) cache, under its own
    /// budget. Restored listings are not fresh, so displayed ones revalidate.
    ///
    /// # Errors
    /// Refuses malformed input without retaining any part of it.
    pub fn restore(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let mut input = Input(bytes);
        if input.take(MAGIC.len())? != MAGIC {
            return Err(SnapshotError);
        }
        let mut metas = Vec::new();
        for _ in 0..input.count()? {
            let path = input.path()?;
            let tail = input.u64()?.checked_sub(1);
            let fork = match input.u8()? {
                0 => None,
                1 => Some((input.path()?, input.u64()?)),
                _ => return Err(SnapshotError),
            };
            let anchor = match input.u8()? {
                0 => None,
                1 => {
                    let at = input.u64()?;
                    let commit: [u8; 32] = input.take(32)?.try_into().map_err(|_| SnapshotError)?;
                    Some((at, CommitId::from_bytes(commit)))
                }
                _ => return Err(SnapshotError),
            };
            metas.push((path, tail, fork, anchor));
        }
        let mut entries = Vec::new();
        for _ in 0..input.count()? {
            match input.u8()? {
                0 => {
                    let path = input.path()?;
                    let sequence = input.u64()?;
                    let value = Bytes::copy_from_slice(input.bytes()?);
                    entries.push((path, sequence, Ok(value)));
                }
                1 => {
                    let path = input.path()?;
                    let index = u32::try_from(input.u64()?).map_err(|_| SnapshotError)?;
                    let after = match input.str()? {
                        "" => None,
                        text => Some(StreamPath::new(text).map_err(|_| SnapshotError)?),
                    };
                    let next = match input.str()? {
                        "" => None,
                        text => Some(StreamPath::new(text).map_err(|_| SnapshotError)?),
                    };
                    let mut children = Vec::new();
                    for _ in 0..input.count()? {
                        children.push(input.path()?);
                    }
                    entries.push((
                        path,
                        u64::from(index),
                        Err(Listing {
                            after,
                            children,
                            next,
                            fresh: false,
                            witnessed: None,
                        }),
                    ));
                }
                _ => return Err(SnapshotError),
            }
        }
        if !input.0.is_empty() {
            return Err(SnapshotError);
        }
        for (path, tail, fork, anchor) in metas {
            if let Some(tail) = tail {
                self.observe_tail(&path, tail);
            }
            if let Some((source, at)) = fork {
                self.set_fork(&path, source, at);
            }
            if let Some((at, commit)) = anchor {
                self.note_commit(&path, at, commit);
            }
        }
        for (path, position, value) in entries {
            match value {
                Ok(value) => {
                    self.insert_record(&path, position, value, None);
                }
                Err(listing) => {
                    // Index came from a u32 above.
                    self.insert_listing(&path, position as u32, listing, None);
                }
            }
        }
        Ok(())
    }
}

fn path_len(key: &Key) -> usize {
    match key {
        Key::Record(path, _) | Key::Listing(path, _) => path.as_str().len(),
    }
}

const MAGIC: &[u8] = b"acyclic.records.v1\n";

/// Persisted bytes are malformed; nothing was restored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotError;

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("malformed record cache snapshot")
    }
}

impl std::error::Error for SnapshotError {}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u64(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], SnapshotError> {
        if self.0.len() < count {
            return Err(SnapshotError);
        }
        let (head, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, SnapshotError> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64, SnapshotError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().map_err(|_| SnapshotError)?;
        Ok(u64::from_le_bytes(bytes))
    }
    /// A count can never exceed the remaining input, so a forged length
    /// cannot drive an unbounded loop.
    fn count(&mut self) -> Result<usize, SnapshotError> {
        let count = usize::try_from(self.u64()?).map_err(|_| SnapshotError)?;
        if count > self.0.len() {
            return Err(SnapshotError);
        }
        Ok(count)
    }
    fn bytes(&mut self) -> Result<&'a [u8], SnapshotError> {
        let count = self.count()?;
        self.take(count)
    }
    fn str(&mut self) -> Result<&'a str, SnapshotError> {
        std::str::from_utf8(self.bytes()?).map_err(|_| SnapshotError)
    }
    fn path(&mut self) -> Result<StreamPath, SnapshotError> {
        StreamPath::new(self.str()?).map_err(|_| SnapshotError)
    }
}
