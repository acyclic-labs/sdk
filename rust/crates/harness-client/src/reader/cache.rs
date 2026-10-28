//! The one client cache: immutable committed records keyed by
//! `(stream, position)`, plus the hierarchy pages that list a subtree, under a
//! single least-recently-used byte budget.
//!
//! The budget governs what is *cached*, never what can be *read*. A value
//! that cannot be cached without evicting what the current frame displays
//! (including one larger than the whole budget) is passed through: served
//! uncached for as long as a want covers it, so it is never re-requested in
//! a loop. Per-stream facts are bounded separately (`max_streams`) and, like
//! displayed entries, are never evicted while the current frame uses them.
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
use std::collections::{BTreeMap, BTreeSet};

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
    /// Use clock of the last learn or display; facts used since the frame
    /// mark are never chosen as eviction victims.
    used: u64,
}

/// Bounded immutable record cache shared by every renderer.
pub struct RecordCache {
    entries: BTreeMap<Key, Entry>,
    lru: BTreeMap<u64, Key>,
    meta: BTreeMap<StreamPath, Meta>,
    meta_lru: BTreeMap<u64, StreamPath>,
    // Reverse adjacency makes revocation visit affected aliases once.
    aliases: BTreeMap<StreamPath, BTreeSet<StreamPath>>,
    /// Values served uncached, with their charge, while a want covers them.
    passed: BTreeMap<Key, (Value, usize)>,
    passed_bytes: usize,
    bytes: usize,
    clock: u64,
    /// Use clock at the start of the current frame.
    mark: u64,
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
            meta_lru: BTreeMap::new(),
            aliases: BTreeMap::new(),
            passed: BTreeMap::new(),
            passed_bytes: 0,
            bytes: 0,
            clock: 0,
            mark: 0,
            max_bytes,
            max_streams,
        }
    }

    /// Cached charge under the byte budget: keys, values and bookkeeping of
    /// every cached record and listing page. Never above the budget.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Charge of values served uncached because the budget could not hold
    /// them beside what is displayed; bounded by what the wants cover.
    #[must_use]
    pub fn passed_bytes(&self) -> usize {
        self.passed_bytes
    }

    /// Streams whose facts are retained.
    #[must_use]
    pub fn streams(&self) -> usize {
        self.meta.len()
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

    /// Start a frame: entries and facts used from now on are displayed and
    /// protected from eviction by live inserts. Returns the mark.
    pub(super) fn begin_frame(&mut self) -> u64 {
        self.mark = self.clock;
        self.mark
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
        let previous = self.meta_mut(stream).fork.replace((source.clone(), at));
        if let Some((previous, _)) = previous {
            self.unlink_alias(stream, &previous);
        }
        self.aliases
            .entry(source)
            .or_default()
            .insert(stream.clone());
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
        let keys =
            self.keys_in(Key::Record(stream.clone(), 0)..=Key::Record(stream.clone(), u64::MAX));
        for key in keys {
            self.remove(&key);
        }
        if let Some(meta) = self.meta.get_mut(stream) {
            meta.tail = None;
            meta.anchor = None;
        }
    }

    /// A denial or lost prefix cannot retain an alias into another history.
    pub(super) fn remove_stream(&mut self, stream: &StreamPath) {
        let mut pending = vec![stream.clone()];
        let mut affected = BTreeSet::new();
        while let Some(path) = pending.pop() {
            if !affected.insert(path.clone()) {
                continue;
            }
            if let Some(children) = self.aliases.remove(&path) {
                pending.extend(children);
            }
            self.forget(&path);
            self.remove_meta(&path);
        }
        let listings =
            self.keys_in(Key::Listing(stream.clone(), 0)..=Key::Listing(stream.clone(), u32::MAX));
        for key in listings {
            self.remove(&key);
        }
    }

    /// Drop a parent's listing pages after `index`: their cursors were cut
    /// at a boundary the refreshed page `index` no longer ends at.
    pub fn drop_listings_after(&mut self, parent: &StreamPath, index: u32) {
        let Some(first) = index.checked_add(1) else {
            return;
        };
        let keys = self
            .keys_in(Key::Listing(parent.clone(), first)..=Key::Listing(parent.clone(), u32::MAX));
        for key in keys {
            self.remove(&key);
        }
    }

    /// Highest observed exclusive tail.
    #[must_use]
    pub fn tail(&self, stream: &StreamPath) -> Option<u64> {
        self.meta.get(stream).and_then(|meta| meta.tail)
    }

    /// Cached and passed-through keys in `range`.
    fn keys_in(&self, range: std::ops::RangeInclusive<Key>) -> Vec<Key> {
        self.entries
            .range(range.clone())
            .map(|(key, _)| key.clone())
            .chain(self.passed.range(range).map(|(key, _)| key.clone()))
            .collect()
    }

    fn meta_mut(&mut self, stream: &StreamPath) -> &mut Meta {
        if !self.meta.contains_key(stream) && self.meta.len() >= self.max_streams {
            // Active facts can exceed the residency target. Find an old
            // victim from its use index without scanning the active frame.
            let victim = self
                .meta_lru
                .first_key_value()
                .filter(|(used, _)| **used <= self.mark)
                .map(|(_, path)| path.clone());
            if let Some(victim) = victim {
                // Cached bytes must not outlive the anchor that identifies them.
                self.forget(&victim);
                self.remove_meta(&victim);
            }
        }
        self.clock += 1;
        let meta = self.meta.entry(stream.clone()).or_default();
        self.meta_lru.remove(&meta.used);
        meta.used = self.clock;
        self.meta_lru.insert(meta.used, stream.clone());
        meta
    }

    fn unlink_alias(&mut self, stream: &StreamPath, source: &StreamPath) {
        if let Some(children) = self.aliases.get_mut(source) {
            children.remove(stream);
            if children.is_empty() {
                self.aliases.remove(source);
            }
        }
    }

    fn remove_meta(&mut self, stream: &StreamPath) {
        if let Some(meta) = self.meta.remove(stream) {
            self.meta_lru.remove(&meta.used);
            if let Some((source, _)) = meta.fork {
                self.unlink_alias(stream, &source);
            }
        }
    }

    // A simple alias path cannot contain more cached links than cached facts.
    // Exhausting the actual graph proves a cycle; no legitimate depth is cut.
    fn walk<'a>(
        &'a self,
        stream: &'a StreamPath,
        sequence: u64,
    ) -> impl Iterator<Item = Result<(&'a StreamPath, Option<(&'a StreamPath, u64)>), ()>> + 'a
    {
        let mut path = Some(stream);
        let mut remaining = self.meta.len().saturating_add(1);
        std::iter::from_fn(move || {
            let current = path.take()?;
            if remaining == 0 {
                return Some(Err(()));
            }
            remaining -= 1;
            let fork = self
                .fork(current)
                .filter(|(_, at)| sequence < *at)
                .map(|(source, at)| (source, *at));
            path = fork.map(|(source, _)| source);
            Some(Ok((current, fork)))
        })
    }

    /// Mark a displayed stream's facts in use this frame.
    pub fn touch_stream(&mut self, stream: &StreamPath) {
        if let Some(meta) = self.meta.get_mut(stream) {
            self.meta_lru.remove(&meta.used);
            self.clock += 1;
            meta.used = self.clock;
            self.meta_lru.insert(meta.used, stream.clone());
        }
    }

    /// Resolve a record through fork aliases to the key that would hold it.
    pub(super) fn resolve(&self, stream: &StreamPath, sequence: u64) -> Key {
        let mut path = stream;
        for step in self.walk(stream, sequence) {
            match step {
                Ok((current, _)) => path = current,
                Err(()) => return Key::Record(stream.clone(), sequence),
            }
        }
        Key::Record(path.clone(), sequence)
    }

    /// Borrow a committed record without touching recency.
    #[must_use]
    pub fn record(&self, stream: &StreamPath, sequence: u64) -> Option<&Bytes> {
        match self.value(&self.resolve(stream, sequence)) {
            Some(Value::Record(value)) => Some(value),
            _ => None,
        }
    }

    /// A cached or passed-through value.
    fn value(&self, key: &Key) -> Option<&Value> {
        self.entries
            .get(key)
            .map(|entry| &entry.value)
            .or_else(|| self.passed.get(key).map(|(value, _)| value))
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
        match self.value(&Key::Listing(parent.clone(), index)) {
            Some(Value::Listing(listing)) => Some(listing),
            _ => None,
        }
    }

    /// Mark displayed entries most recently used.
    pub fn touch_record(&mut self, stream: &StreamPath, sequence: u64) {
        self.touch_range(stream, sequence, sequence.saturating_add(1));
    }

    /// Mark every cached record in `[from, to)` of `stream` displayed, gaps
    /// included, with the facts of each stream it resolves through: an
    /// inherited prefix is displayed through its fork sources, so their
    /// facts (and the records they own) are in use too. The walk visits only
    /// cached entries, so an unbounded window costs what is cached.
    pub fn touch_range(&mut self, stream: &StreamPath, from: u64, to: u64) {
        if from >= to {
            return;
        }
        let mut ranges = Vec::new();
        self.ranges(stream, from, to, |path, from, to| {
            ranges.push((path.clone(), from, to))
        });
        let mut keys = Vec::new();
        for (path, from, to) in ranges {
            self.touch_stream(&path);
            if from < to {
                keys.extend(
                    self.entries
                        .range(Key::Record(path.clone(), from)..Key::Record(path.clone(), to))
                        .map(|(key, _)| key.clone()),
                );
            }
        }
        for key in keys {
            self.touch(&key);
        }
    }

    /// Split a logical range into the source intervals that own its bytes.
    /// Validate the whole alias walk before exposing any alternate identity.
    pub(super) fn ranges(
        &self,
        stream: &StreamPath,
        from: u64,
        to: u64,
        mut visit: impl FnMut(&StreamPath, u64, u64),
    ) {
        if from >= to {
            return;
        }
        let walk: Result<Vec<_>, ()> = self.walk(stream, from).collect();
        let Ok(walk) = walk else {
            visit(stream, from, to);
            return;
        };
        let mut end = to;
        for (path, fork) in walk {
            let own = fork.map_or(from, |(_, at)| from.max(at));
            visit(path, own, end);
            if let Some((_, at)) = fork {
                end = end.min(at);
            }
        }
    }

    /// Retained listing pages of a parent, without scanning unrelated streams.
    pub fn listings<'a>(
        &'a self,
        parent: &'a StreamPath,
    ) -> impl Iterator<Item = (u32, &'a Listing)> + 'a {
        let range = Key::Listing(parent.clone(), 0)..=Key::Listing(parent.clone(), u32::MAX);
        self.entries
            .range(range.clone())
            .map(|(key, entry)| (key, &entry.value))
            .chain(
                self.passed
                    .range(range)
                    .map(|(key, (value, _))| (key, value)),
            )
            .filter_map(|(key, value)| match (key, value) {
                (Key::Listing(_, page), Value::Listing(listing)) => Some((*page, listing)),
                _ => None,
            })
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
    /// stored once, under its source. A live insert passes the frame mark as
    /// `protect`: entries used after it are displayed and never evicted for
    /// it, so nothing (prefetch included) can displace what is on screen. A
    /// live value that cannot be cached beside them, or exceeds the whole
    /// budget, is passed through instead (see [`Self::retain_passed`]).
    /// Without `protect` (a restore) such a value is dropped. Returns whether
    /// the value was cached.
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

    fn insert(&mut self, key: Key, mut value: Value, charge: usize, protect: Option<u64>) -> bool {
        self.unpass(&key);
        if charge <= self.max_bytes {
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
            if self.evict(protect) {
                return true;
            }
            let Some(entry) = self.entries.remove(&key) else {
                return false;
            };
            self.lru.remove(&entry.used);
            self.bytes -= charge;
            value = entry.value;
        }
        if protect.is_some() {
            self.passed_bytes += charge;
            self.passed.insert(key, (value, charge));
        }
        false
    }

    /// Drop every passed-through value no current want covers; called with
    /// each frame's plan, so pass-through memory is bounded by what the
    /// renderer asks for, not by history size.
    pub(super) fn retain_passed(&mut self, mut covered: impl FnMut(&Self, &Key) -> bool) {
        let this = &*self;
        let dropped: Vec<Key> = this
            .passed
            .keys()
            .filter(|key| !covered(this, key))
            .cloned()
            .collect();
        for key in dropped {
            self.unpass(&key);
        }
    }

    fn unpass(&mut self, key: &Key) {
        if let Some((_, charge)) = self.passed.remove(key) {
            self.passed_bytes -= charge;
        }
    }

    fn remove(&mut self, key: &Key) {
        if let Some(entry) = self.entries.remove(key) {
            self.lru.remove(&entry.used);
            self.bytes -= entry.charge;
        }
        self.unpass(key);
    }

    /// Evict least recently used entries until within budget. Returns false
    /// when only protected entries remain over budget. Only cached entries
    /// are charged, so evicting all of them always reaches the budget.
    fn evict(&mut self, protect: Option<u64>) -> bool {
        while self.bytes > self.max_bytes {
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

    /// Replace this cache from persisted bytes under its own budget. Restored
    /// listings are not fresh, so displayed ones revalidate. Parsing retains
    /// at most one bounded staging cache, including on malformed input.
    ///
    /// # Errors
    /// Refuses malformed input without retaining any part of it.
    pub fn restore(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        let mut restored = Self::new(self.max_bytes, self.max_streams);
        restored.clock = self.clock;
        restored.mark = self.mark;
        let mut input = Input(bytes);
        if input.take(MAGIC.len())? != MAGIC {
            return Err(SnapshotError);
        }
        let count = input.count()?;
        for _ in 0..count {
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
            if let Some(tail) = tail {
                restored.observe_tail(&path, tail);
            }
            if let Some((source, at)) = fork {
                restored.set_fork(&path, source, at);
            }
            if let Some((at, commit)) = anchor {
                restored.note_commit(&path, at, commit);
            }
            // Snapshot size does not govern residency: parse all facts and
            // let later ones evict cold facts under the current policy.
            restored.mark = restored.clock;
        }
        for _ in 0..input.count()? {
            match input.u8()? {
                0 => {
                    let path = input.path()?;
                    let sequence = input.u64()?;
                    let value = input.bytes()?;
                    // A retained byte must keep the fact identifying its history.
                    // Facts evicted while streaming a larger snapshot cannot
                    // regain stale bytes under a later, different anchor.
                    if value.len() <= self.max_bytes && restored.meta.contains_key(&path) {
                        restored.insert_record(
                            &path,
                            sequence,
                            Bytes::copy_from_slice(value),
                            None,
                        );
                    }
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
                    let count = input.count()?;
                    if count > acyclic_stream::MAX_ITEMS {
                        return Err(SnapshotError);
                    }
                    for _ in 0..count {
                        children.push(input.path()?);
                    }
                    restored.insert_listing(
                        &path,
                        index,
                        Listing {
                            after,
                            children,
                            next,
                            fresh: false,
                            witnessed: None,
                        },
                        None,
                    );
                }
                _ => return Err(SnapshotError),
            }
        }
        if !input.0.is_empty() {
            return Err(SnapshotError);
        }
        *self = restored;
        Ok(())
    }
}

fn path_len(key: &Key) -> usize {
    match key {
        Key::Record(path, _) | Key::Listing(path, _) => path.as_str().len(),
    }
}

const MAGIC: &[u8] = b"acyclic.records.v1\n";

/// Snapshot input is malformed, or the reader is active.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotError;

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("record cache snapshot rejected: invalid input or active reader")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deep_fork_prefixes_restore_and_revoke_every_descendant() {
        let path = |index| StreamPath::new(format!("test/fork-{index}")).unwrap();
        let mut cache = RecordCache::new(64 * 1024, 130);
        cache.observe_tail(&path(0), 1);
        cache.insert_record(&path(0), 0, Bytes::from_static(b"inherited"), None);
        for index in 1..130 {
            cache.set_fork(&path(index), path(index - 1), 1);
            cache.insert_record(&path(index), 1, Bytes::from_static(b"own"), None);
        }
        assert_eq!(cache.record(&path(129), 0).unwrap().as_ref(), b"inherited");
        cache.touch_range(&path(129), 0, 2);
        let mut restored = RecordCache::new(64 * 1024, 130);
        restored.restore(&cache.snapshot()).unwrap();
        assert_eq!(
            restored.record(&path(129), 0).unwrap().as_ref(),
            b"inherited"
        );
        restored.remove_stream(&path(0));
        assert_eq!(restored.streams(), 0);
        assert!(restored.aliases.is_empty());
        for index in 0..130 {
            assert!(restored.record(&path(index), 0).is_none());
            assert!(restored.record(&path(index), 1).is_none());
        }
    }

    #[test]
    fn changed_aliases_unlink_and_corrupt_cycles_never_resolve_to_another_stream() {
        let path = |name| StreamPath::new(format!("test/{name}")).unwrap();
        let mut cache = RecordCache::new(4096, 4);
        cache.set_fork(&path("child"), path("old"), 1);
        cache.set_fork(&path("child"), path("new"), 1);
        cache.remove_stream(&path("old"));
        assert!(cache.fork(&path("child")).is_some());
        cache.set_fork(&path("new"), path("child"), 1);
        assert_eq!(
            cache.resolve(&path("child"), 0),
            Key::Record(path("child"), 0)
        );
        cache.touch_range(&path("child"), 0, 1);
        cache.remove_stream(&path("new"));
        assert!(cache.fork(&path("child")).is_none());
        assert!(cache.aliases.is_empty());
    }

    #[test]
    fn metadata_eviction_cannot_leave_old_bytes_under_a_new_anchor() {
        let stream = StreamPath::new("test/source").unwrap();
        let other = StreamPath::new("test/other").unwrap();
        let mut cache = RecordCache::new(16 * 1024, 1);
        cache.note_commit(&stream, 0, CommitId::from_bytes([1; 32]));
        cache.insert_record(&stream, 0, Bytes::from_static(b"old"), None);
        cache.begin_frame();
        cache.observe_tail(&other, 0);
        assert!(cache.record(&stream, 0).is_none());
        cache.note_commit(&stream, 0, CommitId::from_bytes([2; 32]));
        cache.insert_record(&stream, 0, Bytes::from_static(b"new"), None);
        assert_eq!(cache.record(&stream, 0).unwrap().as_ref(), b"new");
        assert_eq!(
            cache.anchor(&stream, 0),
            Some(CommitId::from_bytes([2; 32]))
        );
    }

    /// Facts for 4,096 streams cost nothing against a 64 KiB record budget,
    /// so the cache still refills after shrinking. Control: when facts were
    /// charged to the budget (393,216 bytes) every insert was evicted at once.
    #[test]
    fn stream_facts_never_starve_the_record_budget() {
        let mut cache = RecordCache::new(8 << 20, 4096);
        for index in 0..4096 {
            let stream = StreamPath::new(format!("test/s{index}")).unwrap();
            cache.observe_tail(&stream, 1);
        }
        cache.set_budget(64 * 1024);
        let stream = StreamPath::new("test/s0").unwrap();
        assert!(cache.insert_record(&stream, 0, Bytes::from_static(b"row"), None));
        assert_eq!(cache.record(&stream, 0).unwrap().as_ref(), b"row");
        assert!(cache.bytes() <= 64 * 1024);
        assert_eq!(cache.streams(), 4096);
    }

    /// Facts the current frame uses are never eviction victims, so a frame
    /// that needs more than `max_streams` keeps them all; facts from earlier
    /// frames are reclaimed before the count grows.
    #[test]
    fn facts_in_use_this_frame_are_retained_and_cold_ones_reclaimed() {
        let path = |index: usize| StreamPath::new(format!("test/s{index}")).unwrap();
        let mut cache = RecordCache::new(64 * 1024, 2);
        cache.begin_frame();
        for index in 0..4 {
            cache.observe_tail(&path(index), 1);
        }
        assert_eq!(cache.streams(), 4);
        assert!((0..4).all(|index| cache.tail(&path(index)) == Some(1)));
        cache.begin_frame();
        cache.touch_stream(&path(3));
        cache.observe_tail(&path(4), 1);
        assert_eq!(cache.streams(), 4);
        assert_eq!(cache.tail(&path(3)), Some(1));
        assert_eq!(cache.tail(&path(4)), Some(1));
    }

    /// A displayed window marks cached records past a gap and the facts of
    /// the fork source it reads through. Control: stopping at the first gap
    /// left record 3 evictable, and touching only the child's facts let a new
    /// stream evict the source, forgetting the inherited record on screen.
    #[test]
    fn displayed_ranges_mark_records_past_gaps_and_fork_sources() {
        let source = StreamPath::new("test/source").unwrap();
        let child = StreamPath::new("test/child").unwrap();
        let mut cache = RecordCache::new(64 * 1024, 2);
        cache.observe_tail(&source, 2);
        cache.set_fork(&child, source.clone(), 2);
        cache.insert_record(&source, 0, Bytes::from_static(b"a"), None);
        cache.insert_record(&child, 3, Bytes::from_static(b"c"), None);
        let mark = cache.begin_frame();
        cache.touch_range(&child, 0, 4);
        let large = StreamPath::new("test/z").unwrap();
        let cached =
            cache.insert_record(&large, 0, Bytes::from(vec![0; 64 * 1024 - 300]), Some(mark));
        assert!(!cached);
        assert!(cache.record(&large, 0).is_some());
        cache.observe_tail(&StreamPath::new("test/other").unwrap(), 1);
        assert_eq!(cache.record(&child, 0).unwrap().as_ref(), b"a");
        assert_eq!(cache.record(&child, 3).unwrap().as_ref(), b"c");
    }

    #[test]
    fn a_larger_snapshot_streams_into_smaller_residency_without_reviving_evicted_histories() {
        let path = |index| StreamPath::new(format!("test/s{index}")).unwrap();
        let mut source = RecordCache::new(1 << 20, 100);
        for index in 0..100 {
            source.note_commit(&path(index), 0, CommitId::from_bytes([1; 32]));
            source.insert_record(&path(index), 0, Bytes::from_static(b"old"), None);
        }
        let mut target = RecordCache::new(256, 1);
        target.restore(&source.snapshot()).unwrap();
        assert!(target.bytes() <= 256);
        assert_eq!(target.streams(), 1);
        let retained = target.meta.keys().next().unwrap().clone();
        let evicted = (0..100).map(path).find(|path| *path != retained).unwrap();
        assert!(target.record(&evicted, 0).is_none());
        target.note_commit(&evicted, 0, CommitId::from_bytes([2; 32]));
        target.insert_record(&evicted, 0, Bytes::from_static(b"new"), None);
        assert_eq!(target.record(&evicted, 0).unwrap().as_ref(), b"new");
    }

    #[test]
    fn restore_is_bounded_and_replaces_the_previous_snapshot_atomically() {
        let stream = StreamPath::new("test/history").unwrap();
        let obsolete = StreamPath::new("test/obsolete").unwrap();
        let mut source = RecordCache::new(64 * 1024, 16);
        source.note_commit(&stream, 99, CommitId::from_bytes([7; 32]));
        for sequence in 0..100 {
            source.insert_record(&stream, sequence, Bytes::from(vec![7; 64]), None);
        }
        let mut target = RecordCache::new(4096, 16);
        target.insert_record(&obsolete, 0, Bytes::from_static(b"old"), None);
        let before = target.snapshot();
        let mut malformed = source.snapshot();
        malformed.push(255);
        assert!(target.restore(&malformed).is_err());
        assert_eq!(target.snapshot(), before);
        assert!(target.restore(&vec![0; 8193]).is_err());
        assert_eq!(target.snapshot(), before);
        target.restore(&source.snapshot()).unwrap();
        assert!(target.bytes() <= 4096);
        assert!(target.record(&obsolete, 0).is_none());
        assert_eq!(target.record(&stream, 99).unwrap().as_ref(), &[7; 64]);
    }
}
