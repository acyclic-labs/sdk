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

use super::backing::{BackingPool, RecordBytes};
use acyclic_stream::OwnedBytes;
use acyclic_stream::{CommitId, ForkOrigin, OwnedBytesBacking, StreamPath};
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
    Record(RecordBytes),
    Listing(Listing),
}

impl Value {
    fn encoded_len(&self, key: &Key) -> usize {
        match self {
            Self::Record(value) => 25usize
                .saturating_add(path_len(key))
                .saturating_add(value.len()),
            Self::Listing(listing) => listing
                .children
                .iter()
                .fold(41usize.saturating_add(path_len(key)), |bytes, child| {
                    bytes.saturating_add(8).saturating_add(child.as_str().len())
                })
                .saturating_add(listing.after.as_ref().map_or(0, |path| path.as_str().len()))
                .saturating_add(listing.next.as_ref().map_or(0, |path| path.as_str().len())),
        }
    }
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
    /// Exact immediate inherited cut, including its publication identity.
    fork: Option<ForkOrigin>,
    /// A primitive Head or children page observed this path's origin.
    origin_known: bool,
    /// Commit of the highest cached record, checked when a follow resumes.
    anchor: Option<(u64, CommitId)>,
    /// Cached prefix to validate before a newer Head advances its anchor.
    resume: Option<(u64, CommitId)>,
    /// Local publication order, distinct from display recency. An older
    /// in-flight Head cannot replace facts learned after it was requested.
    observed: u64,
    /// Only primitive origin observations advance this stamp. Ordinary
    /// record reads cannot make a changed fork publication look stale.
    origin_observed: u64,
    /// Use clock of the last learn or display; facts used since the frame
    /// mark are never chosen as eviction victims.
    used: u64,
}

/// Bounded immutable record cache shared by every renderer.
pub struct RecordCache {
    pub(super) access: super::Access,
    entries: BTreeMap<Key, Entry>,
    lru: BTreeMap<u64, Key>,
    meta: BTreeMap<StreamPath, Meta>,
    /// Values served uncached, with their charge, while a want covers them.
    passed: BTreeMap<Key, (Value, usize)>,
    passed_bytes: usize,
    bytes: usize,
    /// Payload capacity present in cached entries, charged once per backing.
    cached_payload: usize,
    resident: BTreeMap<OwnedBytesBacking, usize>,
    backings: BackingPool,
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
            access: super::Access::default(),
            entries: BTreeMap::new(),
            lru: BTreeMap::new(),
            meta: BTreeMap::new(),
            passed: BTreeMap::new(),
            passed_bytes: 0,
            bytes: 0,
            cached_payload: 0,
            resident: BTreeMap::new(),
            backings: BackingPool::default(),
            clock: 0,
            mark: 0,
            max_bytes,
            max_streams,
        }
    }

    pub(super) fn reset_access(&mut self, access: super::Access) {
        let mut empty = Self::new(self.max_bytes, self.max_streams);
        empty.backings = self.backings.clone();
        empty.clock = self.clock;
        empty.mark = self.mark;
        empty.access = access;
        *self = empty;
    }

    /// Cached charge under the byte budget: keys, values and bookkeeping of
    /// every cached record and listing page. Never above the budget.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Uncached charges: pass-through bookkeeping plus payload held only by
    /// pass-through records or cloned views. Shared payload is counted once.
    #[must_use]
    pub fn passed_bytes(&self) -> usize {
        self.passed_bytes + self.backings.bytes() - self.cached_payload
    }

    /// Live payload capacity across cached records, pass-throughs and cloned
    /// views. Eviction cannot reclaim a payload while a renderer retains it.
    /// This excludes allocator and ownership headers; it is not an RSS measure.
    #[must_use]
    pub fn backing_bytes(&self) -> usize {
        self.backings.bytes()
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

    /// Learn the exact primitive origin. A changed publication invalidates
    /// records and dependent aliases, even when source and offset match.
    pub fn observe_origin(&mut self, stream: &StreamPath, origin: Option<ForkOrigin>) {
        if self.origin_changed(stream, &origin) {
            self.remove_stream(stream, true);
        }
        let meta = self.meta_mut(stream);
        meta.fork = origin;
        meta.origin_known = true;
        meta.origin_observed = meta.observed;
    }

    pub(super) fn origin_changed(&self, stream: &StreamPath, origin: &Option<ForkOrigin>) -> bool {
        self.meta
            .get(stream)
            .is_some_and(|meta| (meta.origin_known || meta.fork.is_some()) && &meta.fork != origin)
    }

    /// Whether a primitive observation has established this path's origin.
    #[must_use]
    pub fn origin_known(&self, stream: &StreamPath) -> bool {
        self.access.allows() && self.meta.get(stream).is_some_and(|meta| meta.origin_known)
    }

    pub(super) fn observation(&self, stream: &StreamPath) -> Option<(u64, u64)> {
        self.meta
            .get(stream)
            .map(|meta| (meta.observed, meta.origin_observed))
    }

    /// The fork origin, if learned.
    #[must_use]
    pub fn fork(&self, stream: &StreamPath) -> Option<&ForkOrigin> {
        if !self.access.allows() {
            return None;
        }
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
        if !self.access.allows() {
            return None;
        }
        self.meta
            .get(stream)
            .and_then(|meta| {
                meta.resume
                    .filter(|(at, _)| *at == sequence)
                    .or_else(|| meta.anchor.filter(|(at, _)| *at == sequence))
            })
            .map(|(_, commit)| commit)
    }

    pub(super) fn resume_from(&self, stream: &StreamPath) -> Option<u64> {
        self.meta.get(stream)?.resume?.0.checked_add(1)
    }

    pub(super) fn before_head(&mut self, stream: &StreamPath, tail: u64) {
        let meta = self.meta_mut(stream);
        if meta.resume.is_none()
            && let Some(anchor) = meta.anchor
            && anchor.0.checked_add(1).is_some_and(|from| from < tail)
        {
            meta.resume = Some(anchor);
        }
    }

    pub(super) fn resumed(&mut self, stream: &StreamPath, from: u64) {
        if let Some(meta) = self.meta.get_mut(stream)
            && meta
                .resume
                .is_some_and(|(at, _)| at.checked_add(1) == Some(from))
        {
            meta.resume = None;
        }
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
            self.clock += 1;
            meta.observed = self.clock;
            meta.tail = None;
            meta.anchor = None;
            meta.resume = None;
            meta.origin_known = false;
        }
    }

    /// A denial or lost prefix cannot retain an alias into another history.
    pub(super) fn remove_stream(
        &mut self,
        stream: &StreamPath,
        inherited: bool,
    ) -> BTreeSet<StreamPath> {
        let mut pending = vec![stream.clone()];
        if inherited {
            let mut path = stream;
            let mut seen = BTreeSet::from([stream.clone()]);
            while let Some(origin) = self.fork(path) {
                if origin.forked_at == 0 || !seen.insert(origin.source.clone()) {
                    break;
                }
                pending.push(origin.source.clone());
                path = &origin.source;
            }
        }
        let mut dependents = BTreeMap::<_, Vec<_>>::new();
        for (path, meta) in &self.meta {
            if let Some(origin) = &meta.fork {
                dependents.entry(&origin.source).or_default().push(path);
            }
        }
        let mut affected = BTreeSet::new();
        while let Some(path) = pending.pop() {
            if !affected.insert(path.clone()) {
                continue;
            }
            if let Some(children) = dependents.get(&path) {
                pending.extend(children.iter().map(|child| (*child).clone()));
            }
        }
        for path in &affected {
            self.forget(path);
            self.meta.remove(path);
        }
        let listings =
            self.keys_in(Key::Listing(stream.clone(), 0)..=Key::Listing(stream.clone(), u32::MAX));
        for key in listings {
            self.remove(&key);
        }
        affected
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

    pub(super) fn clear_listings(&mut self) {
        let keys: Vec<_> = self
            .entries
            .keys()
            .chain(self.passed.keys())
            .filter(|key| matches!(key, Key::Listing(..)))
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }

    /// Highest observed exclusive tail.
    #[must_use]
    pub fn tail(&self, stream: &StreamPath) -> Option<u64> {
        if !self.access.allows() {
            return None;
        }
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
            // Facts are re-learnable from history; drop one this frame does
            // not use, without a fork first, so aliasing (which saves the
            // most bytes) survives longest. When every retained fact is in
            // use the frame needs them all: retain it rather than thrash.
            let mark = self.mark;
            let victim = self
                .meta
                .iter()
                .find(|(_, meta)| meta.used <= mark && meta.fork.is_none())
                .or_else(|| self.meta.iter().find(|(_, meta)| meta.used <= mark))
                .map(|(path, _)| path.clone());
            if let Some(victim) = victim {
                // Cached bytes must not outlive the anchor that identifies them.
                self.forget(&victim);
                self.meta.remove(&victim);
            }
        }
        self.clock += 1;
        let meta = self.meta.entry(stream.clone()).or_default();
        meta.used = self.clock;
        meta.observed = self.clock;
        meta
    }

    /// Mark a displayed stream's facts in use this frame.
    pub fn touch_stream(&mut self, stream: &StreamPath) {
        if let Some(meta) = self.meta.get_mut(stream) {
            self.clock += 1;
            meta.used = self.clock;
        }
    }

    /// Resolve a record through fork aliases to the key that would hold it.
    pub(super) fn resolve(&self, stream: &StreamPath, sequence: u64) -> Key {
        let mut path = stream;
        // An acyclic chain has at most one edge per retained fact. This also
        // terminates on malformed cyclic input without allocating per lookup.
        for _ in 0..self.meta.len() {
            match self.meta.get(path).and_then(|meta| meta.fork.as_ref()) {
                Some(origin) if sequence < origin.forked_at => path = &origin.source,
                _ => break,
            }
        }
        Key::Record(path.clone(), sequence)
    }

    /// Borrow a committed record without touching recency.
    #[must_use]
    pub fn record(&self, stream: &StreamPath, sequence: u64) -> Option<&RecordBytes> {
        match self.value(&self.resolve(stream, sequence)) {
            Some(Value::Record(value)) => Some(value),
            _ => None,
        }
    }

    /// A cached or passed-through value.
    fn value(&self, key: &Key) -> Option<&Value> {
        if !self.access.allows() {
            return None;
        }
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

    /// Retained pages of one parent, including displayed pass-through pages.
    /// Work is proportional to cached pages, never the requested page index.
    pub fn listings<'a>(
        &'a self,
        parent: &StreamPath,
    ) -> impl Iterator<Item = (u32, &'a Listing)> + use<'a> {
        let range = Key::Listing(parent.clone(), 0)..=Key::Listing(parent.clone(), u32::MAX);
        self.entries
            .range(range.clone())
            .map(|(key, entry)| (key, &entry.value))
            .chain(
                self.passed
                    .range(range)
                    .map(|(key, (value, _))| (key, value)),
            )
            .filter(|_| self.access.allows())
            .filter_map(|(key, value)| match (key, value) {
                (Key::Listing(_, index), Value::Listing(listing)) => Some((*index, listing)),
                _ => None,
            })
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
        let mut keys = Vec::new();
        let mut path = stream.clone();
        let mut end = to;
        for _ in 0..=self.meta.len() {
            self.touch_stream(&path);
            let fork = self.fork(&path).cloned();
            let own = fork
                .as_ref()
                .map_or(from, |origin| from.max(origin.forked_at));
            if own < end {
                keys.extend(
                    self.entries
                        .range(Key::Record(path.clone(), own)..Key::Record(path.clone(), end))
                        .map(|(key, _)| key.clone()),
                );
            }
            match fork {
                Some(origin) if from < origin.forked_at && from < end => {
                    end = end.min(origin.forked_at);
                    path = origin.source;
                }
                _ => break,
            }
        }
        for key in keys {
            self.touch(&key);
        }
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
        value: OwnedBytes,
        protect: Option<u64>,
    ) -> bool {
        let key = self.resolve(stream, sequence);
        if self.entries.contains_key(&key) {
            // Immutable: an existing entry is already this record.
            self.touch(&key);
            return true;
        }
        let value = self.backings.retain(value, self.max_bytes);
        let charge = path_len(&key) + ENTRY_OVERHEAD;
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
            self.retain_backing(&value);
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
            self.release_backing(&entry.value);
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
            self.release_backing(&entry.value);
        }
        self.unpass(key);
    }

    fn retain_backing(&mut self, value: &Value) {
        if let Value::Record(value) = value {
            let backing = value.backing();
            let count = self.resident.entry(backing.clone()).or_default();
            if *count == 0 {
                self.bytes += backing.capacity();
                self.cached_payload += backing.capacity();
            }
            *count += 1;
        }
    }

    fn release_backing(&mut self, value: &Value) {
        if let Value::Record(value) = value {
            let backing = value.backing();
            let count = self
                .resident
                .get_mut(backing)
                .expect("cached records retain their backing");
            *count -= 1;
            if *count == 0 {
                self.bytes -= backing.capacity();
                self.cached_payload -= backing.capacity();
                self.resident.remove(backing);
            }
        }
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

    /// Persist recent entries, most recently used last, as length-prefixed
    /// bytes. The entry section obeys the restore bound independently of
    /// allocation sharing; an older entry is omitted when it cannot fit.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        // Unbound local snapshots cannot establish a remote source partition.
        if !self.access.local() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.bytes);
        out.extend_from_slice(MAGIC);
        put_u64(&mut out, self.meta.len() as u64);
        for (path, meta) in &self.meta {
            put_str(&mut out, path.as_str());
            put_u64(&mut out, meta.tail.map_or(0, |tail| tail + 1));
            out.push(u8::from(meta.origin_known));
            match &meta.fork {
                Some(origin) => {
                    out.push(1);
                    put_str(&mut out, origin.source.as_str());
                    put_u64(&mut out, origin.forked_at);
                    out.extend_from_slice(origin.commit_id.as_bytes());
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
            match &meta.resume {
                Some((at, commit)) => {
                    out.push(1);
                    put_u64(&mut out, *at);
                    out.extend_from_slice(commit.as_bytes());
                }
                None => out.push(0),
            }
        }
        let mut remaining = self.max_bytes.saturating_mul(2);
        let mut selected = Vec::new();
        for key in self.lru.values().rev() {
            if let Some(entry) = self.entries.get(key) {
                let encoded = entry.value.encoded_len(key);
                if encoded <= remaining {
                    remaining -= encoded;
                    selected.push(key);
                }
            }
        }
        put_u64(&mut out, selected.len() as u64);
        for key in selected.into_iter().rev() {
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
        if !self.access.local() {
            return Err(SnapshotError);
        }
        let mut restored = Self::new(self.max_bytes, self.max_streams);
        // Views from the old cache remain charged through an atomic restore.
        restored.backings = self.backings.clone();
        restored.clock = self.clock;
        restored.mark = self.mark;
        let mut input = Input(bytes);
        if input.take(MAGIC.len())? != MAGIC {
            return Err(SnapshotError);
        }
        let count = input.count()?;
        if count > self.max_streams {
            return Err(SnapshotError);
        }
        for _ in 0..count {
            let path = input.path()?;
            let tail = input.u64()?.checked_sub(1);
            let origin_known = match input.u8()? {
                0 => false,
                1 => true,
                _ => return Err(SnapshotError),
            };
            let fork = match input.u8()? {
                0 => None,
                1 => Some(ForkOrigin {
                    source: input.path()?,
                    forked_at: input.u64()?,
                    commit_id: CommitId::from_bytes(
                        input.take(32)?.try_into().map_err(|_| SnapshotError)?,
                    ),
                }),
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
            let resume = match input.u8()? {
                0 => None,
                1 => Some((
                    input.u64()?,
                    CommitId::from_bytes(input.take(32)?.try_into().map_err(|_| SnapshotError)?),
                )),
                _ => return Err(SnapshotError),
            };
            if let Some(tail) = tail {
                restored.observe_tail(&path, tail);
            }
            if fork.as_ref().is_some_and(|origin| origin.source == path) {
                return Err(SnapshotError);
            }
            if origin_known {
                restored.observe_origin(&path, fork);
            }
            if let Some((at, commit)) = anchor {
                restored.note_commit(&path, at, commit);
            }
            restored.meta_mut(&path).resume = resume;
        }
        // Facts are bounded by count above; bound the parsing work of the
        // entry section by the byte budget. Encoding overhead may exceed
        // retained payload bytes for small entries.
        let entries = input.count()?;
        if input.0.len() > self.max_bytes.saturating_mul(2) {
            return Err(SnapshotError);
        }
        for _ in 0..entries {
            match input.u8()? {
                0 => {
                    let path = input.path()?;
                    let sequence = input.u64()?;
                    let value = input.bytes()?;
                    if value.len() <= self.max_bytes {
                        restored.insert_record(
                            &path,
                            sequence,
                            OwnedBytes::copy_from_slice(value),
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

/// Snapshot input exceeds admission bounds, is malformed, or the reader is active.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotError;

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(
            "record cache snapshot rejected: invalid input, exceeded budget, or active reader",
        )
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
    fn shared_payload_stays_charged_until_the_last_view_retires() {
        let stream = StreamPath::new("source").unwrap();
        let child = StreamPath::new("child").unwrap();
        let root = OwnedBytes::from(vec![7; 4096]);
        let first = root.slice(5..9);
        let pointer = first.as_slice().as_ptr();
        let mut cache = RecordCache::new(8192, 16);
        assert!(cache.insert_record(&stream, 0, first, None));
        assert!(cache.insert_record(&stream, 1, root.slice(20..24), None));
        cache.observe_origin(
            &child,
            Some(ForkOrigin {
                source: stream.clone(),
                forked_at: 2,
                commit_id: CommitId::from_bytes([1; 32]),
            }),
        );
        let view = cache.record(&child, 0).unwrap().clone();
        assert_eq!(view.as_ptr(), pointer);
        assert_eq!(cache.backing_bytes(), 4096);
        assert_eq!(
            cache.bytes(),
            4096 + 2 * (ENTRY_OVERHEAD + stream.as_str().len())
        );
        cache.set_budget(0);
        assert_eq!(cache.bytes(), 0);
        assert_eq!(cache.passed_bytes(), 4096);
        let empty = RecordCache::new(0, 16).snapshot();
        cache.restore(&empty).unwrap();
        assert_eq!(view.as_ref(), &[7; 4]);
        assert_eq!(cache.backing_bytes(), 4096);
        drop(view);
        assert_eq!(cache.backing_bytes(), 0);
    }

    #[test]
    fn tiny_slices_detach_unknown_or_over_budget_transport_backing() {
        let stream = StreamPath::new("record").unwrap();
        let root = OwnedBytes::from(vec![3; 1 << 20]);
        let unknown = OwnedBytes::from(bytes::Bytes::from(vec![3; 1 << 20]).slice(11..15));
        for value in [root.slice(11..15), unknown] {
            let pointer = value.as_slice().as_ptr();
            let mut cache = RecordCache::new(4096, 16);
            assert!(cache.insert_record(&stream, 0, value, None));
            let view = cache.record(&stream, 0).unwrap();
            assert_ne!(view.as_ptr(), pointer);
            assert_eq!(view.as_ref(), &[3; 4]);
            assert_eq!(cache.backing_bytes(), 4);
        }
        let full = OwnedBytes::from(vec![5; 8192]);
        let pointer = full.as_slice().as_ptr();
        let mut cache = RecordCache::new(4096, 16);
        assert!(!cache.insert_record(&stream, 0, full, Some(0)));
        assert_eq!(cache.record(&stream, 0).unwrap().as_ptr(), pointer);
        assert_eq!(cache.backing_bytes(), 8192);
        cache.retain_passed(|_, _| false);
        assert_eq!(cache.backing_bytes(), 0);

        // After memory pressure, a new tiny record must not prolong an old
        // giant view's allocation once that view leaves the viewport.
        let mut cache = RecordCache::new(2 << 20, 16);
        assert!(cache.insert_record(&stream, 0, root.clone(), None));
        let old_view = cache.record(&stream, 0).unwrap().clone();
        cache.set_budget(4096);
        assert!(cache.insert_record(&stream, 1, root.slice(11..15), None));
        assert_eq!(cache.backing_bytes(), (1 << 20) + 4);
        drop(old_view);
        assert_eq!(cache.backing_bytes(), 4);
    }

    #[test]
    fn snapshots_bound_encoded_payload_independently_of_shared_capacity() {
        let stream = StreamPath::new("shared").unwrap();
        let root = OwnedBytes::from(vec![9; 4096]);
        let mut cache = RecordCache::new(8192, 16);
        for sequence in 0..32 {
            assert!(cache.insert_record(&stream, sequence, root.clone(), None));
        }
        assert_eq!(cache.backing_bytes(), 4096);
        let bytes = cache.snapshot();
        assert!(bytes.len() <= MAGIC.len() + 16 + 2 * 8192);
        let mut restored = RecordCache::new(8192, 16);
        restored.restore(&bytes).unwrap();
        assert!(restored.record(&stream, 31).is_some());
        assert!(restored.bytes() <= 8192);
    }

    /// Publication identity is part of an alias; the same source and offset
    /// under a different fork must not retain either private bytes or aliases.
    #[test]
    fn changed_fork_publication_discards_dependent_history_and_round_trips() {
        let source = StreamPath::new("source").unwrap();
        let child = StreamPath::new("child").unwrap();
        let grandchild = StreamPath::new("grandchild").unwrap();
        let mut cache = RecordCache::new(64 * 1024, 128);
        let origin = ForkOrigin {
            source: source.clone(),
            forked_at: 0,
            commit_id: CommitId::from_bytes([1; 32]),
        };
        cache.observe_origin(&child, Some(origin.clone()));
        cache.insert_record(&child, 0, OwnedBytes::from_static(b"private"), None);
        cache.observe_origin(
            &grandchild,
            Some(ForkOrigin {
                source: child.clone(),
                forked_at: 1,
                commit_id: CommitId::from_bytes([2; 32]),
            }),
        );
        assert_eq!(cache.record(&grandchild, 0).unwrap().as_ref(), b"private");
        let mut restored = RecordCache::new(64 * 1024, 128);
        restored.restore(&cache.snapshot()).unwrap();
        assert_eq!(restored.fork(&child), Some(&origin));
        cache.observe_origin(
            &child,
            Some(ForkOrigin {
                commit_id: CommitId::from_bytes([3; 32]),
                ..origin
            }),
        );
        assert!(cache.record(&child, 0).is_none());
        assert!(cache.record(&grandchild, 0).is_none());
        assert!(!cache.origin_known(&grandchild));
    }

    #[test]
    fn inherited_cache_resolution_has_no_depth_cutoff() {
        let mut cache = RecordCache::new(64 * 1024, 256);
        let mut source = StreamPath::new("root").unwrap();
        cache.insert_record(&source, 0, OwnedBytes::from_static(b"retained"), None);
        for index in 0..100 {
            let child = StreamPath::new(format!("fork{index}")).unwrap();
            cache.observe_origin(
                &child,
                Some(ForkOrigin {
                    source,
                    forked_at: 1,
                    commit_id: CommitId::from_bytes([1; 32]),
                }),
            );
            source = child;
        }
        assert_eq!(cache.record(&source, 0).unwrap().as_ref(), b"retained");
    }

    #[test]
    fn metadata_eviction_cannot_leave_old_bytes_under_a_new_anchor() {
        let stream = StreamPath::new("test/source").unwrap();
        let other = StreamPath::new("test/other").unwrap();
        let mut cache = RecordCache::new(16 * 1024, 1);
        cache.note_commit(&stream, 0, CommitId::from_bytes([1; 32]));
        cache.insert_record(&stream, 0, OwnedBytes::from_static(b"old"), None);
        cache.begin_frame();
        cache.observe_tail(&other, 0);
        assert!(cache.record(&stream, 0).is_none());
        cache.note_commit(&stream, 0, CommitId::from_bytes([2; 32]));
        cache.insert_record(&stream, 0, OwnedBytes::from_static(b"new"), None);
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
        assert!(cache.insert_record(&stream, 0, OwnedBytes::from_static(b"row"), None));
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
        cache.observe_origin(
            &child,
            Some(ForkOrigin {
                source: source.clone(),
                forked_at: 2,
                commit_id: CommitId::from_bytes([1; 32]),
            }),
        );
        cache.insert_record(&source, 0, OwnedBytes::from_static(b"a"), None);
        cache.insert_record(&child, 3, OwnedBytes::from_static(b"c"), None);
        let mark = cache.begin_frame();
        cache.touch_range(&child, 0, 4);
        let large = StreamPath::new("test/z").unwrap();
        let cached = cache.insert_record(
            &large,
            0,
            OwnedBytes::from(vec![0; 64 * 1024 - 300]),
            Some(mark),
        );
        assert!(!cached);
        assert!(cache.record(&large, 0).is_some());
        cache.observe_tail(&StreamPath::new("test/other").unwrap(), 1);
        assert_eq!(cache.record(&child, 0).unwrap().as_ref(), b"a");
        assert_eq!(cache.record(&child, 3).unwrap().as_ref(), b"c");
    }

    #[test]
    fn restore_is_bounded_and_replaces_the_previous_snapshot_atomically() {
        let stream = StreamPath::new("test/history").unwrap();
        let obsolete = StreamPath::new("test/obsolete").unwrap();
        let mut source = RecordCache::new(64 * 1024, 16);
        for sequence in 0..40 {
            source.insert_record(&stream, sequence, OwnedBytes::from(vec![7; 64]), None);
        }
        let mut target = RecordCache::new(4096, 16);
        target.insert_record(&obsolete, 0, OwnedBytes::from_static(b"old"), None);
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
        assert_eq!(target.record(&stream, 39).unwrap().as_ref(), &[7; 64]);
    }
}
