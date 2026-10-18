//! Metadata-only display window: one insert, remove and eviction path, no payload custody.
use super::super::{Change, FileKey, FileValue};
use std::collections::{BTreeMap, BTreeSet};

/// Display kinds 0..=7 are bounded separately; the last slot holds unclassified files.
const SLOTS: usize = 9;
const UNCLASSIFIED: usize = SLOTS - 1;

struct Entry {
    order: u64,
    slot: usize,
    bytes: usize,
    /// Change that inserted this entry; older entries were visible before the change.
    generation: u64,
}

/// Retained identities ordered by (order, key) within each kind, plus their total size.
pub(super) struct Window {
    entries: BTreeMap<FileKey, Entry>,
    ordered: [BTreeSet<(u64, FileKey)>; SLOTS],
    bytes: usize,
    generation: u64,
    maximum_files: usize,
    maximum_bytes: usize,
    recent: usize,
}

impl Window {
    /// Selects the snapshot's window in place; later duplicates replace earlier ones.
    pub(super) fn new(
        files: &mut Vec<FileValue>,
        maximum_files: usize,
        maximum_bytes: usize,
        recent: usize,
    ) -> Self {
        let mut window = Self {
            entries: BTreeMap::new(),
            ordered: Default::default(),
            bytes: 0,
            generation: 0,
            maximum_files,
            maximum_bytes,
            recent,
        };
        let mut evicted = Vec::new();
        for (order, file) in files.iter().enumerate() {
            if file.bytes.len() <= maximum_bytes {
                window.put(file, order as u64, &mut evicted);
            }
        }
        window.trim(&mut evicted);
        let mut order = 0;
        files.retain(|file| {
            order += 1;
            window
                .entries
                .get(&file.key)
                .is_some_and(|entry| entry.order == order - 1)
        });
        window
    }

    /// Folds one atomic change, keeping only retained files and reporting, in key
    /// order, every key visible before the change and absent after it.
    pub(super) fn apply(&mut self, change: &mut Change) {
        self.generation += 1;
        let mut removed = std::mem::take(&mut change.removed);
        removed.retain(|key| {
            self.take(key)
                .is_some_and(|(_, generation)| generation < self.generation)
        });
        for file in change.file.iter().chain(&change.files) {
            self.put(file, change.cursor.position, &mut removed);
        }
        self.trim(&mut removed);
        removed.retain(|key| !self.entries.contains_key(key));
        removed.sort_unstable();
        change.removed = removed;
        change
            .files
            .retain(|file| self.entries.contains_key(&file.key));
        if change
            .file
            .as_ref()
            .is_some_and(|file| !self.entries.contains_key(&file.key))
        {
            change.file = None;
        }
    }

    /// Replaces any entry for the file's key; an oversized file only removes it.
    /// A surviving replacement keeps the replaced entry's pre-change visibility,
    /// so only actual removals reach `removed`.
    fn put(&mut self, file: &FileValue, order: u64, removed: &mut Vec<FileKey>) {
        let previous = self.take(&file.key);
        let bytes = file.bytes.len();
        if bytes > self.maximum_bytes {
            if let Some((key, generation)) = previous
                && generation < self.generation
            {
                removed.push(key);
            }
            return;
        }
        #[cfg(all(test, not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("insert", order));
        let slot = super::file_kind(file).map_or(UNCLASSIFIED, usize::from);
        let (key, generation) = previous.unwrap_or_else(|| (file.key.clone(), self.generation));
        self.ordered[slot].insert((order, file.key.clone()));
        self.entries.insert(
            key,
            Entry {
                order,
                slot,
                bytes,
                generation,
            },
        );
        self.bytes += bytes;
    }

    /// Reports the removed key only when it was visible before the current change.
    fn remove(&mut self, key: &FileKey, removed: &mut Vec<FileKey>) {
        if let Some((key, generation)) = self.take(key)
            && generation < self.generation
        {
            removed.push(key);
        }
    }

    /// Detaches an entry, returning its stored key and insertion generation.
    fn take(&mut self, key: &FileKey) -> Option<(FileKey, u64)> {
        let (key, entry) = self.entries.remove_entry(key)?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("remove", entry.order));
        let position = (entry.order, key);
        self.ordered[entry.slot].remove(&position);
        self.bytes -= entry.bytes;
        Some((position.1, entry.generation))
    }

    /// Per-kind recency bounds first, then global oldest (order, key) for count and bytes.
    fn trim(&mut self, removed: &mut Vec<FileKey>) {
        for slot in 0..UNCLASSIFIED {
            while self.ordered[slot].len() > self.recent && self.evict(slot..slot + 1, removed) {}
        }
        while (self.entries.len() > self.maximum_files || self.bytes > self.maximum_bytes)
            && self.evict(0..SLOTS, removed)
        {}
    }

    /// Removes the oldest (order, key) among the slots; false when they are empty.
    fn evict(&mut self, slots: std::ops::Range<usize>, removed: &mut Vec<FileKey>) -> bool {
        let Some(((_, key), _)) = slots
            .filter_map(|slot| self.ordered[slot].first().map(|oldest| (oldest, slot)))
            .min()
        else {
            return false;
        };
        let key = key.clone();
        self.remove(&key, removed);
        true
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
