use crate::{GenerationId, Sequence, VolumeId, sync::Mutex};

type Key = (VolumeId, GenerationId);
pub(super) const CAPACITY: usize = 64;

// Disposable hints, never a second authority. Only a verified replay from
// genesis supplies a position; the owning Fs fixes the authority deployment.
pub(super) struct GenerationPositions<const N: usize = CAPACITY> {
    // Terminal lock. Copy fields only: no accounting, allocation, storage or
    // other locks while held. The loom model exercises both operations.
    entries: Mutex<Entries<N>>,
}

struct Entries<const N: usize> {
    slots: [Option<(Key, Sequence)>; N],
    next: usize,
}

impl<const N: usize> Default for GenerationPositions<N> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(Entries {
                slots: [None; N],
                next: 0,
            }),
        }
    }
}

impl<const N: usize> GenerationPositions<N> {
    pub(super) fn get(&self, key: &Key, head: Sequence) -> Option<Sequence> {
        self.entries
            .lock()
            .ok()?
            .slots
            .iter()
            .flatten()
            .find(|(stored, position)| stored == key && *position <= head)
            .map(|(_, position)| *position)
    }

    pub(super) fn record(&self, key: Key, position: Sequence) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        let mut found = None;
        let mut free = None;
        for (index, entry) in entries.slots.iter().enumerate() {
            match entry {
                Some((stored, _)) if *stored == key => {
                    found = Some(index);
                    break;
                }
                None if free.is_none() => free = Some(index),
                _ => {}
            }
        }
        let index = found.or(free).unwrap_or(entries.next);
        let position = entries.slots[index]
            .filter(|(stored, _)| *stored == key)
            .map_or(position, |(_, previous)| previous.min(position));
        entries.slots[index] = Some((key, position));
        if found.is_none() && free.is_none() {
            entries.next = (index + 1) % N;
        }
    }

    #[cfg(all(test, acyclic_fs_loom))]
    fn record_torn(&self, key: Key, position: Sequence) {
        let index = {
            let mut entries = self.entries.lock().unwrap();
            let index = entries.next;
            let previous = entries.slots[index].map_or(position, |(_, position)| position);
            entries.slots[index] = Some((key, previous));
            index
        };
        loom::thread::yield_now();
        self.entries.lock().unwrap().slots[index] = Some((key, position));
    }
}

#[cfg(test)]
mod tests;
