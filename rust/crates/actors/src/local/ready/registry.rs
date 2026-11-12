//! Reusable direct-address slots. Queue operations do not search resident keys.
use super::Entry;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Id {
    slot: usize,
    generation: u64,
}

struct Slot<K> {
    generation: u64,
    entry: Option<Entry<K>>,
    next_free: Option<usize>,
}

pub(super) struct Registry<K> {
    slots: Vec<Slot<K>>,
    free: Option<usize>,
    len: usize,
}

impl<K> Default for Registry<K> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: None,
            len: 0,
        }
    }
}

#[cfg(all(test, not(loom)))]
std::thread_local! {
    static WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

fn visit(comparisons: usize) {
    #[cfg(all(test, not(loom)))]
    WORK.with(|work| {
        let (visits, compared) = work.get();
        work.set((visits + 1, compared + comparisons));
    });
    #[cfg(not(all(test, not(loom))))]
    let _ = comparisons;
}

#[cfg(all(test, not(loom)))]
pub(super) fn work() -> (usize, usize) {
    WORK.with(std::cell::Cell::get)
}

impl<K> Registry<K> {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// Vector growth is amortized O(1); free-slot reuse is worst-case O(1).
    pub(super) fn insert(&mut self, generation: u64, entry: Entry<K>) -> Id {
        visit(0);
        let slot = if let Some(slot) = self.free {
            let vacant = &mut self.slots[slot];
            self.free = vacant.next_free.take();
            vacant.generation = generation;
            vacant.entry = Some(entry);
            slot
        } else {
            let slot = self.slots.len();
            self.slots.push(Slot {
                generation,
                entry: Some(entry),
                next_free: None,
            });
            slot
        };
        self.len += 1;
        Id { slot, generation }
    }

    pub(super) fn get(&self, id: &Id) -> Option<&Entry<K>> {
        let slot = self.slots.get(id.slot)?;
        visit(1);
        (slot.generation == id.generation)
            .then_some(slot.entry.as_ref())
            .flatten()
    }

    pub(super) fn get_mut(&mut self, id: &Id) -> Option<&mut Entry<K>> {
        let slot = self.slots.get_mut(id.slot)?;
        visit(1);
        if slot.generation == id.generation {
            slot.entry.as_mut()
        } else {
            None
        }
    }

    pub(super) fn remove(&mut self, id: &Id) -> Option<Entry<K>> {
        let slot = self.slots.get_mut(id.slot)?;
        visit(1);
        if slot.generation != id.generation {
            return None;
        }
        let entry = slot.entry.take()?;
        slot.next_free = self.free;
        self.free = Some(id.slot);
        self.len -= 1;
        Some(entry)
    }
}
