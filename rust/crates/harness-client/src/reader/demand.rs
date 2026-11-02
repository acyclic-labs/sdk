//! One demand owner with maintained age order; dispatch never sorts the population.

use super::{Demand, OpKey, Paging, Priority};
use std::{
    collections::{BTreeMap, BTreeSet, btree_set},
    iter::Peekable,
};

#[derive(Default)]
pub(super) struct Demands {
    entries: BTreeMap<OpKey, Entry>,
    order: [BTreeSet<(u64, OpKey)>; 2],
    mark: bool,
    #[cfg(test)]
    visits: std::cell::Cell<usize>,
    #[cfg(test)]
    invalidation_visits: usize,
}

struct Entry {
    demand: Demand,
    mark: bool,
    active: bool,
}

pub(super) struct Ordered<'a> {
    entries: &'a BTreeMap<OpKey, Entry>,
    visible: Peekable<btree_set::Iter<'a, (u64, OpKey)>>,
    prefetch: Peekable<btree_set::Iter<'a, (u64, OpKey)>>,
    #[cfg(test)]
    visits: &'a std::cell::Cell<usize>,
}

const fn class(priority: Priority) -> usize {
    match priority {
        Priority::Visible => 0,
        Priority::Prefetch => 1,
    }
}

impl Demands {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(super) fn get(&self, key: &OpKey) -> Option<&Demand> {
        self.entries.get(key).map(|entry| &entry.demand)
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &OpKey) -> bool {
        self.entries.contains_key(key)
    }

    /// A retry keeps the original observations unless a newer plan admitted
    /// its own. A withdrawn demand cannot be revived by the failed flight.
    pub(super) fn restore_paging(&mut self, key: &OpKey, paging: Paging) {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.demand.paging.witnesses.is_empty() {
                entry.demand.paging = paging;
            }
            if entry.active {
                entry.active = false;
                self.order[class(entry.demand.priority)].insert((entry.demand.ticket, key.clone()));
            }
        }
    }

    pub(super) fn begin_plan(&mut self) {
        self.mark = !self.mark;
    }

    pub(super) fn want(
        &mut self,
        key: OpKey,
        priority: Priority,
        paging: Paging,
        next_ticket: &mut u64,
        changed: impl FnOnce(&OpKey, Priority) -> bool,
    ) {
        if let Some(entry) = self.entries.get_mut(&key) {
            let priority = if entry.mark == self.mark {
                entry.demand.paging.witnesses.extend(paging.witnesses);
                entry.demand.priority.min(priority)
            } else {
                entry.demand.paging = paging;
                priority
            };
            if entry.demand.priority != priority {
                changed(&key, priority);
                if !entry.active {
                    self.order[class(entry.demand.priority)]
                        .remove(&(entry.demand.ticket, key.clone()));
                    self.order[class(priority)].insert((entry.demand.ticket, key));
                }
                entry.demand.priority = priority;
            }
            entry.mark = self.mark;
        } else {
            let active = changed(&key, priority);
            let ticket = *next_ticket;
            *next_ticket += 1;
            self.insert(
                key,
                Demand {
                    priority,
                    paging,
                    ticket,
                },
                active,
            );
        }
    }

    pub(super) fn finish_plan(&mut self) -> impl Iterator<Item = OpKey> {
        let mark = self.mark;
        let order = &mut self.order;
        self.entries
            .extract_if(.., move |_, entry| entry.mark != mark)
            .map(move |(key, entry)| {
                entry.unqueue(&key, order);
                key
            })
    }

    pub(super) fn insert(&mut self, key: OpKey, demand: Demand, active: bool) {
        self.remove(&key);
        if !active {
            self.order[class(demand.priority)].insert((demand.ticket, key.clone()));
        }
        self.entries.insert(
            key,
            Entry {
                demand,
                mark: self.mark,
                active,
            },
        );
    }

    pub(super) fn remove(&mut self, key: &OpKey) {
        if let Some(entry) = self.entries.remove(key) {
            entry.unqueue(key, &mut self.order);
        }
    }

    /// Each operation variant already groups a path's entries in the owning
    /// map. Invalidation visits only those ranges, preserving unrelated ages.
    pub(super) fn remove_path(&mut self, path: &super::StreamPath) {
        for range in OpKey::path_ranges(path) {
            self.remove_range(range);
        }
    }

    pub(super) fn remove_children_after(&mut self, path: &super::StreamPath, index: u32) {
        if let Some(first) = index.checked_add(1) {
            self.remove_range(OpKey::children(path, first, None)..OpKey::children_end(path));
        }
    }

    pub(super) fn clear_children(&mut self) {
        // Children is the last OpKey variant: the hierarchy witness covers
        // every listing, but no pending history read or head observation.
        while let Some(entry) = self.entries.last_entry() {
            if !matches!(entry.key(), OpKey::Children(..)) {
                break;
            }
            let (key, entry) = entry.remove_entry();
            #[cfg(test)]
            {
                self.invalidation_visits += 1;
            }
            entry.unqueue(&key, &mut self.order);
        }
    }

    fn remove_range(&mut self, range: impl std::ops::RangeBounds<OpKey>) {
        for (key, entry) in self.entries.extract_if(range, |_, _| true) {
            #[cfg(test)]
            {
                self.invalidation_visits += 1;
            }
            entry.unqueue(&key, &mut self.order);
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.order.iter_mut().for_each(BTreeSet::clear);
    }

    pub(super) fn ordered(&self) -> Ordered<'_> {
        Ordered {
            entries: &self.entries,
            visible: self.order[0].iter().peekable(),
            prefetch: self.order[1].iter().peekable(),
            #[cfg(test)]
            visits: &self.visits,
        }
    }

    #[cfg(test)]
    pub(super) fn take_visits(&self) -> usize {
        self.visits.replace(0)
    }

    #[cfg(test)]
    pub(super) fn take_invalidation_visits(&mut self) -> usize {
        std::mem::take(&mut self.invalidation_visits)
    }

    /// Transfer the admitted observations at the original age rotation.
    pub(super) fn dispatch(&mut self, key: &OpKey, ticket: u64) -> Paging {
        let entry = self
            .entries
            .get_mut(key)
            .expect("dispatch retains its selected demand");
        let order = &mut self.order[class(entry.demand.priority)];
        order.remove(&(entry.demand.ticket, key.clone()));
        entry.active = true;
        entry.demand.ticket = ticket;
        std::mem::take(&mut entry.demand.paging)
    }
}

impl Entry {
    fn unqueue(&self, key: &OpKey, order: &mut [BTreeSet<(u64, OpKey)>; 2]) {
        if !self.active {
            order[class(self.demand.priority)].remove(&(self.demand.ticket, key.clone()));
        }
    }
}

impl<'a> Ordered<'a> {
    /// Merge the maintained age indexes with the original bounded visible
    /// head start. Exhausted prefetch credit stops visiting that index at once.
    pub(super) fn next_admitted(
        &mut self,
        prefetch: bool,
        head_start: usize,
    ) -> Option<(&'a OpKey, &'a Demand)> {
        let next = if prefetch {
            match (self.visible.peek(), self.prefetch.peek()) {
                (Some((visible_ticket, _)), Some((prefetch_ticket, _)))
                    if (u128::from(*visible_ticket), *visible_ticket)
                        <= (
                            u128::from(*prefetch_ticket) + head_start as u128,
                            *prefetch_ticket,
                        ) =>
                {
                    self.visible.next()
                }
                (None, Some(_)) | (Some(_), Some(_)) => self.prefetch.next(),
                (Some(_), None) => self.visible.next(),
                (None, None) => None,
            }
        } else {
            self.visible.next()
        };
        next.map(|(_, key)| {
            #[cfg(test)]
            self.visits.set(self.visits.get() + 1);
            (key, &self.entries[key].demand)
        })
    }
}
