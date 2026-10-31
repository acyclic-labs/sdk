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

    pub(super) fn get(&self, key: &OpKey) -> Option<&Demand> {
        self.entries.get(key).map(|entry| &entry.demand)
    }

    pub(super) fn contains_key(&self, key: &OpKey) -> bool {
        self.entries.contains_key(key)
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
    ) {
        if let Some(entry) = self.entries.get_mut(&key) {
            let priority = if entry.mark == self.mark {
                entry.demand.priority.min(priority)
            } else {
                entry.demand.paging = paging;
                priority
            };
            if entry.demand.priority != priority {
                self.order[class(entry.demand.priority)]
                    .remove(&(entry.demand.ticket, key.clone()));
                self.order[class(priority)].insert((entry.demand.ticket, key));
                entry.demand.priority = priority;
            }
            entry.mark = self.mark;
        } else {
            let ticket = *next_ticket;
            *next_ticket += 1;
            self.insert(
                key,
                Demand {
                    priority,
                    paging,
                    ticket,
                },
            );
        }
    }

    pub(super) fn finish_plan(&mut self) {
        let mark = self.mark;
        self.entries.retain(|key, entry| {
            if entry.mark == mark {
                true
            } else {
                self.order[class(entry.demand.priority)]
                    .remove(&(entry.demand.ticket, key.clone()));
                false
            }
        });
    }

    pub(super) fn insert(&mut self, key: OpKey, demand: Demand) {
        self.remove(&key);
        self.order[class(demand.priority)].insert((demand.ticket, key.clone()));
        self.entries.insert(
            key,
            Entry {
                demand,
                mark: self.mark,
            },
        );
    }

    pub(super) fn remove(&mut self, key: &OpKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.order[class(entry.demand.priority)].remove(&(entry.demand.ticket, key.clone()));
        }
    }

    /// Each operation variant already groups a path's entries in the owning
    /// map. Invalidation visits only those ranges, preserving unrelated ages.
    pub(super) fn remove_path(&mut self, path: &super::StreamPath) {
        self.remove_range(OpKey::Read(path.clone(), 0)..=OpKey::Read(path.clone(), u64::MAX));
        self.remove_range(OpKey::Head(path.clone())..=OpKey::Head(path.clone()));
        self.remove_range(
            OpKey::Children(path.clone(), 0)..=OpKey::Children(path.clone(), u32::MAX),
        );
    }

    pub(super) fn remove_children_after(&mut self, path: &super::StreamPath, index: u32) {
        if let Some(first) = index.checked_add(1) {
            self.remove_range(
                OpKey::Children(path.clone(), first)..=OpKey::Children(path.clone(), u32::MAX),
            );
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
            self.order[class(entry.demand.priority)].remove(&(entry.demand.ticket, key));
        }
    }

    fn remove_range(&mut self, range: std::ops::RangeInclusive<OpKey>) {
        for (key, entry) in self.entries.extract_if(range, |_, _| true) {
            #[cfg(test)]
            {
                self.invalidation_visits += 1;
            }
            self.order[class(entry.demand.priority)].remove(&(entry.demand.ticket, key));
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

    pub(super) fn rotate(&mut self, key: &OpKey, ticket: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            let order = &mut self.order[class(entry.demand.priority)];
            order.remove(&(entry.demand.ticket, key.clone()));
            order.insert((ticket, key.clone()));
            entry.demand.ticket = ticket;
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
