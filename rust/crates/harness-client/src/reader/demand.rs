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

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&OpKey, &Demand) -> bool) {
        self.entries.retain(|key, entry| {
            let retained = keep(key, &entry.demand);
            if !retained {
                self.order[class(entry.demand.priority)]
                    .remove(&(entry.demand.ticket, key.clone()));
            }
            retained
        });
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
