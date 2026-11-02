//! One owner for live request identity, operation lookup and prefetch credit.

use super::{InFlight, OpKey, Priority};
use std::{collections::BTreeMap, ops::RangeBounds};

#[derive(Default)]
pub(super) struct Requests {
    by_id: BTreeMap<u64, InFlight>,
    keys: BTreeMap<OpKey, u64>,
    prefetch: usize,
}

impl Requests {
    pub(super) fn len(&self) -> usize {
        self.by_id.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub(super) fn prefetch(&self) -> usize {
        self.prefetch
    }

    pub(super) fn contains_key(&self, key: &OpKey) -> bool {
        self.keys.contains_key(key)
    }

    pub(super) fn first_key_value(&self) -> Option<(&u64, &InFlight)> {
        self.by_id.first_key_value()
    }

    pub(super) fn get_mut(&mut self, id: &u64) -> Option<&mut InFlight> {
        self.by_id.get_mut(id)
    }

    pub(super) fn insert(&mut self, id: u64, flight: InFlight) {
        self.prefetch += usize::from(flight.priority == Priority::Prefetch);
        self.keys.insert(flight.key.clone(), id);
        self.by_id.insert(id, flight);
    }

    pub(super) fn set_priority(&mut self, key: &OpKey, priority: Priority) {
        if let Some(id) = self.keys.get(key) {
            let flight = self
                .by_id
                .get_mut(id)
                .expect("live operation owns its request");
            if flight.priority != priority {
                self.prefetch -= usize::from(flight.priority == Priority::Prefetch);
                flight.priority = priority;
                self.prefetch += usize::from(priority == Priority::Prefetch);
            }
        }
    }

    pub(super) fn remove(&mut self, id: &u64) -> Option<InFlight> {
        let flight = Self::retire(&mut self.by_id, &mut self.prefetch, id)?;
        self.keys.remove(&flight.key);
        Some(flight)
    }

    pub(super) fn withdraw_prefetch(&mut self, key: &OpKey) -> Option<u64> {
        let id = *self.keys.get(key)?;
        if self.by_id[&id].priority != Priority::Prefetch {
            return None;
        }
        self.remove(&id);
        Some(id)
    }

    pub(super) fn pop_children(&mut self) -> Option<u64> {
        let entry = self.keys.last_entry()?;
        if !matches!(entry.key(), OpKey::Children(..)) {
            return None;
        }
        let (_, id) = entry.remove_entry();
        Self::retire(&mut self.by_id, &mut self.prefetch, &id);
        Some(id)
    }

    pub(super) fn remove_range(
        &mut self,
        range: impl RangeBounds<OpKey>,
    ) -> impl Iterator<Item = u64> {
        let Self {
            by_id,
            keys,
            prefetch,
        } = self;
        keys.extract_if(range, |_, _| true).map(move |(_, id)| {
            Self::retire(by_id, prefetch, &id);
            id
        })
    }

    pub(super) fn drain(&mut self) -> impl Iterator<Item = u64> + use<> {
        self.keys.clear();
        self.prefetch = 0;
        std::mem::take(&mut self.by_id).into_keys()
    }

    fn retire(
        by_id: &mut BTreeMap<u64, InFlight>,
        prefetch: &mut usize,
        id: &u64,
    ) -> Option<InFlight> {
        let flight = by_id.remove(id)?;
        *prefetch -= usize::from(flight.priority == Priority::Prefetch);
        Some(flight)
    }

    #[cfg(test)]
    pub(super) fn values(&self) -> impl Iterator<Item = &InFlight> {
        self.by_id.values()
    }
}
