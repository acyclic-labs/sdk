use super::{CacheState, ObjectStoreError, RecencyKey};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
struct Links {
    key: RecencyKey,
    previous: Option<u64>,
    next: Option<u64>,
}

#[derive(Default)]
pub(super) struct Recency {
    nodes: BTreeMap<u64, Links>,
    oldest: Option<u64>,
    newest: Option<u64>,
}

impl CacheState {
    fn recency_id_mut(&mut self, key: RecencyKey) -> Result<&mut u64, ObjectStoreError> {
        match key {
            RecencyKey::Canonical(id) => self.entries.get_mut(&id).map(|entry| &mut entry.recency),
            RecencyKey::Decoded(key) => self.decoded.get_mut(&key).map(|entry| &mut entry.recency),
        }
        .ok_or(ObjectStoreError::Corrupt)
    }

    pub(super) fn append_recent(
        &mut self,
        key: RecencyKey,
        tick: u64,
    ) -> Result<(), ObjectStoreError> {
        #[cfg(test)]
        if let Some(tree) = &mut self.historical_recency {
            tree.insert((tick, key));
            return Ok(());
        }
        let previous = self.recency.newest;
        if let Some(previous) = previous {
            self.recency
                .nodes
                .get_mut(&previous)
                .ok_or(ObjectStoreError::Corrupt)?
                .next = Some(tick);
        } else {
            self.recency.oldest = Some(tick);
        }
        self.recency.nodes.insert(
            tick,
            Links {
                key,
                previous,
                next: None,
            },
        );
        self.recency.newest = Some(tick);
        Ok(())
    }

    // One cache-state mutex protects the resident maps and all links. Hits
    // relink existing nodes without allocating or acquiring another lock.
    pub(super) fn touch_recent(
        &mut self,
        key: RecencyKey,
        _tick: u64,
    ) -> Result<(), ObjectStoreError> {
        #[cfg(test)]
        let historical = self.historical_recency.is_some();
        let slot = self.recency_id_mut(key)?;
        let prior = *slot;
        #[cfg(test)]
        if historical {
            *slot = _tick;
            let tree = self.historical_recency.as_mut().unwrap();
            tree.remove(&(prior, key));
            tree.insert((_tick, key));
            return Ok(());
        }
        if self.recency.newest == Some(prior) {
            return Ok(());
        }
        let links = *self
            .recency
            .nodes
            .get(&prior)
            .ok_or(ObjectStoreError::Corrupt)?;
        if let Some(previous) = links.previous {
            self.recency
                .nodes
                .get_mut(&previous)
                .ok_or(ObjectStoreError::Corrupt)?
                .next = links.next;
        } else {
            self.recency.oldest = links.next;
        }
        let next = links.next.ok_or(ObjectStoreError::Corrupt)?;
        self.recency
            .nodes
            .get_mut(&next)
            .ok_or(ObjectStoreError::Corrupt)?
            .previous = links.previous;
        let newest = self.recency.newest.ok_or(ObjectStoreError::Corrupt)?;
        self.recency
            .nodes
            .get_mut(&newest)
            .ok_or(ObjectStoreError::Corrupt)?
            .next = Some(prior);
        let node = self
            .recency
            .nodes
            .get_mut(&prior)
            .ok_or(ObjectStoreError::Corrupt)?;
        node.previous = Some(newest);
        node.next = None;
        self.recency.newest = Some(prior);
        Ok(())
    }

    pub(super) fn pop_oldest(&mut self) -> Result<RecencyKey, ObjectStoreError> {
        #[cfg(test)]
        if let Some(tree) = &mut self.historical_recency {
            return tree
                .pop_first()
                .map(|(_, key)| key)
                .ok_or(ObjectStoreError::Corrupt);
        }
        let oldest = self.recency.oldest.ok_or(ObjectStoreError::Corrupt)?;
        let links = self
            .recency
            .nodes
            .remove(&oldest)
            .ok_or(ObjectStoreError::Corrupt)?;
        self.recency.oldest = links.next;
        if let Some(next) = links.next {
            self.recency
                .nodes
                .get_mut(&next)
                .ok_or(ObjectStoreError::Corrupt)?
                .previous = None;
        } else {
            self.recency.newest = None;
        }
        Ok(links.key)
    }

    pub(super) fn clear_recency(&mut self) {
        self.recency = Recency::default();
        #[cfg(test)]
        if let Some(tree) = &mut self.historical_recency {
            tree.clear();
        }
    }

    #[cfg(test)]
    pub(super) fn recency_order(&self) -> Vec<RecencyKey> {
        if let Some(tree) = &self.historical_recency {
            return tree.iter().map(|&(_, key)| key).collect();
        }
        let mut order = Vec::new();
        let mut current = self.recency.oldest;
        let mut previous = None;
        while let Some(slot) = current {
            assert!(order.len() < self.recency.nodes.len(), "recency cycle");
            let links = self.recency.nodes.get(&slot).unwrap();
            assert_eq!(links.previous, previous, "recency predecessor");
            let resident_slot = match links.key {
                RecencyKey::Canonical(id) => self.entries.get(&id).unwrap().recency,
                RecencyKey::Decoded(key) => self.decoded.get(&key).unwrap().recency,
            };
            assert_eq!(resident_slot, slot, "recency identity");
            order.push(links.key);
            previous = Some(slot);
            current = links.next;
        }
        assert_eq!(previous, self.recency.newest, "recency tail");
        assert_eq!(
            order.len(),
            self.recency.nodes.len(),
            "unlinked recency node"
        );
        assert_eq!(
            order.len(),
            self.entries.len() + self.decoded.len(),
            "unlinked resident"
        );
        order
    }
}
