//! Retained-fold residency changes with the fold, never with a metric read.

use super::RetainedView;
use std::{borrow::Borrow, collections::BTreeMap, ops::Deref};

pub(super) struct Views<K> {
    entries: BTreeMap<K, RetainedView>,
    bytes: usize,
}

impl<K> Default for Views<K> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            bytes: 0,
        }
    }
}

impl<K> Deref for Views<K> {
    type Target = BTreeMap<K, RetainedView>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<K: Ord> Views<K> {
    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn insert(&mut self, key: K, view: RetainedView) {
        self.bytes += view.view.metadata_bytes();
        if let Some(prior) = self.entries.insert(key, view) {
            self.bytes -= prior.view.metadata_bytes();
        }
    }

    pub(super) fn apply<Q: Ord + ?Sized>(
        &mut self,
        key: &Q,
        frame: &acyclic_stream::session::Frame,
        revision: u64,
    ) -> Option<Result<(), acyclic_stream::StreamError>>
    where
        K: Borrow<Q>,
    {
        let view = self.entries.get_mut(key)?;
        let prior = view.view.metadata_bytes();
        let result = view.view.apply(frame, revision);
        self.bytes = self.bytes - prior + view.view.metadata_bytes();
        Some(result)
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&K, &RetainedView) -> bool) {
        self.entries.retain(|key, view| {
            let retained = keep(key, view);
            if !retained {
                self.bytes -= view.view.metadata_bytes();
            }
            retained
        });
    }

    pub(super) fn remove<Q: Ord + ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
    {
        if let Some(view) = self.entries.remove(key) {
            self.bytes -= view.view.metadata_bytes();
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}
