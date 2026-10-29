//! Retained-fold residency changes with the fold, never with a metric read.

use super::RetainedView;
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

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
        self.bytes += view.view.retained_bytes();
        if let Some(prior) = self.entries.insert(key, view) {
            self.bytes -= prior.view.retained_bytes();
        }
    }

    pub(super) fn get_mut<Q: Ord + ?Sized>(&mut self, key: &Q) -> Option<ViewMut<'_>>
    where
        K: Borrow<Q>,
    {
        let view = self.entries.get_mut(key)?;
        Some(ViewMut {
            prior: view.view.retained_bytes(),
            entry: view,
            bytes: &mut self.bytes,
        })
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&K, &RetainedView) -> bool) {
        self.entries.retain(|key, view| {
            let retained = keep(key, view);
            if !retained {
                self.bytes -= view.view.retained_bytes();
            }
            retained
        });
    }

    pub(super) fn remove<Q: Ord + ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
    {
        if let Some(view) = self.entries.remove(key) {
            self.bytes -= view.view.retained_bytes();
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}

pub(super) struct ViewMut<'a> {
    entry: &'a mut RetainedView,
    bytes: &'a mut usize,
    prior: usize,
}

impl Deref for ViewMut<'_> {
    type Target = RetainedView;

    fn deref(&self) -> &Self::Target {
        self.entry
    }
}

impl DerefMut for ViewMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.entry
    }
}

impl Drop for ViewMut<'_> {
    fn drop(&mut self) {
        *self.bytes = *self.bytes - self.prior + self.entry.view.retained_bytes();
    }
}
