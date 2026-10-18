//! Count actual route-map searches in tests; production uses `BTreeMap` directly.
use super::{BTreeMap, Route};
use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
};

#[derive(Default)]
pub(super) struct Routes {
    values: BTreeMap<u64, Route>,
    searches: Cell<usize>,
}

impl Routes {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn searches(&self) -> usize {
        self.searches.get()
    }
    fn searched(&self) {
        self.searches.set(self.searches.get() + 1);
    }
    pub(super) fn get(&self, id: &u64) -> Option<&Route> {
        self.searched();
        self.values.get(id)
    }
    pub(super) fn get_mut(&mut self, id: &u64) -> Option<&mut Route> {
        self.searched();
        self.values.get_mut(id)
    }
    pub(super) fn remove(&mut self, id: &u64) -> Option<Route> {
        self.searched();
        self.values.remove(id)
    }
}

impl Deref for Routes {
    type Target = BTreeMap<u64, Route>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl DerefMut for Routes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}
impl IntoIterator for Routes {
    type Item = (u64, Route);
    type IntoIter = std::collections::btree_map::IntoIter<u64, Route>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}
