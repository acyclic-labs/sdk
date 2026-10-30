//! Route identities indexed by their current physical node.
use super::{BTreeMap, Route};
#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
use std::cell::Cell;
use std::{collections::BTreeSet, ops::Deref};

#[derive(Default)]
pub(super) struct Routes {
    values: BTreeMap<u64, Route>,
    nodes: BTreeMap<String, BTreeSet<u64>>,
    #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
    searches: Cell<usize>,
    #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
    visits: Cell<usize>,
}

impl Routes {
    pub(super) fn new() -> Self {
        Self::default()
    }
    #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
    pub(super) fn searches(&self) -> usize {
        self.searches.get()
    }
    fn searched(&self) {
        #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
        self.searches.set(self.searches.get() + 1);
    }
    pub(super) fn get(&self, id: &u64) -> Option<&Route> {
        self.searched();
        self.values.get(id)
    }
    /// Change physical ownership through `rehome`, keeping both indexes joined.
    pub(super) fn get_mut(&mut self, id: &u64) -> Option<&mut Route> {
        self.searched();
        self.values.get_mut(id)
    }
    pub(super) fn insert(&mut self, id: u64, route: Route) -> Option<Route> {
        let old = self.values.insert(id, route);
        if let Some(old) = &old {
            Self::detach(&mut self.nodes, id, &old.node);
        }
        let route = &self.values[&id];
        if let Some(ids) = self.nodes.get_mut(&route.node) {
            ids.insert(id);
        } else {
            self.nodes.insert(route.node.clone(), BTreeSet::from([id]));
        }
        old
    }
    fn detach(nodes: &mut BTreeMap<String, BTreeSet<u64>>, id: u64, node: &str) {
        if let Some(ids) = nodes.get_mut(node) {
            ids.remove(&id);
            if ids.is_empty() {
                nodes.remove(node);
            }
        }
    }
    pub(super) fn remove(&mut self, id: &u64) -> Option<Route> {
        self.searched();
        let route = self.values.remove(id)?;
        Self::detach(&mut self.nodes, *id, &route.node);
        Some(route)
    }
    pub(super) fn clear(&mut self) {
        self.values.clear();
        self.nodes.clear();
    }
    pub(super) fn on_node(&self, node: &str) -> impl Iterator<Item = (&u64, &Route)> {
        self.nodes.get(node).into_iter().flatten().map(|id| {
            #[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
            self.visits.set(self.visits.get() + 1);
            (id, &self.values[id])
        })
    }
    pub(super) fn rehome(&mut self, id: &u64, node: &str) -> Option<&mut Route> {
        self.searched();
        let route = self.values.get_mut(id)?;
        if route.node != node {
            Self::detach(&mut self.nodes, *id, &route.node);
            if let Some(ids) = self.nodes.get_mut(node) {
                ids.insert(*id);
            } else {
                self.nodes.insert(node.to_owned(), BTreeSet::from([*id]));
            }
            route.node.clear();
            route.node.push_str(node);
        }
        Some(route)
    }
}

impl Deref for Routes {
    type Target = BTreeMap<u64, Route>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl IntoIterator for Routes {
    type Item = (u64, Route);
    type IntoIter = std::collections::btree_map::IntoIter<u64, Route>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[cfg(all(test, feature = "grpc", not(loom), not(target_arch = "wasm32")))]
mod tests;
