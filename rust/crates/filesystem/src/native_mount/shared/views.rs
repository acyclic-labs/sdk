//! The names of one shared instance's views, over its [`ViewTable`].
//!
//! A view is listed under a name and a serial at once, and unlisted from
//! both before its leases drain: a lookup by name and a callback by serial
//! see the same set of live views. Once an instance closes it lists no new
//! view, so nothing is admitted to an instance that is tearing down.

use super::view_table::{ViewLease, ViewOwner, ViewTable};
use crate::native_mount::NativeMountError;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::{Arc, PoisonError, RwLock};

pub(crate) struct NamedViews<K, C> {
    table: crate::sync::native::Arc<ViewTable<C>>,
    names: RwLock<Names<K>>,
}

struct Names<K> {
    by_name: HashMap<K, u64>,
    by_serial: BTreeMap<u64, K>,
    closed: bool,
}

impl<K: Clone + Eq + Hash, C> NamedViews<K, C> {
    /// Views with serials `1..=max_serial`.
    pub(crate) fn new(max_serial: u64) -> Self {
        Self {
            table: crate::sync::native::Arc::new(ViewTable::new(max_serial)),
            names: RwLock::new(Names {
                by_name: HashMap::new(),
                by_serial: BTreeMap::new(),
                closed: false,
            }),
        }
    }

    /// Lists a view under `name`. `build` receives the view's serial, never
    /// issued before, and returns its core; the view is reachable only once
    /// `build` returns.
    pub(crate) fn insert(
        self: &Arc<Self>,
        name: K,
        build: impl FnOnce(u64) -> Result<C, NativeMountError>,
    ) -> Result<NamedView<K, C>, NativeMountError> {
        let mut names = self.names.write().unwrap_or_else(PoisonError::into_inner);
        if names.closed {
            return Err(NativeMountError::Lifecycle("shared mount is stopping"));
        }
        if names.by_name.contains_key(&name) {
            return Err(NativeMountError::InvalidDestination);
        }
        let serial = ViewTable::reserve(&self.table)
            .map_err(|_| NativeMountError::Lifecycle("view serials are exhausted"))?;
        let number = serial.get();
        let owner = ViewTable::insert_reserved(&self.table, serial, build(number)?);
        names.by_name.insert(name.clone(), number);
        names.by_serial.insert(number, name);
        Ok(NamedView {
            views: Arc::clone(self),
            owner: Some(owner),
        })
    }

    /// Admits a callback to the live view with `serial`.
    pub(crate) fn enter(&self, serial: u64) -> Option<ViewLease<C>> {
        self.table.enter(serial)
    }

    /// Admits a callback to the live view listed as `name`, with its serial.
    pub(crate) fn enter_name<Q>(&self, name: &Q) -> Option<(u64, ViewLease<C>)>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let serial = *self
            .names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .by_name
            .get(name)?;
        Some((serial, self.enter(serial)?))
    }

    /// Up to `limit` listed views from serial `first` on, in issue order,
    /// for a backend whose root listing is synthetic (FUSE's).
    #[cfg(target_os = "linux")]
    pub(crate) fn listed_from(&self, first: u64, limit: usize) -> Vec<(u64, K)> {
        self.names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .by_serial
            .range(first..)
            .take(limit)
            .map(|(serial, name)| (*serial, name.clone()))
            .collect()
    }

    /// Lists no view from now on, and runs `stop` on every listed view: for
    /// a backend whose kernel can end the session under live views (FUSE's).
    #[cfg(target_os = "linux")]
    pub(crate) fn close(&self, stop: impl Fn(&C)) {
        let serials: Vec<u64> = {
            let mut names = self.names.write().unwrap_or_else(PoisonError::into_inner);
            names.closed = true;
            names.by_serial.keys().copied().collect()
        };
        for serial in serials {
            if let Some(view) = self.table.enter(serial) {
                stop(&view);
            }
        }
    }
}

/// A listed view's only owner. Dropping it retires the view.
pub(crate) struct NamedView<K: Clone + Eq + Hash, C> {
    views: Arc<NamedViews<K, C>>,
    owner: Option<ViewOwner<C>>,
}

impl<K: Clone + Eq + Hash, C> NamedView<K, C> {
    /// Unlists the view, so no new callback is admitted, runs `stop`, then
    /// waits for every admitted callback and returns the name and core.
    /// `None` once retired.
    pub(crate) fn retire_after(&mut self, stop: impl FnOnce()) -> Option<(K, C)> {
        let owner = self.owner.take()?;
        let serial = owner.serial().get();
        let name = {
            let mut names = self
                .views
                .names
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(name) = names.by_serial.remove(&serial) else {
                unreachable!("a view owner has a listed name");
            };
            names.by_name.remove(&name);
            name
        };
        Some((name, owner.retire_after(stop)))
    }
}

impl<K: Clone + Eq + Hash, C> Drop for NamedView<K, C> {
    fn drop(&mut self) {
        drop(self.retire_after(|| {}));
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// A retired view's name and serial both resolve to nothing, a name is
    /// listed once, and a reused name gets a fresh serial; once closed, no
    /// view is listed. Negative control: the live view stays reachable.
    #[test]
    fn names_and_serials_retire_together() -> Result<(), NativeMountError> {
        let views = Arc::new(NamedViews::<Vec<u8>, u32>::new(8));
        let mut first = views.insert(b"a".to_vec(), |_| Ok(1))?;
        let second = views.insert(b"b".to_vec(), |_| Ok(2))?;
        assert!(matches!(
            views.insert(b"a".to_vec(), |_| Ok(3)),
            Err(NativeMountError::InvalidDestination)
        ));
        let (serial, lease) = views.enter_name(b"a".as_slice()).expect("listed");
        assert_eq!(*lease, 1);
        let mut stopped = false;
        assert_eq!(
            first.retire_after(|| {
                assert!(views.enter_name(b"a".as_slice()).is_none());
                assert!(
                    views.enter(serial).is_none(),
                    "stop must close serial admission"
                );
                assert_eq!(*lease, 1, "an admitted callback retains its view");
                drop(lease);
                stopped = true;
            }),
            Some((b"a".to_vec(), 1))
        );
        assert!(stopped);
        assert_eq!(first.retire_after(|| unreachable!()), None);
        assert!(views.enter_name(b"a".as_slice()).is_none());
        assert!(views.enter(serial).is_none(), "a retired serial is stale");
        let again = views.insert(b"a".to_vec(), |issued| {
            assert!(issued > serial, "serials are never reused");
            Ok(4)
        })?;
        assert_eq!(*views.enter_name(b"b".as_slice()).expect("live").1, 2);
        drop((second, again));
        Ok(())
    }
}
