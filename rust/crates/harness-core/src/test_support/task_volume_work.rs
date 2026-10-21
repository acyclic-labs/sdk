use crate::task_volume::FileKey;
use std::{cell::Cell, collections::BTreeMap, ops::RangeBounds};

thread_local! {
    static EXAMINED: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Counts task-index accesses and stream records examined during one synchronous
/// operation. Poll in-memory futures inside the closure; no observer is retained.
///
/// # Panics
/// Panics if measurements are nested on the same thread.
pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    EXAMINED.with(|counter| {
        assert!(counter.get().is_none(), "nested task-volume measurement");
        struct Reset<'a>(&'a Cell<Option<u64>>);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.set(None);
            }
        }
        counter.set(Some(0));
        let _reset = Reset(counter);
        let output = operation();
        (output, counter.get().unwrap_or_default())
    })
}

#[doc(hidden)]
pub fn examined() {
    EXAMINED.with(|counter| {
        if let Some(count) = counter.get() {
            counter.set(Some(count.saturating_add(1)));
        }
    });
}

// The same BTreeMap and access operations as production. Charge iterator items
// before consumer filters, including entries a full scan eventually discards.
#[derive(Clone, Debug, PartialEq)]
#[doc(hidden)]
pub struct Files<V>(BTreeMap<FileKey, V>);

impl<V> Default for Files<V> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<V> Files<V> {
    pub fn get(&self, key: &FileKey) -> Option<&V> {
        examined();
        self.0.get(key)
    }

    pub fn contains_key(&self, key: &FileKey) -> bool {
        examined();
        self.0.contains_key(key)
    }

    pub fn insert(&mut self, key: FileKey, value: V) -> Option<V> {
        examined();
        self.0.insert(key, value)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.0.values().inspect(|_| examined())
    }

    pub fn entry(&mut self, key: FileKey) -> std::collections::btree_map::Entry<'_, FileKey, V> {
        examined();
        self.0.entry(key)
    }

    pub fn range<R: RangeBounds<FileKey>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = (&FileKey, &V)> {
        self.0.range(range).inspect(|_| examined())
    }
}

impl<V> FromIterator<(FileKey, V)> for Files<V> {
    fn from_iter<T: IntoIterator<Item = (FileKey, V)>>(values: T) -> Self {
        Self(values.into_iter().inspect(|_| examined()).collect())
    }
}
impl<V> Files<V> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn as_map(&self) -> &BTreeMap<FileKey, V> {
        &self.0
    }
    pub fn remove(&mut self, key: &FileKey) -> Option<V> {
        examined();
        self.0.remove(key)
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&FileKey, &V)> {
        self.0.iter().inspect(|_| examined())
    }
}
