use std::{borrow::Borrow, cmp::Ordering, collections::BTreeMap};

// Both key forms use the map's lexicographic (name, revision) ordering.
trait Key {
    fn parts(&self) -> (&str, &str);

    fn compare(&self, name: &str, revision: &str) -> Ordering {
        self.parts().cmp(&(name, revision))
    }
}

impl Key for (String, String) {
    #[inline]
    fn parts(&self) -> (&str, &str) {
        (&self.0, &self.1)
    }
}

impl Key for (&str, &str) {
    #[inline]
    fn parts(&self) -> (&str, &str) {
        *self
    }
}

impl<'a> Borrow<dyn Key + 'a> for (String, String) {
    #[inline]
    fn borrow(&self) -> &(dyn Key + 'a) {
        self
    }
}

impl PartialEq for dyn Key + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn Key + '_ {}

impl PartialOrd for dyn Key + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn Key + '_ {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        let (name, revision) = other.parts();
        self.compare(name, revision)
    }
}

#[inline]
pub(super) fn get<'a, T>(
    map: &'a BTreeMap<(String, String), T>,
    name: &str,
    revision: &str,
) -> Option<&'a T> {
    map.get(&(name, revision) as &dyn Key)
}
