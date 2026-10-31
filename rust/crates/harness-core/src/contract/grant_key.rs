//! Borrowed composite lookup in the original ordered grant set.
use std::{borrow::Borrow, cmp::Ordering, collections::BTreeSet};

trait Key {
    fn parts(&self) -> (&str, &str);
}
impl Key for String {
    fn parts(&self) -> (&str, &str) {
        ("", self)
    }
}
impl Key for (&str, &str) {
    fn parts(&self) -> (&str, &str) {
        *self
    }
}
impl<'a> Borrow<dyn Key + 'a> for String {
    fn borrow(&self) -> &(dyn Key + 'a) {
        self
    }
}
impl PartialEq for dyn Key + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for dyn Key + '_ {}
impl PartialOrd for dyn Key + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for dyn Key + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        let (left, tail) = self.parts();
        let (right, suffix) = other.parts();
        left.bytes()
            .chain(tail.bytes())
            .cmp(right.bytes().chain(suffix.bytes()))
    }
}
pub(super) fn contains(grants: &BTreeSet<String>, prefix: &str, suffix: &str) -> bool {
    grants.get(&(prefix, suffix) as &dyn Key).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn split_lookup_agrees_with_contiguous_keys(
            keys in prop::collection::btree_set(".{0,40}", 0..32), prefix in ".{0,20}", suffix in ".{0,20}",
        ) {
            prop_assert_eq!(contains(&keys, &prefix, &suffix), keys.contains(&format!("{prefix}{suffix}")));
            let mut keys = keys;
            keys.insert(format!("{prefix}{suffix}"));
            prop_assert!(contains(&keys, &prefix, &suffix));
            // Negative control: looking up either fragment alone loses the key.
            if !prefix.is_empty() && !suffix.is_empty() {
                let only_full = BTreeSet::from([format!("{prefix}{suffix}")]);
                prop_assert!(!only_full.contains(&prefix));
                prop_assert!(!only_full.contains(&suffix));
            }
        }
    }
}
