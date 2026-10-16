use std::{borrow::Borrow, cmp::Ordering, collections::BTreeMap};

pub(crate) fn paths_conflict(existing: &str, path: &str) -> bool {
    existing != path
        && (existing
            .strip_prefix(path)
            .is_some_and(|suffix| suffix.starts_with('/'))
            || path
                .strip_prefix(existing)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

// Visit a prefix of the original scan, so every query reads each key at most
// once and cannot pass the original first conflict. After the slash-prefixed
// descendant interval, later keys cannot be an ancestor or a descendant.
#[inline]
pub(crate) fn conflicts<K: Borrow<str> + Ord, V>(paths: &BTreeMap<K, V>, path: &str) -> bool {
    for existing in paths.keys() {
        let existing: &str = existing.borrow();
        let common = existing.len().min(path.len());
        match existing.as_bytes()[..common].cmp(&path.as_bytes()[..common]) {
            Ordering::Less => {}
            Ordering::Greater => return false,
            Ordering::Equal => match existing.len().cmp(&path.len()) {
                Ordering::Less => {
                    if path.as_bytes()[existing.len()] == b'/' {
                        return true;
                    }
                }
                Ordering::Equal => {}
                Ordering::Greater => match existing.as_bytes()[path.len()] {
                    b'/' => return true,
                    byte if byte > b'/' => return false,
                    _ => {}
                },
            },
        }
    }
    false
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "conflicts/tests.rs"]
mod tests;
