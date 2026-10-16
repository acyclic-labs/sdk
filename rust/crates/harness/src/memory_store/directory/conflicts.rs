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
        match existing.cmp(path) {
            Ordering::Less => {
                if path.as_bytes().get(existing.len()) == Some(&b'/') && path.starts_with(existing)
                {
                    return true;
                }
            }
            Ordering::Equal => {}
            Ordering::Greater => {
                let Some(suffix) = existing.strip_prefix(path) else {
                    return false;
                };
                match suffix.as_bytes().first() {
                    Some(b'/') => return true,
                    Some(byte) if *byte > b'/' => return false,
                    _ => {}
                }
            }
        }
    }
    false
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "conflicts/tests.rs"]
mod tests;
