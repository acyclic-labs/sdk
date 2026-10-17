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
        let (existing_prefix, existing_suffix) = existing.as_bytes().split_at(common);
        let (path_prefix, path_suffix) = path.as_bytes().split_at(common);
        match existing_prefix.cmp(path_prefix) {
            Ordering::Less => {}
            Ordering::Greater => return false,
            Ordering::Equal => match (existing_suffix.first(), path_suffix.first()) {
                (None, Some(b'/')) | (Some(b'/'), None) => return true,
                (Some(byte), None) if *byte > b'/' => return false,
                _ => {}
            },
        }
    }
    false
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "conflicts/tests.rs"]
mod tests;
