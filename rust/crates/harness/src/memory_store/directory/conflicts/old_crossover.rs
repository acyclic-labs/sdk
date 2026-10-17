// Reviewed selector with its original cutoff. Keep independent as a negative control.
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    ops::Bound::{Excluded, Included, Unbounded},
};

pub(crate) fn paths_conflict(existing: &str, path: &str) -> bool {
    existing != path
        && (existing
            .strip_prefix(path)
            .is_some_and(|suffix| suffix.starts_with('/'))
            || path
                .strip_prefix(existing)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

pub(crate) const DIRECT_SCAN_PATHS: usize = 4;

#[inline]
pub(crate) fn conflicts<K: Borrow<str> + Ord, V>(paths: &BTreeMap<K, V>, path: &str) -> bool {
    if paths.len() <= DIRECT_SCAN_PATHS {
        return paths
            .keys()
            .any(|existing| paths_conflict(existing.borrow(), path));
    }
    ordered_conflicts(paths, path)
}

fn ordered_conflicts<K: Borrow<str> + Ord, V>(paths: &BTreeMap<K, V>, path: &str) -> bool {
    let mut leading = paths.keys();
    let Some(first) = leading.next() else {
        return false;
    };
    let first: &str = first.borrow();
    if paths_conflict(first, path)
        || leading
            .take(3)
            .any(|existing| paths_conflict(existing.borrow(), path))
    {
        return true;
    }
    if let Some((previous, _)) = paths
        .range::<str, _>((Unbounded, Excluded(path)))
        .next_back()
    {
        let previous: &str = previous.borrow();
        let common = path
            .bytes()
            .zip(previous.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        if common == previous.len() && path.as_bytes().get(common) == Some(&b'/') {
            return true;
        }
        // A possible ancestor is a prefix of both the query and preceding key.
        // Include the slash at the mismatch; byte scans avoid slicing a partial
        // UTF-8 character when the two keys differ within its encoding.
        for (offset, byte) in path.as_bytes().iter().take(common + 1).enumerate().rev() {
            if *byte == b'/' {
                let Some(prefix) = path.get(..offset) else {
                    continue;
                };
                // All shorter prefixes sort before this one. The first key was
                // already checked, so none can exist at or below that bound.
                if prefix <= first {
                    break;
                }
                if paths.contains_key::<str>(prefix) {
                    return true;
                }
            }
        }
    }
    if paths.last_key_value().is_some_and(|(last, _)| {
        let last: &str = last.borrow();
        last <= path
    }) {
        return false;
    }
    let Some((following, _)) = paths.range::<str, _>((Excluded(path), Unbounded)).next() else {
        return false;
    };
    let following: &str = following.borrow();
    let Some(suffix) = following.strip_prefix(path) else {
        return false;
    };
    if suffix.starts_with('/') {
        return true;
    }
    // Punctuation can sort between the path and its slash-prefixed subtree:
    // foo-.txt precedes foo/child. Seek across that interval without scanning.
    if suffix.as_bytes().first().is_none_or(|byte| *byte >= b'/') {
        return false;
    }
    let mut small = arrayvec::ArrayString::<64>::new();
    let mut large;
    let prefix = if path.len() < small.capacity() {
        small.push_str(path);
        small.push('/');
        small.as_str()
    } else {
        large = String::with_capacity(path.len() + 1);
        large.push_str(path);
        large.push('/');
        large.as_str()
    };
    paths
        .range::<str, _>((Included(prefix), Unbounded))
        .next()
        .is_some_and(|(name, _)| {
            let name: &str = name.borrow();
            name.starts_with(prefix)
        })
}
