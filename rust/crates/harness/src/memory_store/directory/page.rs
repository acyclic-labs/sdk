use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

/// One entry in a bounded directory page.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MemoryStoreEntry {
    pub(crate) name: String,
    pub(crate) kind: MemoryStoreEntryKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MemoryStoreEntryKind {
    File,
    Directory,
}

/// A generation-pinned page produced by the shared content core.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemoryStorePage {
    pub(crate) generation: u64,
    pub(crate) entries: Vec<MemoryStoreEntry>,
    pub(crate) has_more: bool,
}

pub(super) fn page<K: Borrow<str> + Ord, V>(
    paths: &BTreeMap<K, V>,
    path: &str,
    generation: u64,
    after: Option<&str>,
    maximum: usize,
    created: impl Fn(&V) -> Option<u64>,
) -> MemoryStorePage {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };
    let mut seek = String::new();
    let (mut found, last_path) = select(
        paths, &prefix, generation, after, maximum, &created, &mut seek,
    );
    if let (Some(last_path), Some((&largest, _))) = (last_path, found.last_key_value()) {
        // An unseen smaller entry must be a proper prefix of the largest
        // candidate, with '/' sorting after its next character. Other smaller
        // names sort before an already visited path and cannot appear later.
        let mut copied = None;
        for (offset, character) in largest.char_indices() {
            if offset == 0 || character >= '/' {
                continue;
            }
            let Some(segment) = largest.get(..offset) else {
                continue;
            };
            if (prefix.is_empty() && segment == ".system")
                || after.is_some_and(|cursor| segment <= cursor)
                || found.contains_key(segment)
                || found
                    .last_key_value()
                    .is_some_and(|(last, _)| segment >= *last)
            {
                continue;
            }
            if let Some(previous) = copied {
                seek.truncate(prefix.len() + previous);
            } else {
                seek.clear();
                seek.reserve(prefix.len() + offset + 1);
                seek.push_str(&prefix);
            }
            let Some(suffix) = largest.get(copied.unwrap_or(0)..offset) else {
                continue;
            };
            seek.reserve(suffix.len() + 1);
            seek.push_str(suffix);
            seek.push('/');
            copied = Some(offset);
            // This subtree was already resolved if it precedes the final path
            // visited by select; do not reread generation-pinned histories.
            if seek.as_str() <= last_path {
                continue;
            }
            let visible = paths
                .range::<str, _>((Included(seek.as_str()), Unbounded))
                .take_while(|(name, _)| {
                    let name: &str = (*name).borrow();
                    name.starts_with(&seek)
                })
                .any(|(_, value)| created(value).is_none_or(|created| created <= generation));
            if visible {
                found.pop_last();
                found.insert(segment, MemoryStoreEntryKind::Directory);
            }
        }
    }
    let has_more = found.len() > maximum;
    let entries = found
        .into_iter()
        .take(maximum)
        .map(|(name, kind)| MemoryStoreEntry {
            name: name.to_owned(),
            kind,
        })
        .collect();
    MemoryStorePage {
        generation,
        entries,
        has_more,
    }
}

fn select<'a, K: Borrow<str> + Ord, V>(
    paths: &'a BTreeMap<K, V>,
    prefix: &str,
    generation: u64,
    after: Option<&str>,
    maximum: usize,
    created: &impl Fn(&V) -> Option<u64>,
    seek: &mut String,
) -> (BTreeMap<&'a str, MemoryStoreEntryKind>, Option<&'a str>) {
    if let Some(after) = after.filter(|_| !prefix.is_empty()) {
        seek.reserve(prefix.len() + after.len());
        seek.push_str(prefix);
        seek.push_str(after);
    }
    let start = match after {
        Some(after) => Excluded(if prefix.is_empty() {
            after
        } else {
            seek.as_str()
        }),
        None => Included(prefix),
    };
    let mut rows = paths.range::<str, _>((start, Unbounded));
    let mut found = BTreeMap::new();
    while let Some((name, value)) = rows.next() {
        let name: &str = name.borrow();
        let Some(remainder) = name.strip_prefix(prefix) else {
            break;
        };
        let (segment, kind) = match remainder.split_once('/') {
            Some((segment, _)) => (segment, MemoryStoreEntryKind::Directory),
            None => (remainder, MemoryStoreEntryKind::File),
        };
        let excluded = segment.is_empty()
            || (prefix.is_empty() && segment == ".system")
            || after.is_some_and(|cursor| segment <= cursor);
        let known = kind == MemoryStoreEntryKind::Directory && found.contains_key(segment);
        let visible =
            !excluded && !known && created(value).is_none_or(|created| created <= generation);
        if visible {
            found.entry(segment).or_insert(kind);
            if found.len() > maximum {
                return (found, Some(name));
            }
        }
        if kind == MemoryStoreEntryKind::Directory && (excluded || known || visible) {
            // '/' is immediately before '0'. Seeking here skips exactly this
            // descendant subtree; punctuation siblings have already been seen.
            seek.clear();
            seek.reserve(prefix.len() + segment.len() + 1);
            seek.push_str(prefix);
            seek.push_str(segment);
            seek.push('0');
            rows = paths.range::<str, _>((Included(seek.as_str()), Unbounded));
        }
    }
    (found, None)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
