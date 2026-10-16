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

pub(super) fn page<V>(
    paths: &BTreeMap<String, V>,
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
    let cursor = after.filter(|_| !prefix.is_empty()).map(|after| format!("{prefix}{after}"));
    let start = match after {
        Some(after) => Excluded(cursor.as_deref().unwrap_or(after)),
        None => Included(prefix.as_str()),
    };
    // Retain at most one page plus its lookahead. Keys borrow the path index;
    // only names returned to the caller need an owned copy.
    let mut found = BTreeMap::<&str, MemoryStoreEntryKind>::new();
    for (name, value) in paths.range::<str, _>((start, Unbounded)) {
        let Some(remainder) = name.strip_prefix(&prefix) else {
            break;
        };
        let (segment, kind) = match remainder.split_once('/') {
            Some((segment, _)) => (segment, MemoryStoreEntryKind::Directory),
            None => (remainder, MemoryStoreEntryKind::File),
        };
        if segment.is_empty()
            || (path.is_empty() && segment == ".system")
            || after.is_some_and(|cursor| segment <= cursor)
        {
            continue;
        }
        if created(value).is_some_and(|created| created > generation) {
            continue;
        }
        if found.len() > maximum {
            if found.last_key_value().is_some_and(|(last, _)| segment >= *last)
                || found.contains_key(segment)
            {
                continue;
            }
            found.pop_last();
        }
        // Full-path order differs from entry order around punctuation before
        // '/'; later paths can introduce a smaller directory entry.
        found.entry(segment).or_insert(kind);
    }
    let has_more = found.len() > maximum;
    let entries = found
        .into_iter()
        .take(maximum)
        .map(|(name, kind)| MemoryStoreEntry { name: name.to_owned(), kind })
        .collect();
    MemoryStorePage {
        generation,
        entries,
        has_more,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
