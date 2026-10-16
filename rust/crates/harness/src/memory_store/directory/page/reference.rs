use super::{MemoryStoreEntry, MemoryStoreEntryKind, MemoryStorePage};
use std::borrow::Borrow;
use std::collections::BTreeMap;

pub fn expected<K: Borrow<str> + Ord, V>(
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
    let mut found = BTreeMap::<String, MemoryStoreEntryKind>::new();
    for (name, value) in paths {
        let name: &str = name.borrow();
        if created(value).is_some_and(|created| created > generation) || !name.starts_with(&prefix)
        {
            continue;
        }
        let Some(remainder) = name.strip_prefix(&prefix) else {
            continue;
        };
        let segment = remainder.split('/').next().unwrap_or_default();
        if segment.is_empty() || (path.is_empty() && segment == ".system") {
            continue;
        }
        let kind = if remainder.contains('/') {
            MemoryStoreEntryKind::Directory
        } else {
            MemoryStoreEntryKind::File
        };
        found.entry(segment.to_owned()).or_insert(kind);
    }
    let mut names = found
        .into_iter()
        .filter(|(name, _)| after.is_none_or(|cursor| name.as_str() > cursor));
    let entries = names
        .by_ref()
        .take(maximum)
        .map(|(name, kind)| MemoryStoreEntry { name, kind })
        .collect();
    let has_more = names.next().is_some();
    MemoryStorePage {
        generation,
        entries,
        has_more,
    }
}
