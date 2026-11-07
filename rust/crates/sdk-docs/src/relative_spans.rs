use rustdoc_types::Item;
use std::{collections::BTreeSet, path::Path};

// Validation is a predicate on source paths, independent of how many Rustdoc
// items reference them. Keep every distinct local relative source, including
// invalid spellings for the existing ownership/reparse guard to reject.
pub(crate) fn sources<'a>(items: impl IntoIterator<Item = &'a Item>) -> BTreeSet<&'a Path> {
    items
        .into_iter()
        .filter(|item| item.crate_id == 0)
        .filter_map(|item| item.span.as_ref())
        .map(|span| span.filename.as_path())
        .filter(|path| !path.is_absolute())
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn source_selection_keeps_only_local_relative_paths(name in any::<String>()) {
            let decoded = crate::public_api::decode::from_slice(include_bytes!(
                "../fuzz/corpus/rustdoc_adapter/valid.json"
            )).unwrap();
            let prototype = decoded.index.values().next().unwrap();
            crate::relative_spans_oracle::check(prototype, name.as_bytes());
        }
    }
}
