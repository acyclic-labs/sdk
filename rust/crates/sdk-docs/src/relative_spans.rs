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
    use super::*;
    use proptest::prelude::*;

    fn item(filename: &str, local: bool, present: bool) -> Item {
        serde_json::from_value(serde_json::json!({
            "id":0,"crate_id":u32::from(!local),"name":"item",
            "span":present.then(|| serde_json::json!({"filename":filename,"begin":[1,0],"end":[2,0]})),
            "visibility":"public","docs":null,"links":{},"attrs":[],
            "deprecation":null,"stability":null,"const_stability":null,
            "inner":{"module":{"is_crate":false,"items":[],"is_stripped":false}}
        })).unwrap()
    }

    proptest! {
        #[test]
        fn duplicate_occurrences_never_drop_a_source_or_add_validation_work(
            inputs in prop::collection::vec(("[a-zA-Z_./\\\\:]{0,24}", any::<bool>(), any::<bool>(), 1usize..5), 0..16),
        ) {
            let mut expected = BTreeSet::new();
            let mut items = Vec::new();
            for (name, local, present, copies) in inputs {
                let base = item(&name, local, present);
                if local && present && !Path::new(&name).is_absolute() { expected.insert(std::path::PathBuf::from(&name)); }
                items.extend(std::iter::repeat_n(base, copies));
            }
            let actual = sources(&items).into_iter().map(Path::to_path_buf).collect::<BTreeSet<_>>();
            prop_assert_eq!(&actual, &expected);
            let encoded = serde_json::to_vec(&items).unwrap();
            let decoded: Vec<Item> = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(sources(&decoded).into_iter().map(Path::to_path_buf).collect::<BTreeSet<_>>(), expected);
            let repeated = vec![item("src/lib.rs", true, true); 8];
            prop_assert_eq!(sources(&repeated).len(), 1);
            // Negative control: validating occurrences would perform eight checks.
            prop_assert_ne!(sources(&repeated).len(), repeated.len());
        }
    }
}
