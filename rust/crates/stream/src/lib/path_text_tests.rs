use super::*;
use proptest::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[path = "path_text_tests/baseline.rs"]
mod baseline;
use baseline::BaselinePath;
#[cfg(not(target_arch = "wasm32"))]
#[path = "path_text_tests/native.rs"]
mod native;

fn input() -> impl Strategy<Value = String> {
    prop_oneof![3 => "[a-zA-Z0-9_.-]{1,16}(/[a-zA-Z0-9_.-]{1,16}){0,6}", 1 => any::<String>()]
}
fn fingerprint(value: &impl Hash) -> u64 {
    let mut hash = DefaultHasher::new();
    value.hash(&mut hash);
    hash.finish()
}
fn baseline_observation(value: &str) -> Result<(String, String, Vec<String>, u64), StreamError> {
    let path = BaselinePath::new(value)?;
    let mut parents = Vec::new();
    let mut parent = path.parent();
    while let Some(path) = parent {
        parents.push(path.to_string());
        parent = path.parent();
    }
    Ok((
        path.to_string(),
        format!("{path:?}"),
        parents,
        fingerprint(&path),
    ))
}
fn observation(value: &str) -> Result<(String, String, Vec<String>, u64), StreamError> {
    let path = StreamPath::new(value)?;
    let mut parents = Vec::new();
    let mut parent = path.parent();
    while let Some(path) = parent {
        parents.push(path.to_string());
        parent = path.parent();
    }
    Ok((
        path.to_string(),
        format!("{path:?}"),
        parents,
        fingerprint(&path),
    ))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn text_storage_preserves_complete_paths_errors_parents_hashes_and_order(left in input(), right in input()) {
        prop_assert_eq!(observation(&left), baseline_observation(&left));
        prop_assert_eq!(observation(&right), baseline_observation(&right));
        if let (Ok(left), Ok(right)) = (StreamPath::new(&left), StreamPath::new(&right)) {
            prop_assert_eq!(left.cmp(&right), left.as_str().cmp(right.as_str()));
            prop_assert_eq!(left == right, left.as_str() == right.as_str());
            let map = std::collections::BTreeMap::from([(left.clone(), 7)]);
            prop_assert_eq!(map.get(left.as_str()), Some(&7));
            prop_assert_eq!(StreamPath::new(left.as_str()).unwrap(), left.clone());
            #[cfg(feature = "serde")]
            {
                let bytes = serde_json::to_vec(&left).unwrap();
                prop_assert_eq!(&bytes, &serde_json::to_vec(left.as_str()).unwrap());
                prop_assert_eq!(serde_json::from_slice::<StreamPath>(&bytes).unwrap(), left);
            }
        }
    }
    #[test]
    fn ancestors_only_expose_complete_validated_segments(text in "[a-z]{1,12}(/[a-z]{1,12}){1,8}", other in input()) {
        let path = StreamPath::new(&text).unwrap();
        for end in 0..=text.len() {
            let candidate = &text[..end];
            let ancestor = path.ancestor(candidate);
            prop_assert_eq!(ancestor.is_some(), end > 0 && (end == text.len() || text.as_bytes()[end] == b'/'));
            if let Some(ancestor) = ancestor {
                prop_assert_eq!(ancestor.as_str(), candidate);
                prop_assert_eq!(StreamPath::new(candidate).unwrap(), ancestor);
            }
        }
        let expected = !other.is_empty() && text.strip_prefix(&other)
            .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'));
        prop_assert_eq!(path.ancestor(&other).is_some(), expected);
    }
}

#[test]
fn ancestor_rejects_the_reviewed_invalid_trailing_separator() {
    let path = StreamPath::new("a/b").unwrap();
    assert!(path.ancestor("a/").is_none());
    assert!(path.ancestor("").is_none());
    assert_eq!(path.ancestor("a").unwrap().as_str(), "a");
}

#[test]
fn path_storage_is_three_words_and_old_storage_fails_the_same_bound() {
    let expected = 3 * std::mem::size_of::<usize>();
    eprintln!(
        "path_value_size before={} after={}",
        std::mem::size_of::<BaselinePath>(),
        std::mem::size_of::<StreamPath>()
    );
    assert_eq!(std::mem::size_of::<StreamPath>(), expected);
    assert_ne!(std::mem::size_of::<BaselinePath>(), expected);
}

#[cfg(not(target_arch = "wasm32"))]
std::thread_local! { static COPY_ON_CLONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn copy_on_clone() -> bool {
    COPY_ON_CLONE.with(std::cell::Cell::get)
}
