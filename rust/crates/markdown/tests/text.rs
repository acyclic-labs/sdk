use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/text.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn shared_snapshots_follow_the_string_model(
        actions in prop::collection::vec((0u8..6, prop::collection::vec(any::<char>(), 0..8), any::<u8>(), any::<u8>()), 0..64),
    ) {
        let mut actual = Text::default();
        let mut expected = String::new();
        let mut saved = vec![(actual.clone(), expected.clone())];
        for (action, chars, first, last) in actions {
            let part: String = chars.into_iter().collect();
            match action {
                0 => { actual.push_str(&part); expected.push_str(&part); }
                1 => saved.push((actual.clone(), expected.clone())),
                2 => {
                    let index = usize::from(first) % saved.len();
                    let (snapshot, string) = saved.get(index).expect("saved value");
                    actual = snapshot.clone(); expected = string.clone();
                }
                3 => {
                    let first = usize::from(first) % (expected.len() + 2);
                    let last = usize::from(last) % (expected.len() + 2);
                    let substring = actual.get(first..last);
                    let expected_substring = expected.get(first..last);
                    prop_assert_eq!(substring.as_ref().map(Text::as_str), expected_substring.map(Cow::Borrowed));
                    if let (Some(value), Some(string)) = (substring, expected_substring) {
                        actual = value; expected = string.to_owned();
                    }
                }
                4 => {
                    let boundaries: Vec<_> = expected.char_indices().map(|(offset, _)| offset).chain([expected.len()]).collect();
                    let index = usize::from(first) % boundaries.len();
                    let cut = *boundaries.get(index).expect("UTF-8 boundary");
                    actual.truncate(cut); expected.truncate(cut);
                }
                _ => {
                    actual.extend([Text::from(part.clone())]); expected.push_str(&part);
                }
            }
            prop_assert_eq!(actual.len(), expected.len());
            let rendered = actual.as_str();
            prop_assert_eq!(rendered.as_ref(), expected.as_str());
            prop_assert_eq!(&actual, &Text::from(expected.as_str()));
            for (snapshot, string) in &saved { prop_assert_eq!(snapshot, string); }
        }
    }
}

fn measure(count: usize, copy: bool) -> (usize, usize) {
    COPY_SNAPSHOTS.set(copy);
    let mut value = Text::default();
    let mut snapshots = Vec::new();
    let before = crate::work::current();
    for _ in 0..count {
        value.push_str("x");
        if snapshots.len() == snapshots.capacity() {
            crate::work::copy(snapshots.len() * std::mem::size_of::<Text>());
        }
        snapshots.push(value.clone());
    }
    let work = crate::work::current() - before;
    COPY_SNAPSHOTS.set(false);
    let mut seen = HashSet::new();
    let mut bytes = snapshots.capacity() * std::mem::size_of::<Text>();
    for snapshot in snapshots {
        let (payload, metadata) = snapshot.retained(&mut seen);
        bytes += payload + metadata;
    }
    (work, bytes)
}

#[test]
fn text_snapshots_have_linear_copy_work_and_storage() {
    for count in [64, 128, 256, 1024] {
        let small = measure(count, false);
        let large = measure(count * 2, false);
        assert!(large.0 <= 2 * small.0 + 1024);
        assert!(large.1 <= 2 * small.1 + 1024);
    }
}

#[test]
fn work_property_detects_copying_each_snapshot() {
    let small = measure(64, true);
    let large = measure(128, true);
    assert!(large.0 > 2 * small.0 + 1024);
    assert!(large.1 > 2 * small.1 + 1024);
}

#[test]
fn dropping_a_deep_shared_prefix_does_not_recurse() {
    let mut value = Text::default();
    for _ in 0..20_000 {
        let snapshot = value.clone();
        value.push_str("x");
        drop(snapshot);
    }
    drop(value);
}
