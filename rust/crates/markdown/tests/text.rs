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

// Freeze the previous flattening algorithm independently of Text::as_str.
fn previous_flatten(text: &Text) -> Cow<'_, str> {
    let mut chunks = text.reverse_chunks();
    let first = chunks.next().unwrap();
    let Some(second) = chunks.next() else {
        return Cow::Borrowed(first);
    };
    let mut pieces = vec![first, second];
    pieces.extend(chunks);
    let mut value = String::with_capacity(text.len());
    for chunk in pieces.into_iter().rev() {
        value.push_str(chunk);
    }
    Cow::Owned(value)
}

fn fragmented_pair() -> (Text, Text) {
    let mut first = Text::from("a");
    let snapshot = first.clone();
    first.push_str("b");
    drop(snapshot);
    (first, Text::from("cd"))
}

fn exact_flatten_cost(cost: acyclic_test_support::AllocationInfo) {
    assert_eq!(cost.count_total, 1);
    assert_eq!(cost.bytes_total, 4);
}

#[test]
fn fragmented_canonical_run_flattens_into_one_exact_buffer() {
    use acyclic_test_support::{differential, measure};
    let (old_first, old_second) = fragmented_pair();
    let (first, second) = fragmented_pair();
    let mut old_cost = Default::default();
    let mut cost = Default::default();
    let (output, _) = differential(
        || {
            let mut output = String::new();
            old_cost = measure(|| {
                output = String::with_capacity(4);
                for part in [&old_first, &old_second] {
                    output.push_str(&previous_flatten(part));
                }
            });
            output
        },
        || {
            let view = crate::TextRef::Many(vec![&first, &second]);
            let mut output = Cow::Borrowed("");
            let copied = crate::work::parts()[1];
            cost = measure(|| output = view.as_str());
            assert_eq!(crate::work::parts()[1] - copied, 4);
            output.into_owned()
        },
    );
    assert_eq!(output, "abcd");
    exact_flatten_cost(cost);
    assert_eq!((old_cost.count_total, old_cost.bytes_total), (3, 38));
    assert!(std::panic::catch_unwind(|| exact_flatten_cost(old_cost)).is_err());
}

#[test]
fn concatenated_text_reuses_its_chunk_walk_when_flattening() {
    let baseline: Text = [Text::from("ab"), Text::from("cd")].into_iter().collect();
    let candidate: Text = [Text::from("ab"), Text::from("cd")].into_iter().collect();
    let mut before = Default::default();
    let mut after = Default::default();
    acyclic_test_support::differential(
        || {
            let mut value = Cow::Borrowed("");
            before = acyclic_test_support::measure(|| value = previous_flatten(&baseline));
            value.into_owned()
        },
        || {
            let mut value = Cow::Borrowed("");
            let copied = crate::work::parts()[1];
            after = acyclic_test_support::measure(|| value = candidate.as_str());
            assert_eq!(crate::work::parts()[1] - copied, 4);
            value.into_owned()
        },
    );
    let stack = 4 * std::mem::size_of::<(&Node, std::ops::Range<usize>)>() as u64;
    assert_eq!((before.count_total, before.bytes_total), (3, stack + 36));
    let exact = |cost: acyclic_test_support::AllocationInfo| {
        assert_eq!((cost.count_total, cost.bytes_total), (2, stack + 4));
    };
    exact(after);
    assert!(std::panic::catch_unwind(|| exact(before)).is_err());
}
