use super::*;
use proptest::prelude::*;
use std::collections::BTreeSet;

fn paths(input: Vec<Vec<u8>>) -> Vec<StreamPath> {
    input
        .into_iter()
        .map(|parts| {
            StreamPath::new(
                parts
                    .into_iter()
                    .map(|part| char::from(b'a' + part).to_string())
                    .collect::<Vec<_>>()
                    .join("/"),
            )
            .unwrap()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
fn provider(known: &[StreamPath]) -> MemoryStream {
    let provider = MemoryStream::default();
    for path in known {
        futures::executor::block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![(Bytes::from_static(b"x")).into()],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
    }
    provider
}
fn projected(state: &State, destinations: &[StreamPath]) -> BTreeSet<String> {
    destinations
        .iter()
        .flat_map(|path| {
            path.as_str()
                .match_indices('/')
                .map(|(end, _)| end)
                .chain(std::iter::once(path.as_str().len()))
                .filter_map(move |end| path.as_str().get(..end).map(str::to_owned))
        })
        .filter(|path| !state.paths.contains_key(path.as_str()))
        .collect()
}
fn result<'a>(items: impl Iterator<Item = (&'a StreamPath, usize)>) -> Vec<String> {
    items
        .filter_map(|(path, end)| path.as_str().get(..end).map(str::to_owned))
        .collect()
}
fn verify(state: &State, destinations: &[StreamPath], actual: &[String]) {
    let unique = actual.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(unique.len(), actual.len(), "duplicate path allocation");
    assert_eq!(
        unique,
        projected(state, destinations),
        "missing paths differ from hierarchy closure"
    );
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Published state contains all ancestors. Canonical destinations are sorted
    // and unique, exactly as normalize_commit supplies them to capacity checks.
    #[test]
    fn exact_path_fast_path_preserves_creation_and_capacity_accounting(
        known in prop::collection::vec(prop::collection::vec(0u8..4, 1..5), 0..9),
        destinations in prop::collection::vec(prop::collection::vec(0u8..4, 1..5), 0..9),
    ) {
        let known = paths(known);
        let destinations = paths(destinations);
        let provider = provider(&known);
        let state = futures::executor::block_on(provider.state.read());
        let actual = result(missing_paths(&state, destinations.iter()));
        verify(&state, &destinations, &actual);
        prop_assert_eq!(&actual, &result(old_missing_paths(&state, destinations.iter())));
        let (planned, _) = provider.paths(&state, destinations.iter()).unwrap();
        prop_assert_eq!(actual, planned.iter().map(|(path, _)| path.as_str().to_owned()).collect::<Vec<_>>());

    }
}

#[test]
fn ten_thousand_existing_appends_do_one_path_lookup_each() {
    let path = StreamPath::new("swarm/a00000/files").unwrap();
    let provider = provider(std::slice::from_ref(&path));
    let state = futures::executor::block_on(provider.state.read());
    let destinations = || std::iter::repeat_n(&path, 10_000);
    PATH_LOOKUPS.with(|count| count.set(0));
    assert_eq!(old_missing_paths(&state, destinations()).count(), 0);
    let before = PATH_LOOKUPS.with(std::cell::Cell::get);
    PATH_LOOKUPS.with(|count| count.set(0));
    assert_eq!(missing_paths(&state, destinations()).count(), 0);
    let after = PATH_LOOKUPS.with(std::cell::Cell::get);
    let bound = |lookups| lookups <= 10_000;
    assert!(bound(after), "existing path lookup budget exceeded");
    assert!(
        !bound(before),
        "old ancestor scan must fail the same budget"
    );
    assert_eq!(before, 30_000);
    assert_eq!(after, 10_000);
    eprintln!("existing_path_lookups appends=10000 before={before} after={after}");
}

#[test]
#[should_panic(expected = "missing paths differ from hierarchy closure")]
fn negative_control_existing_parent_does_not_imply_existing_destination() {
    let parent = StreamPath::new("swarm/agent").unwrap();
    let provider = provider(&[parent]);
    let state = futures::executor::block_on(provider.state.read());
    let destinations = [StreamPath::new("swarm/agent/new").unwrap()];
    let incorrectly_skipped = destinations.iter().filter(|path| {
        path.parent()
            .is_none_or(|parent| !contains_path(&state, parent.as_str()))
    });
    verify(
        &state,
        &destinations,
        &result(missing_paths(&state, incorrectly_skipped)),
    );
}

fn old_missing_paths<'a>(
    state: &'a State,
    destinations: impl Iterator<Item = &'a StreamPath> + Clone + 'a,
) -> impl Iterator<Item = (&'a StreamPath, usize)> + 'a {
    let mut previous = "";
    let all = destinations.clone();
    destinations.flat_map(move |destination| {
        let path = destination.as_str();
        let before = previous;
        let all = all.clone();
        previous = path;
        path.match_indices('/')
            .map(|(end, _)| end)
            .chain(std::iter::once(path.len()))
            .filter(move |end| {
                let Some(prefix) = path.get(..*end) else {
                    return false;
                };
                !contains_path(state, prefix)
                    && before != prefix
                    && !before
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                    && (*end == path.len() || !all.clone().any(|other| other.as_str() == prefix))
            })
            .map(move |end| (destination, end))
    })
}

#[test]
fn existing_commit_path_planning_scans_each_destination_once() {
    let path = StreamPath::new("swarm/a00000/files").unwrap();
    let provider = provider(std::slice::from_ref(&path));
    let state = futures::executor::block_on(provider.state.read());
    PATH_LOOKUPS.with(|count| count.set(0));
    for _ in 0..10_000 {
        // Previous production implementation counts then reconstructs the scan.
        assert_eq!(missing_paths(&state, std::iter::once(&path)).count(), 0);
        assert_eq!(missing_paths(&state, std::iter::once(&path)).count(), 0);
    }
    let before = PATH_LOOKUPS.with(std::cell::Cell::get);
    PATH_LOOKUPS.with(|count| count.set(0));
    for _ in 0..10_000 {
        assert!(
            provider
                .paths(&state, std::iter::once(&path))
                .unwrap()
                .0
                .is_empty()
        );
    }
    let after = PATH_LOOKUPS.with(std::cell::Cell::get);
    let bound = |count| count <= 10_000;
    assert!(
        bound(after),
        "commit path planning repeats the destination scan"
    );
    assert!(
        !bound(before),
        "count-then-reconstruct control passed the same bound"
    );
    assert_eq!((before, after), (20_000, 10_000));
    eprintln!("commit_path_lookups commits=10000 before={before} after={after}");
}
