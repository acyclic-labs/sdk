use super::*;
use proptest::prelude::*;
#[path = "reference.rs"]
mod reference;

fn fixture(count: usize, nested: bool) -> BTreeMap<String, Option<u64>> {
    (0..count)
        .map(|index| {
            let name = if nested {
                format!("files/one/{index:08}")
            } else {
                format!("files/{index:08}")
            };
            (name, Some(1))
        })
        .collect()
}

fn selected(
    paths: &BTreeMap<String, Option<u64>>,
    path: &str,
    maximum: usize,
    legacy: bool,
) -> MemoryStorePage {
    if legacy {
        reference::expected(paths, path, 1, None, maximum, |created| *created)
    } else {
        page(paths, path, 1, None, maximum, |created| *created)
    }
}

fn measured(
    paths: &BTreeMap<String, Option<u64>>,
    path: &str,
    maximum: usize,
    legacy: bool,
) -> (allocation_counter::AllocationInfo, MemoryStorePage) {
    let mut result = MemoryStorePage {
        generation: 1,
        entries: Vec::new(),
        has_more: false,
    };
    let allocations = allocation_counter::measure(|| {
        result = selected(paths, path, maximum, legacy);
    });
    (allocations, result)
}

#[test]
fn page_storage_is_independent_of_retained_children() {
    assert!(storage_is_bounded(false));
    assert!(
        !storage_is_bounded(true),
        "the original implementation must fail the same storage bound"
    );
}

fn storage_is_bounded(legacy: bool) -> bool {
    let mut bounded = true;
    for nested in [false, true] {
        let small = fixture(4, nested);
        let large = fixture(4096, nested);
        let (first, expected) = measured(&small, "files", 1, legacy);
        let (last, actual) = measured(&large, "files", 1, legacy);
        assert_eq!(actual, expected);
        bounded &= (last.count_total, last.bytes_total, last.bytes_max)
            == (first.count_total, first.bytes_total, first.bytes_max);
    }
    bounded
}

#[test]
fn empty_root_allocates_nothing() {
    let (allocation, page) = measured(&BTreeMap::new(), "", 1, false);
    assert!(page.entries.is_empty());
    assert!(!page.has_more);
    assert_eq!(
        (
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_max
        ),
        (0, 0, 0)
    );
}

struct CountedKey {
    name: String,
    reads: std::rc::Rc<std::cell::Cell<usize>>,
}

impl std::borrow::Borrow<str> for CountedKey {
    fn borrow(&self) -> &str {
        self.reads.set(self.reads.get() + 1);
        &self.name
    }
}

impl PartialEq for CountedKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for CountedKey {}
impl PartialOrd for CountedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CountedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

#[test]
fn bounded_pages_seek_past_retained_subtrees_and_stop_after_lookahead() {
    for (nested, path, after) in [
        (false, "files", None),
        (true, "files", None),
        (false, "files", Some("00002047")),
        (false, "missing", None),
    ] {
        let source = fixture(4096, nested);
        let expected = reference::expected(&source, path, 1, after, 1, |created| *created);
        let reads = std::rc::Rc::new(std::cell::Cell::new(0));
        let paths = source
            .into_iter()
            .map(|(name, generation)| {
                (
                    CountedKey {
                        name,
                        reads: reads.clone(),
                    },
                    generation,
                )
            })
            .collect();
        let actual = page(&paths, path, 1, after, 1, |created| *created);
        assert_eq!(actual, expected);
        assert!(
            reads.get() <= 128,
            "nested={nested} path={path} after={after:?} reads={}",
            reads.get()
        );
        reads.set(0);
        assert_eq!(
            reference::expected(&paths, path, 1, after, 1, |created| *created),
            expected
        );
        assert_eq!(
            reads.get(),
            4096,
            "negative control must detect the original full scan"
        );
    }
}

#[test]
fn listing_reads_no_generation_values_outside_its_directory_and_cursor() {
    let paths = fixture(4096, false);
    for (path, after, expected_reads) in [
        ("missing", None, 0),
        ("files", Some("00002047"), 2),
        ("", Some("files"), 0),
    ] {
        let reads = std::cell::Cell::new(0);
        let actual = page(&paths, path, 1, after, 1, |created| {
            reads.set(reads.get() + 1);
            *created
        });
        assert_eq!(
            actual,
            reference::expected(&paths, path, 1, after, 1, |created| *created)
        );
        assert_eq!(reads.get(), expected_reads, "path={path} after={after:?}");
    }
    let reads = std::cell::Cell::new(0);
    let nested = fixture(4096, true);
    let actual = page(&nested, "files", 1, None, 1, |created| {
        reads.set(reads.get() + 1);
        *created
    });
    assert_eq!(
        actual,
        reference::expected(&nested, "files", 1, None, 1, |created| *created)
    );
    assert_eq!(
        reads.get(),
        1,
        "a known directory needs no more visibility reads"
    );
}

#[test]
fn punctuation_generation_and_cursor_boundaries() {
    let paths = [
        ("foo", Some(3)),
        ("foo-.txt", Some(1)),
        ("foo/first", Some(1)),
        ("foo/future", Some(4)),
        ("foo/last", None),
        ("foo.bar", Some(2)),
        ("foo-.bar", Some(1)),
        ("foo-.baz", Some(1)),
        ("foo-/child", Some(1)),
        (".system/secret", Some(1)),
        (".system-safe", Some(1)),
        ("é/child", Some(1)),
        ("foo/é/end", Some(4)),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value))
    .collect();
    for path in ["", "foo", "foo/é", "absent", "foo/"] {
        for generation in [0, 1, 2, 4, u64::MAX] {
            for maximum in [1, 2, 4, usize::MAX] {
                for after in [None, Some(""), Some("foo"), Some("foo-.txt"), Some("é")] {
                    let actual = page(&paths, path, generation, after, maximum, |created| *created);
                    assert_eq!(
                        actual,
                        reference::expected(&paths, path, generation, after, maximum, |created| {
                            *created
                        })
                    );
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Property: page content, sort order, visibility, cursor and lookahead equal
    // the original full-map selection for arbitrary path state, and round-trip.
    #[test]
    fn selection_matches_reference_and_roundtrips(
        rows in prop::collection::vec((prop_oneof!["[a-zA-Z0-9._#/é-]{0,20}", prop::collection::vec(any::<char>(), 0..20).prop_map(|chars| chars.into_iter().collect::<String>())], prop::option::of(0..8_u64)), 0..96),
        path in prop_oneof![Just(String::new()), Just("foo".to_owned()), Just("notes".to_owned()), "[a-z/é.-]{0,12}"],
        after in prop::option::of("[a-zA-Z0-9._#é-]{0,20}"),
        generation in prop_oneof![0..8_u64, Just(u64::MAX)],
        maximum in prop_oneof![1..32_usize, Just(usize::MAX)],
    ) {
        let mut paths: BTreeMap<_, _> = rows.iter().cloned().collect();
        for (name, created) in &rows {
            paths.insert(format!("foo/{name}"), *created);
        }
        let actual = page(&paths, &path, generation, after.as_deref(), maximum, |created| *created);
        let encoded = serde_json::to_vec(&actual)?;
        let decoded: MemoryStorePage = serde_json::from_slice(&encoded)?;
        prop_assert_eq!(&decoded, &actual);
        prop_assert_eq!(actual, reference::expected(&paths, &path, generation, after.as_deref(), maximum, |created| *created));
    }
}

#[test]
fn listing_without_subscriber() {
    profile(false);
}
#[test]
fn listing_with_tracing() {
    profile(true);
}

fn profile(tracing: bool) {
    let dispatch = if tracing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _dispatch = tracing::dispatcher::set_default(&dispatch);
    let implementation = std::env::var("HARNESS_DIRECTORY_PROFILE").ok();
    let implementations: &[bool] = match implementation.as_deref() {
        Some("before") => &[true],
        Some("after") => &[false],
        _ => &[true, false],
    };
    let syscalls = std::env::var_os("HARNESS_DIRECTORY_SYSCALLS").is_some();
    for (label, nested, path) in [
        ("distinct", false, "files"),
        ("nested", true, "files"),
        ("absent", false, "missing"),
    ] {
        let paths = fixture(4096, nested);
        for &legacy in implementations {
            let mut times = Vec::with_capacity(32);
            for _ in 0..32 {
                if syscalls {
                    eprintln!("DIRECTORY_BEGIN");
                }
                let started = std::time::Instant::now();
                let page = selected(&paths, path, 1, legacy);
                let elapsed = started.elapsed();
                if syscalls {
                    eprintln!("DIRECTORY_END");
                }
                times.push(elapsed);
                std::hint::black_box(page);
            }
            times.sort_unstable();
            let (allocation, _) = measured(&paths, path, 1, legacy);
            eprintln!(
                "listing tracing={tracing} legacy={legacy} case={label} median={:?} allocations={} bytes={} peak={}",
                times.get(16),
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max
            );
        }
    }
}
