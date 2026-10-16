use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;
#[path = "reference.rs"]
mod reference;

type Paths = BTreeMap<String, Option<u64>>;
struct Fixture {
    paths: Paths,
    index: DirectoryIndex,
}
fn indexed(paths: &Paths) -> DirectoryIndex {
    let mut ordered: Vec<_> = paths.iter().collect();
    ordered.sort_by_key(|(_, generation)| generation.unwrap_or(0));
    let mut index = DirectoryIndex::default();
    for (path, generation) in ordered {
        index.publish(path, generation.unwrap_or(0));
    }
    index
}
fn fixture(count: usize, nested: bool) -> Fixture {
    let paths = (0..count)
        .map(|index| {
            let name = if nested {
                format!("files/one/{index:08}")
            } else {
                format!("files/{index:08}")
            };
            (name, Some(1))
        })
        .collect();
    let index = indexed(&paths);
    Fixture { paths, index }
}
fn selected(fixture: &Fixture, path: &str, maximum: usize, legacy: bool) -> MemoryStorePage {
    if legacy {
        reference::expected(&fixture.paths, path, 1, None, maximum, |created| *created)
    } else {
        fixture.index.page(path, 1, None, maximum, || {})
    }
}
fn measured(
    fixture: &Fixture,
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
        result = selected(fixture, path, maximum, legacy);
    });
    (allocations, result)
}
#[test]
fn page_storage_is_independent_of_retained_children() {
    for nested in [false, true] {
        let small = fixture(4, nested);
        let large = fixture(4096, nested);
        let (first, expected) = measured(&small, "files", 1, false);
        let (last, actual) = measured(&large, "files", 1, false);
        assert_eq!(actual, expected);
        assert_eq!(
            (last.count_total, last.bytes_total, last.bytes_max),
            (first.count_total, first.bytes_total, first.bytes_max)
        );
        assert_eq!(
            last.count_total, 2,
            "only the result vector and returned name allocate"
        );
        let (legacy_first, _) = measured(&small, "files", 1, true);
        let (legacy_last, _) = measured(&large, "files", 1, true);
        assert!(
            legacy_last.bytes_total > legacy_first.bytes_total,
            "the old scan must fail the same storage property"
        );
    }
}
#[test]
fn empty_root_allocates_nothing() {
    let fixture = Fixture {
        paths: BTreeMap::new(),
        index: DirectoryIndex::default(),
    };
    let (allocation, page) = measured(&fixture, "", 1, false);
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
#[test]
fn bounded_pages_seek_past_retained_subtrees_and_stop_after_lookahead() {
    for (nested, path, after) in [
        (false, "files", None),
        (true, "files", None),
        (false, "files", Some("00002047")),
        (false, "missing", None),
    ] {
        let fixture = fixture(4096, nested);
        let mut reads = 0;
        let actual = fixture.index.page(path, 1, after, 1, || {
            reads += 1;
        });
        assert_eq!(
            actual,
            reference::expected(&fixture.paths, path, 1, after, 1, |created| *created)
        );
        assert!(
            reads <= 128,
            "nested={nested} path={path} after={after:?} probes={reads}"
        );
        let legacy_reads = std::cell::Cell::new(0);
        reference::expected(&fixture.paths, path, 1, after, 1, |created| {
            legacy_reads.set(legacy_reads.get() + 1);
            *created
        });
        assert_eq!(
            legacy_reads.get(),
            4096,
            "negative control detects the full scan"
        );
    }
}
#[test]
fn listing_reads_no_generation_values_outside_its_directory_and_cursor() {
    let flat = fixture(4096, false);
    let mut probes = 0;
    let actual = flat.index.page("missing", 1, None, 1, || {
        probes += 1;
    });
    assert!(actual.entries.is_empty());
    assert_eq!(probes, 0);
    let mut probes = 0;
    let actual = flat.index.page("files", 1, Some("00002047"), 1, || {
        probes += 1;
    });
    assert_eq!(actual.entries[0].name, "00002048");
    assert!(probes <= 128);
    let mut probes = 0;
    let actual = flat.index.page("", 1, Some("files"), 1, || {
        probes += 1;
    });
    assert!(actual.entries.is_empty());
    assert!(probes <= 2);
    let nested = fixture(4096, true);
    let mut probes = 0;
    let actual = nested.index.page("files", 1, None, 1, || {
        probes += 1;
    });
    assert_eq!(actual.entries[0].name, "one");
    assert!(probes <= 4);
}
#[test]
fn retained_index_storage_is_linear_and_rewrites_allocate_nothing() {
    for count in [256, 4096] {
        let paths: Vec<_> = (0..count).map(|item| format!("files/{item:08}")).collect();
        let mut index = DirectoryIndex::default();
        let allocation = allocation_counter::measure(|| {
            for (generation, path) in paths.iter().enumerate() {
                index.publish(path, 1 + generation as u64);
            }
        });
        index.assert_linear_storage();
        eprintln!(
            "index entries={count} allocations={} bytes={} retained={} peak={}",
            allocation.count_total,
            allocation.bytes_total,
            allocation.bytes_current,
            allocation.bytes_max
        );
        let rewrites = allocation_counter::measure(|| {
            for (generation, path) in paths.iter().enumerate() {
                index.publish(path, 1 + count as u64 + generation as u64);
            }
        });
        assert_eq!(
            (
                rewrites.count_total,
                rewrites.bytes_total,
                rewrites.bytes_max
            ),
            (0, 0, 0)
        );
        index.assert_linear_storage();
    }
}

#[test]
fn future_publications_cannot_turn_a_pinned_page_into_a_scan() {
    let mut index = DirectoryIndex::default();
    let mut paths = Paths::new();
    for path in ["files/z-first", "files/z-last"] {
        index.publish(path, 1);
        paths.insert(path.into(), Some(1));
    }
    let first = index.page("files", 1, None, 1, || {});
    for future in 0..65_536 {
        let path = format!("files/a-{future:08}");
        let generation = 2 + future;
        index.publish(&path, generation);
        paths.insert(path, Some(generation));
    }
    for after in [None, Some(first.entries[0].name.as_str())] {
        let mut probes = 0;
        let actual = index.page("files", 1, after, 1, || {
            probes += 1;
        });
        let legacy_reads = std::cell::Cell::new(0);
        let expected = reference::expected(&paths, "files", 1, after, 1, |created| {
            legacy_reads.set(legacy_reads.get() + 1);
            *created
        });
        assert_eq!(actual, expected);
        assert!(probes <= 64, "pinned page used {probes} index probes");
        assert!(
            legacy_reads.get() > 65_536,
            "negative control must fail the identical 64-probe bound"
        );
    }
    index.assert_linear_storage();
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
    let index = indexed(&paths);
    for path in ["", "foo", "foo/é", "absent", "foo/"] {
        for generation in [0, 1, 2, 4, u64::MAX] {
            for maximum in [0, 1, 2, 4, usize::MAX] {
                for after in [None, Some(""), Some("foo"), Some("foo-.txt"), Some("é")] {
                    assert_eq!(
                        index.page(path, generation, after, maximum, || {}),
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
    // Every historical root has exactly the original visible, sorted children;
    // paging survives publications between requests and JSON round-trips.
    #[test]
    fn selection_matches_reference_and_roundtrips(
        rows in prop::collection::vec((prop_oneof!["[a-zA-Z0-9._#/é-]{0,20}", prop::collection::vec(any::<char>(), 0..20).prop_map(|chars| chars.into_iter().collect::<String>())], prop::option::of(0..8_u64)), 0..96),
        path in prop_oneof![Just(String::new()), Just("foo".to_owned()), Just("notes".to_owned()), "[a-z/é.-]{0,12}"],
        after in prop::option::of("[a-zA-Z0-9._#é-]{0,20}"),
        generation in prop_oneof![0..8_u64, Just(u64::MAX)],
        maximum in prop_oneof![1..32_usize, Just(usize::MAX)],
    ) {
        let mut paths: Paths = rows.iter().cloned().collect();
        for (name, created) in &rows { paths.insert(format!("foo/{name}"), *created); }
        let mut index = indexed(&paths);
        index.assert_invariants();
        let actual = index.page(&path,generation,after.as_deref(),maximum,||{});
        let encoded = serde_json::to_vec(&actual)?;
        let decoded: MemoryStorePage = serde_json::from_slice(&encoded)?;
        prop_assert_eq!(&decoded,&actual);
        prop_assert_eq!(&actual,&reference::expected(&paths,&path,generation,after.as_deref(),maximum,|created|*created));
        if generation != u64::MAX {
            let cursor = actual.entries.last().map(|entry| entry.name.as_str());
            let expected = reference::expected(&paths,&path,generation,cursor,maximum,|created|*created);
            for (offset, (name, _)) in rows.iter().enumerate() { index.publish(&format!("foo/future/{name}"), 8 + offset as u64); }
            let mut probes = 0;
            let next = index.page(&path,generation,cursor,maximum,||{probes += 1;});
            prop_assert_eq!(next,expected);
            let log = usize::BITS - (paths.len().max(1)).leading_zeros();
            prop_assert!(probes <= 8 * (maximum.min(paths.len()) + log as usize + 1));
            index.assert_invariants();
        }
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
        let fixture = fixture(4096, nested);
        for &legacy in implementations {
            let mut times = Vec::with_capacity(32);
            for _ in 0..32 {
                if syscalls {
                    eprintln!("DIRECTORY_BEGIN");
                }
                let started = std::time::Instant::now();
                let page = selected(&fixture, path, 1, legacy);
                let elapsed = started.elapsed();
                if syscalls {
                    eprintln!("DIRECTORY_END");
                }
                times.push(elapsed);
                std::hint::black_box(page);
            }
            times.sort_unstable();
            let (allocation, _) = measured(&fixture, path, 1, legacy);
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
