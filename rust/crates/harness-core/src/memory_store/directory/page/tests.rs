use super::index::Probe;
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
#[derive(Clone, Copy, Debug)]
enum Implementation {
    Scan,
    Counted,
    SinglePass,
}
use Implementation::*;

fn selected(
    fixture: &Fixture,
    path: &str,
    after: Option<&str>,
    maximum: usize,
    implementation: Implementation,
) -> MemoryStorePage {
    match implementation {
        Scan => reference::expected(&fixture.paths, path, 1, after, maximum, |created| *created),
        Counted => fixture.index.counted_page(path, 1, after, maximum, |_| {}),
        SinglePass => fixture.index.page(path, 1, after, maximum, |_| {}),
    }
}
fn measured(
    fixture: &Fixture,
    path: &str,
    after: Option<&str>,
    maximum: usize,
    implementation: Implementation,
) -> (acyclic_test_support::AllocationInfo, MemoryStorePage) {
    let mut result = MemoryStorePage {
        generation: 1,
        entries: Vec::new(),
        has_more: false,
    };
    let allocations = acyclic_test_support::measure(|| {
        result = selected(fixture, path, after, maximum, implementation);
    });
    (allocations, result)
}
#[test]
fn page_storage_is_independent_of_retained_children() {
    for nested in [false, true] {
        let small = fixture(4, nested);
        let large = fixture(4096, nested);
        let (first, expected) = measured(&small, "files", None, 1, SinglePass);
        let (last, actual) = measured(&large, "files", None, 1, SinglePass);
        assert_eq!(actual, expected);
        assert_eq!(
            (last.count_total, last.bytes_total, last.bytes_max),
            (first.count_total, first.bytes_total, first.bytes_max)
        );
        assert_eq!(
            last.count_total, 2,
            "only the result vector and returned name allocate"
        );
        let (legacy_first, _) = measured(&small, "files", None, 1, Scan);
        let (legacy_last, _) = measured(&large, "files", None, 1, Scan);
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
    let (allocation, page) = measured(&fixture, "", None, 1, SinglePass);
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
        let actual = fixture.index.page(path, 1, after, 1, |probe| {
            reads += usize::from(probe == Probe::Read);
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
pub(super) fn assert_one_root_seek(seeks: usize) {
    assert_eq!(
        seeks, 1,
        "a page must perform exactly one initial root seek"
    );
}

fn page_work_bound(children: usize, returned: usize) -> usize {
    let log = usize::BITS - children.max(1).leading_zeros();
    // At most two reads per emitted child, two AVL-height paths, and a
    // historical-root lookup (this fixture has one version per directory).
    2 * returned + 4 * log as usize + 2
}

#[test]
fn cursor_pages_emit_once_after_one_root_seek() {
    for children in [4096, 65_536] {
        let fixture = fixture(children, false);
        for (after, maximum) in [
            (None, 1024),
            (Some("00002047"), 1024),
            (Some("00002047"), 1),
        ] {
            let mut reads = 0;
            let mut seeks = 0;
            let actual = fixture
                .index
                .page("files", 1, after, maximum, |probe| match probe {
                    Probe::RootSeek => seeks += 1,
                    Probe::Read => reads += 1,
                });
            assert_one_root_seek(seeks);
            assert_eq!(
                actual,
                reference::expected(&fixture.paths, "files", 1, after, maximum, |created| {
                    *created
                })
            );
            let bound = page_work_bound(children, actual.entries.len());
            assert!(
                reads <= bound,
                "children={children} after={after:?} maximum={maximum} probes={reads} bound={bound}"
            );
            eprintln!(
                "page-work children={children} after={after:?} maximum={maximum} probes={reads} seeks={seeks} bound={bound}"
            );
        }
        // Run the former counting implementation against the identical bound.
        let mut repeated_reads = 0;
        fixture
            .index
            .counted_page("files", 1, Some("00002047"), 1024, |probe| {
                repeated_reads += usize::from(probe == Probe::Read);
            });
        assert!(
            repeated_reads > page_work_bound(children, 1024),
            "negative control must reject the duplicate traversal"
        );
    }
}

#[test]
fn cursor_tail_storage_depends_only_on_returned_entries() {
    for children in [4, 4096] {
        let fixture = fixture(children, false);
        let after = format!("{:08}", children - 2);
        let (allocation, page) = measured(&fixture, "files", Some(&after), usize::MAX, SinglePass);
        assert_eq!(page.entries.len(), 1);
        assert!(!page.has_more);
        assert_eq!(
            (
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max
            ),
            (2, 40, 40)
        );
    }
}

#[test]
fn listing_reads_no_generation_values_outside_its_directory_and_cursor() {
    let flat = fixture(4096, false);
    let mut probes = 0;
    let actual = flat.index.page("missing", 1, None, 1, |probe| {
        probes += usize::from(probe == Probe::Read);
    });
    assert!(actual.entries.is_empty());
    assert_eq!(probes, 0);
    let mut probes = 0;
    let actual = flat.index.page("files", 1, Some("00002047"), 1, |probe| {
        probes += usize::from(probe == Probe::Read);
    });
    assert_eq!(actual.entries[0].name, "00002048");
    assert!(probes <= 128);
    let mut probes = 0;
    let actual = flat.index.page("", 1, Some("files"), 1, |probe| {
        probes += usize::from(probe == Probe::Read);
    });
    assert!(actual.entries.is_empty());
    assert!(probes <= 2);
    let nested = fixture(4096, true);
    let mut probes = 0;
    let actual = nested.index.page("files", 1, None, 1, |probe| {
        probes += usize::from(probe == Probe::Read);
    });
    assert_eq!(actual.entries[0].name, "one");
    assert!(probes <= 4);
}
#[test]
fn retained_index_storage_is_linear_and_rewrites_allocate_nothing() {
    for count in [256, 4096] {
        let paths: Vec<_> = (0..count).map(|item| format!("files/{item:08}")).collect();
        let mut index = DirectoryIndex::default();
        let allocation = acyclic_test_support::measure(|| {
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
        let rewrites = acyclic_test_support::measure(|| {
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
    let first = index.page("files", 1, None, 1, |_| {});
    for future in 0..65_536 {
        let path = format!("files/a-{future:08}");
        let generation = 2 + future;
        index.publish(&path, generation);
        paths.insert(path, Some(generation));
    }
    for after in [None, Some(first.entries[0].name.as_str())] {
        let mut probes = 0;
        let actual = index.page("files", 1, after, 1, |probe| {
            probes += usize::from(probe == Probe::Read);
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
                        index.page(path, generation, after, maximum, |_| {}),
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
        let actual = index.page(&path,generation,after.as_deref(),maximum,|_|{});
        let counted = index.counted_page(&path,generation,after.as_deref(),maximum,|_|{});
        prop_assert_eq!(&actual, &counted);
        let encoded = serde_json::to_vec(&actual)?;
        let decoded: MemoryStorePage = serde_json::from_slice(&encoded)?;
        prop_assert_eq!(&decoded,&actual);
        prop_assert_eq!(&actual,&reference::expected(&paths,&path,generation,after.as_deref(),maximum,|created|*created));
        if generation != u64::MAX {
            let cursor = actual.entries.last().map(|entry| entry.name.as_str());
            let expected = reference::expected(&paths,&path,generation,cursor,maximum,|created|*created);
            for (offset, (name, _)) in rows.iter().enumerate() { index.publish(&format!("foo/future/{name}"), 8 + offset as u64); }
            let mut probes = 0;
            let mut seeks = 0;
            let next = index.page(&path,generation,cursor,maximum,|probe| match probe {
                Probe::Read => probes += 1,
                Probe::RootSeek => seeks += 1,
            });
            prop_assert!(seeks <= 1);
            if !next.entries.is_empty() || next.has_more { prop_assert_eq!(seeks, 1); }
            let log = usize::BITS - (paths.len().max(1)).leading_zeros();
            prop_assert!(probes <= 2 * next.entries.len() + 6 * log as usize + 4);
            prop_assert_eq!(next,expected);
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
    let implementation = crate::native_environment::variable("HARNESS_DIRECTORY_PROFILE")
        .and_then(|value| value.into_string().ok());
    let implementations: &[Implementation] = match implementation.as_deref() {
        Some("before") => &[Scan],
        Some("after") => &[SinglePass],
        Some("counted") => &[Counted],
        _ => &[Scan, Counted, SinglePass],
    };
    let syscalls = crate::native_environment::variable("HARNESS_DIRECTORY_SYSCALLS").is_some();
    for (label, nested, path, after, maximum) in [
        ("distinct", false, "files", None, 1),
        ("nested", true, "files", None, 1),
        ("absent", false, "missing", None, 1),
        ("first-1024", false, "files", None, 1024),
        ("cursor-1024", false, "files", Some("00002047"), 1024),
        ("cursor-tail", false, "files", Some("00004094"), usize::MAX),
    ] {
        let fixture = fixture(4096, nested);
        for &implementation in implementations {
            let mut times = Vec::with_capacity(32);
            for _ in 0..32 {
                if syscalls {
                    eprintln!("DIRECTORY_BEGIN");
                }
                let started = std::time::Instant::now();
                let page = selected(&fixture, path, after, maximum, implementation);
                let elapsed = started.elapsed();
                if syscalls {
                    eprintln!("DIRECTORY_END");
                }
                times.push(elapsed);
                std::hint::black_box(page);
            }
            times.sort_unstable();
            let (allocation, _) = measured(&fixture, path, after, maximum, implementation);
            eprintln!(
                "listing tracing={tracing} implementation={implementation:?} case={label} median={:?} allocations={} bytes={} peak={}",
                times.get(16),
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max
            );
        }
    }
}
