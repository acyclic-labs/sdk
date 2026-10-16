use super::*;
use proptest::prelude::*;
use std::{cell::Cell, rc::Rc};

fn reference<K: Borrow<str> + Ord, V>(paths: &BTreeMap<K, V>, path: &str) -> bool {
    paths
        .keys()
        .any(|existing| paths_conflict(existing.borrow(), path))
}
fn fixture(count: usize) -> BTreeMap<String, ()> {
    (0..count)
        .map(|index| (format!("files/{index:08}"), ()))
        .collect()
}
#[test]
fn punctuation_and_utf8_boundaries_preserve_all_conflicts() -> Result<(), Box<dyn std::error::Error>>
{
    for keys in [
        vec!["foo", "foo-.txt", "foo-.bar", "foo-.baz", "other"],
        vec!["foo-.txt", "foo/child", "foo0", "bar", "z"],
        vec!["é/child", "ê", "/foo", "foo//x", "", "foo/"],
    ] {
        let paths: BTreeMap<_, _> = keys.into_iter().map(|key| (key.to_owned(), ())).collect();
        for path in [
            "",
            "/",
            "foo",
            "foo/",
            "foo/bar",
            "foo//x",
            "foo-.txt",
            "foo-.txt/child",
            "é",
            "é/other",
            "ê/x",
        ] {
            assert_eq!(
                conflicts(&paths, path),
                reference(&paths, path),
                "{paths:?}, {path}"
            );
        }
    }
    let paths: BTreeMap<_, _> = ["foo", "foo-.txt", "foo-.bar", "foo-.baz", "other"]
        .into_iter()
        .map(|key| (key.to_owned(), ()))
        .collect();
    let predecessor = paths
        .range::<str, _>((Unbounded, Excluded("foo/child")))
        .next_back()
        .ok_or("missing predecessor")?
        .0;
    assert!(
        !paths_conflict(predecessor, "foo/child"),
        "negative control: the immediate predecessor alone misses an ancestor"
    );
    assert!(conflicts(&paths, "foo/child"));
    Ok(())
}
struct CountedKey {
    name: String,
    reads: Rc<Cell<usize>>,
}
impl Borrow<str> for CountedKey {
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
fn leading_conflicts_read_each_key_once_without_a_root_seek() {
    let reads = Rc::new(Cell::new(0));
    let paths: BTreeMap<_, _> = (0..4096)
        .map(|index| {
            (
                CountedKey {
                    name: format!("files/{index:08}"),
                    reads: reads.clone(),
                },
                (),
            )
        })
        .collect();
    for index in 0..4 {
        let query = format!("files/{index:08}/child");
        reads.set(0);
        assert!(conflicts(&paths, &query));
        assert_eq!(reads.get(), index + 1);
        reads.set(0);
        let preceding = paths
            .range::<str, _>((Unbounded, Excluded(query.as_str())))
            .next_back();
        assert!(preceding.is_some_and(|(key, _)| paths_conflict(key.borrow(), &query)));
        assert!(
            reads.get() > index + 1,
            "negative control: a root seek exceeds the leading-key work bound"
        );
    }
}
#[test]
fn ordered_work_is_independent_of_unrelated_paths_and_punctuation_interval() {
    for punctuation in [false, true] {
        let reads = Rc::new(Cell::new(0));
        let mut paths: BTreeMap<_, _> = (0..65_536)
            .map(|index| {
                (
                    CountedKey {
                        name: if punctuation {
                            format!("foo-{index:08}")
                        } else {
                            format!("files/{index:08}")
                        },
                        reads: reads.clone(),
                    },
                    (),
                )
            })
            .collect();
        if punctuation {
            paths.insert(
                CountedKey {
                    name: "foo/child".into(),
                    reads: reads.clone(),
                },
                (),
            );
        }
        let query = if punctuation { "foo" } else { "files/new" };
        reads.set(0);
        assert_eq!(conflicts(&paths, query), punctuation);
        assert!(reads.get() <= 256, "index used {} key probes", reads.get());
        reads.set(0);
        assert_eq!(reference(&paths, query), punctuation);
        assert!(
            reads.get() >= 65_536,
            "negative control must exceed the same work bound"
        );
    }
}
#[test]
fn ordinary_queries_allocate_nothing_and_punctuation_scratch_is_bounded() {
    let paths = fixture(4096);
    for query in ["files/new", "missing", "files/00002048/child"] {
        let allocation = allocation_counter::measure(|| {
            std::hint::black_box(conflicts(&paths, query));
        });
        assert_eq!((allocation.count_total, allocation.bytes_total), (0, 0));
    }
    let paths: BTreeMap<_, _> = ["foo-.txt", "foo/child", "foo0", "bar", "z"]
        .into_iter()
        .map(|s| (s.to_owned(), ()))
        .collect();
    let allocation = allocation_counter::measure(|| {
        assert!(conflicts(&paths, "foo"));
    });
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
fn long_punctuation_queries_allocate_one_exact_prefix_buffer() {
    for length in [64, 4096] {
        let query = "é".repeat(length / 2);
        let paths: BTreeMap<_, _> = [
            format!("{query}-.txt"),
            format!("{query}-.a"),
            format!("{query}-.b"),
            format!("{query}-.c"),
            format!("{query}-.d"),
            format!("{query}/child"),
            format!("{query}0"),
            "aaa".into(),
            "zzz".into(),
        ]
        .into_iter()
        .map(|path| (path, ()))
        .collect();
        let allocation = allocation_counter::measure(|| {
            assert!(conflicts(&paths, &query));
        });
        assert_eq!(allocation.count_total, 1);
        assert_eq!(
            allocation.bytes_total,
            u64::try_from(query.len() + 1).unwrap_or_default()
        );
        assert_eq!(allocation.bytes_max, allocation.bytes_total);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // For every UTF-8 path set, even overlapping/corrupt namespaces, the ordered
    // predicate equals the original symmetric slash-prefix rule and roundtrips.
    #[test]
    fn ordered_conflicts_equal_full_scan_and_wire_roundtrip(
        names in prop::collection::vec(prop_oneof!["[a-z./_-]{0,32}",prop::collection::vec(any::<char>(),0..32).prop_map(|chars|chars.into_iter().collect::<String>())],0..128),
        path in prop_oneof!["[a-z./_-]{0,32}",prop::collection::vec(any::<char>(),0..32).prop_map(|chars|chars.into_iter().collect::<String>())],
    ) {
        let encoded=serde_json::to_vec(&(&names,&path))?;
        let decoded:(Vec<String>,String)=serde_json::from_slice(&encoded)?;
        prop_assert_eq!((&decoded.0,&decoded.1),(&names,&path));
        let mut paths:BTreeMap<_,_>=names.iter().cloned().map(|name|(name,())).collect();
        for augmented in [false, true] {
            if augmented {
                paths.insert(format!("{path}-.txt"),());
                paths.insert(format!("{path}/child"),());
            }
            let mut queries = vec![path.clone(),String::new(),"foo".into(),"foo/child".into(),"é/".into()];
            for name in &names {
                queries.push(name.clone());
                queries.push(format!("{name}/child"));
                if let Some((parent,_))=name.rsplit_once('/') { queries.push(parent.into()); }
            }
            for query in queries { prop_assert_eq!(conflicts(&paths,&query),reference(&paths,&query)); }
        }
    }
}
#[test]
fn path_conflicts_without_subscriber() {
    profile(false);
}
#[test]
fn path_conflicts_with_tracing() {
    profile(true);
}
fn profile(tracing: bool) {
    let dispatch = if tracing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let mut paths = fixture(4096);
    paths.insert("ancestor".into(), ());
    let mut punctuation: BTreeMap<_, _> = (0..4096)
        .map(|index| (format!("foo-{index:08}"), ()))
        .collect();
    punctuation.insert("bar".into(), ());
    punctuation.insert("foo/child".into(), ());
    let small = fixture(4);
    let implementation = std::env::var("HARNESS_PATH_PROFILE").ok();
    let syscalls = std::env::var_os("HARNESS_PATH_SYSCALLS").is_some();
    let batch = if cfg!(debug_assertions) || syscalls {
        1
    } else {
        32
    };
    let samples = if syscalls { 1 } else { 64 };
    for (label, paths, query) in [
        ("after-last", &paths, "files/new"),
        ("middle-gap", &paths, "files/00002048-"),
        ("rewrite", &paths, "files/00002048"),
        ("existing-ancestor", &paths, "files/00002048/child"),
        ("first-ancestor", &paths, "ancestor/child"),
        ("existing-descendant", &paths, "files"),
        ("missing-root", &paths, "missing"),
        ("punctuation", &punctuation, "foo"),
        ("small", &small, "files/new"),
    ] {
        for legacy in [true, false] {
            if implementation
                .as_deref()
                .is_some_and(|name| (name == "scan") != legacy)
            {
                continue;
            }
            let operation = || {
                let paths = std::hint::black_box(paths);
                let query = std::hint::black_box(query);
                if legacy {
                    reference(paths, query)
                } else {
                    conflicts(paths, query)
                }
            };
            let mut times = Vec::with_capacity(samples);
            for _ in 0..samples {
                if syscalls {
                    eprintln!("PATH_BEGIN");
                }
                let start = std::time::Instant::now();
                for _ in 0..batch {
                    std::hint::black_box(operation());
                }
                let elapsed = start.elapsed() / batch;
                if syscalls {
                    eprintln!("PATH_END");
                }
                times.push(elapsed);
            }
            times.sort_unstable();
            let allocations = allocation_counter::measure(|| {
                std::hint::black_box(operation());
            });
            eprintln!(
                "conflict tracing={tracing} legacy={legacy} case={label} path={query:?} median={:?} alloc={} bytes={} peak={}",
                times.get(samples / 2),
                allocations.count_total,
                allocations.bytes_total,
                allocations.bytes_max
            );
        }
    }
}
