use super::*;
use proptest::prelude::*;
use std::{
    cell::Cell,
    ops::Bound::{Excluded, Unbounded},
    rc::Rc,
};

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
#[path = "old_crossover.rs"]
mod old_crossover;

fn counted(
    names: impl IntoIterator<Item = String>,
    reads: &Rc<Cell<usize>>,
) -> BTreeMap<CountedKey, ()> {
    names
        .into_iter()
        .map(|name| {
            (
                CountedKey {
                    name,
                    reads: reads.clone(),
                },
                (),
            )
        })
        .collect()
}

fn compare_reads(
    paths: &BTreeMap<CountedKey, ()>,
    query: &str,
    reads: &Cell<usize>,
    selector: impl FnOnce(&BTreeMap<CountedKey, ()>, &str) -> bool,
) -> (usize, usize) {
    reads.set(0);
    let expected = reference(paths, query);
    let old = reads.get();
    reads.set(0);
    assert_eq!(selector(paths, query), expected);
    (old, reads.get())
}

#[test]
fn fixed_zero_to_eighty_sweep_never_exceeds_original_reads_at_any_depth() {
    for count in 0..=80 {
        for depth in 0..=16 {
            let prefix = std::iter::repeat_n("d", depth)
                .collect::<Vec<_>>()
                .join("/");
            let prefix = if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            };
            let reads = Rc::new(Cell::new(0));
            let paths = counted(
                (0..count).map(|index| {
                    if index == 0 {
                        "a".into()
                    } else {
                        format!("{prefix}{index:08}")
                    }
                }),
                &reads,
            );
            for query in [
                format!("{prefix}missing"),
                format!("{prefix}{:08}", count / 2),
                format!("{prefix}{:08}-", count / 2),
                format!("{prefix}{:08}/child", count / 2),
            ] {
                let (old, new) = compare_reads(&paths, &query, &reads, conflicts);
                assert!(
                    new <= old,
                    "{count} paths, depth {depth}, {query}: {new} > {old}"
                );
            }
        }
    }
}

#[test]
fn original_crossover_violates_the_same_read_bound() {
    for (count, depth, query) in [
        (5, 1, "d/missing"),
        (41, 4, "d/d/d/d/00000020-"),
        (41, 16, "d/d/d/d/d/d/d/d/d/d/d/d/d/d/d/d/00000020-"),
    ] {
        let reads = Rc::new(Cell::new(0));
        let prefix = std::iter::repeat_n("d", depth)
            .collect::<Vec<_>>()
            .join("/");
        let paths = counted(
            (0..count).map(|index| {
                if index == 0 {
                    "a".into()
                } else {
                    format!("{prefix}/{index:08}")
                }
            }),
            &reads,
        );
        let (old, broken) = compare_reads(&paths, query, &reads, old_crossover::conflicts);
        assert!(
            broken > old,
            "old crossover must violate the production bound: {broken} <= {old}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Every generated tree checks a directory-prefix hit, missing sibling,
    // exact rewrite and file-prefix hit/miss at each level through depth 16.
    #[test]
    fn generated_tree_queries_never_read_more_keys_than_the_original_scan(
        depth in 0..=16_usize,
        width in 0..=80_usize,
        target in 0..=80_usize,
        padding in 0..=80_usize,
    ) {
        let mut names = (0..padding).map(|index| format!("a{index:08}")).collect::<Vec<_>>();
        let mut prefix = String::new();
        for _ in 0..depth {
            names.push(format!("{prefix}a"));
            names.push(format!("{prefix}f"));
            prefix.push_str("d/");
        }
        names.extend((0..width).map(|index| format!("{prefix}{index:08}")));
        let reads = Rc::new(Cell::new(0));
        let paths = counted(names, &reads);
        let mut prefix = String::new();
        for _ in 0..=depth {
            for query in [
                prefix.trim_end_matches('/').to_owned(),
                format!("{prefix}b"),
                format!("{prefix}a"),
                format!("{prefix}a/child"),
                format!("{prefix}{target:08}-"),
                format!("{prefix}{target:08}/child"),
            ] {
                let (old, new) = compare_reads(&paths, &query, &reads, conflicts);
                prop_assert!(new <= old, "depth {}, width {}, padding {}, {}: {} > {}", depth, width, padding, query, new, old);
            }
            prefix.push_str("d/");
        }
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
fn former_index_has_bounded_seeks_but_is_rejected_by_the_scan_regression_guard() {
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
        assert_eq!(old_crossover::conflicts(&paths, query), punctuation);
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
        let allocation = acyclic_test_support::measure(|| {
            std::hint::black_box(conflicts(&paths, query));
        });
        assert_eq!((allocation.count_total, allocation.bytes_total), (0, 0));
    }
    let paths: BTreeMap<_, _> = ["foo-.txt", "foo/child", "foo0", "bar", "z"]
        .into_iter()
        .map(|s| (s.to_owned(), ()))
        .collect();
    let allocation = acyclic_test_support::measure(|| {
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
fn long_punctuation_queries_need_no_prefix_buffer() {
    for length in [64, 4096] {
        let query = "é".repeat(length / 2);
        let mut paths: BTreeMap<_, _> = (0..=80)
            .map(|index| (format!("{query}-{index:08}"), ()))
            .collect();
        paths.insert(format!("{query}/child"), ());
        paths.insert("aaa".into(), ());
        paths.insert("zzz".into(), ());
        let allocation = acyclic_test_support::measure(|| {
            assert!(conflicts(&paths, &query));
        });
        assert_eq!(
            (
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max
            ),
            (0, 0, 0)
        );
        let old = acyclic_test_support::measure(|| assert!(old_crossover::conflicts(&paths, &query)));
        assert_eq!(old.count_total, 1);
        assert_eq!(
            old.bytes_total,
            u64::try_from(query.len() + 1).unwrap_or_default()
        );
        assert_eq!(old.bytes_max, old.bytes_total);
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
                for index in 0..=80 { paths.insert(format!("padding-{index:08}"),()); }
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
