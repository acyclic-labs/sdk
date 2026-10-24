use super::*;
use acyclic_test_support::{AllocationInfo, differential};
use proptest::prelude::*;

#[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
mod provider;

fn id(value: u8) -> ObjectId {
    ObjectId {
        kind: crate::ObjectKind::BlobChunk,
        digest: crate::Digest::from_bytes([value; 32]),
    }
}
fn key(value: u8) -> DecodedCacheKey {
    DecodedCacheKey::new::<Vec<u8>>(id(value), Default::default())
}
fn bytes(value: u8) -> Vec<u8> {
    vec![value; usize::from(value % 5) + 1]
}
fn decoded(value: u8, length: u64) -> DecodedCacheValue {
    DecodedCacheValue {
        value: Arc::new(bytes(value)),
        logical_bytes: length,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    }
}
fn cache(entries: u32, maximum_bytes: u64, historical: bool) -> CachedObjectStore<()> {
    let cache = CachedObjectStore::new(
        (),
        ObjectCacheOptions {
            maximum_entries: entries,
            maximum_bytes,
            maximum_in_flight: 1,
            maximum_waiters_per_object: 1,
        },
    )
    .unwrap();
    if historical {
        cache.lock_state().unwrap().historical_recency = Some(BTreeSet::new());
    }
    cache
}

#[derive(Debug, PartialEq, Eq)]
enum Output {
    Bool(bool),
    Bytes(Option<Vec<u8>>),
    Admission(bool, Vec<u8>),
    Clear,
}
fn operation(
    cache: &CachedObjectStore<()>,
    op: u8,
    value: u8,
    omit_touch: bool,
) -> Result<Output, ObjectStoreError> {
    Ok(match op % 10 {
        0 => Output::Bool(cache.insert(id(value), bytes(value).into())?),
        1 => Output::Bool(cache.cached_contains(id(value))?),
        2 => match cache.read_plan(
            id(value),
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )? {
            ReadPlan::Hit(bytes) => Output::Bytes(Some(bytes.to_vec())),
            ReadPlan::Leader(flight) => {
                cache.remove_flight(id(value), &flight)?;
                Output::Bytes(None)
            }
            ReadPlan::Follower(_) => panic!("sequential history retained a flight"),
        },
        3 | 8 => {
            let length = if op % 10 == 8 {
                81
            } else {
                bytes(value).len() as u64
            };
            let (shared, value) = match cache.decoded_admit(key(value), decoded(value, length))? {
                DecodedCacheAdmission::Shared(value) => (true, value),
                DecodedCacheAdmission::Uncached(value) => (false, value),
            };
            Output::Admission(
                shared,
                value.value.downcast_ref::<Vec<u8>>().unwrap().clone(),
            )
        }
        4 => {
            let value = if omit_touch {
                cache.decoded_get_mode::<true, false>(key(value))?
            } else {
                cache.decoded_get(key(value))?
            };
            Output::Bytes(value.map(|value| value.value.downcast_ref::<Vec<u8>>().unwrap().clone()))
        }
        5 => {
            cache.clear()?;
            Output::Clear
        }
        6 => Output::Bool(cache.insert(id(value), vec![255; bytes(value).len()].into())?),
        7 => Output::Bool(cache.insert(id(value), vec![value; 81].into())?),
        _ => {
            cache.lock_state()?.clock = u64::MAX;
            Output::Bool(cache.cached_contains(id(value))?)
        }
    })
}

fn history(entries: u32, maximum_bytes: u64, ops: &[(u8, u8)], omit_touch: bool) {
    let mut old = true;
    let [baseline, candidate] = differential::pair::<_, _, (u8, u8)>(
        || {
            let store = cache(entries, maximum_bytes, old);
            old = false;
            store
        },
        |store, historical| {
            let mut output = Vec::new();
            for &(op, value) in [
                (0, 0),
                (3, 0),
                (0, 1),
                (1, 0),
                (4, 0),
                (2, 0),
                (0, 2),
                (3, 1),
                (6, 0),
                (7, 3),
                (8, 3),
            ]
            .iter()
            .chain(ops)
            {
                differential::record((op, value));
                let result = operation(&store, op, value, !historical && omit_touch)
                    .map_err(|error| format!("{error:?}"));
                let stats = store.stats().unwrap();
                let state = store.lock_state().unwrap();
                output.push((result, stats, state.recency_order(), state.clock));
            }
            output
        },
    );
    assert_eq!(
        candidate, baseline,
        "cache recency differs from historical tree"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn mixed_recency_histories_preserve_full_values_errors_order_and_counters(
        entries in 1_u32..12, maximum_bytes in 1_u64..81,
        ops in prop::collection::vec((0_u8..10, 0_u8..16), 0..96),
    ) {
        history(entries, maximum_bytes, &ops, false);
    }
}

#[test]
#[should_panic(expected = "cache recency differs from historical tree")]
fn omitted_touch_fails_the_same_history_proof() {
    history(4, 80, &[(4, 0), (0, 3), (4, 0)], true);
}

fn hit_counts(historical: bool, count: u8) -> AllocationInfo {
    let store = cache(128, 4096, historical);
    for value in 0..count {
        store.insert(id(value), bytes(value).into()).unwrap();
        store.decoded_admit(key(value), decoded(value, 8)).unwrap();
    }
    // Warm every hit path, including the first shared-byte clone, before capture.
    for value in 0..count {
        assert!(store.cached_contains(id(value)).unwrap());
        let ReadPlan::Hit(_) = store
            .read_plan(
                id(value),
                #[cfg(not(target_arch = "wasm32"))]
                None,
            )
            .unwrap()
        else {
            panic!("warm canonical entry disappeared")
        };
        assert!(store.decoded_get(key(value)).unwrap().is_some());
    }
    let before = store.stats().unwrap();
    // Fixed work at both K:
    // 96 contains hits, 96 canonical-read hits and 96 decoded hits.
    let start = std::time::Instant::now();
    let counts = acyclic_test_support::measure(|| {
        for step in 0_u8..96 {
            let value = step % count;
            assert!(store.cached_contains(id(value)).unwrap());
            let ReadPlan::Hit(canonical) = store
                .read_plan(
                    id(value),
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                )
                .unwrap()
            else {
                panic!("warm canonical entry disappeared")
            };
            assert!(canonical.iter().all(|byte| *byte == value));
            let value = store.decoded_get(key(value)).unwrap().unwrap();
            assert!(!value.value.downcast_ref::<Vec<u8>>().unwrap().is_empty());
        }
    });
    eprintln!(
        "recency hits old={historical} K={count}: {counts:?} {:?}",
        start.elapsed()
    );
    let stats = store.stats().unwrap();
    assert_eq!(stats.hits - before.hits, 288);
    assert_eq!(stats.decoded_hits - before.decoded_hits, 96);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.evictions, 0);
    store.lock_state().unwrap().recency_order();
    counts
}
fn pin(historical: bool) {
    let counts = [8, 24].map(|count| hit_counts(historical, count));
    for count in counts {
        assert_eq!(
            count,
            AllocationInfo::default(),
            "cache hits allocate recency nodes"
        );
    }
}
#[test]
fn all_three_hit_entry_points_allocate_zero_at_both_history_sizes() {
    pin(false);
}
#[test]
#[should_panic(expected = "cache hits allocate recency nodes")]
fn restored_tree_fails_the_same_zero_allocation_pin() {
    pin(true);
}
