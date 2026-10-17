//! A Vec of input identities is the independent bounded-LRU oracle.
use super::*;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

fn key(id: u8) -> DecodedCacheKey {
    DecodedCacheKey::new::<u64>(
        ObjectId {
            kind: crate::ObjectKind::FileTablePage,
            digest: crate::Digest::from_bytes([id / 2; 32]),
        },
        crate::kernel::DecodeLimits {
            maximum_page_items: 2 + u32::from(id % 2),
            ..Default::default()
        },
    )
}
fn value(id: u8) -> DecodedCacheValue {
    DecodedCacheValue {
        value: Arc::new(u64::from(id)),
        logical_bytes: 8,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    }
}
fn observed(value: Option<DecodedCacheValue>) -> Option<u64> {
    value.map(|v| *v.value.downcast::<u64>().unwrap())
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    AliasIdentity,
    OmitEviction,
}
fn history(entries: u32, bytes: u64, ops: &[(u8, u8)], fault: Fault) -> TestCaseResult {
    let store = CachedObjectStore::new(
        (),
        ObjectCacheOptions {
            maximum_entries: entries,
            maximum_bytes: bytes,
            maximum_in_flight: 1,
            maximum_waiters_per_object: 1,
        },
    )
    .unwrap();
    let mut lru = Vec::<u8>::new();
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    // Mandatory hits, distinct decoder limits for the same object, eviction,
    // and clear ensure healthy and rejected cache lookups are always observed.
    for (op, id) in [
        (0, 0),
        (1, 0),
        (1, 1),
        (0, 1),
        (0, 2),
        (1, 0),
        (1, 2),
        (2, 0),
        (1, 2),
    ]
    .iter()
    .chain(ops)
    {
        match op % 3 {
            0 => {
                if matches!(fault, Fault::OmitEviction) {
                    store
                        .decoded_admit_bounded::<false>(key(*id), value(*id))
                        .unwrap();
                } else {
                    store.decoded_admit(key(*id), value(*id)).unwrap();
                }
                if bytes >= 8 && !lru.contains(id) {
                    while lru.len() >= entries as usize || (lru.len() as u64 + 1) * 8 > bytes {
                        lru.remove(0);
                    }
                    lru.push(*id);
                }
            }
            1 => {
                actual.push(observed(if matches!(fault, Fault::AliasIdentity) {
                    store.decoded_get_matching::<false>(key(*id)).unwrap()
                } else {
                    store.decoded_get(key(*id)).unwrap()
                }));
                let index = lru.iter().position(|v| v == id);
                expected.push(index.map(|_| u64::from(*id)));
                if let Some(index) = index {
                    lru.remove(index);
                    lru.push(*id);
                }
            }
            _ => {
                store.clear().unwrap();
                lru.clear();
            }
        }
        let stats = store.stats().unwrap();
        actual.push(Some(stats.resident_entries));
        actual.push(Some(stats.resident_bytes));
        expected.push(Some(lru.len() as u64));
        expected.push(Some(lru.len() as u64 * 8));
    }
    prop_assert_eq!(
        actual,
        expected,
        "decoded cache differs from input LRU history"
    );
    Ok(())
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn decoded_histories_preserve_identity_limits_and_bounds(
        entries in 1u32..4, bytes in 1u64..25,
        ops in prop::collection::vec((0u8..3,0u8..6),0..16),
    ) { history(entries,bytes,&ops,Fault::None)?; }
}
#[test]
fn decoded_history_negative_controls() {
    for fault in [Fault::AliasIdentity, Fault::OmitEviction] {
        let error = history(1, 8, &[], fault).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("decoded cache differs from input LRU history"),
            "{error}"
        );
    }
}
