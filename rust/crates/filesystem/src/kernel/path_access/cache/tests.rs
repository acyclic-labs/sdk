use super::*;

#[test]
fn operation_cache_preserves_the_providers_decoded_cache_capability() {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let plain = crate::memory::MemoryObjectStore::default();
    let cache = OperationReadCache::new(&plain, 1, &mut accounting, None).unwrap();
    assert!(!crate::AsyncObjectStore::supports_decoded_cache(&cache));
    let hooks = crate::test_support::OwnedReadObjectStore::default();
    let cache = OperationReadCache::new(&hooks, 1, &mut accounting, None).unwrap();
    assert!(crate::AsyncObjectStore::supports_decoded_cache(&cache));
}
use proptest::prelude::*;

std::thread_local! {
    static SLOT_ACCESSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(super) fn record_slot_access() {
    SLOT_ACCESSES.with(|count| count.set(count.get() + 1));
}

fn take_slot_accesses() -> u64 {
    SLOT_ACCESSES.with(|count| count.replace(0))
}

fn id(value: u8) -> ObjectId {
    let mut bytes = [0; 32];
    bytes[0] = value;
    ObjectId {
        kind: crate::ObjectKind::Blob,
        digest: crate::Digest::from_bytes(bytes),
    }
}

// Independent open-addressed table: no production probing or cache state is
// consulted. Count every occupied collision slot and the terminating slot,
// including old-table lookup, each rehash insertion, and the post-growth lookup.
struct ProbeModel {
    slots: Vec<Option<ObjectId>>,
    entries: usize,
}

impl ProbeModel {
    fn new() -> Self {
        Self {
            slots: vec![None; 16],
            entries: 0,
        }
    }

    fn hash(key: ObjectId) -> usize {
        key.digest
            .as_bytes()
            .iter()
            .fold(usize::from(key.kind.canonical_tag()), |hash, byte| {
                (hash ^ usize::from(*byte)).wrapping_mul(16_777_619)
            })
    }

    fn find(slots: &[Option<ObjectId>], key: ObjectId) -> (usize, bool, u64) {
        let start = Self::hash(key) % slots.len();
        for visits in 1..=slots.len() {
            let index = (start + visits - 1) % slots.len();
            match slots[index] {
                Some(found) if found != key => continue,
                found => return (index, found.is_some(), visits as u64),
            }
        }
        panic!("model must retain a vacant slot")
    }

    fn admit(&mut self, key: ObjectId) -> (bool, u64) {
        if self.entries == 32 {
            return (false, 0);
        }
        let (mut vacant, duplicate, mut visits) = Self::find(&self.slots, key);
        if duplicate {
            return (false, visits);
        }
        if self.entries * 2 >= self.slots.len() && self.slots.len() < 64 {
            let mut grown = vec![None; self.slots.len() * 2];
            for old in self.slots.iter().flatten().copied() {
                let (index, duplicate, cost) = Self::find(&grown, old);
                assert!(!duplicate);
                visits += cost;
                grown[index] = Some(old);
            }
            self.slots = grown;
            let (index, duplicate, cost) = Self::find(&self.slots, key);
            assert!(!duplicate);
            vacant = index;
            visits += cost;
        }
        self.slots[vacant] = Some(key);
        self.entries += 1;
        (true, visits)
    }
}

fn compare<const CHARGE_REHASH: bool, const EXTRA_UNCHARGED_PROBE: bool>(
    keys: &[u8],
) -> proptest::test_runner::TestCaseResult {
    let cancellation = CancellationToken::new();
    let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let cache = OperationReadCache::new(&(), 32, &mut setup, None).unwrap();
    let bytes = OwnedBytes::from_static(b"payload");
    let mut model = ProbeModel::new();
    for &key in keys {
        let object = id(key);
        let expected = model.admit(object);
        let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        take_slot_accesses();
        if EXTRA_UNCHARGED_PROBE {
            let state = cache.lock_state();
            let _ = probe_slots_with_charge::<false>(&state.slots.value, object, &mut work, None)
                .unwrap();
        }
        let inserted = cache
            .admit_with_rehash::<true, CHARGE_REHASH>(object, &bytes, 0, &mut work)
            .unwrap();
        let observed = take_slot_accesses();
        prop_assert_eq!(inserted, expected.0);
        prop_assert_eq!(work.spent().items_examined, expected.1);
        prop_assert_eq!(
            observed,
            work.spent().items_examined,
            "slot visits must match charged items"
        );
        prop_assert_eq!(observed, expected.1);
        for previous in model.slots.iter().flatten().copied() {
            let (_, found, visits) = ProbeModel::find(&model.slots, previous);
            let mut read = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            prop_assert!(found);
            take_slot_accesses();
            prop_assert_eq!(
                cache.probe(previous, &mut read).unwrap().unwrap().bytes,
                bytes.clone()
            );
            let observed = take_slot_accesses();
            prop_assert_eq!(observed, visits);
            prop_assert_eq!(read.spent().items_examined, observed);
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn vacant_slot_reuse_keeps_exact_probes_and_all_prior_entries(
        keys in prop::collection::vec(any::<u8>(), 1..97),
    ) {
        compare::<true, false>(&keys)?;
    }
}

#[test]
fn collision_growth_duplicates_and_capacity_match_independent_counts() {
    let colliding: Vec<_> = (0..=255)
        .filter(|&key| ProbeModel::hash(id(key)) % 16 == 0)
        .collect();
    assert_eq!(colliding.len(), 16);
    let mut keys = colliding.clone();
    keys.extend(colliding.iter().copied());
    keys.extend(0..40);
    keys.extend(0..40);
    compare::<true, false>(&keys).unwrap();
}

#[test]
fn independent_counts_detect_uncharged_rehash_probes() {
    let keys: Vec<_> = (0..17).collect();
    compare::<true, false>(&keys).unwrap();
    assert!(compare::<false, false>(&keys).is_err());
}

#[test]
fn actual_slot_observer_detects_an_extra_discarded_uncharged_probe() {
    compare::<true, false>(&[1]).unwrap();
    let error = compare::<true, true>(&[1]).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("slot visits must match charged items")
    );
}

#[test]
fn one_required_probe_is_sufficient_at_the_exact_item_limit() {
    for native in [
        false,
        #[cfg(not(target_arch = "wasm32"))]
        true,
    ] {
        let cancellation = CancellationToken::new();
        #[cfg(not(target_arch = "wasm32"))]
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                items_examined: 1,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancellation,
        )
        .unwrap();
        let mut setup = match native {
            #[cfg(not(target_arch = "wasm32"))]
            true => KernelAccounting::native(&original, &cancellation),
            _ => KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
        };
        let cache = OperationReadCache::new(&(), 2, &mut setup, None).unwrap();
        let mut exact = match native {
            #[cfg(not(target_arch = "wasm32"))]
            true => KernelAccounting::native(&original, &cancellation),
            _ => KernelAccounting::standalone(
                WorkBudget {
                    items_examined: 1,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            ),
        };
        assert!(
            cache
                .admit(id(1), &OwnedBytes::from_static(b"one"), 0, &mut exact)
                .unwrap()
        );
        assert_eq!(exact.spent().items_examined, 1);
        let mut verify = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        assert!(cache.probe(id(1), &mut verify).unwrap().is_some());
    }
}
