use super::*;
use proptest::prelude::*;

fn id(value: u8) -> ObjectId {
    let mut bytes = [0; 32];
    bytes[0] = value;
    ObjectId {
        kind: crate::ObjectKind::Blob,
        digest: crate::Digest::from_bytes(bytes),
    }
}

proptest! {
    // Under one guard a non-growing insert performs exactly its first probe.
    // Every successfully admitted key remains reachable across table growth.
    #[test]
    fn vacant_slot_reuse_keeps_exact_probes_and_all_prior_entries(
        keys in prop::collection::vec(any::<u8>(), 1..33),
    ) {
        let cancellation = CancellationToken::new();
        let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let cache = OperationReadCache::new(&(), 32, &mut setup, None).unwrap();
        let bytes = OwnedBytes::from_static(b"payload");
        let mut admitted = std::collections::BTreeSet::new();
        for key in keys {
            let object = id(key);
            let mut oracle = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let state = cache.lock_state();
            let grow = state.entry_count * 2 >= state.slots.value.len()
                && state.slots.value.len() < cache.slot_limit;
            let duplicate = probe_slots(&state.slots.value, object, &mut oracle, None).unwrap().0.is_some();
            drop(state);
            let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let inserted = cache.admit(object, &bytes, 0, &mut work).unwrap();
            prop_assert_eq!(inserted, !duplicate);
            if !grow || duplicate {
                prop_assert_eq!(work.spent().items_examined, oracle.spent().items_examined);
            }
            admitted.insert(key);
            for &previous in &admitted {
                prop_assert_eq!(cache.probe(id(previous), &mut setup).unwrap().unwrap().bytes, bytes.clone());
            }
        }
    }
}

#[test]
fn one_required_probe_is_sufficient_at_the_exact_item_limit() {
    for native in [false, true] {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                items_examined: 1,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancellation,
        )
        .unwrap();
        let mut setup = if native {
            KernelAccounting::native(&original, &cancellation)
        } else {
            KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation)
        };
        let cache = OperationReadCache::new(&(), 2, &mut setup, None).unwrap();
        let mut exact = if native {
            KernelAccounting::native(&original, &cancellation)
        } else {
            KernelAccounting::standalone(
                WorkBudget {
                    items_examined: 1,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
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
