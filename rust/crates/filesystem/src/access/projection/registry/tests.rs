use super::*;
use proptest::prelude::*;

fn volume(id: u8) -> Volume {
    Volume {
        owner: [id / 4; 16],
        id: crate::VolumeId::from_bytes([id % 4; 16]),
    }
}

fn invariant(tree: &Tree, lower: Option<Key>, upper: Option<Key>) -> (usize, State) {
    let Some(node) = tree else {
        return (0, State::default());
    };
    assert!(lower.is_none_or(|key| key < node.key));
    assert!(upper.is_none_or(|key| key > node.key));
    let (left_height, left) = invariant(&node.left, lower, Some(node.key));
    let (right_height, right) = invariant(&node.right, Some(node.key), upper);
    assert!(left_height.abs_diff(right_height) <= 1);
    let height = 1 + left_height.max(right_height);
    let total = State {
        projections: left.projections + right.projections + usize::from(node.projection),
        editors: left.editors + right.editors + usize::from(!node.projection),
    };
    assert_eq!((node.height, node.total), (height, total));
    (height, total)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn rotations_and_removals_preserve_each_volumes_live_leases(
        steps in prop::collection::vec((0u8..8, any::<bool>(), any::<bool>(), any::<usize>()), 0..192),
    ) {
        let mut registry = Registry::default();
        let mut live = Vec::new();
        for (id, projection, add, index) in steps {
            if add || live.is_empty() {
                let key = registry.insert(volume(id), projection).unwrap();
                live.push((key, projection));
            } else {
                let (key, _) = live.swap_remove(index % live.len());
                let removed = registry.remove(key).unwrap();
                prop_assert_eq!(removed.key, key);
                prop_assert!(removed.left.is_none() && removed.right.is_none());
            }
            invariant(&registry.root, None, None);
            for id in 0..8 {
                let mut expected = State::default();
                for (_, projection) in live.iter().filter(|(key, _)| key.0 == volume(id)) {
                    expected.projections += usize::from(*projection);
                    expected.editors += usize::from(!projection);
                }
                prop_assert_eq!(registry.counts(volume(id)), expected);
            }
        }
    }
}

#[test]
fn sorted_registration_and_reverse_retirement_remain_balanced() {
    let mut registry = Registry::default();
    let keys: Vec<_> = (0..1024)
        .map(|index| {
            registry
                .insert(volume(u8::try_from(index % 4).unwrap()), index % 2 == 0)
                .unwrap()
        })
        .collect();
    invariant(&registry.root, None, None);
    for key in keys.into_iter().rev() {
        assert_eq!(registry.remove(key).unwrap().key, key);
        invariant(&registry.root, None, None);
    }
    assert!(registry.is_empty());
}

#[test]
fn every_lease_owns_exactly_its_allocation_through_retirement() {
    let mut expected = None;
    for existing in [0, 1, 11, 12, 64] {
        let mut registry = Registry::default();
        for _ in 0..existing {
            registry.insert(volume(0), false).unwrap();
        }
        let allocation = acyclic_test_support::measure(|| {
            let key = registry.insert(volume(1), true).unwrap();
            drop(registry.remove(key).unwrap());
        });
        assert_eq!(allocation.count_total, 1);
        assert_eq!((allocation.count_current, allocation.bytes_current), (0, 0));
        assert_eq!(allocation.count_max, 1);
        assert_eq!(allocation.bytes_total, size_of::<Node>() as u64);
        assert_eq!(allocation.bytes_max, allocation.bytes_total);
        assert_eq!(*expected.get_or_insert(allocation), allocation);
    }
}

#[test]
fn shared_tree_nodes_fail_the_independent_lease_allocation_control() {
    let mut old = std::collections::BTreeMap::new();
    let empty = acyclic_test_support::measure(|| {
        old.insert(volume(1), State::default());
        old.remove(&volume(1));
        old = std::collections::BTreeMap::new();
    });
    old.insert(volume(0), State::default());
    let populated = acyclic_test_support::measure(|| {
        old.insert(volume(1), State::default());
        old.remove(&volume(1));
    });
    assert_ne!(empty, populated);
    assert_eq!(populated.count_total, 0);
}

#[test]
fn registry_initialization_never_borrows_the_first_operations_allocation() {
    let registry = std::sync::OnceLock::new();
    let mut expected = None;
    for _ in 0..2 {
        let allocation = acyclic_test_support::measure(|| {
            let owner = registry.get_or_init(super::super::ProjectionPolicies::default);
            let (lease, _) =
                super::super::ProjectionPolicies::enter(owner, volume(0), false).unwrap();
            drop(lease);
        });
        assert_eq!(allocation.count_total, 1);
        assert_eq!(*expected.get_or_insert(allocation), allocation);
    }
    let old = std::sync::OnceLock::new();
    let first = acyclic_test_support::measure(|| {
        old.get_or_init(|| std::sync::Arc::new(super::super::ProjectionPolicies::default()));
    });
    let next = acyclic_test_support::measure(|| {
        old.get_or_init(|| std::sync::Arc::new(super::super::ProjectionPolicies::default()));
    });
    assert_ne!(first, next);
}
