use super::*;
use crate::Digest;

fn key(volume: u8, generation: u8) -> Key {
    (
        VolumeId::from_bytes([volume; 16]),
        GenerationId::new(Digest::from_bytes([generation; 32])),
    )
}

#[cfg(not(acyclic_fs_loom))]
mod ordinary {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn collisions_and_old_heads_never_return_another_identity() {
        let positions = GenerationPositions::<1>::default();
        positions.record(key(1, 2), Sequence::new(7));
        assert_eq!(positions.get(&key(1, 2), Sequence::new(6)), None);
        assert_eq!(positions.get(&key(2, 2), Sequence::new(9)), None);
        assert_eq!(positions.get(&key(1, 3), Sequence::new(9)), None);
        positions.record(key(2, 2), Sequence::new(8));
        assert_eq!(positions.get(&key(1, 2), Sequence::new(9)), None);
        assert_eq!(
            positions.get(&key(2, 2), Sequence::new(9)),
            Some(Sequence::new(8))
        );
        positions.record(key(2, 2), Sequence::new(10));
        assert_eq!(
            positions.get(&key(2, 2), Sequence::new(9)),
            Some(Sequence::new(8))
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // A hit always belongs to the complete volume/generation identity and
        // lies in the observed authority prefix, despite arbitrary collisions.
        #[test]
        fn cache_hits_are_verified_positions(
            operations in prop::collection::vec((any::<u8>(), any::<u8>(), any::<u8>(), any::<bool>()), 0..64)
        ) {
            let positions = GenerationPositions::<4>::default();
            for (volume, generation, head, write) in operations {
                let position = Sequence::new(u64::from(volume) + u64::from(generation) + 1);
                let key = key(volume, generation);
                if write {
                    positions.record(key, position);
                }
                if let Some(found) = positions.get(&key, Sequence::new(u64::from(head))) {
                    prop_assert_eq!(found, position);
                    prop_assert!(found.get() <= u64::from(head));
                }
            }
        }
    }
}

#[cfg(acyclic_fs_loom)]
mod model {
    use super::*;
    use loom::{sync::Arc, thread};

    fn publication(torn: bool) {
        loom::model(move || {
            let positions = Arc::new(GenerationPositions::<1>::default());
            positions.record(key(1, 1), Sequence::new(1));
            let writer = Arc::clone(&positions);
            let writer = thread::spawn(move || {
                if torn {
                    writer.record_torn(key(2, 2), Sequence::new(2));
                } else {
                    writer.record(key(2, 2), Sequence::new(2));
                }
            });
            let reader = Arc::clone(&positions);
            let reader = thread::spawn(move || {
                if let Some(position) = reader.get(&key(2, 2), Sequence::new(2)) {
                    assert_eq!(
                        position,
                        Sequence::new(2),
                        "cache publication returned a torn entry"
                    );
                }
            });
            writer.join().unwrap();
            reader.join().unwrap();
            assert_eq!(
                positions.get(&key(2, 2), Sequence::new(2)),
                Some(Sequence::new(2))
            );
        });
    }

    #[test]
    fn real_cache_publishes_complete_identities() {
        publication(false);
    }

    #[test]
    #[should_panic(expected = "cache publication returned a torn entry")]
    fn negative_control_detects_split_publication() {
        publication(true);
    }
}
