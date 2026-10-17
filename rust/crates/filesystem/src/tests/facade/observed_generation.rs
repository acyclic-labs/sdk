use super::*;
use proptest::prelude::*;

fn observations(steps: &[(bool, u8)], forget_operation: bool) {
    let mut observation = ObservedGeneration::UNKNOWN;
    let mut operation = None;
    assert_eq!(observation.position(GenerationId::new(Digest::ZERO)), None);
    for (index, (commit, identity)) in steps.iter().enumerate() {
        let generation = GenerationId::new(Digest::from_bytes([*identity; 32]));
        let position = Sequence::new(index as u64 + 1);
        if *commit {
            let authored = OperationId::from_bytes([*identity; 16]);
            observation = ObservedGeneration::committed(LastCommit {
                operation_id: authored,
                generation_id: generation,
                head: Head {
                    epoch: Epoch::GENESIS,
                    sequence: position,
                    digest: Digest::from_bytes([*identity; 32]),
                },
            });
            operation = Some(authored);
        } else if forget_operation {
            observation = ObservedGeneration::head(generation, position);
        } else {
            observation.refresh(generation, position);
        }
        assert_eq!(
            observation.operation(),
            operation,
            "refresh lost the authored operation"
        );
        assert_eq!(observation.position(generation), Some(position));
        let different = GenerationId::new(Digest::from_bytes([identity.wrapping_add(1); 32]));
        assert_eq!(
            observation.position(different),
            None,
            "a private data identity replaced the observed head"
        );
        assert_eq!(observation.commit().is_some(), *commit);
        let retained = observation;
        observation.refresh(generation, position);
        assert!(
            observation == retained,
            "unchanged head lost its publication cache"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn observations_keep_their_head_and_authored_operation(steps in prop::collection::vec((any::<bool>(), any::<u8>()), 0..32)) {
        observations(&steps, false);
    }
}

#[test]
#[should_panic(expected = "refresh lost the authored operation")]
fn forgetting_authored_operation_negative_control() {
    observations(&[(true, 1), (false, 2)], true);
}
