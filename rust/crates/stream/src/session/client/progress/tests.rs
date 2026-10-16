use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // The application cut is the fold of only current-generation consumed
    // deliveries. Ordinary records and opaque child positions never skip.
    #[test]
    fn latest_progress_matches_the_consumed_generation_fold(
        steps in prop::collection::vec((any::<bool>(), 0_u64..16, any::<bool>()), 0..32),
    ) {
        let progress = Progress::new(0);
        let mut generation = 1;
        let mut cut = 0;
        for (fence, advance, stale) in steps {
            if fence { generation += 1; prop_assert_eq!(progress.fence()?, generation); }
            let received_generation = if stale { generation - 1 } else { generation };
            let expected = received_generation == generation;
            let sequence = cut + advance;
            prop_assert_eq!(progress.consume_latest(received_generation, sequence)?, expected);
            if expected { cut = sequence + 1; }
            prop_assert_eq!(progress.next()?, cut);
            prop_assert_eq!(progress.position()?.1, Vec::<u8>::new());
        }
    }
}

#[test]
fn negative_control_strict_consumption_cannot_apply_a_conflated_jump() {
    let progress = Progress::new(1);
    assert_eq!(
        progress.consume_opaque(1, 7, &[]),
        Err(StreamError::Unavailable)
    );
    assert_eq!(progress.next().unwrap(), 1);
    assert_eq!(progress.consume_latest(1, 7), Ok(true));
    assert_eq!(progress.next().unwrap(), 8);
    assert_eq!(
        progress.consume_record(1, 9, [7; 32]),
        Err(StreamError::Unavailable)
    );
    assert_eq!(progress.next().unwrap(), 8);
}

#[test]
fn overflow_and_checked_history_cannot_partially_apply_a_latest_cut() {
    let progress = Progress::new(1);
    assert_eq!(
        progress.consume_latest(1, u64::MAX),
        Err(StreamError::OutOfRange)
    );
    assert_eq!(progress.next().unwrap(), 1);
    let guarded = Progress::with_preceding(1, Some([7; 32]));
    assert_eq!(guarded.consume_latest(1, 3), Err(StreamError::Unavailable));
    assert_eq!(
        guarded.subscription().unwrap(),
        Position {
            next: 1,
            opaque: vec![],
            preceding: Some([7; 32])
        }
    );
}
