use super::progress::{Position, Progress};

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn checked_cursor_is_the_fold_of_only_current_generation_deliveries(
        events in proptest::collection::vec((proptest::bool::ANY, proptest::bool::ANY, proptest::array::uniform32(proptest::num::u8::ANY)), 0..32)
    ) {
        loom::model(move || {
            let progress = Progress::with_preceding(1, Some([0; 32]));
            let (mut next, mut generation, mut anchor) = (1, 1, [0; 32]);
            for (fence, stale, commit) in &events {
                if *fence { generation = progress.fence().unwrap(); }
                let delivery = if *stale { generation - 1 } else { generation };
                let consumed = progress.consume_record(delivery, next, *commit).unwrap();
                assert_eq!(consumed, !stale);
                if consumed { next += 1; anchor = *commit; }
                assert_eq!(progress.subscription().unwrap(), Position { next, opaque: Vec::new(), preceding: Some(anchor) });
            }
        });
    }
}
