use super::{
    Arc, StreamError,
    progress::{Position, Progress},
};

fn moving(broken: u8) {
    loom::model(move || {
        let progress = Arc::new(Progress::with_preceding(1, Some([1; 32])));
        let consuming = Arc::clone(&progress);
        let consuming = loom::thread::spawn(move || consuming.consume_record(1, 1, [2; 32]));
        let moving = Arc::clone(&progress);
        let moving = loom::thread::spawn(move || {
            let early = if broken == 1 {
                moving.subscription()?.preceding
            } else {
                None
            };
            moving.fence()?;
            let Position {
                next,
                opaque,
                preceding: anchor,
            } = moving.subscription()?;
            Ok::<_, StreamError>((
                next,
                opaque,
                match broken {
                    1 => early,
                    2 => Some([1; 32]),
                    _ => anchor,
                },
            ))
        });
        let consumed = consuming.join().unwrap().unwrap();
        let cut = moving.join().unwrap().unwrap();
        assert_eq!(
            cut,
            if consumed {
                (2, Vec::new(), Some([2; 32]))
            } else {
                (1, Vec::new(), Some([1; 32]))
            },
            "checked resume must retain the last yielded commit atomically"
        );
        assert!(!progress.consume_record(1, cut.0, [3; 32]).unwrap());
        assert_eq!(
            progress.subscription().unwrap(),
            Position {
                next: cut.0,
                opaque: cut.1.clone(),
                preceding: cut.2
            }
        );
        assert!(progress.consume_record(2, cut.0, [4; 32]).unwrap());
        assert_eq!(
            progress.subscription().unwrap(),
            Position {
                next: cut.0 + 1,
                opaque: Vec::new(),
                preceding: Some([4; 32])
            }
        );
    });
}

#[test]
fn checked_consumption_and_movement_share_one_history_cut() {
    moving(0);
}
#[test]
#[should_panic(expected = "checked resume must retain the last yielded commit atomically")]
fn negative_control_reads_the_commit_before_fencing() {
    moving(1);
}
#[test]
#[should_panic(expected = "checked resume must retain the last yielded commit atomically")]
fn negative_control_reuses_the_initial_snapshot_commit() {
    moving(2);
}

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
