//! The reducer is exercised through actual volume writes and platform frames.
#[path = "../../harness-fuzz/support/swarm_limits.rs"]
mod limits;
#[path = "../../harness-fuzz/support/swarm_views.rs"]
mod oracle;

#[path = "client_swarm_views_follow.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod follow;

#[path = "support/quiescent.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod quiescent;
#[path = "client_swarm_views_retention.rs"]
mod retention;
#[path = "support/swarm_session.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod runtime;

#[test]
fn standalone_watermarks_and_same_cut_status_keys_survive_promotion() {
    oracle::exercise(b"live text", false);
}

#[test]
fn negative_control_dropping_standalone_watermarks_is_detected() {
    assert!(std::panic::catch_unwind(|| oracle::exercise(b"live text", true)).is_err());
}

#[test]
fn negative_control_dropping_status_attention_is_detected() {
    assert!(std::panic::catch_unwind(|| oracle::exercise_without_attention(b"attention")).is_err());
}

#[test]
fn negative_control_dropping_discovery_is_detected() {
    assert!(std::panic::catch_unwind(|| oracle::check_discovery(&[0, 1, 0], true)).is_err());
}

proptest::proptest! {
    #[test]
    fn swarm_bounds_round_trip_and_reject_unknown_metadata(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..128)) {
        limits::exercise(&bytes, false);
    }
    #[test]
    fn selected_files_and_overview_are_independent_folds(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..128)) {
        oracle::exercise(&bytes, false);
    }
}

#[test]
fn negative_control_omitting_a_swarm_bound_is_rejected() {
    assert!(std::panic::catch_unwind(|| limits::exercise(b"", true)).is_err());
}

#[path = "client_swarm_views_files.rs"]
mod files;

#[path = "support/swarm_status.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod status;

#[path = "client_swarm_membership.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod membership;

#[path = "../../harness-fuzz/support/swarm_membership.rs"]
mod membership_codec;
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn membership_status_and_paths_round_trip(task in proptest::array::uniform16(proptest::num::u8::ANY), selected in proptest::num::u8::ANY, inputs in proptest::num::u64::ANY, settled in proptest::bool::ANY, bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..512)) {
        membership_codec::check(membership_codec::encoded(task,selected,inputs,settled), false);
        membership_codec::exercise(&bytes);
    }
}
#[test]
fn membership_ignoring_archive_mutant_is_rejected() {
    let archived = membership_codec::encoded([1; 16], 4, 1, true);
    assert!(std::panic::catch_unwind(|| membership_codec::check(archived, true)).is_err());
}
#[test]
fn membership_status_checked_seed_round_trips() {
    let bytes = include_bytes!("../../harness-fuzz/corpus/client_swarm_views/settled-archive");
    assert!(
        !acyclic_harness::task_volume::ChildStatus::parse(bytes)
            .unwrap()
            .active()
    );
    membership_codec::exercise(bytes);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn membership_source_archive_churn_matches_fresh_snapshots(commands in proptest::collection::vec(proptest::num::u8::ANY, 0..32), reference in "[a-z][a-z0-9_-]{0,20}", message in "[a-z][a-z0-9_-]{0,20}", turn in 1u64..=u64::MAX) {
        membership_codec::query(&commands, false);
        membership_codec::paths(&reference, &message, turn);
    }
}
#[test]
fn dropped_membership_removal_mutant_fails_the_same_source_oracle() {
    assert!(std::panic::catch_unwind(|| membership_codec::query(&[0, 0, 1, 0], true)).is_err());
}

#[path = "../../harness-fuzz/support/notification_files.rs"]
mod notification_files;
proptest::proptest! {
    #[test]
    fn notification_schema_round_trips_and_rejects_wrong_owner(
        event in proptest::num::u8::ANY,
        sequence in proptest::num::u64::ANY,
        detail in ".{0,64}",
        foreign in proptest::bool::ANY,
        bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..256),
    ) {
        let mut record = notification_files::encoded(event, sequence, detail);
        if foreign { record.task = "foreign".into(); }
        notification_files::check(&record, false);
        notification_files::exercise(&bytes);
    }
}
#[test]
fn notification_parser_control_cannot_drop_valid_records() {
    assert!(
        std::panic::catch_unwind(|| notification_files::check(
            &notification_files::encoded(4, 1, "blocked".into()),
            true,
        ))
        .is_err()
    );
    let seed = include_bytes!("../../harness-fuzz/corpus/client_swarm_views/notification-blocked");
    notification_files::exercise(seed);
}
#[path = "client_task_files.rs"]
mod task_files;
