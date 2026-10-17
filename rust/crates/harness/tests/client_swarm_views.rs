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

// Swarm bounds are admitted without any transport feature; every broken
// relation is rejected by the same rule SwarmClient::new applies.
#[test]
fn every_out_of_range_swarm_bound_is_rejected_without_a_transport() {
    use acyclic_harness::client::{ClientViewLimits, QueryViewLimits, SwarmClientLimits};
    let limits = SwarmClientLimits {
        agents: 2,
        selected: 1,
        layout_bytes: 1,
        config_files: 1,
        snapshot_files: 1,
    };
    let views = ClientViewLimits {
        agents: 2,
        selected: 1,
        overview_values: 4,
        overview_bytes: 512,
        query: QueryViewLimits {
            files: 1,
            segments: 1,
            bytes: 1,
        },
    };
    limits.validate(&views).expect("valid bounds");
    let broken: [fn(&mut SwarmClientLimits, &mut ClientViewLimits); 7] = [
        |l, _| l.agents = 0,
        |l, _| l.selected = 3,
        |l, _| l.layout_bytes = 0,
        |l, _| l.config_files = 0,
        |l, _| l.snapshot_files = 0,
        |_, v| v.agents = 1,
        |_, v| v.selected = 0,
    ];
    for breaking in broken {
        let (mut l, mut v) = (limits, views);
        breaking(&mut l, &mut v);
        assert!(l.validate(&v).is_err(), "{l:?} {v:?}");
    }
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

#[path = "client_task_files.rs"]
mod task_files;
