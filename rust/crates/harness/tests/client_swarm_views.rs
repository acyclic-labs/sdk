//! The reducer is exercised through actual volume writes and platform frames.
#[path = "../../harness-fuzz/support/swarm_limits.rs"]
mod limits;
#[path = "../../harness-fuzz/support/swarm_views.rs"]
mod oracle;

#[path = "client_swarm_views_follow.rs"]
mod follow;

#[path = "support/quiescent.rs"]
mod quiescent;
#[path = "client_swarm_views_retention.rs"]
mod retention;
#[path = "support/swarm_session.rs"]
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
