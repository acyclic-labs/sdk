//! Uses the production provider's crate-local query_sync loom shim.
#![cfg(all(acyclic_loom, feature = "local"))]

#[test]
fn session_registration_fences_recovery() {
    acyclic_stream::verification::session_registration_and_recovery_never_splice(false);
}

#[test]
fn negative_control_unchecked_session_registration_is_detected() {
    assert!(
        std::panic::catch_unwind(|| {
            acyclic_stream::verification::session_registration_and_recovery_never_splice(true)
        })
        .is_err()
    );
}
