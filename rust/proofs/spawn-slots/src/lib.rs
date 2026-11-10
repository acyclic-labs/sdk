#![cfg(kani)]

#[path = "../../../crates/harness-core/src/agent/spawn_admission.rs"]
mod spawn_admission;

#[kani::proof]
fn live_parents_admit_new_children() {
    assert!(spawn_admission::admits(false, kani::any()));
}

#[kani::proof]
fn closed_parents_refuse_new_children() {
    assert!(!spawn_admission::admits(true, false));
}

#[kani::proof]
fn replay_preserves_an_existing_child() {
    assert!(spawn_admission::admits(kani::any(), true));
}
