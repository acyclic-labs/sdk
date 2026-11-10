//! Solver harness for the production agent control kernel.
//!
//! Run: `kani tests/formal/agent_control.rs` (must verify) and
//! `kani tests/formal/agent_control_negative.rs` (every harness must fail).
//! The model drives the kernel through every sequence of six wakes (any record
//! class or the alarm), any turn driver result and any queue, approval and
//! children shape, and checks:
//! - `ChildDone` is sent at most once, and exactly once after archive drains;
//! - no turn runs or is retained after completion or archive;
//! - no lost wake-up: whenever work is runnable without new input, an alarm is set;
//! - every alarm is a real deadline (at least `CONTINUE_NOW`).
//! - a condition wait parks only when its open children can meet it;
//! - a condition wait starts no step for its arrivals; only its condition
//!   (`satisfy`) or its deadline resumes it, and `satisfy` touches no other
//!   control.

#[path = "../../src/agent/kernel.rs"]
mod production;

#[path = "../../src/agent/release.rs"]
mod release;

include!("agent_control_model.rs");

#[kani::proof]
#[kani::unwind(8)]
fn agent_control_is_exactly_once_and_never_loses_a_wake() {
    exactly_once_and_never_loses_a_wake(PRODUCTION);
}

#[kani::proof]
#[kani::unwind(8)]
fn archive_cascade_drains_and_reports_exactly_once() {
    archive_drains_and_reports_exactly_once(PRODUCTION);
}

#[kani::proof]
#[kani::unwind(4)]
fn released_generation_is_terminal_and_has_no_committed_turn() {
    let base: u64 = kani::any();
    let tail: u64 = kani::any();
    let actions: bool = kani::any();
    kani::assume(release::released(true, tail, base, actions));
    assert_eq!(tail, base);
    assert!(!actions);
    let before = Control {
        mode: Mode::Done,
        closed: kani::any(),
        archive_refused: kani::any(),
        reported: kani::any(),
        turn: false,
        input: false,
    };
    for kind in [Admit::Input, Admit::Cancel, Admit::Archive] {
        let admitted = production::admit(before, kind);
        assert_eq!(admitted.control.mode, Mode::Done);
        assert!(!admitted.enqueue);
        assert_eq!(
            production::pick(admitted.control, true, true, true),
            Pick::Nothing
        );
    }
    assert!(release::released(true, tail, base, actions));
}

#[kani::proof]
fn a_condition_wait_parks_only_when_its_open_children_can_meet_it() {
    parks_only_on_a_reachable_condition(PRODUCTION);
}

#[kani::proof]
fn input_wait_wakes_on_input_without_a_timer() {
    input_wait_has_an_original_wake(PRODUCTION);
}

fn main() {}
