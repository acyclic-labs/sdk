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
//! - a condition wait starts no step for its arrivals; only its condition
//!   (`satisfy`) or its deadline resumes it, and `satisfy` touches no other
//!   control.

#[path = "../../src/agent/kernel.rs"]
mod production;

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

fn main() {}
