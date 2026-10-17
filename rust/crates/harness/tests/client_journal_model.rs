//! Production journal with its crate-local file-lock shim instrumented by loom.
#![cfg(acyclic_harness_loom)]
#[path = "support/client_command.rs"]
mod client_command;

#[test]
fn checkpoint_and_paused_opener_preserve_exclusion_and_every_committed_command() {
    acyclic_harness::client::verify_journal(client_command::command(), false);
}

#[test]
#[should_panic(expected = "two writers")]
fn negative_control_detects_missing_exclusion() {
    acyclic_harness::client::verify_journal(client_command::command(), true);
}
