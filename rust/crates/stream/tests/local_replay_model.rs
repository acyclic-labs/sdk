//! Exhaustive checks of the production replay durability handoff.
#![cfg(all(acyclic_loom, feature = "local"))]

#[test]
fn replay_and_flush_preserve_durability() {
    acyclic_stream::replay_and_flush_preserve_durability();
}

#[test]
#[should_panic(expected = "replay omitted the original durability requirement")]
fn negative_control_acknowledges_an_unflushed_original() {
    acyclic_stream::negative_control_acknowledges_an_unflushed_original();
}
