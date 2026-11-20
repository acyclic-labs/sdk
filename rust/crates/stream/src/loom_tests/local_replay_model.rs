//! Exhaustive checks of the production replay durability handoff.

#[test]
fn replay_and_flush_preserve_durability() {
    crate::replay_and_flush_preserve_durability();
}

#[test]
#[should_panic(expected = "replay omitted the original durability requirement")]
fn negative_control_acknowledges_an_unflushed_original() {
    crate::negative_control_acknowledges_an_unflushed_original();
}
