mod error {
    use thiserror::Error;
    include!("../src/lib/error.rs");
}
pub use error::StreamError;
#[path = "../src/change.rs"]
mod change;
#[path = "../src/interest.rs"]
mod interest;
mod probe;
pub use acyclic_stream::query_sync;
#[path = "../src/wasm/follow_state.rs"]
mod cursor;
#[path = "../src/http/shared.rs"]
mod shared;
mod shared_models {
    use super::shared::*;
    include!("shared.rs");
}

use acyclic_stream::discovery_verification as discovery;

#[test]
fn parent_creation_has_no_discovery_gap() {
    discovery::registration_race(false, true);
}
#[test]
fn switching_to_the_parent_watch_has_no_discovery_gap() {
    discovery::registration_race(false, false);
}
#[test]
#[should_panic(expected = "discovery lost a publication wake")]
fn parent_creation_negative_control_registers_after_reading() {
    discovery::registration_race(true, true);
}
#[test]
#[should_panic(expected = "discovery lost a publication wake")]
fn watch_switch_negative_control_registers_after_reading() {
    discovery::registration_race(true, false);
}
#[test]
fn cancelling_discovery_preserves_other_subscriptions() {
    discovery::cancellation_race(false);
}
#[test]
#[should_panic(expected = "cancellation destroyed an independent discovery subscription")]
fn cancellation_negative_control_destroys_the_shared_watch() {
    discovery::cancellation_race(true);
}
#[test]
fn local_discovery_ignores_the_device_barrier() {
    discovery::local_accepted(false);
}
#[test]
#[should_panic(expected = "discovery waited for a device acknowledgement")]
fn local_visibility_negative_control_joins_durability() {
    discovery::local_accepted(true);
}
#[test]
fn local_failure_wakes_parked_discovery() {
    discovery::local_failure(false);
}
#[test]
#[should_panic(expected = "provider failure stranded discovery")]
fn local_failure_negative_control_omits_the_failure_subscription() {
    discovery::local_failure(true);
}
