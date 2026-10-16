#![no_main]

use acyclic_actors::guest::{AdmittedCaller, Checkpoint, GuestEvent, GuestEventResult};
use libfuzzer_sys::fuzz_target;

#[path = "../../rust/crates/actors/tests/support/metadata.rs"]
mod metadata;

fuzz_target!(|input: &[u8]| {
    metadata::check(input);
    if let Ok(result) = serde_json::from_slice::<GuestEventResult>(input) {
        let encoded = serde_json::to_vec(&result).expect("event result must encode");
        assert_eq!(
            serde_json::from_slice::<GuestEventResult>(&encoded).expect("result round trip"),
            result
        );
    }
    if let Ok(caller) = serde_json::from_slice::<AdmittedCaller>(input) {
        let encoded = serde_json::to_vec(&caller).expect("caller projection must encode");
        assert_eq!(
            serde_json::from_slice::<AdmittedCaller>(&encoded).expect("caller round trip"),
            caller
        );
    }
    if let Ok(event) = serde_json::from_slice::<GuestEvent>(input) {
        let encoded = serde_json::to_vec(&event).expect("accepted event must encode");
        assert_eq!(
            serde_json::from_slice::<GuestEvent>(&encoded).expect("event round trip"),
            event
        );
    }
    if let Ok(checkpoint) = serde_json::from_slice::<Checkpoint>(input) {
        let encoded = serde_json::to_vec(&checkpoint).expect("accepted checkpoint must encode");
        assert_eq!(
            serde_json::from_slice::<Checkpoint>(&encoded).expect("checkpoint round trip"),
            checkpoint
        );
    }
});
