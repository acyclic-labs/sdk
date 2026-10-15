#![no_main]

use acyclic_actors::guest::{Checkpoint, GuestEvent};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
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
