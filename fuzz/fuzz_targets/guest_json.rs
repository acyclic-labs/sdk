//! Check admitted guest envelopes and the independent metadata oracle.
#![no_main]

use acyclic_actors::guest::{AdmittedCaller, Checkpoint, GuestEvent, GuestEventResult};
use libfuzzer_sys::fuzz_target;

#[path = "../../rust/crates/actors/tests/support/metadata.rs"]
mod metadata;

macro_rules! round_trip {
    ($input:expr, $ty:ty) => {
        if let Ok(original) = serde_json::from_slice::<$ty>($input) {
            let decoded = serde_json::to_vec(&original)
                .map_err(|error| ("serialize", error))
                .and_then(|encoded| {
                    serde_json::from_slice::<$ty>(&encoded).map_err(|error| ("decode", error))
                });
            assert!(
                decoded.as_ref().is_ok_and(|decoded| decoded == &original),
                "guest round trip failed: type={}; original={original:?}; decoded={decoded:?}",
                stringify!($ty)
            );
        }
    };
}

fuzz_target!(|input: &[u8]| {
    metadata::check(input);
    round_trip!(input, GuestEventResult);
    round_trip!(input, AdmittedCaller);
    round_trip!(input, GuestEvent);
    round_trip!(input, Checkpoint);
});
