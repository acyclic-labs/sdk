#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_actors::guest::{AdmittedCaller, Checkpoint, GuestEvent, GuestEventResult};

#[path = "../../rust/crates/actors/tests/support/metadata.rs"]
mod metadata;

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    metadata::check(input);
    if let Some(result) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<GuestEventResult>)
    {
        let encoded = serde_json::to_vec(&result).expect("event result must encode");
        let _ = fuzz_support::round_trip(
            &(result),
            serde_json::from_slice::<GuestEventResult>(&encoded),
        );
    }
    if let Some(caller) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<AdmittedCaller>)
    {
        let encoded = serde_json::to_vec(&caller).expect("caller projection must encode");
        let _ = fuzz_support::round_trip(
            &(caller),
            serde_json::from_slice::<AdmittedCaller>(&encoded),
        );
    }
    if let Some(event) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<GuestEvent>)
    {
        let encoded = serde_json::to_vec(&event).expect("accepted event must encode");
        let _ = fuzz_support::round_trip(&(event), serde_json::from_slice::<GuestEvent>(&encoded));
    }
    if let Some(checkpoint) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<Checkpoint>)
    {
        let encoded = serde_json::to_vec(&checkpoint).expect("accepted checkpoint must encode");
        let _ = fuzz_support::round_trip(
            &(checkpoint),
            serde_json::from_slice::<Checkpoint>(&encoded),
        );
    }
});
