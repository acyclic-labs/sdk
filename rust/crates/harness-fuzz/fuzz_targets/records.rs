#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{
    AgentId, OperationId, TaskId,
    conversation::VolumeRef,
    tool::files::{EditFileInput, ReadFileInput, WriteFileInput},
    workflow,
};
use serde::{Serialize, de::DeserializeOwned};

// Compile the private production record without adding a public parser API.
#[path = "../../harness/src/local/owner_record.rs"]
mod owner_record;
#[path = "../../harness/src/goal/request_head.rs"]
mod turn_record;

fn round_trip<T: Serialize + DeserializeOwned>(fuzz_input: fuzz_support::FuzzInput<'_>) {
    if let Some(value) = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<T>) {
        let encoded = serde_json::to_vec(&value).unwrap();
        let _ = fuzz_support::round_trip(
            &encoded,
            serde_json::from_slice::<T>(&encoded).and_then(|value| serde_json::to_vec(&value)),
        );
    }
}

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if let Some(turn) = fuzz_support::FuzzInput::parse(fuzz_input, turn_record::decode) {
        let bytes = serde_json::to_vec(&turn).unwrap();
        let _ = fuzz_support::round_trip(&(turn), turn_record::decode(&bytes));
    }
    round_trip::<owner_record::Owner>(fuzz_input);
    round_trip::<acyclic_fs::GitCompatState>(fuzz_input);
    round_trip::<ReadFileInput>(fuzz_input);
    round_trip::<WriteFileInput>(fuzz_input);
    round_trip::<EditFileInput>(fuzz_input);
});
