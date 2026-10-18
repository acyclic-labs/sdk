#![no_main]

use acyclic_harness::{
    AgentId, OperationId, TaskId, conversation::VolumeRef, tool::files::*, workflow,
};
use libfuzzer_sys::fuzz_target;
use serde::{Serialize, de::DeserializeOwned};

// Compile the private production record without adding a public parser API.
#[path = "../../harness/src/local/owner_record.rs"]
mod owner_record;
#[path = "../../harness/src/goal/request_head.rs"]
mod turn_record;

fn round_trip<T: Serialize + DeserializeOwned>(input: &[u8]) {
    if let Ok(value) = serde_json::from_slice::<T>(input) {
        let encoded = serde_json::to_vec(&value).unwrap();
        let recovered: T = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(serde_json::to_vec(&recovered).unwrap(), encoded);
    }
}

fuzz_target!(|input: &[u8]| {
    if let Ok(turn) = turn_record::decode(input) {
        let bytes = serde_json::to_vec(&turn).unwrap();
        assert_eq!(turn_record::decode(&bytes).unwrap(), turn);
    }
    round_trip::<owner_record::Owner>(input);
    round_trip::<acyclic_fs::GitCompatState>(input);
    round_trip::<ReadFileInput>(input);
    round_trip::<WriteFileInput>(input);
    round_trip::<EditFileInput>(input);
    round_trip::<acyclic_harness::Outcome<serde_json::Value>>(input);
    round_trip::<acyclic_harness::scheduler::SchedulerEvent>(input);
    round_trip::<acyclic_harness::scheduler::OperationState>(input);
});
