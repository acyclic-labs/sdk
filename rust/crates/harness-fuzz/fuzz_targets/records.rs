#![no_main]
#[cfg(test)]
use acyclic_harness::{conversation, resources};
// The included production records derive `crate::HeapLayout`, which names
// its crate by the core package path that `acyclic_harness` re-exports.
extern crate acyclic_harness as acyclic_harness_core;
use acyclic_harness::HeapLayout;

use acyclic_harness::tool::files::*;
use libfuzzer_sys::fuzz_target;
use serde::{Serialize, de::DeserializeOwned};

fn round_trip<T: Serialize + DeserializeOwned>(input: &[u8]) {
    if let Ok(value) = serde_json::from_slice::<T>(input) {
        let encoded = serde_json::to_vec(&value).unwrap();
        let recovered: T = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(serde_json::to_vec(&recovered).unwrap(), encoded);
    }
}

fuzz_target!(|input: &[u8]| {
    acyclic_harness::goal::fuzz_request_head(input);
    acyclic_harness::local::fuzz_owner_record(input);
    round_trip::<acyclic_fs::GitCompatState>(input);
    round_trip::<ReadFileInput>(input);
    round_trip::<WriteFileInput>(input);
    round_trip::<EditFileInput>(input);
    round_trip::<acyclic_harness::Outcome<serde_json::Value>>(input);
    round_trip::<acyclic_harness::scheduler::SchedulerEvent>(input);
    round_trip::<acyclic_harness::scheduler::OperationState>(input);
});
