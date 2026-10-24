#![no_main]
#[cfg(test)]
use acyclic_harness::{conversation, resources};

use acyclic_harness::{
    AgentId, OperationId, TaskId, conversation::VolumeRef, tool::files::*, workflow,
};
use libfuzzer_sys::fuzz_target;
use serde::{Serialize, de::DeserializeOwned};

// Compile the private production record without adding a public parser API.
#[path = "../../filesystem/src/native_host/leaf.rs"]
mod native_leaf;
#[path = "../../filesystem/src/native_host/held_files/record.rs"]
mod native_record;
#[path = "../../harness/src/local/owner_record.rs"]
mod owner_record;
#[path = "../../harness-core/src/goal/request_head.rs"]
mod turn_record;

fn round_trip<T: Serialize + DeserializeOwned>(input: &[u8]) {
    if let Ok(value) = serde_json::from_slice::<T>(input) {
        let encoded = serde_json::to_vec(&value).unwrap();
        let recovered: T = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(serde_json::to_vec(&recovered).unwrap(), encoded);
    }
}

fuzz_target!(|input: &[u8]| {
    #[cfg(unix)]
    {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt, path::Path};
        let expected =
            !input.is_empty() && input != b"." && input != b".." && !input.contains(&b'/');
        assert_eq!(
            native_leaf::require_leaf(Path::new(OsStr::from_bytes(input))).is_ok(),
            expected
        );
    }
    let record = native_record::read::<16>(&mut &*input);
    assert_eq!(record.is_ok(), input.len() == 16);
    if let Ok(record) = record {
        assert_eq!(record.as_slice(), input);
        assert_eq!(
            native_record::read::<16>(&mut record.as_slice()).unwrap(),
            record
        );
    }
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
