#![no_main]

use acyclic_harness::task_volume::TaskStatus;
use libfuzzer_sys::fuzz_target;

// Untrusted `.tasks/<id>/status.json` bytes through the production decoder:
// whatever decodes re-encodes to bytes that decode to the same status.
fuzz_target!(|input: &[u8]| {
    if let Ok(status) = TaskStatus::parse(input) {
        let bytes = status.encode().unwrap();
        assert_eq!(TaskStatus::parse(&bytes).unwrap(), status);
        assert_eq!(status.encode().unwrap(), bytes);
    }
});
