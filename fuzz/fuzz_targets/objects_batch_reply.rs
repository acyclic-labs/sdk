#![no_main]
use acyclic_objects::{test_meter::CountingMeter, v1::grpc::fuzz_batch_reply};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 4096 {
        return;
    }
    for expected in [bytes.first().copied().unwrap_or(0) as usize % 9, 2] {
        for cancel in [false, true] {
            for budget in [None, Some(bytes.last().copied().unwrap_or(0) as u64 % 3)] {
                let meter = std::sync::Arc::new(CountingMeter::default());
                assert!(fuzz_batch_reply(bytes, expected, meter, cancel, budget).is_ok());
            }
        }
    }
});
