#![no_main]

use acyclic_harness::executor::json::{JsonBounds, JsonCodecCharge, json_value_bounded};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let Some((&prefix, input)) = input.split_first() else {
        return;
    };
    if input.len() > 4096 {
        return;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(input) else {
        return;
    };
    let bounds = JsonBounds {
        maximum_bytes: 65_536,
        maximum_items: 4096,
        maximum_depth: 128,
    };
    let prefix = u64::from(prefix);
    let expected: serde_json::Value =
        serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(
        json_value_bounded(&value, bounds, None, &mut |_| Ok(())).unwrap(),
        expected
    );
    let mut charged = 0;
    let mut result = None;
    let measured = allocation_counter::measure(|| {
        result = Some(json_value_bounded(&value, bounds, None, &mut |event| {
            let operations = match event {
                JsonCodecCharge::AllocationOperation { bytes } => u64::from(bytes != 0),
                JsonCodecCharge::AllocationOperations { operations } => operations,
                _ => 0,
            };
            if charged + operations > prefix {
                return Err(acyclic_harness::Error::Indeterminate(
                    acyclic_harness::OperationId::from_bytes([0; 16]),
                ));
            }
            charged += operations;
            Ok(())
        }));
    });
    match result.unwrap() {
        Ok(copy) => {
            assert_eq!(copy, expected);
            assert_eq!(measured.count_total, charged);
        }
        Err(acyclic_harness::Error::Indeterminate(_)) => assert_eq!(measured.count_total, charged),
        Err(error) => panic!("unexpected bounded conversion error: {error}"),
    }
});
