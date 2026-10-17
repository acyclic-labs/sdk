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
    let raw = prefix & 1 != 0;
    let prefix = u64::from(prefix >> 1);
    let expected: serde_json::Value =
        serde_json::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap();
    if raw {
        let raw: &serde_json::value::RawValue = serde_json::from_slice(input).unwrap();
        // Both RawValue preparation passes see even overwritten duplicate
        // values. Count all raw entries, not only the surviving Value tree.
        exercise(raw, expected, bounds, prefix, 2 * raw_entries(input) / 5);
    } else {
        exercise(&value, expected, bounds, prefix, 0);
    }
});

fn slack(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Object(entries) => {
            let count = entries.len() as u64;
            let maximum = if count == 0 { 0 } else { 1 + (count - 1) / 5 };
            maximum - count.div_ceil(11) + entries.values().map(slack).sum::<u64>()
        }
        serde_json::Value::Array(entries) => entries.iter().map(slack).sum(),
        _ => 0,
    }
}

fn raw_entries(input: &[u8]) -> u64 {
    let (mut string, mut escaped, mut entries) = (false, false, 0);
    for byte in input {
        if string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                string = false;
            }
        } else if *byte == b'"' {
            string = true;
        } else if *byte == b':' {
            entries += 1;
        }
    }
    entries
}

fn exercise<T: serde::Serialize + ?Sized>(
    value: &T,
    expected: serde_json::Value,
    bounds: JsonBounds,
    prefix: u64,
    extra_slack: u64,
) {
    let slack = slack(&expected) + extra_slack;
    assert_eq!(
        json_value_bounded(value, bounds, None, &mut |_| Ok(())).unwrap(),
        expected
    );
    let mut charged = 0;
    let mut result = None;
    let measured = acyclic_test_support::measure(|| {
        result = Some(json_value_bounded(value, bounds, None, &mut |event| {
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
            assert!(charged >= measured.count_total && charged <= measured.count_total + slack);
        }
        Err(acyclic_harness::Error::Indeterminate(_)) => {
            assert!(charged >= measured.count_total && charged <= measured.count_total + slack)
        }
        Err(error) => panic!("unexpected bounded conversion error: {error}"),
    }
}
