#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::executor::json::{JsonBounds, JsonCodecCharge, json_value_bounded};

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    let Some((&prefix, input)) = input.split_first() else {
        return;
    };
    if input.len() > 4096 {
        return;
    }
    let Some(fuzz_input) = fuzz_input.view(input) else {
        return;
    };
    let Some(value) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<serde_json::Value>)
    else {
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
        fuzz_support::must_decode(serde_json::from_slice(&serde_json::to_vec(&value).unwrap()));
    if raw {
        let raw: &serde_json::value::RawValue =
            fuzz_support::must_decode(serde_json::from_slice(input));
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
    let mut required = 0;
    let _ = fuzz_support::round_trip(
        &expected,
        json_value_bounded(value, bounds, None, &mut |event| {
            required += operations(event);
            Ok(())
        })
        .map_err(|error| (error.code(), error.to_string())),
    );
    let expected_result = if prefix < required {
        let error = acyclic_harness::Error::Indeterminate(
            acyclic_harness::OperationId::from_bytes([0; 16]),
        );
        Err((error.code(), error.to_string()))
    } else {
        Ok(expected)
    };
    let mut charged = 0;
    let mut result = None;
    let measured = acyclic_test_support::measure(|| {
        result = Some(json_value_bounded(value, bounds, None, &mut |event| {
            let operations = operations(event);
            if charged + operations > prefix {
                return Err(acyclic_harness::Error::Indeterminate(
                    acyclic_harness::OperationId::from_bytes([0; 16]),
                ));
            }
            charged += operations;
            Ok(())
        }));
    });
    let _ = fuzz_support::same_result(
        &expected_result,
        result
            .unwrap()
            .map_err(|error| (error.code(), error.to_string())),
    );
    assert!(charged >= measured.count_total && charged <= measured.count_total + slack);
}

fn operations(event: JsonCodecCharge) -> u64 {
    match event {
        JsonCodecCharge::AllocationOperation { bytes } => u64::from(bytes != 0),
        JsonCodecCharge::AllocationOperations { operations } => operations,
        _ => 0,
    }
}
