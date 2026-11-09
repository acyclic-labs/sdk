//! Account for actual bounded JSON allocations and every selected refusal prefix.
#![no_main]
acyclic_test_support::counting_allocator!();

use acyclic_harness::executor::json::{JsonBounds, JsonCodecCharge, json_value_bounded};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| assert_eq!(check(input), Ok(())));

fn check(input: &[u8]) -> Result<(), String> {
    let Some((&prefix, input)) = input.split_first() else {
        return Ok(());
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(input) else {
        return Ok(());
    };
    let encoded = serde_json::to_vec(&value)
        .map_err(|error| format!("reference JSON encode failed: {error}; value {value:?}"))?;
    let expected: serde_json::Value = serde_json::from_slice(&encoded)
        .map_err(|error| format!("reference JSON decode failed: {error}; bytes {encoded:?}"))?;
    // The independently parsed document supplies its capacity. Long valid
    // inputs exercise the same accounting instead of disappearing at a cutoff.
    let bounds = JsonBounds {
        maximum_bytes: input.len().max(encoded.len()) as u64,
        maximum_items: input.len() as u64,
        maximum_depth: 128,
    };
    let raw = prefix & 1 != 0;
    let prefix = u64::from(prefix >> 1);
    if raw {
        let raw: &serde_json::value::RawValue = serde_json::from_slice(input)
            .map_err(|error| format!("valid JSON raw decode failed: {error}; input {input:?}"))?;
        // Both RawValue preparation passes see even overwritten duplicate
        // values. Count all raw entries, not only the surviving Value tree.
        exercise(raw, &expected, bounds, prefix, 2 * raw_entries(input) / 5)
    } else {
        exercise(&value, &expected, bounds, prefix, 0)
    }
}

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
    expected: &serde_json::Value,
    bounds: JsonBounds,
    prefix: u64,
    extra_slack: u64,
) -> Result<(), String> {
    let slack = slack(expected) + extra_slack;
    let copied = json_value_bounded(value, bounds, None, &mut |_| Ok(())).map_err(|error| {
        format!("unrestricted JSON conversion failed: {error}; expected {expected:?}")
    })?;
    assert_eq!(
        &copied, expected,
        "unrestricted JSON conversion changed the value"
    );
    drop(copied);
    let mut charged = 0;
    let mut result = Ok(serde_json::Value::Null);
    let measured = acyclic_test_support::measure(|| {
        result = json_value_bounded(value, bounds, None, &mut |event| {
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
        });
    });
    match result {
        Ok(copy) => {
            assert_eq!(&copy, expected, "funded JSON conversion changed the value");
        }
        Err(acyclic_harness::Error::Indeterminate(operation)) => assert_eq!(
            operation,
            acyclic_harness::OperationId::from_bytes([0; 16]),
            "JSON allocation refusal changed its operation identity"
        ),
        Err(error) => {
            return Err(format!(
                "unexpected bounded conversion error: {error}; prefix {prefix}; expected {expected:?}"
            ));
        }
    }
    assert!(
        charged >= measured.count_total && charged <= measured.count_total + slack,
        "JSON allocation accounting differs: charged {charged}, actual {}, slack {slack}, prefix {prefix}; expected {expected:?}",
        measured.count_total,
    );
    Ok(())
}
