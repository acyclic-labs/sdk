use super::*;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;
use std::cell::Cell;

struct Once<'a> {
    value: &'a Value,
    calls: &'a Cell<usize>,
}
impl Serialize for Once<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.calls.set(self.calls.get() + 1);
        self.value.serialize(serializer)
    }
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    Repeat,
    Corrupt,
    OmitCharge,
    OmitScanCharge,
}
fn check(value: &Value, canonical: bool, fault: Fault) -> TestCaseResult {
    let calls = Cell::new(0);
    let once = Once {
        value,
        calls: &calls,
    };
    let mut retained = 0_u64;
    let mut encoded = 0_u64;
    let mut inspected = 0_u64;
    let mut allocations = 0;
    let mut charge = |event| {
        match event {
            JsonCodecCharge::Retain { bytes } => retained += bytes,
            JsonCodecCharge::Release { bytes } => retained = retained.checked_sub(bytes).unwrap(),
            JsonCodecCharge::AllocationOperation { .. } => allocations += 1,
            JsonCodecCharge::Work { bytes, .. } if !matches!(fault, Fault::OmitCharge) => {
                encoded += bytes
            }
            JsonCodecCharge::Inspect { items } if !matches!(fault, Fault::OmitScanCharge) => {
                inspected += items
            }
            _ => {}
        }
        Ok(())
    };
    let bounds = JsonBounds {
        maximum_bytes: 65536,
        maximum_items: 65536,
        maximum_depth: 256,
    };
    let mut actual = if canonical {
        canonical_json_bytes_bounded(&once, bounds, None, &mut charge).unwrap()
    } else {
        json_bytes_bounded(&once, bounds, None, &mut charge).unwrap()
    };
    if matches!(fault, Fault::Repeat) {
        serde_json::to_vec(&once).unwrap();
    }
    if matches!(fault, Fault::Corrupt) {
        actual.pop();
    }
    let mut expected_value = value.clone();
    if canonical {
        expected_value.sort_all_objects();
    }
    let expected = serde_json::to_vec(&expected_value).unwrap();
    let minimum = expected.len() as u64;
    prop_assert!(
        calls.get() == 1
            && actual == expected
            && retained == actual.capacity() as u64
            && encoded == minimum * if canonical { 2 } else { 1 }
            && inspected
                == if canonical && has_array(value) {
                    lexical_items(&expected)
                } else {
                    0
                }
            && allocations > 0,
        "single serialization, funded storage or canonical output differs: canonical={}, calls={}, actual={:?}, expected={:?}, retained={}, capacity={}, encoded={}, minimum={}, inspected={}, allocations={}",
        canonical,
        calls.get(),
        actual,
        expected,
        retained,
        actual.capacity(),
        encoded,
        minimum,
        inspected,
        allocations
    );
    Ok(())
}
// Independent count of the items a compact document's scan examines: each
// string, opened container and scalar literal.
fn lexical_items(document: &[u8]) -> u64 {
    let (mut items, mut string, mut escape, mut atom) = (0, false, false, false);
    for &byte in document {
        if string {
            (string, escape) = match (escape, byte) {
                (true, _) => (true, false),
                (false, b'\\') => (true, true),
                (false, b'"') => (false, false),
                _ => (true, false),
            };
            continue;
        }
        let starts = match byte {
            b'"' => {
                string = true;
                true
            }
            b'[' | b'{' => true,
            b']' | b'}' | b':' | b',' => false,
            _ => !atom,
        };
        atom = !matches!(byte, b'"' | b'[' | b'{' | b']' | b'}' | b':' | b',');
        items += u64::from(starts);
    }
    items
}
fn has_array(value: &Value) -> bool {
    match value {
        Value::Array(_) => true,
        Value::Object(fields) => fields.values().any(has_array),
        _ => false,
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn single_pass_encoding_preserves_bytes_and_all_live_storage(
        text in "[a-z\\x00\\x7fé🦀]{0,96}", width in 0_usize..96, depth in 0_usize..5,
    ) {
        let mut value = serde_json::json!({"z": text, "a": (0..width).collect::<Vec<_>>()});
        for _ in 0..depth { value = Value::Array(vec![value, Value::Array(vec![])]); }
        for canonical in [false, true] { check(&value, canonical, Fault::None)?; }
    }
}
#[test]
fn single_pass_negative_controls() {
    let value = serde_json::json!({"z": ["a", "b"], "a": 7});
    for canonical in [false, true] {
        check(&value, canonical, Fault::None).unwrap();
        let mut faults = vec![Fault::Repeat, Fault::Corrupt, Fault::OmitCharge];
        if canonical {
            faults.push(Fault::OmitScanCharge);
        }
        for fault in faults {
            let error = check(&value, canonical, fault).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("single serialization, funded storage or canonical output differs")
            );
        }
    }
}

// The index scan is funded with exactly the serializer's item count; funding
// one item fewer or more is rejected rather than silently absorbed.
#[test]
fn index_scan_examines_exactly_its_admitted_items() {
    let document = br#"{"a":7,"z":["a",["b\"]"],[],true]}"#;
    let items = lexical_items(document);
    let bounds = JsonBounds {
        maximum_bytes: 65536,
        maximum_items: 65536,
        maximum_depth: 256,
    };
    for (funded, reason) in [
        (items, None),
        (items - 1, Some("scan exceeds its admission")),
        (items + 1, Some("scan differs from its admission")),
    ] {
        let mut inspected = 0;
        let result = super::super::arrays::build(document, 3, funded, bounds, None, &mut |event| {
            if let JsonCodecCharge::Inspect { items } = event {
                inspected += items;
            }
            Ok(())
        });
        assert_eq!(inspected, funded);
        match reason {
            None => assert!(result.is_ok()),
            Some(reason) => assert!(format!("{:?}", result.unwrap_err()).contains(reason)),
        }
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod allocation_prefixes {
    use super::*;
    #[derive(Clone, Copy)]
    enum Fault {
        None,
        EarlyAllocation,
        IgnoreRefusal,
    }
    fn check(
        value: &Value,
        limit: usize,
        success: bool,
        fault: Fault,
    ) -> std::result::Result<usize, proptest::test_runner::TestCaseError> {
        let mut live = 0_u64;
        let mut old_capacity = 0;
        let mut admitted = 0;
        let mut funded = true;
        let mut output = Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "not encoded",
        });
        let measured = acyclic_test_support::measure(|| {
            let premature = if matches!(fault, Fault::EarlyAllocation) {
                let mut bytes = Vec::<u8>::new();
                bytes.try_reserve_exact(1).unwrap();
                Some(bytes)
            } else {
                None
            };
            std::hint::black_box(&premature);
            output = json_bytes_bounded(
                value,
                JsonBounds {
                    maximum_bytes: 65536,
                    maximum_items: 65536,
                    maximum_depth: 256,
                },
                Some(crate::wire::ErrorCode::Invalid),
                &mut |event| {
                    match event {
                        JsonCodecCharge::Retain { bytes } => live += bytes,
                        JsonCodecCharge::Release { bytes } => {
                            funded &= bytes <= live;
                            live = live.saturating_sub(bytes);
                        }
                        JsonCodecCharge::AllocationOperation { bytes } if bytes != 0 => {
                            funded &= live >= old_capacity + bytes;
                            if admitted == limit {
                                if !matches!(fault, Fault::IgnoreRefusal) {
                                    return Err(crate::Error::NativeBoundary {
                                        code: crate::wire::ErrorCode::Invalid,
                                        reason: "allocation prefix exhausted",
                                    });
                                }
                            } else {
                                admitted += 1;
                                old_capacity = bytes;
                            }
                        }
                        _ => {}
                    }
                    Ok(())
                },
            );
        });
        let expected = serde_json::to_vec(value).unwrap();
        let final_storage = match &output {
            Ok(bytes) => {
                bytes == &expected
                    && live == bytes.capacity() as u64
                    && measured.bytes_current == live as i64
                    && measured.count_current == 1
            }
            Err(_) => live == 0 && measured.bytes_current == 0 && measured.count_current == 0,
        };
        prop_assert!(
            output.is_ok() == success
                && funded
                && final_storage
                && measured.count_total == admitted as u64,
            "encoding allocated outside its admitted prefix or lost retained storage"
        );
        Ok(admitted)
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn every_output_allocation_is_preceded_by_its_original_funding(text in ".{0,64}", width in 0_usize..64) {
            let value = serde_json::json!({"text": text, "wide": (0..width).collect::<Vec<_>>(), "growth": "x".repeat(512)});
            let total = check(&value, usize::MAX, true, Fault::None)?;
            for limit in 0..total { check(&value, limit, false, Fault::None)?; }
        }
    }
    #[test]
    fn allocation_prefix_negative_controls() {
        let value = serde_json::json!({"growth": "x".repeat(512)});
        check(&value, 0, false, Fault::None).unwrap();
        for fault in [Fault::EarlyAllocation, Fault::IgnoreRefusal] {
            let error = check(&value, 0, false, fault).unwrap_err();
            assert!(error.to_string().contains(
                "encoding allocated outside its admitted prefix or lost retained storage"
            ));
        }
    }
}
