use super::*;
use serde::Serialize;

const BOUNDS: JsonBounds = JsonBounds {
    maximum_bytes: 65_536,
    maximum_items: 4_096,
    maximum_depth: 32,
};

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    // Every source byte is funded before lexical inspection; denial never
    // starts inspection, and successful admission charges exactly its input.
    #[test]
    fn source_admission_funds_all_bytes_before_inspection(text in ".{0,4096}", allowance in 0_u64..16384) {
        let bytes = serde_json::to_vec(&text).unwrap();
        let mut read = 0;
        let mut inspected = false;
        let result = admission::source(&bytes, BOUNDS, None, None, &mut |event| {
            match event {
                JsonCodecCharge::Read { bytes } => {
                    if bytes > allowance - read {
                        return Err(Error::Unsupported("read denied".into()));
                    }
                    read += bytes;
                }
                JsonCodecCharge::Work { items, .. } if items > 0 => {
                    assert_eq!(read, bytes.len() as u64);
                    inspected = true;
                }
                _ => {}
            }
            Ok(())
        });
        proptest::prop_assert_eq!(result.is_ok(), bytes.len() as u64 <= allowance);
        if result.is_ok() {
            proptest::prop_assert_eq!(read, bytes.len() as u64);
            proptest::prop_assert!(inspected);
        } else {
            proptest::prop_assert_eq!(read, 0);
            proptest::prop_assert!(!inspected);
            proptest::prop_assert!(matches!(result, Err(Error::Unsupported(_))));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn assert_encoding_needs_no_heap(value: &Value) -> Result<()> {
    let bytes = crate::contract::canonical_json_bytes(value)?;
    assert!(bytes.len() > 8 * 4096);
    let expected = *blake3::hash(&bytes).as_bytes();
    let mut converted = Ok(Value::Null);
    let conversion = allocation_counter::measure(|| {
        converted = serde_json::to_value(value);
        if let Ok(value) = &mut converted {
            value.sort_all_objects();
        }
    });
    drop(std::hint::black_box(
        converted.map_err(|error| Error::Invalid(error.to_string()))?,
    ));
    let mut digest = Ok([0; 32]);
    let encoding = allocation_counter::measure(|| {
        digest = canonical_json_digest(value, u64::MAX, &mut None);
    });
    assert_eq!(digest?, expected);
    assert_eq!(encoding.count_total, conversion.count_total);
    assert_eq!(encoding.bytes_total, conversion.bytes_total);
    assert_eq!(encoding.bytes_max, conversion.bytes_max);
    assert_eq!(encoding.count_current, 0);
    assert_eq!(encoding.bytes_current, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn large_string_digest_has_no_encoding_heap() -> Result<()> {
    for text in ["x".repeat(1024 * 1024), "\0\n\"\\é".repeat(65_536)] {
        assert_encoding_needs_no_heap(&Value::String(text))?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn nested_digest_has_no_encoding_heap() -> Result<()> {
    let rows: Vec<_> = (0..256)
        .map(|index| serde_json::json!({"z": [index, "\0\n\"\\é".repeat(64)], "a": {"id": index}}))
        .collect();
    assert_encoding_needs_no_heap(&serde_json::json!({"z": rows, "a": [true, null]}))
}

#[cfg(feature = "filesystem")]
#[test]
fn canonical_digest_preserves_hash_admission_and_charge() -> Result<()> {
    let value = serde_json::json!({"z": [0, 1, 255], "a": "\0\n\"é"});
    let bytes = crate::contract::canonical_json_bytes(&value)?;
    let length = bytes.len() as u64;
    for allowance in [length, 0] {
        let io = crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::WorkBudget {
                allocation_operations: 1_000_000,
                peak_allocation_bytes: 64 * 1024 * 1024,
                bytes_copied: 64 * 1024 * 1024,
                bytes_encoded: 64 * 1024 * 1024,
                bytes_hashed: allowance,
                items_examined: 1_000_000,
                ..Default::default()
            },
            &acyclic_fs::CancellationToken::default(),
        )?;
        let mut work = crate::store::HistoryWork::new(io);
        let result = canonical_json_digest(&value, length, &mut Some(&mut work));
        if allowance == 0 {
            assert!(result.is_err());
        } else {
            assert_eq!(result?, *blake3::hash(&bytes).as_bytes());
            assert_eq!(work.receipt().bytes_hashed, length);
            println!("canonical digest charges: {:?}", work.receipt());
        }
    }
    Ok(())
}

#[test]
fn streamed_identity_matches_canonical_bytes() -> Result<()> {
    #[derive(Serialize)]
    struct Record<'a> {
        z: f32,
        a: &'a serde_json::value::RawValue,
        text: String,
    }
    struct Counted<'a, T>(&'a std::cell::Cell<usize>, &'a T);
    impl<T: Serialize> Serialize for Counted<'_, T> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.0.set(self.0.get() + 1);
            self.1.serialize(serializer)
        }
    }
    let raw = serde_json::value::RawValue::from_string(
        r#"{"z":[0.1,-0.0],"a":18446744073709551615,"z":[2]}"#.into(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    for length in [0, 1, 4095, 4096, 4097, 65536] {
        for text in ["\0\n\"\\é".repeat(length), "x".repeat(length)] {
            let record = Record {
                z: 0.1,
                a: &raw,
                text,
            };
            let bytes = crate::contract::canonical_json_bytes(&record)?;
            let calls = std::cell::Cell::new(0);
            assert_eq!(
                canonical_json_digest(&Counted(&calls, &record), u64::MAX, &mut None)?,
                *blake3::hash(&bytes).as_bytes(),
            );
            assert_eq!(calls.get(), 1);
        }
    }
    for encoded_length in [4095, 4096, 4097] {
        let text = "x".repeat(encoded_length - 2);
        let bytes = crate::contract::canonical_json_bytes(&text)?;
        assert_eq!(bytes.len(), encoded_length);
        assert_eq!(
            canonical_json_digest(&text, u64::MAX, &mut None)?,
            *blake3::hash(&bytes).as_bytes(),
        );
    }
    assert!(canonical_json_digest(&u128::MAX, u64::MAX, &mut None).is_err());
    Ok(())
}

#[test]
fn exact_byte_item_depth_and_escape_admission() -> Result<()> {
    let value = serde_json::json!({"z": ["\u{0}\"\\\n", u64::MAX], "a": i64::MIN});
    let expected = crate::contract::canonical_json_bytes(&value)?;
    let bounds = JsonBounds {
        maximum_bytes: expected.len() as u64,
        maximum_items: 7,
        maximum_depth: 2,
    };
    assert_eq!(
        canonical_json_bytes_bounded(&value, bounds, None, &mut |_| Ok(()))?,
        expected
    );
    assert!(
        canonical_json_bytes_bounded(
            &value,
            JsonBounds {
                maximum_bytes: bounds.maximum_bytes - 1,
                ..bounds
            },
            None,
            &mut |_| Ok(())
        )
        .is_err()
    );
    assert!(
        canonical_json_bytes_bounded(
            &value,
            JsonBounds {
                maximum_items: 6,
                ..bounds
            },
            None,
            &mut |_| Ok(())
        )
        .is_err()
    );
    assert!(
        canonical_json_bytes_bounded(
            &value,
            JsonBounds {
                maximum_depth: 1,
                ..bounds
            },
            None,
            &mut |_| Ok(())
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn allocation_denial_precedes_growth_and_preserves_its_error() {
    let value = serde_json::json!({"b": ["escape\n", 0.1], "a": {"later": 2, "earlier": 1}});
    let operation = crate::OperationId::from_bytes([17; 16]);
    let mut allocated = false;
    let result = canonical_json_bytes_bounded(&value, BOUNDS, None, &mut |event| match event {
        JsonCodecCharge::Retain { .. } => Err(Error::Indeterminate(operation)),
        JsonCodecCharge::AllocationOperation { .. } => {
            allocated = true;
            Ok(())
        }
        _ => Ok(()),
    });
    assert!(matches!(result, Err(Error::Indeterminate(original)) if original == operation));
    assert!(!allocated);
}

#[test]
fn ordinary_record_bytes_and_raw_value_identity_remain_exact() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        z: f32,
        a: u64,
    }
    let record = Record {
        z: 0.1,
        a: u64::MAX,
    };
    let expected =
        serde_json::to_vec(&record).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        json_bytes_bounded(&record, BOUNDS, None, &mut |_| Ok(()))?,
        expected
    );
    let raw = serde_json::value::RawValue::from_string(
        r#"{"z":[0.1,-0.0],"a":18446744073709551615,"z":[2]}"#.into(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut old = serde_json::to_value(&raw).map_err(|error| Error::Invalid(error.to_string()))?;
    old.sort_all_objects();
    let expected = serde_json::to_vec(&old).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        canonical_json_bytes_bounded(&raw, BOUNDS, None, &mut |_| Ok(()))?,
        expected
    );
    Ok(())
}
