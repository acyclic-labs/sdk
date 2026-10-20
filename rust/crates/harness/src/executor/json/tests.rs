use super::*;
use serde::Serialize;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod digest_properties {
    use super::*;
    use proptest::prelude::*;

    // Count from the value tree, independently of the byte lexer: containers,
    // object keys and scalar values each contribute exactly one item.
    fn items(value: &Value) -> (u64, bool) {
        match value {
            Value::Array(values) => values.iter().fold((1, true), |(count, arrays), value| {
                let (child, nested) = items(value);
                (count + child, arrays || nested)
            }),
            Value::Object(fields) => fields.values().fold((1, false), |(count, arrays), value| {
                let (child, nested) = items(value);
                (count + 1 + child, arrays || nested)
            }),
            _ => (1, false),
        }
    }

    fn values() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::from),
            ".{0,16}".prop_map(Value::String)
        ]
        .prop_recursive(3, 32, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::btree_map(".{0,8}", inner, 0..4)
                    .prop_map(|entries| Value::Object(entries.into_iter().collect())),
            ]
        })
    }

    proptest! {
        // Flat arrays need one output buffer and at most one array allocation,
        // regardless of length. The former reserve-per-element path fails this.
        #[test]
        fn flat_arrays_reserve_once(entries in prop::collection::vec(any::<i64>(), 0..512)) {
            let value = Value::Array(entries.iter().copied().map(Value::from).collect());
            let mut charged = 0;
            let measured = acyclic_test_support::measure(|| {
                let copied = json_value_bounded(&value, BOUNDS, None, &mut |event| {
                    charged += match event {
                        JsonCodecCharge::AllocationOperation { bytes } => u64::from(bytes != 0),
                        JsonCodecCharge::AllocationOperations { operations } => operations,
                        _ => 0,
                    };
                    Ok(())
                }).unwrap();
                assert_eq!(copied, value);
            });
            prop_assert!(charged >= measured.count_total);
            prop_assert_eq!(charged, 1 + u64::from(!entries.is_empty()));
            prop_assert_eq!(measured.count_total, charged);
            prop_assert_eq!(measured.bytes_current, 0);
            let legacy = acyclic_test_support::measure(|| {
                let mut values = Vec::new();
                for entry in &entries {
                    if values.len() == values.capacity() {
                        values.try_reserve_exact(1).unwrap();
                    }
                    values.push(Value::from(*entry));
                }
                std::hint::black_box(values);
            });
            if entries.len() > 1 {
                prop_assert!(legacy.count_total > 1);
            }
        }

        // Explicit reserves are exact; only whole-object B-tree funding has
        // bounded slack, from node occupancy rather than a split schedule.
        #[test]
        fn plain_maps_charge_bounded_physical_allocations(
            entries in prop::collection::btree_map("[a-z]{0,8}", any::<i64>(), 1..512),
        ) {
            let legacy_operations = 1 + entries.keys().filter(|key| !key.is_empty()).count() as u64;
            let value = Value::Object(entries.into_iter().map(|(key, value)| (key, Value::from(value))).collect());
            let mut operations = 0;
            let mut copied = None;
            let measured = acyclic_test_support::measure(|| {
                copied = Some(json_value_bounded(&value, BOUNDS, None, &mut |event| {
                    operations += match event {
                        JsonCodecCharge::AllocationOperation { bytes } => u64::from(bytes != 0),
                        JsonCodecCharge::AllocationOperations { operations } => operations,
                        _ => 0,
                    };
                    Ok(())
                }));
            });
            prop_assert_eq!(copied.unwrap().unwrap(), value.clone());
            prop_assert!(operations >= measured.count_total);
            prop_assert!(operations <= measured.count_total + map_slack(&value));
            // Negative control: the previous buffer/string-only charges omit
            // at least the root node for every generated nonempty map.
            prop_assert!(legacy_operations < measured.count_total);
        }

        // Every prefix must either build the same value or return the original
        // typed denial. Only B-tree node occupancy may introduce bounded slack.
        #[test]
        fn bounded_value_exhausted_prefixes(value in prop_oneof![
            values(),
            any::<u64>().prop_map(Value::from),
            any::<f64>().prop_filter_map("finite JSON number", |number| serde_json::Number::from_f64(number).map(Value::Number)),
        ]) {
            exhausted_prefixes(&value);
        }

        #[test]
        fn generic_serializers_exhausted_prefixes(value in values()) {
            #[derive(Serialize)]
            struct Record<'a> { borrowed: &'a Value, raw: &'a serde_json::value::RawValue, ratio: f32 }
            #[derive(Serialize)]
            enum Envelope<'a> { Record(Record<'a>) }
            let raw = serde_json::value::RawValue::from_string(serde_json::to_string(&value).unwrap()).unwrap();
            let extra_slack = 2 * map_slack(&value);
            exhausted_serialization(raw.as_ref(), extra_slack);
            exhausted_serialization(&Envelope::Record(Record { borrowed: &value, raw: &raw, ratio: 0.25 }), extra_slack);
        }

        // Property: canonical identity and hash charges stay exact, while the
        // final encoded buffer is absent and all temporary custody is released.
        #[test]
        fn observed_digest_preserves_identity_without_an_encoded_copy(value in values()) {
            let budget = acyclic_fs::performance::work_budget(1_000_000, 64_000_000);
            let native = acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new()).unwrap();
            let baseline = native.live_allocation_bytes();
            let mut work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native).unwrap());
            let expected = crate::contract::canonical_json_bytes(&value).unwrap();
            let mut digest = Ok([0; 32]);
            let measured = acyclic_test_support::measure(|| {
                digest = canonical_json_digest(&value, 65_536, &mut Some(&mut work));
            });
            prop_assert_eq!(digest.unwrap(), *blake3::hash(&expected).as_bytes());
            prop_assert_eq!(work.receipt().bytes_hashed, expected.len() as u64);
            let (items, arrays) = items(&value);
            prop_assert_eq!(work.receipt().items_examined, items * if arrays { 2 } else { 1 });
            prop_assert_eq!(measured.bytes_current, 0);
            prop_assert!(measured.bytes_max as u64 <= work.receipt().peak_allocation_bytes);
            prop_assert_eq!(native.live_allocation_bytes(), baseline);
            // Negative control: the former full-buffer path performs one extra
            // encoded copy; it cannot meet the new copy count.
            let legacy_native = acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new()).unwrap();
            let mut legacy = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&legacy_native).unwrap());
            let encoded = crate::store::history::canonical_bytes_with_work(&value, 65_536, &mut Some(&mut legacy)).unwrap();
            prop_assert_eq!(&encoded, &expected);
            prop_assert_eq!(legacy.receipt().bytes_copied, work.receipt().bytes_copied + encoded.len() as u64);
        }
    }

    fn map_slack(value: &Value) -> u64 {
        match value {
            Value::Object(entries) => {
                let count = entries.len() as u64;
                let maximum = if count == 0 { 0 } else { 1 + (count - 1) / 5 };
                maximum - count.div_ceil(11) + entries.values().map(map_slack).sum::<u64>()
            }
            Value::Array(entries) => entries.iter().map(map_slack).sum(),
            _ => 0,
        }
    }

    fn exhausted_prefixes(value: &Value) {
        exhausted_serialization(value, 0);
    }

    fn exhausted_serialization<T: Serialize + ?Sized>(value: &T, extra_slack: u64) {
        let mut normalized = serde_json::to_value(value).unwrap();
        normalized.sort_all_objects();
        let encoded = serde_json::to_vec(&normalized).unwrap();
        let expected: Value = serde_json::from_slice(&encoded).unwrap();
        let slack = map_slack(&expected) + extra_slack;
        exhausted_encoding(&expected, slack, |work| {
            json_value_bounded(
                value,
                BOUNDS,
                Some(crate::wire::ErrorCode::Invalid),
                &mut |event| crate::store::HistoryIo::codec_charge(work, event),
            )
        });
        exhausted_encoding(&encoded, slack, |work| {
            canonical_json_bytes_bounded(
                value,
                BOUNDS,
                Some(crate::wire::ErrorCode::Invalid),
                &mut |event| crate::store::HistoryIo::codec_charge(work, event),
            )
        });
    }

    fn exhausted_encoding<T: PartialEq + std::fmt::Debug>(
        expected: &T,
        slack: u64,
        encode: impl Fn(&mut crate::store::HistoryWork) -> Result<T>,
    ) {
        let budget = acyclic_fs::performance::work_budget(1_000_000, 64_000_000);
        let reference = acyclic_fs::NativeMaterializationWork::new(
            budget,
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap();
        let construction = reference.receipt().allocation_operations;
        let mut work = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&reference).unwrap(),
        );
        let result = encode(&mut work).unwrap();
        assert_eq!(&result, expected);
        drop(result);
        let operations = reference.receipt().allocation_operations - construction;
        for prefix in 0..=operations {
            let mut budget = budget;
            budget.allocation_operations = construction + prefix;
            let native = acyclic_fs::NativeMaterializationWork::new(
                budget,
                &acyclic_fs::CancellationToken::new(),
            )
            .unwrap();
            let baseline = native.live_allocation_bytes();
            let mut work = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(&native).unwrap(),
            );
            let mut succeeded = false;
            let measured = acyclic_test_support::measure(|| {
                let result = encode(&mut work);
                succeeded = result.is_ok();
                if let Err(error) = &result {
                    assert!(matches!(error, Error::NativeWork { .. }));
                }
                drop(result);
            });
            assert_eq!(
                succeeded,
                prefix == operations,
                "prefix {prefix}/{operations}"
            );
            let charged = native.receipt().allocation_operations - construction;
            assert!(
                charged >= measured.count_total,
                "prefix {prefix}/{operations}: {charged} charges / {} allocations",
                measured.count_total
            );
            assert!(
                charged <= measured.count_total + slack,
                "prefix {prefix}/{operations}: {charged} charges / {} allocations / {slack} slack",
                measured.count_total
            );
            assert_eq!(measured.bytes_current, 0);
            assert!(measured.bytes_max as u64 <= native.receipt().peak_allocation_bytes);
            if !succeeded {
                assert_eq!(native.live_allocation_bytes(), baseline);
            }
            // Successful decoding transfers its retained bytes to the caller.
            drop(work);
            assert_eq!(native.live_allocation_bytes(), baseline);
        }
    }

    #[test]
    fn exhausted_prefixes_cover_escapes_and_node_splits() {
        exhausted_prefixes(&serde_json::json!({"": null}));
        exhausted_prefixes(&Value::from(6.237624688224859e252));
        exhausted_prefixes(&Value::from(-0.0));
        exhausted_prefixes(&serde_json::json!(["],}", "\"\\,", [], {"a,b": [1, 2]}, 3]));
        exhausted_prefixes(
            &serde_json::json!({"\u{0}\n\\\"": ["\u{1f600}\t\r", -0.25, 1e100, u64::MAX]}),
        );
        for size in [11, 12, 18, 19, 78] {
            let value = Value::Object(
                (0..size)
                    .map(|i| (format!("{i:04}"), Value::Null))
                    .collect(),
            );
            exhausted_prefixes(&value);
            if size == 19 {
                let raw = serde_json::value::RawValue::from_string(
                    serde_json::to_string(&value).unwrap(),
                )
                .unwrap();
                exhausted_serialization(raw.as_ref(), 2 * map_slack(&value));
            }
        }
        let raw = serde_json::value::RawValue::from_string(r#"{"":null}"#.into()).unwrap();
        exhausted_serialization(raw.as_ref(), 0);
        let raw = serde_json::value::RawValue::from_string(
            r#"{"b":["\u0000\n\"",{"a":1}],"b":false,"a":2}"#.into(),
        )
        .unwrap();
        exhausted_serialization(raw.as_ref(), 0);
        // Negative control: reproducing the old visitor's diagnostic after a
        // denied charge allocates despite no admitted operation.
        let diagnostic = acyclic_test_support::measure(|| {
            let error: serde_json::Error = serde::de::Error::custom("JSON admission interrupted");
            drop(error);
        });
        assert!(diagnostic.count_total > 0);
    }
}

const BOUNDS: JsonBounds = JsonBounds {
    maximum_bytes: 65_536,
    maximum_items: 4_096,
    maximum_depth: 32,
};

#[test]
fn resident_copy_preserves_literal_raw_keys_and_enforces_bounds() -> Result<()> {
    let value = serde_json::json!({"$serde_json::private::RawValue": ["\n", {"": null}]});
    let copied = json_value_copy_bounded(&value, BOUNDS, None, &mut |_| Ok(()))?;
    assert_eq!(copied, value);
    for bounds in [
        JsonBounds {
            maximum_bytes: 1,
            ..BOUNDS
        },
        JsonBounds {
            maximum_items: 1,
            ..BOUNDS
        },
        JsonBounds {
            maximum_depth: 1,
            ..BOUNDS
        },
    ] {
        let mut allocations = 0;
        assert!(
            json_value_copy_bounded(&value, bounds, None, &mut |event| {
                if matches!(event, JsonCodecCharge::AllocationOperation { .. }) {
                    allocations += 1;
                }
                Ok(())
            })
            .is_err()
        );
        assert_eq!(allocations, 0);
    }
    Ok(())
}

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
    let conversion = acyclic_test_support::measure(|| {
        converted = serde_json::to_value(value);
        if let Ok(value) = &mut converted {
            value.sort_all_objects();
        }
    });
    drop(std::hint::black_box(
        converted.map_err(|error| Error::Invalid(error.to_string()))?,
    ));
    let mut digest = Ok([0; 32]);
    let encoding = acyclic_test_support::measure(|| {
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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn preparation_digest_batches_stream_canonical_output_on_original_work() -> Result<()> {
    let value = serde_json::json!({"z": "q".repeat(128 * 1024), "a": [1, 2, 3]});
    let canonical = crate::contract::canonical_json_bytes(&value)?;
    let expected = *blake3::hash(&canonical).as_bytes();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
    let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&native)?;
    for _ in 0..16 {
        assert_eq!(
            crate::executor::preparation_digest(&value, Some(&io))?,
            expected
        );
    }
    assert_eq!(native.receipt().bytes_hashed, 16 * canonical.len() as u64);
    cancellation.cancel();
    assert!(crate::executor::preparation_digest(&value, Some(&io)).is_err());

    let mut no_hash = budget;
    no_hash.bytes_hashed = 0;
    let denied =
        acyclic_fs::NativeMaterializationWork::new(no_hash, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&denied)?;
    assert!(crate::executor::preparation_digest(&value, Some(&io)).is_err());
    assert_eq!(denied.receipt().bytes_hashed, 0);
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

mod object_work {
    use super::*;
    use proptest::prelude::*;
    mod oracle {
        use crate as acyclic_harness;
        include!("../../../tests/support/bounded_json.rs");
    }
    proptest! {
        // Object work depends on structural nodes, never on descendant string bytes.
        #[test]
        fn object_construction_visits_each_node_once(depth in 1usize..8, text in "[a-z]{64,128}") {
            let mut value = Value::String(text);
            for _ in 0..depth { value = serde_json::json!({"child": value}); }
            let mut items = 0;
            let copied = json_value_bounded(&value, BOUNDS, None, &mut |event| {
                items += match event {
                    JsonCodecCharge::Inspect { items } | JsonCodecCharge::Work { items, .. } => items,
                    _ => 0,
                };
                Ok(())
            }).unwrap();
            prop_assert_eq!(&copied, &value);
            let bound = (2 * depth + 1) as u64;
            prop_assert_eq!(items, bound);
            let bytes = serde_json::to_vec(&value).unwrap();
            oracle::check(&bytes);
            // Negative control: even one of the former descendant scans violates the bound.
            let mut full_scan = items;
            for _ in &bytes { full_scan += 1; }
            prop_assert!(full_scan > bound);
        }
    }
    proptest! {
        // Counts depend on nodes, independently of array width, nesting and text size.
        #[test]
        fn nested_and_wide_arrays_do_not_rescan(
            depth in 1usize..12, width in 1usize..48, text in "[a-z]{32,96}",
        ) {
            let mut value = Value::Array((0..width).map(|_| Value::String(text.clone())).collect());
            for _ in 0..depth { value = Value::Array(vec![value, Value::Array(Vec::new())]); }
            let nodes = (width + 1 + 2 * depth) as u64;
            let mut work = (0, 0);
            let copied = json_value_bounded(&value, BOUNDS, None, &mut |event| {
                match event {
                    JsonCodecCharge::Inspect { items } | JsonCodecCharge::Work { items, .. } => work.0 += items,
                    JsonCodecCharge::Read { bytes } => work.1 += bytes,
                    _ => {},
                }
                Ok(())
            }).unwrap();
            prop_assert_eq!(&copied, &value);
            prop_assert_eq!(work, (nodes, 0));
            let bytes = serde_json::to_vec(&value).unwrap();
            oracle::check(&bytes);
            // Negative control: traverse every descendant as the former pre-count did.
            fn rescans(value: &Value) -> usize {
                match value {
                    Value::Array(entries) => serde_json::to_vec(value).unwrap().len() - 1
                        + entries.iter().map(rescans).sum::<usize>(),
                    _ => 0,
                }
            }
            let scanned = rescans(&value) as u64;
            prop_assert!(work.0 + scanned > nodes);
            prop_assert!(work.1 + scanned > 0);
        }
    }
    #[test]
    fn checked_codec_fuzz_corpus() {
        for bytes in [
            include_bytes!("../../../../harness-fuzz/corpus/bounded_json/empty").as_slice(),
            include_bytes!("../../../../harness-fuzz/corpus/bounded_json/nested"),
            include_bytes!("../../../../harness-fuzz/corpus/bounded_json/duplicates"),
            include_bytes!("../../../../harness-fuzz/corpus/bounded_json/wide"),
            include_bytes!("../../../../harness-fuzz/corpus/bounded_json/compact_growth"),
        ] {
            oracle::check(bytes);
        }
    }
}
