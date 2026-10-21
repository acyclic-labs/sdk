use super::*;
use proptest::prelude::*;
use serde_json::Value;
use std::cell::Cell;

fn values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|value| Value::Number(value.into())),
        any::<f64>().prop_map(|value| serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)),
        ".{0,64}".prop_map(Value::String),
    ]
    .prop_recursive(4, 64, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
            prop::collection::btree_map(".{0,32}", inner, 0..8)
                .prop_map(|entries| Value::Object(entries.into_iter().collect())),
        ]
    })
}

proptest! {
    // Property: streaming comparison is exactly encoded-byte equality, before
    // and after JSON round trips, including mismatches and unequal lengths.
    #[test]
    fn matches_materialized_canonical_bytes(
        value in values(), arbitrary in prop::collection::vec(any::<u8>(), 0..512),
        position in any::<usize>(),
    ) {
        let encoded = canonical_json_bytes(&value)?;
        let restored: Value = serde_json::from_slice(&encoded)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let mut changed = encoded.clone();
        let offset = position % changed.len();
        if let Some(byte) = changed.get_mut(offset) { *byte ^= 1; }
        let mut extended = encoded.clone(); extended.push(b' ');
        let shortened = encoded.get(..encoded.len().saturating_sub(1))
            .ok_or_else(|| TestCaseError::fail("invalid prefix"))?;
        for bytes in [encoded.as_slice(), &changed, &extended, shortened, &arbitrary] {
            let expected = encoded.as_slice() == bytes;
            prop_assert_eq!(canonical_json_matches(&value, bytes)?, expected);
            prop_assert_eq!(canonical_json_matches(&restored, bytes)?, expected);
        }
    }
}

struct Counted<'a, T: ?Sized>(&'a Cell<usize>, &'a T);
impl<T: Serialize + ?Sized> Serialize for Counted<'_, T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.0.set(self.0.get() + 1);
        self.1.serialize(serializer)
    }
}

#[test]
fn comparison_serializes_once_and_preserves_errors() -> Result<()> {
    struct Fails;
    impl Serialize for Fails {
        fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("serialization rejected"))
        }
    }
    let calls = Cell::new(0);
    for value in [
        serde_json::json!({"z": [null, true, -1], "a": "\0\n\"\\é"}),
        Value::Null,
    ] {
        let bytes = canonical_json_bytes(&value)?;
        for candidate in [bytes.as_slice(), b"wrong"] {
            calls.set(0);
            assert_eq!(
                canonical_json_matches(&Counted(&calls, &value), candidate)?,
                bytes == candidate
            );
            assert_eq!(calls.get(), 1);
        }
    }
    calls.set(0);
    let actual = canonical_json_matches(&Counted(&calls, &u128::MAX), b"wrong");
    assert_eq!(calls.get(), 1);
    assert_eq!(
        actual.map(|_| ()).map_err(|error| error.to_string()),
        canonical_json_bytes(&u128::MAX)
            .map(|_| ())
            .map_err(|error| error.to_string())
    );
    calls.set(0);
    let actual = canonical_json_matches(&Counted(&calls, &Fails), b"wrong");
    assert_eq!(calls.get(), 1);
    assert!(actual.is_err());
    assert_eq!(
        actual.map(|_| ()).map_err(|error| error.to_string()),
        canonical_json_bytes(&Fails)
            .map(|_| ())
            .map_err(|error| error.to_string())
    );
    Ok(())
}

fn no_encoded_storage(value: &Value) -> Result<()> {
    let expected = canonical_json_bytes(value)?;
    assert!(expected.len() > 8 * 4096);
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
    for candidate in [
        expected.as_slice(),
        expected
            .get(..1)
            .ok_or_else(|| Error::Invalid("empty encoding".into()))?,
    ] {
        let mut result = Ok(false);
        let matching =
            acyclic_test_support::measure(|| result = canonical_json_matches(value, candidate));
        assert_eq!(result?, candidate == expected.as_slice());
        eprintln!(
            "storage encoded={} candidate={} conversion={}/{}/{} matching={}/{}/{}",
            expected.len(),
            candidate.len(),
            conversion.count_total,
            conversion.bytes_total,
            conversion.bytes_max,
            matching.count_total,
            matching.bytes_total,
            matching.bytes_max
        );
        assert_eq!(matching.count_total, conversion.count_total);
        assert_eq!(matching.bytes_total, conversion.bytes_total);
        assert_eq!(matching.bytes_max, conversion.bytes_max);
        assert_eq!(matching.bytes_current, 0);
        assert_eq!(matching.count_current, 0);
    }
    Ok(())
}

#[test]
fn large_strings_need_no_encoded_storage() -> Result<()> {
    for text in ["x".repeat(1024 * 1024), "\0\n\"\\é".repeat(65_536)] {
        no_encoded_storage(&Value::String(text))?;
    }
    Ok(())
}

#[test]
fn nested_collections_need_no_encoded_storage() -> Result<()> {
    let rows: Vec<_> = (0..256)
        .map(|index| serde_json::json!({"z": [index, "\0\n\"\\é".repeat(64)], "a": {"id": index}}))
        .collect();
    no_encoded_storage(&serde_json::json!({"z": rows, "a": [true, null]}))
}

#[test]
fn comparison_without_subscriber() -> Result<()> {
    comparison_cost(false)
}

#[test]
fn comparison_with_tracing() -> Result<()> {
    comparison_cost(true)
}

fn comparison_cost(observing: bool) -> Result<()> {
    let dispatch = if observing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let rows: Vec<_> = (0..256)
        .map(|index| serde_json::json!({"z": [index, "\0\n\"\\é".repeat(64)], "a": {"id": index}}))
        .collect();
    for (label, value) in [
        (
            "small",
            serde_json::json!({"z": [0, true, null], "a": "text"}),
        ),
        ("string", Value::String("x".repeat(1024 * 1024))),
        ("nested", serde_json::json!({"z": rows, "a": [true, null]})),
    ] {
        let expected = canonical_json_bytes(&value)?;
        for equal in [true, false] {
            let candidate = if equal { expected.as_slice() } else { b"wrong" };
            let mut counts = None;
            let mut times = Vec::with_capacity(32);
            for _ in 0..32 {
                let mut result = Ok(false);
                let started = std::time::Instant::now();
                let allocation = acyclic_test_support::measure(|| {
                    result = canonical_json_matches(std::hint::black_box(&value), candidate);
                });
                times.push(started.elapsed());
                assert_eq!(result?, equal);
                let actual = (
                    allocation.count_total,
                    allocation.bytes_total,
                    allocation.bytes_max,
                );
                assert_eq!(*counts.get_or_insert(actual), actual);
            }
            times.sort_unstable();
            eprintln!(
                "match tracing={observing} case={label} equal={equal} encoded={}: allocation={counts:?}; median={:?}",
                expected.len(),
                times.get(times.len() / 2)
            );
        }
    }
    Ok(())
}
