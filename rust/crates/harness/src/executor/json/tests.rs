use super::*;
use serde::Serialize;

const BOUNDS: JsonBounds = JsonBounds {
    maximum_bytes: 65_536,
    maximum_items: 4_096,
    maximum_depth: 32,
};
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
fn every_codec_cancellation_schedule_preserves_original_error() -> Result<()> {
    let value = serde_json::json!({"b": ["escape\n", 0.1], "a": {"later": 2, "earlier": 1}});
    let mut calls = 0usize;
    canonical_json_bytes_bounded(&value, BOUNDS, None, &mut |event| {
        if !matches!(event, JsonCodecCharge::Release { .. }) {
            calls += 1;
        }
        Ok(())
    })?;
    for stop in 0..calls {
        let mut seen = 0;
        let mut live = 0u64;
        let operation = crate::OperationId::from_bytes((stop as u128).to_le_bytes());
        let mut error = Some(Error::Indeterminate(operation));
        let result = canonical_json_bytes_bounded(&value, BOUNDS, None, &mut |event| {
            if let JsonCodecCharge::Release { bytes } = event {
                live = live
                    .checked_sub(bytes)
                    .expect("release only admitted quota");
                return Ok(());
            }
            if seen == stop {
                return Err(error
                    .take()
                    .expect("codec must stop after its first failure"));
            }
            seen += 1;
            if let JsonCodecCharge::Retain { bytes } = event {
                live += bytes;
            }
            Ok(())
        });
        assert!(matches!(result, Err(Error::Indeterminate(original)) if original == operation));
        assert_eq!(live, 0, "cancel schedule {stop} leaks live codec quota");
    }
    Ok(())
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
