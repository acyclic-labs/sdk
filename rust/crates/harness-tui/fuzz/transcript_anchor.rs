//! Persisted anchor checks shared by generated values and arbitrary wire input.
use super::TranscriptAnchor;

/// Check an admitted persisted anchor's round trip, strict fields and provenance.
///
/// # Panics
/// Panics if production decoding changes a value or admits an invalid mutation.
pub fn check(anchor: &TranscriptAnchor) -> Result<(), String> {
    let encoded =
        serde_json::to_vec(anchor).map_err(|error| format!("anchor serialization: {error}"))?;
    let decoded = serde_json::from_slice::<TranscriptAnchor>(&encoded)
        .map_err(|error| format!("anchor round-trip decode: {error}"));
    assert_eq!(decoded, Ok(*anchor), "anchor round trip changed its value");
    let original = serde_json::to_value(anchor)
        .map_err(|error| format!("anchor value serialization: {error}"))?;
    for field in [None, Some("position"), Some("origin")] {
        let mut changed = original.clone();
        let target = match field {
            None => Some(&mut changed),
            Some(field) => changed.get_mut(field),
        };
        let object = target
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| format!("anchor lacks object at {field:?}"))?;
        object.insert("unknown".into(), serde_json::Value::Bool(true));
        assert!(
            serde_json::from_value::<TranscriptAnchor>(changed).is_err(),
            "anchor admitted an unknown field at {field:?}"
        );
    }
    let serde_json::Value::Object(mut original) = original else {
        return Err("anchor value is not an object".to_owned());
    };
    assert!(
        original.remove("origin").is_some(),
        "anchor lacks provenance"
    );
    assert!(
        serde_json::from_value::<TranscriptAnchor>(serde_json::Value::Object(original.clone()))
            .is_err(),
        "anchor admitted missing provenance"
    );
    original.insert("origin".into(), serde_json::Value::Null);
    assert!(
        serde_json::from_value::<TranscriptAnchor>(serde_json::Value::Object(original)).is_err(),
        "anchor admitted null provenance"
    );
    Ok(())
}
