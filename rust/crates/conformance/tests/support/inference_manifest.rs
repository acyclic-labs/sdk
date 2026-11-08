//! Independent provenance and unknown-field checks over original manifest captures.
use acyclic_conformance::inference::replay::Manifest;

/// Check every admitted manifest against the provenance and strict-field contract.
///
/// # Panics
/// Panics if an admitted manifest violates the independently checked contract.
pub fn arbitrary(bytes: &[u8]) {
    assert_eq!(check_arbitrary(bytes), Ok(()));
}

fn check_arbitrary(bytes: &[u8]) -> Result<(), String> {
    let Ok(manifest) = Manifest::decode(bytes) else {
        return Ok(());
    };
    assert!(!manifest.source.trim().is_empty());
    assert!(!manifest.captured_at.trim().is_empty());
    assert!(!manifest.rate_source.trim().is_empty());

    // An accepted manifest is a nonempty JSON object. Preserve every original
    // byte except its closing brace while adding an unrecognised member.
    let prefix = bytes
        .rsplitn(2, |byte| *byte == b'}')
        .nth(1)
        .ok_or_else(|| "accepted manifest lacks its closing brace".to_owned())?;
    let mut unknown = prefix.to_vec();
    unknown.extend_from_slice(b",\"unrecognised_capture_field\":null}");
    assert!(Manifest::decode(&unknown).is_err());
    Ok(())
}

/// Require admission of the original capture and refusal of every absent provenance field.
///
/// # Panics
/// Panics if original admission or any provenance refusal changes.
pub fn recorded() {
    assert_eq!(check_recorded(), Ok(()));
}

fn check_recorded() -> Result<(), String> {
    let bytes = include_bytes!("../../fixtures/inference/llamacpp/manifest.json");
    let manifest = Manifest::decode(bytes)
        .map_err(|error| format!("original manifest capture was refused: {error}"))?;
    let original: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("original manifest JSON was refused: {error}"))?;
    assert_eq!(
        Some(manifest.rate_source.as_str()),
        original
            .get("rate_source")
            .and_then(serde_json::Value::as_str)
    );
    arbitrary(bytes);
    let serde_json::Value::Object(mut original) = original else {
        return Err("original manifest capture is not an object".to_owned());
    };
    for field in ["source", "captured_at", "rate_source"] {
        let retained = original
            .get_mut(field)
            .ok_or_else(|| format!("original manifest capture lacks {field}"))?
            .take();
        for absent in [serde_json::Value::Null, serde_json::json!(" \t\n")] {
            *original
                .get_mut(field)
                .ok_or_else(|| format!("original manifest capture lost {field}"))? = absent;
            let encoded = serde_json::to_vec(&original)
                .map_err(|error| format!("manifest mutation encoding: {error}"))?;
            assert!(Manifest::decode(&encoded).is_err());
        }
        original.remove(field);
        let encoded = serde_json::to_vec(&original)
            .map_err(|error| format!("manifest mutation encoding: {error}"))?;
        assert!(Manifest::decode(&encoded).is_err());
        original.insert(field.to_owned(), retained);
    }
    Ok(())
}
