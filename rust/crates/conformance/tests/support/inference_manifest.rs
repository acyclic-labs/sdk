use acyclic_conformance::inference::replay::Manifest;

pub fn arbitrary(bytes: &[u8]) {
    let Ok(manifest) = Manifest::decode(bytes) else {
        return;
    };
    assert!(!manifest.source.trim().is_empty());
    assert!(!manifest.captured_at.trim().is_empty());
    assert!(!manifest.rate_source.trim().is_empty());

    // An accepted manifest is a nonempty JSON object. Preserve every original
    // byte except its closing brace while adding an unrecognised member.
    let end = bytes.iter().rposition(|byte| *byte == b'}').unwrap();
    let mut unknown = bytes[..end].to_vec();
    unknown.extend_from_slice(b",\"unrecognised_capture_field\":null}");
    assert!(Manifest::decode(&unknown).is_err());
}

pub fn recorded() {
    let bytes = include_bytes!("../../fixtures/inference/llamacpp/manifest.json");
    let manifest = Manifest::decode(bytes).expect("original capture must be admitted");
    let mut original: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(
        Some(manifest.rate_source.as_str()),
        original["rate_source"].as_str()
    );
    arbitrary(bytes);

    for field in ["source", "captured_at", "rate_source"] {
        let retained = original[field].take();
        for absent in [serde_json::Value::Null, serde_json::json!(" \t\n")] {
            original[field] = absent;
            assert!(Manifest::decode(&serde_json::to_vec(&original).unwrap()).is_err());
        }
        original.as_object_mut().unwrap().remove(field);
        assert!(Manifest::decode(&serde_json::to_vec(&original).unwrap()).is_err());
        original[field] = retained;
    }
}
