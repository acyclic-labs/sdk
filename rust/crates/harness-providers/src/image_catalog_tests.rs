use super::*;

#[test]
fn image_catalog_bounds_are_declarative_and_reject_invalid_models() -> Result<()> {
    let (_, transport) = provider(Adapter::Chat, 200);
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport);
    let valid = json!({
        "adapter":"chat", "authentication":"optional", "base_url":"http://localhost:1234/v1",
        "models":{"vision":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":true}}},
        "token_counter":{"path":"count_tokens","adapter":"chat","response_field":"input_tokens","framing_tokens":0},
        "images":{"vision":{"maximum_bytes":4096,"maximum_total_bytes":8192,"maximum_images":4,"maximum_dimension":1024,"maximum_tokens":1024}},
        "max_event_bytes":4096,
    });
    let spec: ProviderSpec = serde_json::from_value(valid.clone()).expect("catalog");
    registry.register("vision".into(), spec)?;
    assert!(
        registry
            .resolve("vision/vision")?
            .config
            .images
            .contains_key("vision")
    );
    for (field, value) in [
        ("maximum_bytes", 0),
        ("maximum_bytes", 9007199254740992u64),
        ("maximum_total_bytes", 4095),
        ("maximum_total_bytes", 9007199254740992u64),
        ("maximum_images", 0),
        ("maximum_images", u32::MAX as u64),
        ("maximum_dimension", 0),
        ("maximum_tokens", 0),
        ("maximum_tokens", 8193),
    ] {
        let mut bad = valid.clone();
        bad["images"]["vision"][field] = json!(value);
        assert!(
            registry
                .register(
                    "bad".into(),
                    serde_json::from_value(bad).expect("catalog shape")
                )
                .is_err(),
            "{field}={value}"
        );
        assert!(!registry.catalog().contains_key("bad"));
    }
    let mut unknown = valid;
    unknown["models"] = json!({});
    assert!(
        registry
            .register(
                "bad".into(),
                serde_json::from_value(unknown).expect("catalog shape")
            )
            .is_err()
    );
    Ok(())
}
