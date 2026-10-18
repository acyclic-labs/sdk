use super::*;
use proptest::prelude::*;

#[test]
fn codex_capacities_match_the_studied_metadata_without_an_output_claim() {
    // models_cache.json metadata, 2026-10-18; no authentication/config files.
    let catalog = default_catalog();
    let metadata: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/codex-capacities.json")).unwrap();
    for entry in metadata["models"].as_array().unwrap() {
        let name = entry["slug"].as_str().unwrap();
        let model = &catalog["openai"].models[name];
        assert_eq!(
            u64::from(model.capacity.context_tokens),
            entry["context_window"].as_u64().unwrap()
        );
        assert_eq!(model.capacity.output_tokens, None);
        assert_eq!(
            model.max_context_tokens.map(u64::from),
            entry["max_context_window"].as_u64()
        );
        let encoded = serde_json::to_value(model).unwrap();
        assert!(encoded["capacity"]["output_tokens"].is_null());
        model.capacity.validate().unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // The production parser preserves defaults, nested capacities and unknown
    // output bounds under serialization; malformed fields retain path/location.
    #[test]
    fn catalog_round_trip_and_diagnostics(
        context in 1u32..=u32::MAX,
        output in proptest::option::of(1u32..=u32::MAX),
        prefix_lines in 0usize..8,
    ) {
        let mut value = serde_json::json!({
            "default_model":"fixture/model",
            "fixture": {"adapter":"responses", "authentication":"optional",
                "base_url":"http://localhost:1234", "max_event_bytes":4096,
                "models":{"model":{"capacity":{"context_tokens":context,
                    "output_tokens": output.map(|n| n.min(context))}}}}
        });
        let bytes = serde_json::to_vec(&value).unwrap();
        let document = crate::catalog_document::parse(&bytes).unwrap();
        let encoded = serde_json::to_vec(&document).unwrap();
        let replay = crate::catalog_document::parse(&encoded).unwrap();
        prop_assert_eq!(document.default_model, replay.default_model);
        prop_assert_eq!(document.providers["fixture"].models["model"], replay.providers["fixture"].models["model"]);
        value["fixture"]["models"]["model"]["capacity"]["context_tokens"] = serde_json::json!("invalid");
        let malformed = format!("{}{}", "\n".repeat(prefix_lines), serde_json::to_string(&value).unwrap());
        let error = crate::catalog_document::parse(malformed.as_bytes()).err().unwrap().to_string();
        prop_assert!(error.contains("fixture.models.model.capacity.context_tokens"), "{}", error);
        prop_assert!(error.contains(&format!("line {}", prefix_lines + 1)), "{}", error);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Adding a member with an unknown bound must preserve every known ceiling.
    // If neither member advertises a ceiling, the aggregate remains unknown.
    #[test]
    fn alloy_unknown_output_preserves_known_member_ceilings(
        a in proptest::option::of(1u32..=65_536),
        b in proptest::option::of(1u32..=65_536),
        context_a in 1u32..=65_536, context_b in 1u32..=65_536,
    ) {
        let a = a.map(|n| n.min(context_a));
        let b = b.map(|n| n.min(context_b));
        let capacity = alloy_capacity(
            ModelContextCapacity { context_tokens: context_a, output_tokens: a },
            ModelContextCapacity { context_tokens: context_b, output_tokens: b },
        );
        prop_assert_eq!(capacity.context_tokens, context_a.min(context_b));
        let output = capacity.output_tokens;
        prop_assert_eq!(output.is_none(), a.is_none() && b.is_none());
        for ceiling in [a,b].into_iter().flatten() {
            prop_assert!(output.unwrap() <= ceiling);
        }
        if let Some(ceiling) = output {
            prop_assert!(ceiling <= capacity.context_tokens);
            prop_assert!([a,b,Some(capacity.context_tokens)].contains(&Some(ceiling)));
        }
    }
}

#[test]
fn catalog_default_and_provider_overrides_commit_together() -> Result<()> {
    let (_, script) = provider(Adapter::Responses, 200);
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), script);
    let original = registry.catalog()["openai"].models["gpt-5"];
    let mut spec = registry.catalog()["openai"].as_ref().clone();
    spec.models.get_mut("gpt-5").unwrap().capacity = ModelContextCapacity {
        context_tokens: 1234,
        output_tokens: Some(100),
    };
    let file = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog"))?;
    let mut document = json!({"default_model":"openai/missing", "openai":spec});
    std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(registry.load_catalog(file.path()).is_err());
    assert_eq!(registry.default_model(), "openai/gpt-5");
    assert_eq!(registry.catalog()["openai"].models["gpt-5"], original);
    document["default_model"] = json!("openai/gpt-6-luna");
    std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
    registry.load_catalog(file.path())?;
    assert_eq!(registry.default_model(), "openai/gpt-6-luna");
    assert_eq!(
        registry.catalog()["openai"].models["gpt-5"]
            .capacity
            .context_tokens,
        1234
    );
    Ok(())
}

fn alloy_capacity(a: ModelContextCapacity, b: ModelContextCapacity) -> ModelContextCapacity {
    let (_, script) = provider(Adapter::Responses, 200);
    let mut registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([("openai".into(), "fixture".into())])),
        script,
    );
    let mut spec = registry.catalog()["openai"].as_ref().clone();
    let mut model = spec.models["gpt-5"];
    model.capacity = a;
    spec.models.clear();
    spec.models.insert("a".into(), model);
    model.capacity = b;
    spec.models.insert("b".into(), model);
    registry.register("openai".into(), spec).unwrap();
    registry
        .resolve_alloy(
            AlloySpec {
                models: vec!["openai/a".into(), "openai/b".into()],
                select: AlloySelection::First,
            },
            ModelFeatures::default(),
        )
        .unwrap()
        .capacity()
}

#[test]
fn alloy_unknown_member_context_can_be_below_the_known_output_ceiling() {
    let capacity = alloy_capacity(
        ModelContextCapacity {
            context_tokens: 65_536,
            output_tokens: Some(65_536),
        },
        ModelContextCapacity {
            context_tokens: 1,
            output_tokens: None,
        },
    );
    assert_eq!(
        capacity,
        ModelContextCapacity {
            context_tokens: 1,
            output_tokens: Some(1)
        }
    );
}
