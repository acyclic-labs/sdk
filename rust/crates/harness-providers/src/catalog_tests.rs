use super::*;
use proptest::prelude::*;

#[tokio::test]
async fn arbitrary_provider_ids_follow_their_catalog_authentication_policy() -> Result<()> {
    let (_, script) = provider(Adapter::Chat, 200);
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), script.clone());
    let mut spec = ProviderSpec {
        adapter: Adapter::Chat,
        authentication: Authentication::Optional,
        base_url: "http://127.0.0.1:1234/v1".into(),
        headers: BTreeMap::new(),
        token_counter: Some(crate::TokenCounterSpec {
            path: "count_tokens".into(),
            adapter: Adapter::Chat,
            response_field: crate::TokenCountField::InputTokens,
            framing_tokens: 0,
        }),
        max_event_bytes: 4096,
        retry: crate::RetrySpec::DEFAULT,
        timeouts: crate::TimeoutSpec::DEFAULT,
        models: BTreeMap::from([(
            "m".into(),
            ModelSpec {
                max_context_tokens: None,
                capacity: ModelContextCapacity {
                    context_tokens: 8192,
                    output_tokens: Some(1024),
                },
                features: ModelFeatures::default(),
            },
        )]),
    };
    registry.register("local".into(), spec.clone())?;
    let local = registry.resolve("local/m")?;
    let (request, dispatch) = request(&local)?;
    local
        .generate(request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert!(
        !script.sent.lock().expect("fixture")[0]
            .headers
            .contains_key("authorization")
    );
    spec.authentication = Authentication::ApiKey;
    registry.register("ollama".into(), spec)?;
    assert!(
        registry.resolve("ollama/m").is_err(),
        "built-in names do not bypass catalog policy"
    );
    Ok(())
}

#[test]
fn serialized_catalogs_own_capacities_without_name_based_fallbacks() -> Result<()> {
    let (_, script) = provider(Adapter::Chat, 200);
    let catalog = default_catalog();
    let encoded = serde_json::to_vec(&catalog).expect("catalog");
    let catalog: BTreeMap<String, ProviderSpec> =
        serde_json::from_slice(&encoded).expect("catalog round trip");
    let mut registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([("openai".into(), "fixture".into())])),
        script,
    );
    for (id, spec) in catalog {
        registry.register(id, spec)?;
    }
    let mut openai = registry.catalog()["openai"].as_ref().clone();
    openai.models.clear();
    registry.register("openai".into(), openai.clone())?;
    assert!(registry.resolve("openai/gpt-5").is_err());
    openai.models.insert(
        "gpt-5".into(),
        ModelSpec {
            max_context_tokens: None,
            capacity: ModelContextCapacity {
                context_tokens: 32768,
                output_tokens: Some(2048),
            },
            features: ModelFeatures::default(),
        },
    );
    registry.register("openai".into(), openai)?;
    let model = registry.resolve("openai/gpt-5")?;
    assert_eq!(model.context_capacity(model.model())?.context_tokens, 32768);
    Ok(())
}

#[test]
fn resolution_binds_options_and_rejects_mutated_model_identity() -> Result<()> {
    let (_, script) = provider(Adapter::Responses, 200);
    let registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([
            ("openai".into(), "fixture".into()),
            ("anthropic".into(), "fixture".into()),
            ("gemini".into(), "fixture".into()),
        ])),
        script,
    );
    for (selection, options, field) in [
        (
            "openai/gpt-5",
            json!({"reasoning":{"effort":"high"},"text":{"format":{"type":"json_object"}}}),
            "reasoning",
        ),
        (
            "anthropic/claude-sonnet-4-5",
            json!({"thinking":{"type":"enabled","budget_tokens":2048}}),
            "thinking",
        ),
        (
            "gemini/gemini-2.5-pro",
            json!({"generationConfig":{"thinkingConfig":{"thinkingBudget":2048},"responseMimeType":"application/json"}}),
            "generationConfig",
        ),
    ] {
        let provider = registry.resolve_with_options(selection, options.clone())?;
        let (prepared, _) = request(&provider)?;
        assert_eq!(provider.model().options, options);
        let body = projection::body(&provider, prepared.request())?;
        for (field, configured) in options.as_object().expect("fixture options") {
            for (key, value) in configured.as_object().expect("fixture option controls") {
                assert_eq!(&body[field][key], value);
            }
        }
        if field == "generationConfig" {
            assert_eq!(
                body[field]["maxOutputTokens"],
                prepared
                    .request()
                    .max_output_tokens
                    .expect("fixture output limit")
            );
        }
        let mut changed = provider.model().clone();
        changed.options = json!({});
        assert!(provider.context_capacity(&changed).is_err());
        assert!(futures::executor::block_on(provider.count_tokens(&prepared)).is_ok());
    }
    assert!(
        registry
            .resolve_with_options("openai/gpt-5", json!([]))
            .is_err()
    );
    Ok(())
}

#[test]
fn invalid_catalog_files_do_not_partially_replace_registered_policy() -> Result<()> {
    let (_, transport) = provider(Adapter::Responses, 200);
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport);
    let before = registry.catalog().keys().cloned().collect::<Vec<_>>();
    let valid = json!({"adapter":"chat","authentication":"optional","base_url":"http://localhost:1234/v1","models":{"fixture":{"capacity":{"context_tokens":4096,"output_tokens":512}}},"max_event_bytes":4096});
    let mut bad = valid.clone();
    bad["models"]["fixture"]["capacity"]["context_tokens"] = json!(0);
    let file = tempfile::NamedTempFile::new().map_err(|_| invalid("fixture catalog I/O"))?;
    std::fs::write(
        file.path(),
        serde_json::to_vec(&json!({"aaa":valid,"zzz":bad})).expect("catalog fixture"),
    )
    .map_err(|_| invalid("fixture catalog I/O"))?;
    assert!(registry.load_catalog(file.path()).is_err());
    assert_eq!(
        registry.catalog().keys().cloned().collect::<Vec<_>>(),
        before
    );
    std::fs::write(file.path(), b"{sensitive fixture content")
        .map_err(|_| invalid("fixture catalog I/O"))?;
    assert_eq!(
        registry
            .load_catalog(file.path())
            .expect_err("malformed catalog")
            .to_string(),
        "invalid request: provider catalog: key must be a string at line 1 column 2"
    );
    Ok(())
}

fn stock_registry() -> (Registry, Arc<Script>) {
    let (_, transport) = provider(Adapter::Responses, 200);
    let registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([
            ("openai".into(), "fixture".into()),
            ("anthropic".into(), "fixture".into()),
            ("gemini".into(), "fixture".into()),
        ])),
        transport.clone(),
    );
    (registry, transport)
}

fn stock_models(registry: &Registry) -> Vec<String> {
    registry
        .catalog()
        .iter()
        .flat_map(|(provider, spec)| {
            spec.models
                .keys()
                .map(move |model| format!("{provider}/{model}"))
        })
        .collect()
}

#[test]
fn stock_catalog_alloys_support_tools_and_images_and_overrides_fail_closed() -> Result<()> {
    let (mut registry, transport) = stock_registry();
    let required = ModelFeatures {
        tools: true,
        images: true,
    };
    let models = stock_models(&registry);
    assert!(models.iter().any(|model| model == "openai/gpt-5"));
    registry.resolve_alloy(
        AlloySpec {
            models,
            select: AlloySelection::RoundRobin,
        },
        required,
    )?;
    for missing in [
        ModelFeatures::default(),
        ModelFeatures {
            tools: false,
            images: true,
        },
        ModelFeatures {
            tools: true,
            images: false,
        },
    ] {
        let mut spec = registry.catalog()["openai"].as_ref().clone();
        spec.models.get_mut("gpt-5").unwrap().features = missing;
        registry.register("openai".into(), spec)?;
        assert!(
            registry
                .resolve_alloy(
                    AlloySpec {
                        models: vec!["openai/gpt-5".into()],
                        select: AlloySelection::First,
                    },
                    required
                )
                .is_err(),
            "missing capability must reject before dispatch"
        );
    }
    assert!(transport.sent.lock().unwrap().is_empty());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Any nonempty stock subset accepts supported requirements, and selection
    // retains the subset's minimum capacity without contacting a provider.
    #[test]
    fn stock_alloy_subsets_preserve_declared_features_and_minimum_capacity(
        indices in proptest::collection::btree_set(0usize..10, 1..=10),
        tools in any::<bool>(), images in any::<bool>(), ordinal in any::<u64>(),
    ) {
        let (registry, transport) = stock_registry();
        let stock = stock_models(&registry);
        prop_assert_eq!(stock.len(), 10);
        let models: Vec<_> = indices.into_iter().map(|index| stock[index].clone()).collect();
        let mut expected = ModelContextCapacity { context_tokens: u32::MAX, output_tokens: None};
        for selection in &models {
            let (provider, name) = selection.split_once('/').unwrap();
            let entry = &registry.catalog()[provider].models[name];
            expected.context_tokens = expected.context_tokens.min(entry.capacity.context_tokens);
            expected.output_tokens = expected.output_tokens.into_iter().chain(entry.capacity.output_tokens).min();
        }
        let alloy = registry.resolve_alloy(AlloySpec {
            models: models.clone(), select: AlloySelection::RoundRobin,
        }, ModelFeatures { tools, images }).unwrap();
        prop_assert_eq!(alloy.capacity(), expected);
        let selected = alloy.select(AlloyTurn {
            operation_id: acyclic_harness_core::OperationId::from_bytes([1; 16]), ordinal, role: "worker",
        });
        prop_assert_eq!(selected.context_capacity(selected.model()).unwrap(), expected);
        prop_assert!(models.contains(&format!("{}/{}", selected.model().provider, selected.model().name)), "selected model belongs to the stock subset");
        prop_assert!(transport.sent.lock().unwrap().is_empty());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn explicit_headers_round_trip_and_cannot_replace_credentials(
        suffix in "[a-z0-9-]{1,16}", value in "[ -~]{0,32}", uppercase in any::<bool>(),
    ) {
        let (provider, _) = provider(Adapter::Chat, 200);
        let mut spec = provider.config.as_ref().clone();
        let name = format!("x-owner-{suffix}");
        spec.headers.insert(name.clone(), value.clone());
        let encoded = serde_json::to_vec(&spec).unwrap();
        let replay: ProviderSpec = serde_json::from_slice(&encoded).unwrap();
        prop_assert!(crate::registry::validate_spec("fixture", &replay).is_ok());
        let mut selected = provider;
        selected.config = Arc::new(replay);
        let headers = crate::dispatch::headers(&selected, Some("fixture-key"), None, true);
        prop_assert_eq!(&headers[&name], &value);
        prop_assert_eq!(&headers["authorization"], "Bearer fixture-key");
        let reserved = if uppercase { "AUTHORIZATION" } else { "authorization" };
        Arc::make_mut(&mut selected.config).headers.insert(reserved.into(), value);
        prop_assert!(crate::registry::validate_spec("fixture", &selected.config).is_err());
    }
}
