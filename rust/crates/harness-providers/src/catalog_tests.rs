use super::*;
use proptest::prelude::*;

#[tokio::test]
async fn captured_container_catalog_keeps_exact_host_grants_on_every_route() -> Result<()> {
    let captured = include_bytes!("../fuzz/corpus/catalog/benchmark-http-host.json");
    let document = crate::catalog_document::parse(captured)?;
    let spec = &document.providers["benchmark"];
    assert_eq!(
        spec.allow_http_host.as_deref(),
        Some("172.17.0.1"),
        "catalog HTTP host grant changed at decode"
    );
    let (_, transport) = provider(Adapter::Responses, 200);
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), transport.clone());
    let file = tempfile::NamedTempFile::new().map_err(|cause| invalid(cause.to_string()))?;
    std::fs::write(file.path(), captured).map_err(|cause| invalid(cause.to_string()))?;
    registry.load_catalog(file.path())?;
    let selected = registry.resolve("benchmark/fixture")?;
    let (request, _) = request(&selected)?;
    let request = request.request().clone();
    let request = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    let dispatch = ModelDispatch {
        operation_id: acyclic_harness_core::OperationId::new(),
        step: 0,
        request_digest: request.request_digest(),
    };
    assert_eq!(
        crate::projection::endpoint(&selected)?,
        "http://172.17.0.1:9/t/trial/openai/v1/responses"
    );
    assert_eq!(
        crate::counting::wire::endpoint(
            &selected.config,
            "fixture",
            selected.config.token_counter.as_ref().unwrap(),
        )?,
        "http://172.17.0.1:9/t/trial/openai/v1/responses/input_tokens"
    );
    let selected = Arc::new(selected);
    let prepared = crate::prepared::PreparedInput::new(selected.clone(), request.clone()).await?;
    assert!(
        crate::tests::count_result(crate::prepared::PreparedInput::count(Arc::new(prepared)))
            .await?
            .input_tokens
            > 0
    );
    assert!(transport.counted.lock().unwrap().is_some());
    selected
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        transport.sent.lock().unwrap()[0].url,
        "http://172.17.0.1:9/t/trial/openai/v1/responses"
    );
    let sent = transport.sent.lock().unwrap().len();
    for (host, base, count_path) in [
        (None, "http://172.17.0.1:9/v1", "responses/input_tokens"),
        (
            Some("172.17.0.2"),
            "http://172.17.0.1:9/v1",
            "responses/input_tokens",
        ),
        (
            Some("*.example.test"),
            "http://one.example.test/v1",
            "responses/input_tokens",
        ),
        (
            Some("one.example.test"),
            "http://two.example.test/v1",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://user@172.17.0.1:9/v1",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://172.17.0.1:9/v1?token=value",
            "responses/input_tokens",
        ),
        (
            Some("172.17.0.1"),
            "http://172.17.0.1:9/v1",
            "http://172.17.0.2:9/count",
        ),
        (Some("172.17.0.1"), "http://172.17.0.1:9/v1", "responses"),
    ] {
        let mut refused = spec.clone();
        refused.allow_http_host = host.map(str::to_owned);
        refused.base_url = base.into();
        refused.token_counter.as_mut().unwrap().path = count_path.into();
        assert!(registry.register("refused".into(), refused).is_err());
    }
    assert_eq!(transport.sent.lock().unwrap().len(), sent);
    for base in [
        "https://provider.example/v1",
        "http://localhost:9/v1",
        "http://127.0.0.1:9/v1",
        "http://[::1]:9/v1",
    ] {
        let mut default = spec.clone();
        default.allow_http_host = None;
        default.base_url = base.into();
        registry.register("default".into(), default)?;
        registry.resolve("default/fixture")?;
    }
    Ok(())
}

#[tokio::test]
async fn arbitrary_provider_ids_follow_their_catalog_authentication_policy() -> Result<()> {
    let (_, script) = provider(Adapter::Chat, 200);
    let mut registry =
        Registry::with_transport(Credentials::explicit(BTreeMap::new()), script.clone());
    let mut spec = ProviderSpec {
        adapter: Adapter::Chat,
        authentication: Authentication::Optional,
        base_url: "http://127.0.0.1:1234/v1".into(),
        allow_http_host: None,
        headers: BTreeMap::new(),
        token_counter: Some(crate::TokenCounterSpec {
            path: "count_tokens".into(),
            adapter: Adapter::Chat,
            response_field: crate::TokenCountField::InputTokens,
            framing_tokens: 0,
        }),
        max_event_bytes: Some(4096),
        retry: crate::RetrySpec::DEFAULT,
        timeouts: crate::TimeoutSpec::DEFAULT,
        prompt_cache: crate::PromptCache::Native,
        models: BTreeMap::from([(
            "m".into(),
            ModelSpec {
                tool_profile: None,
                thinking_levels: false,
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
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert!(
        !script.sent.lock().expect("fixture")[0]
            .headers
            .contains_key("authorization")
    );
    spec.authentication = Authentication::ApiKey;
    registry.register("ollama".into(), spec)?;
    let required = registry.resolve("ollama/m")?;
    let (request, dispatch) = crate::tests::request(&required)?;
    let sent = script.sent.lock().expect("fixture").len();
    let error = required
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("API key is unavailable"),
        "built-in names do not bypass catalog policy"
    );
    assert_eq!(script.sent.lock().expect("fixture").len(), sent);
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
            tool_profile: None,
            thinking_levels: false,
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
fn resolution_rejects_mutated_model_identity() -> Result<()> {
    let (_, script) = provider(Adapter::Responses, 200);
    let mut registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([
            ("openai".into(), "fixture".into()),
            ("anthropic".into(), "fixture".into()),
            ("gemini".into(), "fixture".into()),
            ("morph".into(), "fixture".into()),
        ])),
        script,
    );
    for selection in [
        "openai/gpt-5",
        "anthropic/claude-sonnet-4-5",
        "gemini/gemini-2.5-pro",
    ] {
        let provider = registry.resolve(selection)?;
        let (prepared, _) = request(&provider)?;
        let mut changed = provider.model().clone();
        changed.revision.push_str("-foreign");
        assert!(provider.context_capacity(&changed).is_err());
        assert!(futures::executor::block_on(provider.counted(&prepared, 0)).is_ok());
        let mut old = serde_json::to_value(provider.model()).unwrap();
        old["options"] = json!({"tools":[{"type":"forged-native-tool"}]});
        assert!(serde_json::from_value::<Model>(old).is_err());
        let (provider_id, name) = selection.split_once('/').unwrap();
        let mut config = registry.catalog()[provider_id].as_ref().clone();
        let selected = config.models[name];
        config.models.insert("unrelated-model".into(), selected);
        registry.register(provider_id.into(), config.clone())?;
        assert_eq!(registry.resolve(selection)?.model(), provider.model());
        config.base_url.push_str("/another-route");
        registry.register(provider_id.into(), config)?;
        let rotated = registry.resolve(selection)?;
        assert_ne!(rotated.model(), provider.model());
        assert!(rotated.context_capacity(provider.model()).is_err());
    }
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
            ("morph".into(), "fixture".into()),
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
    let models = stock_models(&registry)
        .into_iter()
        .filter(|selection| {
            let (provider, name) = selection.split_once('/').unwrap();
            let features = registry.catalog()[provider].models[name].features;
            features.tools && features.images
        })
        .collect::<Vec<_>>();
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
        models in {
            let models = stock_models(&stock_registry().0);
            let count = models.len();
            proptest::sample::subsequence(models, 1..=count)
        },
        tools in any::<bool>(), images in any::<bool>(), ordinal in any::<u64>(),
    ) {
        let (registry, transport) = stock_registry();
        let mut expected = ModelContextCapacity { context_tokens: u32::MAX, output_tokens: None};
        let mut supported = true;
        for selection in &models {
            let (provider, name) = selection.split_once('/').unwrap();
            let entry = &registry.catalog()[provider].models[name];
            supported &= (!tools || entry.features.tools) && (!images || entry.features.images);
            expected.context_tokens = expected.context_tokens.min(entry.capacity.context_tokens);
            expected.output_tokens = expected.output_tokens.into_iter().chain(entry.capacity.output_tokens).min();
        }
        let resolved = registry.resolve_alloy(AlloySpec {
            models: models.clone(), select: AlloySelection::RoundRobin,
        }, ModelFeatures { tools, images });
        if !supported {
            prop_assert!(resolved.is_err());
            prop_assert!(transport.sent.lock().unwrap().is_empty());
            return Ok(());
        }
        let alloy = resolved.unwrap();
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

#[tokio::test]
async fn omitted_event_capacity_reaches_protocol_admission() -> Result<()> {
    let value = json!({"adapter":"chat","authentication":"optional",
        "base_url":"http://localhost:1234/v1",
        "models":{"fixture":{"capacity":{"context_tokens":8192,"output_tokens":1024}}}});
    let spec: ProviderSpec =
        serde_json::from_value(value).map_err(|cause| invalid(cause.to_string()))?;
    let (_, script) = provider(Adapter::Chat, 200);
    let mut registry = Registry::with_transport(Credentials::explicit(BTreeMap::new()), script);
    registry.register("local".into(), spec)?;
    let provider = registry.resolve("local/fixture")?;
    let (request, dispatch) = request(&provider)?;
    let events = provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ModelEvent::Completed { .. }))
    );
    Ok(())
}

#[test]
fn configured_seed_role_models_resolve_in_the_shipped_catalog() -> Result<()> {
    let (_, transport) = provider(Adapter::Chat, 200);
    let registry = Registry::with_transport(
        Credentials::explicit(BTreeMap::from([("morph".into(), "fixture".into())])),
        transport,
    );
    for role in ["orchestrator", "worker", "advisor", "reviewer"] {
        let path = format!("templates/{role}/AGENT.md");
        let bytes =
            acyclic_harness_core::seed::file(&path).ok_or_else(|| invalid("role absent"))?;
        let source = std::str::from_utf8(bytes).map_err(|error| invalid(error.to_string()))?;
        let selected = source
            .lines()
            .find_map(|line| line.strip_prefix("model: "))
            .ok_or_else(|| invalid("role model absent"))?;
        registry.resolve(selected)?;
    }
    Ok(())
}
