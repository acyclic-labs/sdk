//! Requested aliases share one catalog and the existing protocol conformance transport.
use super::*;
use acyclic_harness_core::model::{
    ModelDispatch, ModelEvent, ModelMessage, ModelProvider as _, ModelRequest, ModelRole,
    PreparedModelRequest,
};
use futures::TryStreamExt as _;

fn mixed(routes: Arc<Vec<(String, Arc<Scripted>)>>) -> ScriptedHttp {
    ScriptedHttp::respond_owned(move |request| {
        let routes = routes.clone();
        Box::pin(async move {
            let route = routes
                .iter()
                .find(|(prefix, _)| request.url.starts_with(prefix))
                .ok_or_else(|| {
                    acyclic_harness_core::Error::Invalid("unknown mixed route".into())
                })?;
            let provider = route
                .0
                .strip_prefix("https://")
                .and_then(|url| url.split('.').next())
                .ok_or_else(|| {
                    acyclic_harness_core::Error::Invalid("invalid mixed route".into())
                })?;
            let (header, value) = match provider {
                "anthropic" => ("x-api-key", format!("fixture-{provider}")),
                "google" => ("x-goog-api-key", format!("fixture-{provider}")),
                _ => ("authorization", format!("Bearer fixture-{provider}")),
            };
            if request.headers.get(header) != Some(&value) {
                return Err(acyclic_harness_core::Error::Invalid(
                    "mixed provider credential route differs".into(),
                ));
            }
            ScriptedHttp::reply(200, route.1.reply(request)?)
        })
    })
}

#[tokio::test]
async fn mixed_provider_catalog_executes_every_route_and_rejects_a_swapped_adapter() -> TestResult {
    let providers = [
        ("anthropic", Adapter::Anthropic, "messages"),
        (
            "google",
            Adapter::Gemini,
            "models/model:streamGenerateContent",
        ),
        ("deepseek", Adapter::Chat, "chat/completions"),
        ("fireworks", Adapter::Chat, "chat/completions"),
    ];
    let mut routes = Vec::new();
    let mut document = BTreeMap::new();
    for (name, adapter, path) in providers {
        let transport = Arc::new(Scripted {
            path,
            state: Mutex::new(("agent", VecDeque::new(), Vec::new())),
            context: Mutex::default(),
            cache: Mutex::default(),
        });
        let catalog = registry(adapter, PromptCache::Native, transport.clone())?;
        let mut spec = catalog.catalog()["recorded"].as_ref().clone();
        spec.base_url = format!("https://{name}.invalid/v1");
        spec.authentication = Authentication::ApiKey;
        routes.push((spec.base_url.clone(), transport));
        document.insert(name, spec);
    }
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), serde_json::to_vec(&document)?)?;
    let routes = Arc::new(routes);
    let transport = Arc::new(mixed(routes.clone()));
    let credentials = providers
        .iter()
        .map(|(name, _, _)| (name.to_string(), format!("fixture-{name}")))
        .collect();
    let mut catalog =
        Registry::with_transport(Credentials::explicit(credentials), transport.clone());
    catalog.load_catalog(file.path())?;
    for (index, (name, adapter, path)) in providers.into_iter().enumerate() {
        let selection = format!("{name}/model");
        let model = catalog.resolve(&selection)?;
        let request = PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: model.model().clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text("conformance".into()),
                }],
                tools: Vec::new(),
                sampling: Default::default(),
            },
            Limits::default(),
            None,
        )?;
        let dispatch = ModelDispatch {
            operation_id: acyclic_harness_core::OperationId::from_bytes([42; 16]),
            step: u32::try_from(index)?,
            request_digest: request.request_digest(),
        };
        let events: Vec<_> = model.generate(&request, dispatch).try_collect().await?;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::Content { delta } if delta == "ok"))
        );
        assert!(matches!(events.last(), Some(ModelEvent::Completed { .. })));
        let sent = routes[index].1.state.lock().unwrap();
        assert_eq!(sent.2.len(), 1);
        assert!(
            sent.2[0]
                .url
                .split('?')
                .next()
                .is_some_and(|url| url.ends_with(path))
        );
        drop(sent);
        // The original count endpoint cannot be paired with another input dialect.
        let mut broken = document[name].clone();
        broken.adapter = if adapter == Adapter::Anthropic {
            Adapter::Chat
        } else {
            Adapter::Anthropic
        };
        assert!(
            catalog.register(name.into(), broken).is_err(),
            "{name} admitted a count endpoint for another provider's input dialect"
        );
        println!("PROVIDER_CONFORMANCE {name} PASS");
    }
    Ok(())
}
