//! Optional context edits preserve the frozen provider prefix.
use super::*;
use acyclic_harness_core::AgentId as StorageAgentId;
use acyclic_harness_core::projection::*;
use acyclic_harness_core::{
    OperationId, context::*, filesystem::MemoryHarnessStorage, model::*, tool::ToolRegistry,
};
use futures::TryStreamExt as _;

const FAMILIES: [(Adapter, &str); 4] = [
    (Adapter::Chat, "chat/completions"),
    (Adapter::Responses, "responses"),
    (Adapter::Anthropic, "messages"),
    (Adapter::Gemini, "models/model:streamGenerateContent"),
];

#[derive(Clone, Copy, Debug)]
enum Feature {
    Source(&'static str),
    Instructions,
    Environment,
    Skills,
    SelectedSkills,
    Mcp,
}

fn message(role: ModelRole, text: impl Into<String>) -> ModelMessage {
    ModelMessage {
        role,
        content: ModelContent::Text(text.into()),
    }
}

fn selected(messages: Vec<ModelMessage>) -> SelectedModelContext {
    SelectedModelContext {
        selection: ModelContextSelection {
            conversation_revision: messages.len() as u64,
            message_ids: (1..=messages.len() as u128)
                .map(uuid::Uuid::from_u128)
                .collect(),
            checkpoint: None,
        },
        messages,
    }
}
fn transport(path: &'static str) -> Arc<Scripted> {
    Arc::new(Scripted {
        path,
        state: Mutex::new(("agent", VecDeque::new(), Vec::new())),
        context: Mutex::default(),
        cache: Mutex::new(harness_mock_model::PromptCache::default().with_implicit_chat()),
    })
}

// Stage immutable versions rather than overwrite retained FileRefs.
async fn manifest(
    storage: &MemoryHarnessStorage,
    feature: Feature,
    version: u8,
) -> TestResult<(ContextManifest, ToolRegistry)> {
    let mut extra = Vec::new();
    if matches!(feature, Feature::Mcp) && version != 0 {
        let catalog: acyclic_harness_core::mcp::McpCatalog = serde_json::from_value(json!({
            "server":"fixture", "revision":version.to_string(),
            "schema_exposure":{"kind":"eager"}, "discovery":"disabled",
            "projection_schema":acyclic_harness_core::tool::json_projection_schema(json!({"type":"object"})),
            "tools":[{"name":"inspect", "description":format!("feature-v{version}"),
                "inputSchema":{"type":"object"}}],
        }))?;
        for definition in catalog.model_definitions(None, None)? {
            let mut tool =
                crate::prefix_cache_cases::integration_helpers::echo_tool(&definition.name);
            tool.definition = definition;
            extra.push(tool);
        }
    }
    let tools = acyclic_harness_core::tools::registry(&[], |_| false, extra)?;
    let mut manifest = ContextManifest::new(Default::default(), Vec::new(), &tools)?;
    manifest.conventions.push("Keep the cached policy.".into());
    if version == 0 {
        return Ok((manifest, tools));
    }
    let value = format!("feature-v{version}");
    let body = format!("BODY_{version}: {value}");
    let path = match feature {
        Feature::Source(path) => path.to_owned(),
        Feature::Instructions => "AGENT.md".into(),
        _ => "skills/inspect/SKILL.md".into(),
    };
    let staged = format!("versions/{version}/{path}");
    let file = storage
        .stage(
            OperationId::new(),
            &staged,
            body.as_bytes(),
            "text/markdown",
            path.rsplit('/').next().unwrap(),
        )
        .await?;
    let discovery = &mut manifest.discovery;
    match feature {
        Feature::Source(_) => manifest.sources.push(file),
        Feature::Instructions => discovery.instructions.push(ProjectInstruction::Captured {
            source: InstructionIdentity::new(file.volume().clone(), "AGENT.md".into())?,
            file,
        }),
        Feature::Environment => {
            manifest.environment = Some(EnvironmentFacts {
                cwd: Some(format!("/workspace/{version}")),
                tooling: Some(value),
                scratch: None,
                current_date: format!("2026-11-{version:02}"),
                ..Default::default()
            })
        }
        Feature::Skills | Feature::SelectedSkills => {
            let page = storage
                .list_private_directory("versions", None, None, 1)
                .await?;
            discovery.skills.push(serde_json::from_value(json!({
                "name":"inspect", "description":value, "fields":{}, "identity":"fixture:inspect",
                "source":{"root":{"volume":file.volume(), "directory":"versions"},
                    "generation":page.generation, "path":staged},
            }))?);
            if matches!(feature, Feature::SelectedSkills) {
                manifest.selected_skills.push("inspect".into());
            }
        }
        Feature::Mcp => {}
    }
    Ok((manifest, tools))
}

// Execute the real encoder and decoder; only the HTTP transport is scripted.
async fn complete(provider: &dyn ModelProvider, request: ModelRequest) -> TestResult {
    let request = PreparedModelRequest::prepare(request, Limits::default(), None)?;
    let _: Vec<_> = provider
        .generate(
            &request,
            ModelDispatch {
                operation_id: OperationId::from_bytes([42; 16]),
                step: 0,
                request_digest: request.request_digest(),
            },
        )
        .try_collect()
        .await?;
    Ok(())
}

async fn guarded_run(
    feature: Feature,
    adapter: Adapter,
    path: &'static str,
    enabled: bool,
) -> TestResult<Vec<Bytes>> {
    let label = format!("{feature:?}/{adapter:?}/{enabled}");
    let transport = transport(path);
    let storage = MemoryHarnessStorage::new(StorageAgentId::from_bytes([91; 16]), 1 << 20).await?;
    let bindings = storage.content_bindings();
    let resolved = registry(adapter, PromptCache::Native, transport.clone())?
        .with_content(bindings.reader.clone(), None)
        .resolve("recorded/model")?;
    // Both arms run in one conversation: the Chat cache key follows it, so the
    // arms differ only in what the feature puts on the wire.
    let provider = resolved
        .bind_conversation(OperationId::from_bytes([93; 16]))
        .ok_or("the provider binds its conversation")?;
    let writer = bindings.writer.as_deref().unwrap();
    let limits = Limits::default();
    let op = ContextEpoch::operation("feature-guard", None)?;
    let (seed, catalogue) = manifest(&storage, feature, u8::from(enabled)).await?;
    let epoch = ContextEpoch::open(writer, op, limits, || async { Ok(seed.clone()) }).await?;
    let mut told = epoch.digests(bindings.reader.as_ref(), limits).await?;
    let mut history = Vec::new();
    // Select, edit, remove. Reopening the same epoch must retain its prefix.
    let versions = if enabled { [1, 2, 0] } else { [0; 3] };
    for (turn, version) in versions.into_iter().enumerate() {
        let live = if turn == 0 {
            seed.clone()
        } else {
            manifest(&storage, feature, version).await?.0
        };
        if enabled && turn > 0 {
            assert_ne!(seed.encode(limits)?, live.encode(limits)?, "{label}");
        }
        let reopened =
            ContextEpoch::open(writer, op, limits, || async { Ok(live.clone()) }).await?;
        assert_eq!(reopened.manifest(), epoch.manifest(), "{label}");
        let (tools, stage) = ContextManifest::bind(
            reopened.manifest().clone(),
            bindings.reader.as_ref(),
            Some(writer),
            catalogue.clone(),
            limits,
        )
        .await?;
        let before = history.len();
        if turn > 0 && seed.tools != live.tools {
            history.push(message(
                ModelRole::User,
                serde_json::to_string(&json!({"kind":"tools.changed", "tools":live.tools}))?,
            ));
        }
        let changes = ContextEpoch::changes(&told, &live, bindings.reader.as_ref()).await?;
        for change in changes {
            let removed = change.notice.body["removed"] == true;
            assert_eq!(removed, version == 0, "{label}");
            history.push(message(
                ModelRole::User,
                serde_json::to_string(&change.notice)?,
            ));
        }
        let dynamic = !matches!(feature, Feature::Source(_) | Feature::Environment);
        let announced = enabled && turn > 0 && dynamic;
        assert_eq!(history.len() > before, announced, "{label}");
        told = live.digests()?;
        history.push(message(ModelRole::User, format!("Task {turn}")));
        let input = ContextInput {
            input: history.last().unwrap().content.clone(),
            selected_context: Some(selected(history.clone())),
            step: 0,
            prior_messages: Vec::new(),
        };
        let context = ContextPipeline::new([stage])
            .run(&input, limits, None)
            .await?;
        let request = ModelRequest {
            summary: false,
            compose_tools: false,
            model: resolved.model().clone(),
            messages: context.messages,
            tools: tools.definitions(None)?,
            sampling: Default::default(),
        };
        complete(provider.as_ref(), request.clone()).await?;
        {
            let sent = transport.state.lock().unwrap();
            let body = &sent.2[turn].body;
            let visible = enabled && !matches!(feature, Feature::Source(_));
            assert_eq!(holds(body, b"feature-v1"), visible, "{label}");
            let selected_body = matches!(feature, Feature::Instructions | Feature::SelectedSkills);
            assert_eq!(holds(body, b"BODY_1:"), enabled && selected_body, "{label}");
            assert_eq!(holds(body, b"feature-v2"), announced, "{label}");
            for notice in &history[before..history.len() - 1] {
                let ModelContent::Text(text) = &notice.content else {
                    unreachable!()
                };
                assert!(holds(body, &serde_json::to_vec(text)?), "{label}");
            }
            if turn > 0 {
                assert!(
                    departure(&sent.2[turn - 1].body, &sent.2[turn].body).is_none(),
                    "{label}"
                );
                assert!(sent.2[turn].cached.read > 0, "{label}");
            }
        }
        // Both independent checks must reject a rewritten frozen instruction.
        if turn == 1 {
            let mut changed = request;
            changed.messages[0] = message(ModelRole::System, "rewritten optional policy");
            complete(provider.as_ref(), changed).await?;
            let mut sent = transport.state.lock().unwrap();
            let changed = sent.2.pop().unwrap();
            assert!(
                departure(&sent.2[turn].body, &changed.body).is_some(),
                "{label}"
            );
            assert!(changed.cached.read < sent.2[turn].cached.read, "{label}");
        }
        history.push(message(ModelRole::Assistant, "ok"));
    }
    let sent = transport.state.lock().unwrap();
    assert_eq!(sent.2.len(), 3, "{label}");
    Ok(sent.2.iter().map(|request| request.body.clone()).collect())
}

#[tokio::test]
async fn optional_features_preserve_prefix() -> TestResult {
    let features = [
        Feature::Source("ui/index.md"),
        Feature::Source("approvals/pending.json"),
        Feature::Source("config/settings.json"),
        Feature::Source("sessions/index.json"),
        Feature::Source("evals/report.json"),
        Feature::Source("GOAL.md"),
        Feature::Source("plans/current.md"),
        Feature::Source("hooks/after-turn.json"),
        Feature::Instructions,
        Feature::Environment,
        Feature::Skills,
        Feature::SelectedSkills,
        Feature::Mcp,
    ];
    for feature in features {
        for (adapter, path) in FAMILIES {
            let off = guarded_run(feature, adapter, path, false)
                .await
                .map_err(|error| format!("{feature:?}/{adapter:?}/off: {error}"))?;
            let on = guarded_run(feature, adapter, path, true)
                .await
                .map_err(|error| format!("{feature:?}/{adapter:?}/on: {error}"))?;
            if matches!(feature, Feature::Source(_)) {
                assert_eq!(on, off, "{feature:?}/{adapter:?}");
            }
        }
    }
    Ok(())
}
