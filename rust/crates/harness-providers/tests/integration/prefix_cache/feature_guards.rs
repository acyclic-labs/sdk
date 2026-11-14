//! Optional context edits preserve the frozen provider prefix.
use super::*;
use acyclic_harness::{
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

fn transport(path: &'static str) -> Arc<Scripted> {
    Arc::new(Scripted {
        path,
        state: Mutex::new(("agent", VecDeque::new(), Vec::new())),
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
        let catalog: acyclic_harness::mcp::McpCatalog = serde_json::from_value(json!({
            "server":"fixture", "revision":version.to_string(),
            "schema_exposure":{"kind":"eager"}, "discovery":"disabled",
            "projection_schema":acyclic_harness::tool::json_projection_schema(json!({"type":"object"})),
            "tools":[{"name":"inspect", "description":format!("Inspect revision {version}"),
                "inputSchema":{"type":"object"}}],
        }))?;
        for definition in catalog.model_definitions(None, None)? {
            let mut tool = super::super::providers::echo_tool(&definition.name);
            tool.definition = definition;
            extra.push(tool);
        }
    }
    let tools = acyclic_harness::tools::registry(&[], |_| false, extra)?;
    let mut manifest = ContextManifest::new(Default::default(), Vec::new(), &tools)?;
    manifest.conventions.push("Keep the cached policy.".into());
    if version == 0 {
        return Ok((manifest, tools));
    }
    let value = format!("optional feature version {version}");
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
            value.as_bytes(),
            "text/markdown",
            &path,
        )
        .await?;
    match feature {
        Feature::Source(_) => manifest.sources.push(file),
        Feature::Instructions => {
            manifest
                .discovery
                .instructions
                .push(ProjectInstruction::Captured {
                    source: InstructionIdentity::new(file.volume().clone(), "AGENT.md".into())?,
                    file,
                })
        }
        Feature::Environment => {
            manifest.environment = Some(EnvironmentFacts {
                cwd: Some(format!("/workspace/{version}")),
                tooling: Some(value),
                current_date: format!("2026-11-{version:02}"),
                ..Default::default()
            })
        }
        Feature::Skills | Feature::SelectedSkills => {
            let page = storage
                .list_private_directory("versions", None, None, 1)
                .await?;
            manifest.discovery.skills.push(SkillMetadata {
                name: "inspect".into(),
                description: value,
                fields: BTreeMap::new(),
                identity: "fixture:inspect".into(),
                source: PinnedContextPath {
                    root: ContextRoot {
                        volume: file.volume().clone(),
                        directory: "versions".into(),
                    },
                    generation: page.generation,
                    path: staged,
                },
            });
            if matches!(feature, Feature::SelectedSkills) {
                manifest.selected_skills.push("inspect".into());
            }
        }
        Feature::Mcp => {}
    }
    Ok((manifest, tools))
}

// Execute the real encoder and decoder; only the HTTP transport is scripted.
async fn complete(
    provider: &acyclic_harness_providers::ResolvedModel,
    request: ModelRequest,
) -> TestResult {
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
) -> TestResult<[u64; 2]> {
    let label = format!("{feature:?}/{adapter:?}/{enabled}");
    let transport = transport(path);
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes([91; 16]), 1 << 20).await?;
    let bindings = storage.content_bindings();
    let provider = registry(adapter, PromptCache::Native, transport.clone())?
        .with_content(bindings.reader.clone(), None)
        .resolve("recorded/model")?;
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
            history.push(message(
                ModelRole::User,
                serde_json::to_string(&change.notice)?,
            ));
        }
        let dynamic = !matches!(feature, Feature::Source(_) | Feature::Environment);
        assert_eq!(
            history.len() > before,
            enabled && turn > 0 && dynamic,
            "{label}"
        );
        told = live.digests()?;
        let input = ContextInput {
            input: ModelContent::Text("Return 19 + 23.".into()),
            selected_context: None,
            step: 0,
            prior_messages: history.clone(),
        };
        let context = ContextPipeline::new([stage])
            .run(&input, limits, None)
            .await?;
        let request = ModelRequest {
            summary: false,
            compose_tools: false,
            model: provider.model().clone(),
            messages: context.messages,
            tools: tools.definitions(None)?,
            sampling: Default::default(),
        };
        complete(&provider, request.clone()).await?;
        {
            let sent = transport.state.lock().unwrap();
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
            complete(&provider, changed).await?;
            let mut sent = transport.state.lock().unwrap();
            let changed = sent.2.pop().unwrap();
            assert!(
                departure(&sent.2[turn].body, &changed.body).is_some(),
                "{label}"
            );
            assert!(changed.cached.read < sent.2[turn].cached.read, "{label}");
        }
        history.extend([
            message(ModelRole::User, "Return 19 + 23."),
            message(ModelRole::Assistant, "ok"),
        ]);
    }
    let sent = transport.state.lock().unwrap();
    assert_eq!(sent.2.len(), 3, "{label}");
    Ok([
        sent.2.iter().map(|request| request.body.len() as u64).sum(),
        sent.2.iter().map(|request| request.cached.read).sum(),
    ])
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
            let off = guarded_run(feature, adapter, path, false).await?;
            let on = guarded_run(feature, adapter, path, true).await?;
            if matches!(feature, Feature::Source(_)) {
                assert_eq!(on, off, "{feature:?}/{adapter:?}");
            }
        }
    }
    Ok(())
}
// Compaction may replace history, but must retain its reusable header.
#[tokio::test]
async fn compaction_summaries_preserve_prefix() -> TestResult {
    for (adapter, path) in FAMILIES {
        use acyclic_harness::{
            executor::{StockExecutor, TurnInput},
            projection::{ModelContextSelection, SelectedModelContext},
        };
        let transport = transport(path);
        let storage = MemoryHarnessStorage::new(AgentId::from_bytes([92; 16]), 1 << 20).await?;
        let bindings = storage.content_bindings();
        let provider = Arc::new(
            registry(adapter, PromptCache::Native, transport.clone())?
                .with_content(bindings.reader.clone(), None)
                .resolve("recorded/model")?,
        );
        use acyclic_harness::context::{Context, ContextPlacement, SourceStage};
        let stage = SourceStage::new(
            "prefix",
            "1",
            Arc::new(Context {
                messages: vec![message(ModelRole::System, "Keep the cached policy.")],
                ..Default::default()
            }),
            ContextPlacement::Prepend,
        );
        let executor = StockExecutor::new(
            provider.model().clone(),
            provider,
            ContextPipeline::new([Arc::new(stage) as _]),
            ToolRegistry::default(),
        )
        .with_compaction_policy(CompactionPolicy::Capacity(CapacityCompaction {}));
        let journal = storage.journal();
        let mut history = vec![
            message(ModelRole::User, "older-0:".repeat(6_000)),
            message(ModelRole::Assistant, "older-1:".repeat(6_000)),
        ];
        for turn in 0..2_u8 {
            history.push(message(
                ModelRole::User,
                format!("Return 19 + 23. {}", "y".repeat(20_000)),
            ));
            let op = OperationId::from_bytes([turn + 1; 16]);
            let input = TurnInput::from_selected_context(
                op,
                SelectedModelContext {
                    selection: ModelContextSelection {
                        conversation_revision: history.len() as u64,
                        message_ids: (0..history.len())
                            .map(|index| uuid::Uuid::from_u128(index as u128 + 1))
                            .collect(),
                        checkpoint: None,
                    },
                    messages: history.clone(),
                },
            )?;
            if turn == 1 {
                transport.state.lock().unwrap().1.push_back(text(
                    r#"{"version":1,"drop_through":2,"summary":"retained summary"}"#,
                ));
            }
            transport.state.lock().unwrap().1.push_back(text("42"));
            let output = executor.execute(input.clone(), journal.as_ref()).await?;
            assert_eq!(output.text, "42");
            let count = transport.state.lock().unwrap().2.len();
            let replay = executor.execute(input, journal.as_ref()).await?;
            assert_eq!(replay.text, output.text);
            assert_eq!(
                transport.state.lock().unwrap().2.len(),
                count,
                "replay redispatched a model"
            );
            history.push(message(ModelRole::Assistant, output.text));
        }
        let sent = transport.state.lock().unwrap();
        assert_eq!(sent.2.len(), 3);
        let before = &sent.2[0];
        let after = &sent.2[2];
        assert!(holds(&sent.2[1].body, b"Source count:"));
        assert_eq!(header(path, &before.body)?, header(path, &after.body)?);
        assert!(after.cached.read > 0, "compaction lost the cached header");
        assert!(holds(&after.body, b"retained summary"));
        assert!(!holds(&after.body, b"older-0:"));
    }
    Ok(())
}

fn header(path: &str, body: &[u8]) -> TestResult<Value> {
    let mut value: Value = serde_json::from_slice(body)?;
    let field = match path {
        "responses" => "input",
        "models/model:streamGenerateContent" => "contents",
        _ => "messages",
    };
    value[field]
        .as_array_mut()
        .unwrap()
        .retain(|message| message["role"] == "system");
    Ok(value)
}
