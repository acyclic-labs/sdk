//! R44 paired, offline evaluations over the real epoch, manifest and encoders.
//! Token units are serialized UTF-8 bytes (the shared scripted counter), not
//! estimates of a live model's tokenizer or evidence of model quality.
use super::*;
use acyclic_harness::{
    context::{
        ContextEpoch, ContextInput, ContextManifest, ContextPipeline, ContextRoot,
        EnvironmentFacts, PinnedContextPath, SkillMetadata,
    },
    model::{
        ModelDispatch, ModelEvent, ModelMessage, ModelProvider as _, ModelRequest, ModelRole,
        PreparedModelRequest,
    },
    tool::ToolRegistry,
};
use futures::TryStreamExt as _;

const FAMILIES: [(Adapter, &str); 4] = [
    (Adapter::Chat, "chat/completions"),
    (Adapter::Responses, "responses"),
    (Adapter::Anthropic, "messages"),
    (Adapter::Gemini, "models/model:streamGenerateContent"),
];

#[derive(Clone, Copy)]
enum Feature {
    Source(&'static str),
    Instructions,
    Environment,
    Skills,
    Mcp,
}

#[derive(Debug, Default, PartialEq, serde::Serialize)]
struct Evaluation {
    success: u64,
    requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
}

fn report(feature: &str, adapter: Adapter, off: &Evaluation, on: &Evaluation) {
    println!(
        "FEATURE_EVAL {}",
        json!({
            "version": 1, "feature": feature, "provider": format!("{adapter:?}"),
            "token_unit": "scripted_utf8_byte", "off": off, "on": on,
            "delta": {
                "success": i128::from(on.success) - i128::from(off.success),
                "input_tokens": i128::from(on.input_tokens) - i128::from(off.input_tokens),
                "output_tokens": i128::from(on.output_tokens) - i128::from(off.output_tokens),
                "cache_read_tokens": i128::from(on.cache_read_tokens) - i128::from(off.cache_read_tokens),
            }
        })
    );
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

/// Change exactly one actual manifest input. A new retained version is staged
/// each time; attempting to overwrite an immutable FileRef would test storage
/// conflict handling instead of the prefix contract.
async fn manifest(
    storage: &acyclic_harness::filesystem::MemoryHarnessStorage,
    feature: Feature,
    version: u8,
) -> TestResult<(ContextManifest, ToolRegistry)> {
    let tools = acyclic_harness::tools::registry(
        &[],
        |_| false,
        if matches!(feature, Feature::Mcp) && version != 0 {
            vec![super::super::providers::echo_tool(if version == 1 {
                "mcp.before"
            } else {
                "mcp.after"
            })]
        } else {
            Vec::new()
        },
    )?;
    let mut manifest = ContextManifest::new(Default::default(), Vec::new(), &tools)?;
    manifest
        .conventions
        .push(std::str::from_utf8(acyclic_harness::seed::file("AGENT.md").unwrap())?.into());
    if version == 0 {
        return Ok((manifest, tools));
    }
    let value = format!("optional feature version {version}");
    match feature {
        Feature::Source(path) => {
            manifest.sources.push(
                storage
                    .stage(
                        acyclic_harness::OperationId::new(),
                        &format!("versions/{version}/{path}"),
                        value.as_bytes(),
                        "text/plain",
                        path,
                    )
                    .await?,
            );
        }
        Feature::Instructions => {
            let file = storage
                .stage(
                    acyclic_harness::OperationId::new(),
                    &format!("versions/{version}/AGENT.md"),
                    value.as_bytes(),
                    "text/markdown",
                    "AGENT.md",
                )
                .await?;
            manifest.discovery.instructions.push(
                acyclic_harness::model::ProjectInstruction::Captured {
                    source: acyclic_harness::model::InstructionIdentity::new(
                        file.volume().clone(),
                        "AGENT.md".into(),
                    )?,
                    file,
                },
            );
        }
        Feature::Environment => {
            manifest.environment = Some(EnvironmentFacts {
                cwd: Some(format!("/workspace/{version}")),
                os: format!("os-{version}"),
                shell: format!("shell-{version}"),
                tool_path: Some(format!("/bin/{version}")),
                tooling: Some(value),
                current_date: format!("2026-11-{version:02}"),
                timezone: "UTC".into(),
            })
        }
        Feature::Skills => {
            let path = format!("skills/inspect-{version}/SKILL.md");
            let file = storage
                .stage(
                    acyclic_harness::OperationId::new(),
                    &path,
                    value.as_bytes(),
                    "text/markdown",
                    "SKILL.md",
                )
                .await?;
            let page = storage
                .list_private_directory("skills", None, None, 1)
                .await?;
            manifest.discovery.skills.push(SkillMetadata {
                name: "inspect".into(),
                description: value,
                fields: BTreeMap::new(),
                identity: "fixture:inspect".into(),
                source: PinnedContextPath {
                    root: ContextRoot {
                        volume: file.volume().clone(),
                        directory: "skills".into(),
                    },
                    generation: page.generation,
                    path,
                },
            });
        }
        Feature::Mcp => {}
    }
    Ok((manifest, tools))
}

async fn evaluate(
    feature: Feature,
    adapter: Adapter,
    path: &'static str,
    enabled: bool,
) -> TestResult<Evaluation> {
    let transport = transport(path);
    let storage = acyclic_harness::filesystem::MemoryHarnessStorage::new(
        AgentId::from_bytes([91; 16]),
        1 << 20,
    )
    .await?;
    let bindings = storage.content_bindings();
    let provider = registry(adapter, PromptCache::Native, transport.clone())?
        .with_content(bindings.reader.clone(), None)
        .resolve("recorded/model")?;
    let publisher = bindings
        .writer
        .as_deref()
        .ok_or("missing fixture publisher")?;
    let limits = Limits::default();
    let operation = ContextEpoch::operation("feature-evaluation", None)?;
    let (initial, initial_tools) = manifest(&storage, feature, u8::from(enabled)).await?;
    let epoch = ContextEpoch::open(publisher, operation, limits, || async {
        Ok(initial.clone())
    })
    .await?;
    let mut told = epoch.digests(bindings.reader.as_ref(), limits).await?;
    let mut history = Vec::new();
    let mut evaluation = Evaluation::default();
    let mut previous: Option<Bytes> = None;
    // Initial selection, edited selection, then removal. Both branches perform
    // exactly the same task and admit the same three worker completions.
    for (turn, version) in [u8::from(enabled), if enabled { 2 } else { 0 }, 0]
        .into_iter()
        .enumerate()
    {
        let (live, _) = manifest(&storage, feature, version).await?;
        if enabled && turn > 0 {
            assert_ne!(initial.encode(limits)?, live.encode(limits)?);
        }
        let reopened =
            ContextEpoch::open(publisher, operation, limits, || async { Ok(live.clone()) }).await?;
        assert_eq!(
            reopened.manifest(),
            epoch.manifest(),
            "live sources replaced the epoch"
        );
        let (tools, stage) = ContextManifest::bind(
            reopened.manifest().clone(),
            bindings.reader.as_ref(),
            Some(publisher),
            initial_tools.clone(),
            limits,
        )
        .await?;
        if turn > 0 && initial.tools != live.tools {
            history.push(message(
                ModelRole::User,
                serde_json::to_string(&json!({
                    "kind": "tools.changed", "tools": live.tools,
                }))?,
            ));
        }
        let changes = ContextEpoch::changes(&told, &live, bindings.reader.as_ref()).await?;
        for change in changes {
            history.push(message(
                ModelRole::User,
                serde_json::to_string(&change.notice)?,
            ));
        }
        told = live.digests()?;
        let input = ContextInput {
            input: ModelContent::Text(format!("Task {turn}: return 19 + 23 as a decimal integer.")),
            selected_context: None,
            step: 0,
            prior_messages: history.clone(),
        };
        let context = ContextPipeline::new([stage])
            .run(&input, limits, None)
            .await?;
        let prepared = PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: provider.model().clone(),
                messages: context.messages,
                tools: tools.definitions(None)?,
                sampling: Default::default(),
            },
            limits,
            None,
        )?;
        transport.state.lock().unwrap().1.push_back(text("42"));
        let events: Vec<_> = provider
            .generate(
                &prepared,
                ModelDispatch {
                    operation_id: acyclic_harness::OperationId::from_bytes([42; 16]),
                    step: turn as u32,
                    request_digest: prepared.request_digest(),
                },
            )
            .try_collect()
            .await?;
        let answer: String = events
            .iter()
            .filter_map(|event| match event {
                ModelEvent::Content { delta } => Some(delta.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(answer, "42", "feature task failed");
        assert!(
            matches!(events.last(), Some(ModelEvent::Completed { .. })),
            "incomplete feature task"
        );
        let sent = transport.state.lock().unwrap();
        let request = sent.2.last().ok_or("missing encoded request")?;
        if let Some(previous) = &previous {
            extends(
                "feature evaluation",
                "source mutation",
                previous,
                &request.body,
            )?;
            assert!(
                request.cached.read > 0,
                "source mutation lost cache residency"
            );
        }
        record(&mut evaluation, adapter, &events)?;
        previous = Some(request.body.clone());
        // Each feature has a semantic negative control: rewriting the frozen
        // instruction changes both the encoded prefix and cache-read evidence.
        if turn == 1 {
            let mut changed = prepared.request().clone();
            changed.messages[0] = message(ModelRole::System, "rewritten optional policy");
            let changed = PreparedModelRequest::prepare(changed, limits, None)?;
            drop(sent);
            transport.state.lock().unwrap().1.push_back(text("42"));
            let _: Vec<_> = provider
                .generate(
                    &changed,
                    ModelDispatch {
                        operation_id: acyclic_harness::OperationId::from_bytes([43; 16]),
                        step: 0,
                        request_digest: changed.request_digest(),
                    },
                )
                .try_collect()
                .await?;
            let mut sent = transport.state.lock().unwrap();
            let changed = sent.2.pop().ok_or("missing negative control")?;
            assert!(
                departure(previous.as_ref().unwrap(), &changed.body).is_some(),
                "guard accepted a prefix rewrite"
            );
            assert!(
                changed.cached.read < request_read(&sent.2)?,
                "cache control accepted a prefix rewrite"
            );
        }
        history.extend([
            message(
                ModelRole::User,
                match &input.input {
                    ModelContent::Text(text) => text,
                    _ => unreachable!(),
                },
            ),
            message(ModelRole::Assistant, answer),
        ]);
    }
    assert_eq!(evaluation.success, 3);
    let sent = transport.state.lock().unwrap();
    assert_eq!(
        evaluation.input_tokens,
        sent.2
            .iter()
            .map(|request| request.body.len() as u64)
            .sum::<u64>()
    );
    assert_eq!(
        evaluation.cache_read_tokens,
        sent.2
            .iter()
            .map(|request| request.cached.read)
            .sum::<u64>()
    );
    Ok(evaluation)
}

fn request_read(sent: &[Sent]) -> TestResult<u64> {
    Ok(sent.last().ok_or("missing positive request")?.cached.read)
}

/// Read the completed provider's native usage fields, including Anthropic's
/// exclusive uncached input convention. Do not synthesize success from a script.
fn record(evaluation: &mut Evaluation, adapter: Adapter, events: &[ModelEvent]) -> TestResult {
    let Some(ModelEvent::Completed { metadata }) = events.last() else {
        return Err("incomplete evaluation".into());
    };
    let usage = &metadata["usage"];
    let number = |field: &str| {
        usage[field]
            .as_u64()
            .ok_or_else(|| format!("missing {adapter:?} usage {field}"))
    };
    let (input, output, cached) = match adapter {
        Adapter::Chat => (
            number("prompt_tokens")?,
            number("completion_tokens")?,
            usage["prompt_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
        Adapter::Responses => (
            number("input_tokens")?,
            number("output_tokens")?,
            usage["input_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
        Adapter::Anthropic => (
            number("input_tokens")?
                + usage["cache_read_input_tokens"].as_u64().unwrap_or(0)
                + usage["cache_creation_input_tokens"].as_u64().unwrap_or(0),
            number("output_tokens")?,
            usage["cache_read_input_tokens"].as_u64().unwrap_or(0),
        ),
        Adapter::Gemini => (
            number("promptTokenCount")?,
            number("candidatesTokenCount")?,
            usage["cachedContentTokenCount"].as_u64().unwrap_or(0),
        ),
    };
    evaluation.requests += 1;
    evaluation.input_tokens += input;
    evaluation.output_tokens += output;
    evaluation.cache_read_tokens += cached;
    evaluation.success += 1;
    Ok(())
}

async fn paired(name: &str, feature: Feature) -> TestResult {
    for (adapter, path) in FAMILIES {
        let off = evaluate(feature, adapter, path, false).await?;
        let on = evaluate(feature, adapter, path, true).await?;
        report(name, adapter, &off, &on);
        assert_eq!(on.success, off.success);
        if matches!(feature, Feature::Source(_)) {
            assert_eq!(on, off, "execution-only input entered the prompt");
        }
    }
    Ok(())
}

macro_rules! guard {
    ($test:ident, $feature:expr) => {
        #[tokio::test]
        async fn $test() -> TestResult {
            paired(stringify!($test), $feature).await
        }
    };
}

guard!(preset_ui_prefix_digest, Feature::Source("ui/index.md"));
guard!(
    preset_approvals_prefix_digest,
    Feature::Source("approvals/pending.json")
);
guard!(
    preset_configuration_prefix_digest,
    Feature::Source("config/settings.json")
);
guard!(
    preset_sessions_prefix_digest,
    Feature::Source("sessions/index.json")
);
guard!(
    preset_evaluations_prefix_digest,
    Feature::Source("evals/report.json")
);
guard!(preset_goal_prefix_digest, Feature::Source("GOAL.md"));
guard!(
    preset_plans_prefix_digest,
    Feature::Source("plans/current.md")
);
guard!(
    preset_hooks_prefix_digest,
    Feature::Source("hooks/after-turn.json")
);
guard!(project_instructions_prefix_digest, Feature::Instructions);
guard!(environment_facts_prefix_digest, Feature::Environment);
guard!(skills_prefix_digest, Feature::Skills);
guard!(mcp_listings_prefix_digest, Feature::Mcp);

/// A compaction intentionally replaces older conversation messages. Its
/// reusable system/tool header must survive, and its cost includes the separate
/// summary dispatch. Exercise the stock executor and its durable checkpoint,
/// rather than hand-constructing a request that merely resembles a summary.
async fn compaction(adapter: Adapter, path: &'static str, enabled: bool) -> TestResult<Evaluation> {
    use acyclic_harness::{
        executor::{ExecutionEvent, StockExecutor, TurnInput},
        projection::{ModelContextSelection, SelectedModelContext},
    };
    let transport = transport(path);
    let storage = acyclic_harness::filesystem::MemoryHarnessStorage::new(
        AgentId::from_bytes([92; 16]),
        1 << 20,
    )
    .await?;
    let bindings = storage.content_bindings();
    let provider = Arc::new(
        registry(adapter, PromptCache::Native, transport.clone())?
            .with_content(bindings.reader.clone(), None)
            .resolve("recorded/model")?,
    );
    let (manifest, tools) = manifest(&storage, Feature::Source("config/settings.json"), 0).await?;
    let reference = manifest
        .publish(
            bindings.writer.as_deref().ok_or("missing publisher")?,
            acyclic_harness::OperationId::new(),
            "manifest.json",
            Limits::default(),
        )
        .await?;
    let (_, stage) = ContextManifest::bind(
        reference,
        bindings.reader.as_ref(),
        bindings.writer.as_deref(),
        tools.clone(),
        Limits::default(),
    )
    .await?;
    let executor = StockExecutor::new(
        provider.model().clone(),
        provider,
        ContextPipeline::new([stage]),
        tools,
    )
    .with_compaction_policy(if enabled {
        CompactionPolicy::Capacity(CapacityCompaction {})
    } else {
        CompactionPolicy::Disabled
    });
    let journal = storage.journal();
    let mut history: Vec<_> = (0..6)
        .map(|index| {
            message(
                if index % 2 == 0 {
                    ModelRole::User
                } else {
                    ModelRole::Assistant
                },
                format!("older-{index}: {}", "x".repeat(17_000)),
            )
        })
        .collect();
    for turn in 0..2_u8 {
        history.push(message(
            ModelRole::User,
            format!("Return 19 + 23. {}", "y".repeat(17_000)),
        ));
        let operation = acyclic_harness::OperationId::from_bytes([turn + 1; 16]);
        let input = TurnInput::from_selected_context(
            operation,
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
        if enabled && turn == 1 {
            transport.state.lock().unwrap().1.push_back(text(
                r#"{"version":1,"drop_through":2,"summary":"Retained: the task is to return 42."}"#,
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
        let records = journal.replay(operation, 0, u32::MAX, None)?.await?;
        let compacted = records
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ContextCompacted { .. }));
        assert_eq!(
            compacted,
            enabled && turn == 1,
            "summary was not durably adopted"
        );
        history.push(message(ModelRole::Assistant, output.text));
    }
    let sent = transport.state.lock().unwrap();
    assert_eq!(sent.2.len(), if enabled { 3 } else { 2 });
    let workers: Vec<_> = sent
        .2
        .iter()
        .filter(|request| !holds(&request.body, b"Source count:"))
        .collect();
    assert_eq!(
        workers.len(),
        2,
        "summary and worker requests were not distinct"
    );
    assert_eq!(
        header(path, &workers[0].body)?,
        header(path, &workers[1].body)?,
        "summary changed reusable header"
    );
    assert!(
        workers[1].cached.read > 0,
        "compaction lost the cached header"
    );
    if enabled {
        assert!(
            holds(&workers[1].body, b"Retained: the task is to return 42."),
            "summary never entered the worker request"
        );
        assert!(
            !holds(&workers[1].body, b"older-0:"),
            "compaction retained the retired history"
        );
    } else {
        extends(
            "compaction off",
            "next turn",
            &workers[0].body,
            &workers[1].body,
        )?;
    }
    Ok(Evaluation {
        success: 2,
        requests: sent.2.len() as u64,
        input_tokens: sent.2.iter().map(|request| request.body.len() as u64).sum(),
        output_tokens: 2 * sent.2.len() as u64,
        cache_read_tokens: sent.2.iter().map(|request| request.cached.read).sum(),
    })
}

fn header(path: &str, body: &[u8]) -> TestResult<Value> {
    let mut value: Value = serde_json::from_slice(body)?;
    let field = match path {
        "responses" => "input",
        "models/model:streamGenerateContent" => "contents",
        _ => "messages",
    };
    if let Some(messages) = value[field].as_array_mut() {
        messages.truncate(
            messages
                .iter()
                .take_while(|message| message["role"] == "system")
                .count(),
        );
    }
    Ok(value)
}

#[tokio::test]
async fn compaction_summaries_prefix_digest() -> TestResult {
    for (adapter, path) in FAMILIES {
        let off = compaction(adapter, path, false).await?;
        let on = compaction(adapter, path, true).await?;
        report("compaction_summaries_prefix_digest", adapter, &off, &on);
        assert_eq!(off.success, on.success);
        assert_eq!(on.requests, off.requests + 1, "summary cost was omitted");
    }
    Ok(())
}
