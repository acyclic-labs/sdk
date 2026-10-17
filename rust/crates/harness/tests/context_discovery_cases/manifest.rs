use super::*;
use acyclic_harness::{
    OperationId,
    context::{CompactionPolicy, ContextManifest},
    conversation::ContentPublisher,
    executor::{StockExecutor, TurnInput},
    filesystem::FilesystemContentPublisher,
    model::{Model, ModelRole},
    runtime::RuntimeScope,
    tool::{ToolRegistry, files},
};
use acyclic_stream::MemoryStream;
use serde_json::Value;

fn tools() -> Result<ToolRegistry> {
    let mut tools = ToolRegistry::default();
    for tool in [files::write()?, files::read()?] {
        tools.register((tool, None), &mut None, None)?;
    }
    Ok(tools)
}

async fn storage(
    fixture: &Fixture,
) -> Result<(
    Arc<dyn ContentPublisher>,
    Arc<dyn ContentResidencyVerifier>,
    VolumeRef,
    acyclic_harness::core::Scope,
)> {
    let (private, scope) = fixture.journal_volume().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    Ok((
        FilesystemContentPublisher::new(
            fixture.host.clone(),
            &private,
            &verifier,
            &scope,
            32_768,
            None,
        )?,
        FilesystemContentVerifier::new(fixture.host.clone(), &verifier, &scope, 32_768, None)?,
        private,
        scope,
    ))
}

fn stock(
    provider: Arc<ScriptedModelProvider>,
    tools: ToolRegistry,
    limits: Limits,
) -> Result<StockExecutor> {
    let grants = Capabilities::new(
        tools
            .definitions(None)?
            .into_iter()
            .map(|tool| format!("tool:call:{}", tool.name)),
    );
    StockExecutor::new(
        Model::new("test".into(), "manifest".into(), "1".into(), Value::Null)?,
        provider,
        ContextPipeline::default(),
        tools,
    )
    .with_limits(limits)
    .with_compaction_policy(CompactionPolicy::Disabled)
    .with_tool_authority(RuntimeScope::new(grants, limits)?, None)
}

const INSTRUCTIONS: [(&str, &str); 4] = [
    ("AGENTS.md", "broad instructions"),
    ("CLAUDE.md", "broad Claude instructions"),
    ("project/AGENTS.md", "specific instructions"),
    ("project/CLAUDE.md", "specific Claude instructions"),
];

async fn seed(fixture: &Fixture) -> Result<ContextManifest> {
    for (path, text) in [
        (
            "skills/a/SKILL.md",
            "---\nname: inspect\ndescription: Inspect files\n---\nSELECTED BODY",
        ),
        (
            "skills/b/SKILL.md",
            "---\nname: hidden\ndescription: Hidden skill\n---\nUNSELECTED BODY",
        ),
    ] {
        fixture.put(path, text.as_bytes(), path).await?;
    }
    for (path, text) in INSTRUCTIONS {
        fixture.put(path, text.as_bytes(), path).await?;
    }
    let discovered = fixture
        .declaration()
        .capture(fixture.reader.as_ref())
        .await?;
    let mut manifest = ContextManifest::new(discovered, vec!["inspect".into()], &tools()?)?;
    manifest.tools.retain(|tool| tool.name == "write");
    Ok(manifest)
}

async fn assert_request(
    provider: &ScriptedModelProvider,
    manifest: &ContextManifest,
    reader: &dyn ContentResidencyVerifier,
) -> Result<()> {
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    let request = requests
        .first()
        .ok_or_else(|| Error::NotFound("model request".into()))?;
    assert_eq!(request.tools, manifest.tools);
    let conventions: Vec<String> =
        serde_json::from_str(include_str!("../../src/context/instructions.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(manifest.conventions, conventions);
    assert!(conventions[1].contains("plans/<id>.md"));
    for (message, expected) in request.messages.iter().zip(&conventions) {
        assert_eq!(message.role, ModelRole::System);
        assert_eq!(message.content, ModelContent::Text(expected.clone()));
    }
    let text = serde_json::to_string(request).map_err(|e| Error::Invalid(e.to_string()))?;
    assert!(text.contains("SELECTED BODY"));
    assert!(!text.contains("UNSELECTED BODY"));
    assert!(!text.contains("replacement"));
    for ((message, pinned), (path, expected)) in request
        .messages
        .iter()
        .skip(manifest.conventions.len())
        .zip(&manifest.discovery.instructions)
        .zip(INSTRUCTIONS)
    {
        assert_eq!(pinned.path(), path);
        assert_eq!(message.role, ModelRole::User);
        assert_eq!(
            message.content.file_refs().collect::<Vec<_>>(),
            vec![&**pinned]
        );
        assert_eq!(
            reader.read(pinned, 32_768, None)?.await?.as_ref(),
            expected.as_bytes()
        );
    }
    Ok(())
}

#[tokio::test]
async fn pinned_manifest_drives_scripted_requests_and_restart_without_wrapper_state() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let manifest = seed(&fixture).await?;
    let limits = Limits {
        render_bytes: 32_768,
        file_bytes: 32_768,
        context_messages: 32,
        ..Default::default()
    };
    let (publisher, reader, private, scope) = storage(&fixture).await?;
    let reference = manifest
        .publish(
            publisher.as_ref(),
            OperationId::new(),
            "context/1/manifest.json",
            limits,
        )
        .await?;
    let stored = reader.read(&reference, 32_768, None)?.await?;
    assert_eq!(ContextManifest::decode(&stored, limits)?, manifest);
    assert!(!String::from_utf8_lossy(&stored).contains("SELECTED BODY"));
    let replacement = ContextManifest::default()
        .publish(
            publisher.as_ref(),
            OperationId::new(),
            "context/1/manifest.json",
            limits,
        )
        .await?;
    assert_ne!(replacement, reference);
    fixture
        .put("AGENTS.md", b"replacement instructions", "replace")
        .await?;
    fixture
        .put("skills/a/SKILL.md", b"replacement body", "replace-skill")
        .await?;

    let counted = CountingReader {
        inner: reader.as_ref(),
        reads: AtomicUsize::new(0),
    };
    let (selected, stage) =
        ContextManifest::bind(reference.clone(), &counted, tools()?, limits).await?;
    assert_eq!(counted.reads.load(Ordering::Relaxed), 1);
    assert_eq!(selected.definitions(None)?, manifest.tools);
    let input = ContextInput {
        input: ModelContent::Text("inspect".into()),
        selected_context: None,
        step: 0,
        prior_messages: Vec::new(),
    };
    let rendered = ContextPipeline::new([stage])
        .run(&input, limits, None)
        .await?;
    assert_eq!(rendered.metadata.get("context.manifest"), Some(&reference));
    assert_eq!(
        rendered.current_input_index,
        Some(u32::try_from(6 + manifest.conventions.len()).expect("bounded message count"))
    );
    assert_eq!(
        rendered.messages.last().map(|message| message.role),
        Some(ModelRole::User)
    );
    assert_eq!(counted.reads.load(Ordering::Relaxed), 1);

    let provider = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Text(
        "Done".into(),
    )]));
    let executor = stock(provider.clone(), tools()?, limits)?
        .with_context_manifest(reference.clone(), reader.clone())
        .await?;
    assert!(
        executor
            .clone()
            .with_context_manifest(reference.clone(), reader.clone())
            .await
            .is_err()
    );
    let stream = Arc::new(MemoryStream::default());
    let journal = Arc::new(
        fixture
            .journal(stream.clone(), private.clone(), scope.clone())?
            .with_input_verifier(reader.clone()),
    );
    acyclic_harness::mcp::callbacks::McpRuntimeCallbacks::new(
        executor.clone(),
        journal.clone(),
        "sampling".into(),
        ModelContent::Text("continue".into()),
        None,
        None,
    )?;
    let turn = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("inspect".into()),
        selected_context: None,
        max_steps: 1,
    };
    let first = executor.model_step(journal.as_ref(), &turn, 0, &[]).await?;
    assert_request(provider.as_ref(), &manifest, reader.as_ref()).await?;
    let restarted = stock(provider.clone(), tools()?, limits)?
        .with_context_manifest(reference, reader.clone())
        .await?;
    let recovered = fixture
        .journal(stream, private, scope)?
        .with_input_verifier(reader);
    assert_eq!(
        first,
        restarted.model_step(&recovered, &turn, 0, &[]).await?
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

#[tokio::test]
async fn skill_order_is_explicit_and_cached_and_empty_tools_allow_sampling() -> Result<()> {
    let fixture = Fixture::new().await?;
    let mut manifest = seed(&fixture).await?;
    manifest.selected_skills = vec!["hidden".into(), "inspect".into()];
    manifest.tools.clear();
    let limits = Limits {
        render_bytes: 32_768,
        file_bytes: 32_768,
        context_messages: 32,
        ..Default::default()
    };
    let (publisher, reader, private, scope) = storage(&fixture).await?;
    let reference = manifest
        .publish(
            publisher.as_ref(),
            OperationId::new(),
            "context/order.json",
            limits,
        )
        .await?;
    let counted = CountingReader {
        inner: reader.as_ref(),
        reads: AtomicUsize::new(0),
    };
    let (selected, stage) =
        ContextManifest::bind(reference.clone(), &counted, tools()?, limits).await?;
    let pipeline = ContextPipeline::new([stage.clone()]);
    for step in 0..2 {
        let context = pipeline
            .run(
                &ContextInput {
                    input: ModelContent::Text("inspect".into()),
                    selected_context: None,
                    step,
                    prior_messages: Vec::new(),
                },
                limits,
                None,
            )
            .await?;
        let offset = manifest.conventions.len();
        for (message, name) in context.messages[5 + offset..7 + offset]
            .iter()
            .zip(["hidden", "inspect"])
        {
            let ModelContent::Text(text) = &message.content else {
                return Err(Error::Invalid("expected cached skill text".into()));
            };
            assert!(text.contains(&format!("name: {name}\n")));
        }
        assert_eq!(
            context.current_input_index,
            Some(u32::try_from(7 + offset).expect("bounded message count"))
        );
        assert_eq!(counted.reads.load(Ordering::Relaxed), 2);
    }
    assert!(
        ContextManifest::bind(
            reference,
            &counted,
            tools()?,
            Limits {
                context_messages: 1,
                ..limits
            }
        )
        .await
        .is_err()
    );
    assert_eq!(counted.reads.load(Ordering::Relaxed), 2);
    let executor = stock(Arc::new(ScriptedModelProvider::new([])), selected, limits)?
        .with_context_stage(stage)?;
    acyclic_harness::mcp::callbacks::McpRuntimeCallbacks::new(
        executor,
        Arc::new(fixture.journal(Arc::new(MemoryStream::default()), private, scope)?),
        "sampling".into(),
        ModelContent::Text("continue".into()),
        None,
        None,
    )?;
    Ok(())
}

#[tokio::test]
async fn skill_path_bounds_reject_before_any_body_read_at_a_short_manifest_path() -> Result<()> {
    let fixture = Fixture::new().await?;
    for (path, name) in [
        ("skills/a/SKILL.md", "first"),
        ("skills/long-name/SKILL.md", "inspect"),
    ] {
        fixture
            .put(
                path,
                format!("---\nname: {name}\ndescription: Inspect files\n---\nbody").as_bytes(),
                name,
            )
            .await?;
    }
    let mut declaration = fixture.declaration();
    declaration.instructions.clear();
    let manifest = ContextManifest::new(
        declaration.capture(fixture.reader.as_ref()).await?,
        vec!["first".into(), "inspect".into()],
        &ToolRegistry::default(),
    )?;
    let limits = Limits {
        render_bytes: 32_768,
        file_bytes: 32_768,
        ..Default::default()
    };
    let (publisher, reader, _, _) = storage(&fixture).await?;
    let reference = manifest
        .publish(publisher.as_ref(), OperationId::new(), "m", limits)
        .await?;
    let counted = CountingReader {
        inner: reader.as_ref(),
        reads: AtomicUsize::new(0),
    };
    assert!(matches!(
        ContextManifest::bind(
            reference.clone(),
            &counted,
            ToolRegistry::default(),
            Limits { path_bytes: 16, ..limits },
        ).await,
        Err(Error::Invalid(message)) if message == "manifest skill path exceeds path bound"
    ));
    assert_eq!(counted.reads.load(Ordering::Relaxed), 0);
    ContextManifest::bind(
        reference,
        &counted,
        ToolRegistry::default(),
        Limits {
            path_bytes: "skills/long-name/SKILL.md".len(),
            ..limits
        },
    )
    .await?;
    assert_eq!(counted.reads.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn invalid_tool_bindings_and_bounds_fail_before_skill_reads() -> Result<()> {
    let fixture = Fixture::new().await?;
    fixture
        .put(
            "skills/a/SKILL.md",
            b"---\nname: inspect\ndescription: Inspect files\n---\nbody",
            "skill",
        )
        .await?;
    let mut declaration = fixture.declaration();
    declaration.instructions.clear();
    let base = ContextManifest::new(
        declaration.capture(fixture.reader.as_ref()).await?,
        vec!["inspect".into()],
        &tools()?,
    )?;
    let limits = Limits {
        render_bytes: 32_768,
        file_bytes: 32_768,
        context_messages: 32,
        ..Default::default()
    };
    let (publisher, reader, _, _) = storage(&fixture).await?;
    for bound in [
        Limits {
            file_bytes: 1,
            ..limits
        },
        Limits {
            path_bytes: 1,
            ..limits
        },
    ] {
        assert!(
            base.publish(
                publisher.as_ref(),
                OperationId::new(),
                "context/oversized.json",
                bound
            )
            .await
            .is_err()
        );
    }
    for case in [
        "input_schema",
        "output_schema",
        "projection_schema",
        "description",
        "revision",
        "name",
    ] {
        let mut manifest = base.clone();
        match case {
            "input_schema" => manifest.tools[0].input_schema["title"] = "changed".into(),
            "output_schema" => manifest.tools[0].output_schema["title"] = "changed".into(),
            "projection_schema" => manifest.tools[0].projection_schema["title"] = "changed".into(),
            "description" => manifest.tools[0].description.push_str(" changed"),
            "revision" => manifest.tools[0].revision.push('2'),
            _ => manifest.tools[0].name = "missing".into(),
        }
        let reference = manifest
            .publish(
                publisher.as_ref(),
                OperationId::new(),
                &format!("context/{case}.json"),
                limits,
            )
            .await?;
        let counted = CountingReader {
            inner: reader.as_ref(),
            reads: AtomicUsize::new(0),
        };
        assert!(
            ContextManifest::bind(reference, &counted, tools()?, limits)
                .await
                .is_err()
        );
        assert_eq!(counted.reads.load(Ordering::Relaxed), 0);
    }
    let reference = base
        .publish(
            publisher.as_ref(),
            OperationId::new(),
            "context/valid.json",
            limits,
        )
        .await?;
    assert!(
        ContextManifest::bind(
            reference.clone(),
            reader.as_ref(),
            tools()?,
            Limits {
                render_bytes: 1,
                ..limits
            }
        )
        .await
        .is_err()
    );
    assert!(
        ContextManifest::bind(reference.clone(), fixture.reader.as_ref(), tools()?, limits)
            .await
            .is_err()
    );
    let mut duplicated = base.clone();
    duplicated.selected_skills.push("inspect".into());
    assert!(matches!(
        duplicated.validate(limits),
        Err(Error::Conflict(_))
    ));
    let mut absent = base.clone();
    absent.selected_skills[0] = "absent".into();
    assert!(matches!(absent.validate(limits), Err(Error::NotFound(_))));
    Ok(())
}
