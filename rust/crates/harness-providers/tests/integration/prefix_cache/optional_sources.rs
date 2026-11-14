//! Pinned execution inputs never become unsolicited provider context.
use super::*;
use acyclic_harness::{
    context::{ContextInput, ContextManifest, ContextPipeline, EnvironmentFacts},
    model::{ModelDispatch, ModelProvider as _, ModelRequest, ModelRole, PreparedModelRequest},
    tool::ToolRegistry,
};
use futures::TryStreamExt as _;

#[tokio::test]
async fn optional_source_versions_preserve_provider_requests() -> TestResult {
    assert_eq!(
        serde_json::to_vec(&acyclic_harness::preset::Preset::graphcoder())?.len(),
        115,
        "update the reviewed preset byte pin"
    );
    for (adapter, path) in [
        (Adapter::Chat, "chat/completions"),
        (Adapter::Responses, "responses"),
        (Adapter::Anthropic, "messages"),
        (Adapter::Gemini, "models/model:streamGenerateContent"),
    ] {
        let transport = Arc::new(Scripted {
            path,
            state: Mutex::new(("parent", VecDeque::new(), Vec::new())),
            cache: Mutex::new(harness_mock_model::PromptCache::default().with_implicit_chat()),
        });
        let provider = resolve(adapter, PromptCache::Native, transport.clone())?;
        let mut baseline = None;
        for source in [
            "ui/index.md",
            "approvals/pending.json",
            "config/settings.json",
            "sessions/index.json",
            "evals/report.json",
        ] {
            for body in [
                None,
                Some("OPTIONAL_SOURCE_OFF"),
                Some("OPTIONAL_SOURCE_ON"),
            ] {
                let storage = acyclic_harness::filesystem::MemoryHarnessStorage::new(
                    acyclic_harness::AgentId::new(),
                    1 << 20,
                )
                .await?;
                let bindings = storage.content_bindings();
                let tools = ToolRegistry::default();
                let mut manifest = ContextManifest::new(Default::default(), Vec::new(), &tools)?;
                manifest.conventions.push(
                    std::str::from_utf8(acyclic_harness::seed::file("AGENT.md").unwrap())?.into(),
                );
                manifest.environment = Some(EnvironmentFacts {
                    cwd: Some("/project".into()),
                    shell: "sh".into(),
                    os: "test".into(),
                    current_date: "2026-01-01".into(),
                    timezone: "UTC".into(),
                    ..Default::default()
                });
                if let Some(body) = body {
                    manifest.sources.push(
                        storage
                            .stage(
                                acyclic_harness::OperationId::new(),
                                source,
                                body.as_bytes(),
                                "text/plain",
                                source,
                            )
                            .await?,
                    );
                }
                let reference = manifest
                    .publish(
                        bindings.writer.as_deref().unwrap(),
                        acyclic_harness::OperationId::new(),
                        "manifest.json",
                        Limits::default(),
                    )
                    .await?;
                let (_, stage) = ContextManifest::bind(
                    reference,
                    bindings.reader.as_ref(),
                    bindings.writer.as_deref(),
                    tools,
                    Limits::default(),
                )
                .await?;
                let context = ContextPipeline::new([stage])
                    .run(
                        &ContextInput {
                            input: ModelContent::Text("Solve the current task.".into()),
                            selected_context: None,
                            step: 0,
                            prior_messages: Vec::new(),
                        },
                        Limits::default(),
                        None,
                    )
                    .await?;
                assert_eq!(
                    context
                        .messages
                        .iter()
                        .filter(|message| message.role == ModelRole::System)
                        .count(),
                    1
                );
                let prepared = PreparedModelRequest::prepare(
                    ModelRequest {
                        summary: false,
                        compose_tools: false,
                        model: provider.model().clone(),
                        messages: context.messages,
                        tools: Vec::new(),
                        sampling: Default::default(),
                    },
                    Limits::default(),
                    None,
                )?;
                let dispatch = ModelDispatch {
                    operation_id: acyclic_harness::OperationId::from_bytes([42; 16]),
                    step: 0,
                    request_digest: prepared.request_digest(),
                };
                let _: Vec<_> = provider.generate(&prepared, dispatch).try_collect().await?;
                let wire = transport
                    .state
                    .lock()
                    .unwrap()
                    .2
                    .last()
                    .unwrap()
                    .body
                    .clone();
                assert!(holds(&wire, b"/project"), "environment was not projected");
                for forbidden in [b"OPTIONAL_SOURCE_".as_slice(), b"<model>", b"<git_status>"] {
                    assert!(
                        !holds(&wire, forbidden),
                        "{adapter:?}: optional {source} entered provider context"
                    );
                }
                if let Some((digest, expected)) = &baseline {
                    assert_eq!(
                        &prepared.request_digest(),
                        digest,
                        "{source}: request identity changed"
                    );
                    assert_eq!(
                        &wire, expected,
                        "{adapter:?}: {source} rewrote provider bytes"
                    );
                } else {
                    baseline = Some((prepared.request_digest(), wire));
                }
            }
        }
    }
    Ok(())
}
