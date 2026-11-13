//! Identical manifests, different physical cwd, growing
//! conversations and two forks from one pinned parent, through every encoder.
use super::*;
use acyclic_harness::{
    context::{Context, ContextInput, ContextManifest, ContextPipeline, EnvironmentFacts},
    model::{
        ModelDispatch, ModelMessage, ModelPrefix, ModelProvider as _, ModelRequest, ModelRole,
        PreparedModelRequest,
    },
};
use futures::TryStreamExt as _;

#[tokio::test]
async fn binding_facts_follow_shared_bytes_across_turns_and_forks() -> TestResult {
    const TURNS: usize = 12;
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
        let storage = acyclic_harness::filesystem::MemoryHarnessStorage::new(
            acyclic_harness::AgentId::from_bytes([96; 16]),
            1 << 20,
        )
        .await?;
        let bindings = storage.content_bindings();
        let tools = acyclic_harness::tools::registry(
            &[],
            |_| false,
            vec![super::super::providers::echo_tool("cached.echo")],
        )?;
        let limits = Limits::default();
        let mut manifest = ContextManifest::new(Default::default(), Vec::new(), &tools)?;
        manifest
            .conventions
            .push("Shared project instructions and catalog. ".repeat(256));
        let input = |text: String| ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(text),
        };
        let answer = || ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text("ok".into()),
        };
        let request = |messages, tools| ModelRequest {
            summary: false,
            compose_tools: false,
            model: provider.model().clone(),
            messages,
            tools,
            sampling: Default::default(),
        };
        let mut inherited = None;
        for agent in 0..3_u8 {
            manifest.environment = Some(EnvironmentFacts {
                cwd: Some(format!("/workspace/.volumes/agents/.physical/{agent:032x}")),
                shell: "bash".into(),
                os: "linux".into(),
                current_date: "2026-11-11".into(),
                timezone: "UTC".into(),
                ..Default::default()
            });
            let reference = manifest
                .publish(
                    bindings.writer.as_deref().unwrap(),
                    acyclic_harness::OperationId::from_bytes([agent + 1; 16]),
                    &format!("manifests/{agent}.json"),
                    limits,
                )
                .await?;
            let (bound_tools, stage) =
                ContextManifest::bind(reference, bindings.reader.as_ref(), tools.clone(), limits)
                    .await?;
            let pipeline = ContextPipeline::new([stage]);
            let mut history = Vec::new();
            let mut previous: Option<Bytes> = None;
            let mut final_request = None;
            for turn in 0..TURNS {
                let current = format!("agent {agent}, turn {turn}");
                let mut messages = history.clone();
                messages.push(input(current.clone()));
                let context = pipeline
                    .transform(
                        &ContextInput {
                            input: ModelContent::Text(current.clone()),
                            selected_context: None,
                            step: 0,
                            prior_messages: history.clone(),
                        },
                        Context {
                            current_input_index: Some(history.len() as u32),
                            messages,
                            ..Default::default()
                        },
                        limits,
                        None,
                    )
                    .await?;
                let mut messages = context.messages;
                let environment = messages
                    .iter()
                    .position(|m| {
                        matches!(&m.content,
                    ModelContent::Text(text) if text.starts_with("<environment_context>"))
                    })
                    .unwrap();
                assert!(
                    environment > 0,
                    "binding facts preceded reusable instructions"
                );
                assert_eq!(
                    messages[environment].role,
                    ModelRole::User,
                    "binding facts must not be hoisted into a provider system header"
                );
                if agent != 0 {
                    // Only the fork's new facts and growing local history are deltas;
                    // its inherited instruction bytes remain in the pinned parent.
                    messages.drain(..environment);
                }
                let local = request(messages, bound_tools.definitions(None)?);
                let prepared = match &inherited {
                    Some(prefix) => {
                        PreparedModelRequest::inherit(
                            local,
                            prefix,
                            bindings.reader.as_ref(),
                            limits,
                            None,
                        )
                        .await?
                    }
                    None => PreparedModelRequest::prepare(local, limits, None)?,
                };
                let dispatch = ModelDispatch {
                    operation_id: acyclic_harness::OperationId::from_bytes([agent + 1; 16]),
                    step: turn as u32,
                    request_digest: prepared.request_digest(),
                };
                let _: Vec<_> = provider.generate(&prepared, dispatch).try_collect().await?;
                let body = transport
                    .state
                    .lock()
                    .unwrap()
                    .2
                    .last()
                    .unwrap()
                    .body
                    .clone();
                if let Some(previous) = &previous {
                    extends("binding facts", "next turn", previous, &body)?;
                }
                if agent != 0 && turn == 0 {
                    let sent = &transport.state.lock().unwrap().2;
                    extends("binding facts", "fork", &sent[TURNS - 1].body, &body)?;
                }
                previous = Some(body);
                final_request = Some(prepared);
                history.extend([input(current), answer()]);
            }
            if agent == 0 {
                let mut parent = final_request.unwrap().request().clone();
                parent.messages.push(answer());
                let parent = PreparedModelRequest::prepare(parent, limits, None)?;
                let bytes = ModelPrefix::select(&parent, None)?.canonical_bytes(None)?;
                inherited = Some(
                    storage
                        .stage(
                            acyclic_harness::OperationId::from_bytes([99; 16]),
                            "prefix.json",
                            &bytes,
                            ModelPrefix::MEDIA_TYPE,
                            "prefix.json",
                        )
                        .await?,
                );
            }
        }
        assert_eq!(transport.state.lock().unwrap().2.len(), 3 * TURNS);
    }
    Ok(())
}
