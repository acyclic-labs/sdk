use super::*;
use crate::agent::{AgentPath, AgentRecord as _, Phase};
use crate::client::{AgentCommand, AgentReply};
use crate::model::{ModelEvent, ToolCall};
use crate::test_support::ScriptedEvent;

fn call(name: &str, arguments: serde_json::Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            "question-call".into(),
            name.into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}

// These scenarios exercise question authority and replies, not capacity admission.
// Use the same disabled-compaction fixture policy as filesystem command tests.
fn question_config(
    directory: &Path,
    volume: VolumeRef,
    provider: Arc<dyn ModelProvider>,
) -> Result<LocalHarnessConfig> {
    Ok(volume_config(directory, volume, provider)?
        .workspace_mounting(crate::local::WorkspaceMounting::Physical)
        .compaction(crate::context::CompactionPolicy::Disabled))
}
const ASK: &str = "Choose the color\n```input choose id=color\n- blue\n- green\n```\n";

#[tokio::test]
async fn headless_question_plugin_has_no_declaration_and_file_writes_cannot_ask() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let provider = scripted([
        call(
            "write",
            json!({"path":"volume:///private/ui/color.md","content":ASK,"ifVersion":"absent"}),
        ),
        ScriptedTurn::Text("I chose blue within the given authority.".into()),
    ]);
    let harness = Arc::new(
        open_scripted(question_config(
            directory.path(),
            volume(directory.path()).await?,
            provider.clone(),
        )?)
        .await?,
    );
    let mut observed = ObservedConversation::open(&harness).await?;
    let entry = observed.send("headless", "Choose a color").await?;
    assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
    let requests = provider.requests();
    assert!(!requests.is_empty());
    assert!(
        requests
            .iter()
            .all(|request| request.tools.iter().all(|tool| tool.name != "ask_question"))
    );
    let refusal = request_text(&harness, &requests[1]).await?;
    assert!(
        refusal.contains("only the orchestrator role may ask the human"),
        "{refusal}"
    );
    assert!(!matches!(
        observed.conversation.phase().await?,
        Phase::WaitingUser { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn interactive_orchestrator_asks_directly_and_receives_a_generation_bound_reply() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let provider = scripted([
        call(
            "ask_question",
            json!({"path":"volume:///private/ui/color.md","content":ASK}),
        ),
        ScriptedTurn::Text("Awaiting your color.".into()),
        ScriptedTurn::Text("Blue selected.".into()),
    ]);
    let harness = Arc::new(
        open_scripted(
            question_config(
                directory.path(),
                volume(directory.path()).await?,
                provider.clone(),
            )?
            .question_plugin(true),
        )
        .await?,
    );
    let mut observed = ObservedConversation::open(&harness).await?;
    observed.send("interactive", "Choose a color").await?;
    assert!(matches!(
        observed.conversation.phase().await?,
        Phase::WaitingUser { .. }
    ));
    assert!(
        provider.requests()[0]
            .tools
            .iter()
            .any(|tool| tool.name == "ask_question")
    );
    let snapshot = observed
        .conversation
        .agent_files(vec!["ui/color.md".into()], None)
        .await?;
    let file = snapshot.files["ui/color.md"]
        .as_ref()
        .expect("published ask");
    let command = crate::client::ui::Reply {
        path: crate::client::ui::UiPath::new("color.md")
            .map_err(|e| Error::Invalid(e.to_string()))?,
        generation: file.file.version().into(),
        answers: Default::default(),
        text: "blue".into(),
    }
    .command(
        observed.conversation.agent().clone(),
        crate::agent::IntentId::try_from("color-reply")
            .map_err(|e| Error::Invalid(e.to_string()))?,
        &crate::client::ui::blocks::parse(&file.text).map_err(|e| Error::Invalid(e.to_string()))?,
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    observed.conversation.connection().execute(&command).await?;
    super::agents::settled(&harness, observed.conversation.agent()).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    let reply = request_text(&harness, &requests[2]).await?;
    assert!(
        reply.contains("color.md") && reply.contains(file.file.version()) && reply.contains("blue")
    );
    Ok(())
}

#[tokio::test]
async fn nested_orchestrator_keeps_question_authority_and_worker_attempt_is_a_tool_failure()
-> Result<()> {
    async fn requests_for_agent(
        harness: &Arc<LocalHarness>,
        provider: &crate::test_support::ScriptedModelProvider,
        agent: &AgentPath,
    ) -> Result<Vec<crate::model::ModelRequest>> {
        let entries = super::agents::settled(harness, agent).await?;
        let entry = crate::agent::ConversationEntry::decode(
            entries
                .last()
                .ok_or_else(|| Error::Invalid("agent did not settle".into()))?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let (agents, _) = harness.agents().await?;
        let state = agents
            .state(agent)
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let operation = crate::agent::turn_id(&state, &entry.input)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(provider
            .requests()
            .into_iter()
            .zip(provider.dispatches())
            .filter_map(|(request, dispatch)| {
                (dispatch.operation_id == operation).then_some(request)
            })
            .collect())
    }
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let provider = scripted([
        call(
            "ask_question",
            json!({"path":"volume:///private/ui/color.md","content":ASK}),
        ),
        ScriptedTurn::Text("Worker reports ambiguity to its parent.".into()),
        ScriptedTurn::Text("Parent received worker result.".into()),
        ScriptedTurn::Text("Nested orchestrator ready.".into()),
        ScriptedTurn::Text("Parent received orchestrator result.".into()),
    ]);
    let config = question_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .question_plugin(true);
    let harness = Arc::new(open_scripted(config).await?);
    let connection = harness.connect().await?;
    let root = connection.root().clone();
    let spawn = |parent: AgentPath, id: &str, role: &str| -> Result<AgentCommand> {
        Ok(AgentCommand::Spawn {
            parent,
            id: crate::agent::IntentId::try_from(id).map_err(|e| Error::Invalid(e.to_string()))?,
            brief: crate::model::ModelContent::Text(id.into()),
            role: role.into(),
        })
    };
    let AgentReply::Spawned { agent: worker, .. } = connection
        .execute(&spawn(root.clone(), "worker", "worker")?)
        .await?
    else {
        return Err(Error::Invalid("worker spawn".into()));
    };
    let requests = requests_for_agent(&harness, &provider, &worker).await?;
    // Child results wake their parent through the ordinary inbox. Settle that
    // input before spawning again, rather than sharing a racing script cursor.
    super::agents::settled(&harness, &root).await?;
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.tools.iter().all(|tool| tool.name != "ask_question"))
    );
    let mut transcript = String::new();
    for request in &requests {
        transcript.push_str(&request_text(&harness, request).await?);
    }
    assert!(transcript.contains("tool execution failed"), "{transcript}");
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&spawn(root, "orchestrator", "orchestrator")?)
        .await?
    else {
        return Err(Error::Invalid("orchestrator spawn".into()));
    };
    let requests = requests_for_agent(&harness, &provider, &child).await?;
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == "ask_question")
    );
    Ok(())
}
#[tokio::test]
async fn sessions_pin_question_choice_independently_of_the_serving_workspace() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let provider = scripted(
        ["headless", "interactive", "headless again"].map(|text| ScriptedTurn::Text(text.into())),
    );
    let harness = Arc::new(
        open_scripted(question_config(
            directory.path(),
            volume(directory.path()).await?,
            provider.clone(),
        )?)
        .await?,
    );
    let directory_connection = harness.connect_sessions().await?;
    let headless =
        crate::local::sessions::create(&directory_connection, "headless-session", Some(false))
            .await?;
    let interactive =
        crate::local::sessions::create(&directory_connection, "interactive-session", Some(true))
            .await?;
    let mut headless = ObservedConversation::observe(Conversation::open(headless), None).await?;
    let mut interactive =
        ObservedConversation::observe(Conversation::open(interactive), None).await?;
    headless
        .send("first", "choose within your authority")
        .await?;
    interactive
        .send("second", "a user surface is attached")
        .await?;
    let reattached = headless
        .conversation
        .select(headless.conversation.agent().clone())
        .await?;
    let mut reattached = ObservedConversation::observe(reattached, None).await?;
    reattached.send("third", "a user attached later").await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    for (request, expected) in requests.iter().zip([false, true, false]) {
        assert_eq!(
            request.tools.iter().any(|tool| tool.name == "ask_question"),
            expected
        );
    }
    Ok(())
}

#[tokio::test]
async fn question_tool_cannot_publish_workspace_files() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let paths = ["approved.txt", ".volumes/private/ui/color.md"];
    let provider = scripted(paths.into_iter().flat_map(|path| {
        [
            call(
                "ask_question",
                json!({"path":path, "content":"must not publish"}),
            ),
            ScriptedTurn::Text("The question tool rejected a workspace write.".into()),
        ]
    }));
    let harness = Arc::new(
        open_scripted(
            question_config(
                directory.path(),
                volume(directory.path()).await?,
                provider.clone(),
            )?
            .question_plugin(true),
        )
        .await?,
    );
    let mut observed = ObservedConversation::open(&harness).await?;
    for (index, _) in paths.iter().enumerate() {
        observed
            .send(
                &format!("restricted-{index}"),
                "try a workspace write through question",
            )
            .await?;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2 * (index + 1));
        let refusal = request_text(&harness, &requests[2 * index + 1]).await?;
        assert!(
            refusal.contains("ask_question requires a private ui/ file"),
            "{refusal}"
        );
        assert!(!matches!(
            observed.conversation.phase().await?,
            Phase::WaitingUser { .. }
        ));
        let private = observed
            .conversation
            .agent_files(vec!["ui/color.md".into()], None)
            .await?;
        assert!(private.files["ui/color.md"].is_none());
    }
    Ok(())
}
