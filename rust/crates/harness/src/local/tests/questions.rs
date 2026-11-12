use super::*;
use crate::agent::{AgentPath, Phase};
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
        LocalHarness::open(volume_config(
            directory.path(),
            volume(directory.path()).await?,
            provider.clone(),
        )?)
        .await?,
    );
    let mut observed = ObservedConversation::open(&harness).await?;
    observed.send("headless", "Choose a color").await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.tools.iter().all(|tool| tool.name != "ask_question"))
    );
    assert!(
        request_text(&harness, &requests[1])
            .await?
            .contains("only the orchestrator role may ask the human")
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
        LocalHarness::open(
            volume_config(
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
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let provider = scripted([
        call(
            "ask_question",
            json!({"path":"volume:///private/ui/color.md","content":ASK}),
        ),
        ScriptedTurn::Text("Worker reports ambiguity to its parent.".into()),
        ScriptedTurn::Text("Nested orchestrator ready.".into()),
    ]);
    let config = volume_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .question_plugin(true);
    let harness = Arc::new(LocalHarness::open(config).await?);
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
    super::agents::settled(&harness, &worker).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.tools.iter().all(|tool| tool.name != "ask_question"))
    );
    let transcript = request_text(&harness, &requests[1]).await?;
    assert!(transcript.contains("tool execution failed"), "{transcript}");
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&spawn(root, "orchestrator", "orchestrator")?)
        .await?
    else {
        return Err(Error::Invalid("orchestrator spawn".into()));
    };
    super::agents::settled(&harness, &child).await?;
    let requests = provider.requests();
    assert!(
        requests[2]
            .tools
            .iter()
            .any(|tool| tool.name == "ask_question")
    );
    assert_eq!(requests[2].model, requests[0].model);
    Ok(())
}
#[tokio::test]
async fn sessions_pin_question_choice_independently_of_the_serving_workspace() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|e| Error::Storage(e.to_string()))?;
    let provider = scripted(
        ["headless", "interactive", "headless again"].map(|text| ScriptedTurn::Text(text.into())),
    );
    let harness = Arc::new(
        LocalHarness::open(volume_config(
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
        LocalHarness::open(
            volume_config(
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
        assert!(
            request_text(&harness, &requests[2 * index + 1])
                .await?
                .contains("ask_question requires a private ui/ file")
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
