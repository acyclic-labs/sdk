//! A content disagreement travels through the served model/tool boundary.
use super::*;
use crate::model::{ModelContent, ModelEvent, ToolCall};
use crate::test_support::{ScriptedEvent, ScriptedTurn};
use acyclic_harness_client::{AgentCommand, AgentReply};

fn call(id: &str, name: &str, arguments: serde_json::Value) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            id.into(),
            name.into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::json!({}),
        }),
    ])
}

fn write(content: &str) -> ScriptedTurn {
    call(
        "write-note",
        "write",
        serde_json::json!({"path":"note.txt", "content":content}),
    )
}

async fn spawn(harness: &Arc<LocalHarness>, parent: AgentPath, name: &str) -> Result<AgentPath> {
    let connection = harness.connect().await?;
    let AgentReply::Spawned { agent, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent,
            id: crate::agent::IntentId::try_from(name).map_err(super::agent)?,
            brief: ModelContent::Text("Edit the note in your fork.".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("repair worker did not open".into()));
    };
    let turns = settled(harness, &agent).await?;
    let entry = ConversationEntry::decode(
        turns
            .last()
            .ok_or_else(|| Error::NotFound("repair worker's initial turn".into()))?,
    )
    .map_err(super::agent)?;
    assert_eq!(entry.outcome, crate::agent::TurnOutcome::Completed);
    Ok(agent)
}

#[tokio::test]
async fn served_agents_repair_same_line_conflicts_and_continue_the_session() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::agent)?;
    let provider = super::super::scripted([
        write("base\n"),
        ScriptedTurn::Text("base ready".into()),
        ScriptedTurn::Text("fork ready".into()),
    ]);
    let harness = Arc::new(
        super::super::open_scripted(
            super::super::checkout_config(
                directory.path(),
                (super::super::model()?, provider.clone()),
            )?
            .workspace_mounting(crate::local::WorkspaceMounting::Physical)
            .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let root = harness.connect().await?.root().clone();
    let second = spawn(&harness, root, "second").await?;
    let first = spawn(&harness, second.clone(), "first").await?;
    let mut second_conversation = ObservedConversation::observe(
        Conversation::open(harness.attach(Some(second.clone()), true).await?),
        None,
    )
    .await?;
    let mut first_conversation = ObservedConversation::observe(
        Conversation::open(harness.attach(Some(first), true).await?),
        None,
    )
    .await?;

    // Both edits start from the same settled fork cut. Scheduling is explicit:
    // no sleeps, model service, timing assumptions or concurrent script queue.
    provider.extend([
        write("second\n"),
        ScriptedTurn::Text("second edited".into()),
    ]);
    assert_eq!(
        second_conversation
            .send("second-edit", "Edit the note.")
            .await?
            .outcome,
        crate::agent::TurnOutcome::Completed
    );
    provider.extend([write("first\n"), ScriptedTurn::Text("first edited".into())]);
    assert_eq!(
        first_conversation
            .send("first-edit", "Edit the same line.")
            .await?
            .outcome,
        crate::agent::TurnOutcome::Completed
    );

    let before = provider.request_count();
    provider.extend([
        call(
            "unresolved",
            "fs",
            serde_json::json!({"command":"merge --continue"}),
        ),
        call("markers", "read", serde_json::json!({"path":"note.txt"})),
        write("first and second repaired\n"),
        call("add", "fs", serde_json::json!({"command":"add note.txt"})),
        call(
            "continue",
            "fs",
            serde_json::json!({"command":"merge --continue"}),
        ),
        call("status", "fs", serde_json::json!({"command":"status"})),
        ScriptedTurn::Text("repair complete".into()),
    ]);
    assert_eq!(
        second_conversation
            .send("repair", "Repair the merge and continue.")
            .await?
            .outcome,
        crate::agent::TurnOutcome::Completed
    );
    let request = provider
        .request(before + 2)
        .ok_or_else(|| Error::NotFound("model request after conflict and read".into()))?;
    let visible = super::super::request_text(&harness, &request).await?;
    let results = serde_json::to_string(&request.messages).map_err(super::agent)?;
    let conflict = request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } if call_id == "unresolved" => Some(content),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("model-visible merge conflict result".into()))?;
    let crate::model::ToolResultContent::Json { value } = conflict else {
        return Err(Error::Invalid(format!(
            "merge conflict lost its typed projection: {conflict:?}"
        )));
    };
    assert_eq!(value["kind"], "merge conflict");
    assert_eq!(value["paths"], serde_json::json!(["/note.txt"]));
    for marker in [
        "<<<<<<< ours",
        "second",
        "=======",
        "first",
        ">>>>>>> theirs",
    ] {
        assert!(
            visible.contains(marker) || results.contains(marker),
            "model did not see {marker}: {visible}; {results}"
        );
    }
    let finished = provider
        .request(before + 6)
        .ok_or_else(|| Error::NotFound("model request after repair".into()))?;
    let results = serde_json::to_string(&finished.messages).map_err(super::agent)?;
    for id in ["add", "continue", "status"] {
        let result = finished
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                crate::model::ModelContentPart::ToolResult {
                    call_id, content, ..
                } if call_id == id => Some(content),
                _ => None,
            })
            .ok_or_else(|| Error::NotFound(format!("{id} result")))?;
        let crate::model::ToolResultContent::Json { value } = result else {
            return Err(Error::Invalid(format!("{id} failed: {result:?}")));
        };
        assert!(value.get("error").is_none(), "{id}: {value}; {results}");
    }
    // A fresh served connection and another turn prove repair did not end the
    // owner or leave the agent stuck behind its original pending join.
    provider.extend([ScriptedTurn::Text("still serving".into())]);
    assert_eq!(
        second_conversation
            .send("after-repair", "Continue.")
            .await?
            .outcome,
        crate::agent::TurnOutcome::Completed
    );
    harness.connect().await?;
    Ok(())
}
