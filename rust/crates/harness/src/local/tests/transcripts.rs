//! A live tree export must retain each agent's own original journal bytes.
use super::*;
use acyclic_harness_client::{AgentCommand, AgentReply, ExecutionPosition};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::TryStreamExt as _;
use serde_json::Value;

#[tokio::test]
async fn transcripts_follow_two_spawn_levels_and_match_original_journals() -> Result<()> {
    use crate::model::{ModelContent, ModelEvent, ToolCall};
    use crate::test_support::ScriptedEvent;

    let report = |count| {
        let mut events = Vec::new();
        for number in 0..count {
            events.push(ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                format!("feedback-{number}"),
                "write".into(),
                json!({
                    "path": crate::live::feedback::PATH,
                    "content": "{\"version\":1,\"kind\":\"slow\",\"what\":\"Waited for a result\",\"evidence\":\"shell step 3\"}"
                }),
            ))));
        }
        events.push(ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }));
        ScriptedTurn::Events(events)
    };
    let provider = scripted([
        ScriptedTurn::Text("root answer".into()),
        report(2),
        ScriptedTurn::Text("none".into()),
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "child-error".into(),
                "absent-tool".into(),
                json!({}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("child answer after error".into()),
        report(1),
        ScriptedTurn::Text("none".into()),
        ScriptedTurn::Text("grandchild answer".into()),
        report(1),
        ScriptedTurn::Text("none".into()),
    ]);
    let directory = tempfile::tempdir().map_err(crate::local::storage)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let conversation = Conversation::open(connection.clone());
    let root = connection.root().clone();
    let output = directory.path().join("transcripts");
    let run = async {
        conversation.send("root-input", "root task").await?;
        agents::settled(&harness, &root).await?;
        conversation.send("root-input", "root task").await?;
        let mut paths = vec![root.clone()];
        for name in ["child", "grandchild"] {
            let parent = paths.last().expect("root or child").clone();
            let AgentReply::Spawned { agent: child, .. } = connection
                .execute(&AgentCommand::Spawn {
                    parent,
                    id: crate::agent::IntentId::try_from(name).map_err(crate::local::storage)?,
                    brief: ModelContent::Text(format!("{name} task")),
                    role: "worker".into(),
                })
                .await?
            else {
                panic!("spawn did not return a child");
            };
            agents::settled(&harness, &child).await?;
            // Reopening either descendant for saved reads must keep the
            // session's root audience while enrolling the child's read grant.
            let saved = Conversation::open(connection.select(child.clone()).await?)
                .observation()
                .await?;
            let tail = saved.settled().await?;
            assert!(tail > 0, "the child committed a conversation");
            let entries = saved.entries(0, tail).await?;
            let entry = entries.last().expect("saved child conversation");
            assert!(!saved.output_text_at(tail - 1, entry).await?.is_empty());
            paths.push(child);
        }
        Ok::<_, Error>(paths)
    };
    let paths = conversation
        .with_transcripts(&output, "tree-export", run)
        .await??;
    assert_eq!(
        std::fs::read_dir(&output)
            .map_err(crate::local::storage)?
            .count(),
        paths.len() + 1,
        "every spawned agent needs a transcript"
    );
    let index = std::fs::read(output.join("agents.jsonl")).map_err(crate::local::storage)?;
    let index = serde_json::Deserializer::from_slice(&index)
        .into_iter::<Value>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(crate::local::storage)?;
    assert_eq!(
        index.len(),
        paths.len(),
        "one index row per discovered agent"
    );
    let mut usage = Vec::new();
    conversation.write_usage_jsonl(&mut usage).await?;
    let usage = serde_json::Deserializer::from_slice(&usage)
        .into_iter::<Value>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(crate::local::storage)?;
    let joins_usage = |index: &[Value]| {
        paths.iter().all(|path| {
            let mut mappings = index
                .iter()
                .filter(|row| row["id"] == path.id().to_string());
            let Some(mapping) = mappings.next() else {
                return false;
            };
            mappings.next().is_none()
                && usage.iter().any(|row| {
                    row["usage"]["agent_path"] == path.as_str()
                        && mapping["agent_path"] == row["usage"]["agent_path"]
                })
        })
    };
    assert!(
        joins_usage(&index),
        "every transcript must join its own usage"
    );
    // A valid but wrong agent path must fail even though it occurs in usage.
    let mut wrong = index.clone();
    wrong[1]["agent_path"] = index[0]["agent_path"].clone();
    assert!(
        !joins_usage(&wrong),
        "a child mapped to its parent must fail"
    );
    eprintln!("TRANSCRIPTS_IDENTITY_NEGATIVE_CONTROL=PASS (child mapped to parent rejected)");
    let inspections = conversation.inspect_tree();
    futures::pin_mut!(inspections);
    let mut aggregate = crate::executor::ToolUsage::default();
    for (path, mapping) in paths.into_iter().zip(index) {
        let original_usage = &usage
            .iter()
            .find(|row| row["usage"]["agent_path"] == path.as_str())
            .expect("each agent has an original model call")["usage"];
        assert_eq!(
            mapping,
            json!({
                "version": 1,
                "id": path.id(),
                "agent_path": path.as_str(),
                "parent": path.parent().map(|parent| parent.id()),
                "role": if path.parent().is_none() { "orchestrator" } else { "worker" },
                "model": format!("{}/{}", original_usage["provider"].as_str().unwrap(), original_usage["model"].as_str().unwrap()),
                "topology": null,
            })
        );
        let selected = Conversation::open(connection.select(path.clone()).await?);
        let mut inspection = inspections
            .try_next()
            .await?
            .expect("each tree member is inspected");
        assert_eq!(inspection.snapshot().agent, &path);
        while !inspection.snapshot().synchronized {
            inspection.advance().await?;
        }
        assert_eq!(
            inspection.snapshot().tools.feedback[&crate::live::feedback::Kind::Slow],
            if path.depth() == 0 { 2 } else { 1 },
            "live inspection counts only this agent's original reports"
        );
        aggregate = aggregate.merged(inspection.snapshot().tools)?;
        let bytes = std::fs::read(output.join(format!("{}.jsonl", path.id())))
            .map_err(crate::local::storage)?;
        let rows = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<Value>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(crate::local::storage)?;
        let mut rows = rows.iter();
        let mut position = ExecutionPosition::default();
        let mut calls = 0;
        let mut failed_tool = false;
        loop {
            let query = selected
                .execution_page(position.input, position.after, false)
                .await?;
            position = query.continuation().map_err(crate::local::storage)?;
            let Some(page) = query.page else {
                break;
            };
            for record in page.items {
                if matches!(
                    record.event,
                    crate::executor::ExecutionEvent::ToolCompleted {
                        record_kind: Some(crate::executor::ToolRecordKind::Feedback),
                        ..
                    }
                ) {
                    assert!(
                        record.committed_at_micros.is_some(),
                        "feedback time comes from its original stream record"
                    );
                }
                calls += usize::from(matches!(
                    record.event,
                    crate::executor::ExecutionEvent::ModelStarted { .. }
                ));
                failed_tool |= matches!(
                    record.event,
                    crate::executor::ExecutionEvent::ToolFailed { .. }
                );
                let row = rows.next().expect("every journal record exported");
                assert_eq!(row["version"], 1);
                assert_eq!(row["operation"], "tree-export");
                assert_eq!(row["agent"], path.as_str());
                assert_eq!(row["input"], query.input);
                assert_eq!(row["kind"], "execution");
                assert_eq!(
                    row["record"],
                    serde_json::to_value(&record).map_err(crate::local::storage)?
                );
                for (field, reference) in
                    crate::executor::ExecutionPayloadField::fields(&record.event)
                {
                    let mut payload = Vec::new();
                    loop {
                        let row = rows.next().expect("every payload chunk exported");
                        assert_eq!(row["kind"], "execution_payload");
                        assert_eq!(row["agent"], path.as_str());
                        assert_eq!(row["input"], query.input);
                        assert_eq!(row["sequence"], record.sequence);
                        assert_eq!(
                            row["field"],
                            serde_json::to_value(field).map_err(crate::local::storage)?
                        );
                        assert_eq!(row["offset"], payload.len() as u64);
                        payload.extend(
                            STANDARD
                                .decode(row["data"].as_str().expect("base64 payload"))
                                .map_err(crate::local::storage)?,
                        );
                        if payload.len() as u64 == reference.descriptor().byte_length() {
                            break;
                        }
                    }
                    let mut original = Vec::new();
                    loop {
                        let (bytes, length) = selected
                            .execution_payload(
                                query.input,
                                record.sequence,
                                field,
                                original.len() as u64,
                            )
                            .await?;
                        original.extend(bytes);
                        if original.len() as u64 == length {
                            break;
                        }
                    }
                    assert_eq!(
                        payload, original,
                        "payload differs from its original journal"
                    );
                }
            }
        }
        assert!(calls > 0, "agent's own model calls are absent");
        if path.depth() == 1 {
            assert!(failed_tool, "child's tool error is absent");
        }
        assert!(
            rows.next().is_none(),
            "inherited or duplicate calls exported"
        );
    }
    assert!(inspections.try_next().await?.is_none());
    assert_eq!(aggregate.feedback[&crate::live::feedback::Kind::Slow], 4);
    assert_eq!(provider.request_count(), 10);
    Ok(())
}
