use super::*;
use crate::runtime::RuntimeTask;

fn admitted(admission: Admission<RuntimeTask<crate::executor::TurnOutput>>) -> crate::TaskId {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("durable admission required"),
    }
}

#[tokio::test]
async fn manual_summary_recovers_every_publication_cut_and_never_recaptures_published_source()
-> Result<()> {
    for cut in 0..3 {
        for after in [false, true] {
            let directory = tempfile::tempdir().map_err(super::super::storage)?;
            let project = volume(directory.path()).await?;
            let provider = scripted([
                ScriptedTurn::Text("late target answer".into()),
                ScriptedTurn::Text("retained summary".into()),
            ]);
            let config = || volume_config(directory.path(), project.clone(), provider.clone());
            let local = LocalHarness::open(config()?).await?;
            let target = admitted(local.submit(OperationId::new(), "original prompt").await?);
            let operation = OperationId::new();
            let publisher = local.input_publisher()?;
            let interrupted = context::crash_cuts::Interrupted::new(publisher.as_ref(), cut, after);
            assert!(matches!(
                local.stage_summary_with(operation, target, "keep the invariant", &interrupted).await,
                Err(Error::Storage(message)) if message == "injected publication interruption"
            ));
            assert!(provider.requests().is_empty());
            // Change the source after the selected commit/ack boundary: execution
            // publishes an outcome and transcript that the earlier source lacked.
            assert_eq!(local.resume(target).await?.text, "late target answer");
            drop(interrupted);
            drop(publisher);
            drop(local);
            let local = LocalHarness::open(config()?).await?;
            assert_eq!(
                local
                    .compact(operation, target, "keep the invariant")
                    .await?["summary"],
                "retained summary"
            );
            let request = provider.requests().last().unwrap().clone();
            let source = request
                .messages
                .iter()
                .filter_map(|message| match &message.content {
                    crate::model::ModelContent::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(source.contains("original prompt"));
            assert!(source.contains("keep the invariant"));
            assert_eq!(
                source.contains("late target answer"),
                cut == 0 && !after,
                "recaptured a published source at cut {cut}, after={after}"
            );
            assert!(request.tools.is_empty());
            assert!(request.max_output_tokens.is_some());
            let requests = provider.requests().len();
            // Replaying the same request reuses its admitted summary task.
            assert_eq!(
                local
                    .compact(operation, target, "keep the invariant")
                    .await?["summary"],
                "retained summary"
            );
            assert_eq!(provider.requests().len(), requests);
        }
    }
    Ok(())
}

#[tokio::test]
async fn manual_summary_rejects_tool_calls_while_an_ordinary_turn_executes_them() -> Result<()> {
    for summary in [true, false] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider = scripted([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                    "attempt".into(),
                    "test.custom".into(),
                    json!({}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]),
            ScriptedTurn::Text("tool executed".into()),
        ]);
        let local = LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?.tool(tool("test.custom")),
        )
        .await?;
        let target = admitted(local.submit(OperationId::new(), "original prompt").await?);
        if summary {
            assert!(local.compact(OperationId::new(), target, "").await.is_err());
            assert_eq!(provider.requests().len(), 1);
            assert!(provider.requests()[0].tools.is_empty());
        } else {
            // The same malicious script is accepted by the coding loop, so the
            // detector would fail if /compact were routed through an ordinary turn.
            assert_eq!(local.resume(target).await?.text, "tool executed");
            assert_eq!(provider.requests().len(), 2);
            assert!(!provider.requests()[0].tools.is_empty());
        }
    }
    Ok(())
}
