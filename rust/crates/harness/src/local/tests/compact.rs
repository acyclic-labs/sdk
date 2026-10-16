use super::*;
use crate::runtime::RuntimeTask;

fn admitted(admission: Admission<RuntimeTask<crate::executor::TurnOutput>>) -> crate::TaskId {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("durable admission required"),
    }
}

#[tokio::test]
async fn manual_summary_recovers_source_command_and_mailbox_publication_cuts_without_recapture()
-> Result<()> {
    futures::future::try_join_all((0..3).flat_map(|cut| {
        [false, true]
            .into_iter()
            .map(move |after| publication_cut(cut, after))
    }))
    .await?;
    Ok(())
}

async fn publication_cut(cut: usize, after: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("late target answer".into()),
        ScriptedTurn::Text("retained summary".into()),
    ]);
    let config = || volume_config(directory.path(), project.clone(), provider.clone());
    let local = LocalHarness::open(config()?).await?;
    let target_operation = OperationId::new();
    local
        .retain_settings(target_operation, &json!({"model":"selected-summary"}))
        .await?;
    let target = admitted(local.submit(target_operation, "original prompt").await?);
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
    assert_eq!(request.model.name, "selected-summary");
    let requests = provider.requests().len();
    // Replaying the same request reuses its admitted summary task.
    assert_eq!(
        local
            .compact(operation, target, "keep the invariant")
            .await?["summary"],
        "retained summary"
    );
    assert_eq!(provider.requests().len(), requests);
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

#[tokio::test]
async fn manual_summary_reserves_actual_input_on_a_small_model() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let capacity = ModelContextCapacity {
        context_tokens: 8_192,
        output_tokens: 8_192,
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("small-model summary".into())])
            .with_context_capacity(capacity),
    );
    let local = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await?;
    let target = admitted(
        local
            .submit(OperationId::new(), &"source".repeat(200))
            .await?,
    );
    let operation = OperationId::new();
    assert_eq!(
        local.compact(operation, target, "keep facts").await?["summary"],
        "small-model summary"
    );
    let requests = provider.requests();
    let request = crate::model::PreparedModelRequest::prepare(
        requests[0].clone(),
        crate::conversation::Limits::default(),
        None,
    )?;
    let count = provider.count_tokens(&request).await?;
    let input = count.validate(&request, &mut None)?;
    let output = request.request().max_output_tokens.unwrap();
    assert!(output > 0);
    assert!(input + u64::from(output) <= u64::from(capacity.context_tokens));
    assert!(request.request().tools.is_empty());
    // The old ceiling leaves only two input tokens; the same admission
    // predicate must reject it for this exact captured request.
    assert!(input + 8_190 > u64::from(capacity.context_tokens));
    let requests_before = requests.len();
    drop(requests);
    local.compact(operation, target, "keep facts").await?;
    assert_eq!(provider.requests().len(), requests_before);
    Ok(())
}

proptest::proptest! {
    // Every accepted ceiling leaves room for the complete counted input;
    // rejection is exactly exhausted input capacity. Values shrink to boundaries.
    #[test]
    fn summary_budget_preserves_input_and_output_bounds(
        context in 1u32..1_000_000,
        ceiling in 1u32..1_000_000,
        input in 0u64..1_000_001,
    ) {
        let actual = super::super::compact::summary_output_budget(context, ceiling, input);
        if input >= u64::from(context) {
            proptest::prop_assert!(actual.is_err());
        } else {
            let output = actual.unwrap();
            proptest::prop_assert_eq!(u64::from(output), u64::from(ceiling).min(u64::from(context) - input));
            let fits = |output: u32| output > 0 && output <= ceiling
                && input + u64::from(output) <= u64::from(context);
            proptest::prop_assert!(fits(output));
            if u64::from(ceiling) > u64::from(context) - input {
                // Deliberately omit input reservation; the same detector must fail.
                proptest::prop_assert!(!fits(ceiling));
            }
        }
    }
}
