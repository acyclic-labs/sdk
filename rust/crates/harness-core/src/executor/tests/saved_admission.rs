use super::*;

/// The saved disabled-compaction format predates mandatory request accounting.
async fn save_unaccounted_request(
    executor: &StockExecutor,
    journal: &Journal,
    input: &TurnInput,
) -> Result<crate::model::PreparedModelRequest> {
    let execution = StockExecution::ordinary(executor);
    let mut binding = json!({
        "executor": "acyclic.stock.v8",
        "input": input,
        "model": executor.model,
        "max_output_tokens": executor.max_output_tokens,
        "context": [],
        "tools": [],
        "limits": executor.limits,
        "tool_scope": [executor.tool_scope.grants(), executor.tool_scope.limits()],
        "policy": null,
        "inherited_prefix": null,
        "compaction": crate::context::CompactionPolicy::Disabled,
        "model_capacity": null,
    });
    let sampling = execution.sampling()?;
    if !sampling.is_default() {
        binding["sampling"] =
            serde_json::to_value(sampling).map_err(|error| Error::Invalid(error.to_string()))?;
    }
    journal
        .append(
            input.operation_id,
            "execution:started",
            &ExecutionEvent::Started {
                request_digest: preparation_digest(&binding, None)?,
            },
            None,
        )?
        .await?;
    let context_input = ContextInput::copy_for_turn(input, 0, &[], None)?;
    let base = ContextPipeline::base_context(&context_input, executor.limits, None)?;
    let (mut context, prepared) = execution
        .prepare_transformed_request(&context_input, base)
        .await?;
    context.messages = prepared.request().messages.clone();
    let projection = json!({
        "context": context,
        "steering": null,
        "canonical": null,
        "checkpoint": null,
    });
    let projection =
        stage_json(journal, input.operation_id, "context:0", &projection, None).await?;
    journal
        .append(
            input.operation_id,
            "context:0:prepared",
            &ExecutionEvent::ContextPrepared {
                step: 0,
                projection,
                steering: None,
            },
            None,
        )?
        .await?;
    Ok(prepared)
}

#[tokio::test]
async fn saved_unaccounted_projection_requires_admission_before_fresh_dispatch() -> Result<()> {
    for (length, fault, admitted) in [
        (10, None, true),
        (5_000, None, false),
        (10, Some(AccountingFault::Digest), false),
    ] {
        let journal = Journal::default();
        let (provider, counts) =
            counted_model(4_096, fault, [ScriptedTurn::Text("done".into())], []);
        let executor = uncompacted_executor(
            Model::new("test".into(), "saved".into(), "1".into())?,
            provider.clone(),
            ContextPipeline::default(),
            ToolRegistry::default(),
        )
        .with_max_output_tokens(1_024)?;
        let input = TurnInput {
            operation_id: OperationId::new(),
            max_steps: None,
            input: ModelContent::Text("x".repeat(length)),
            selected_context: None,
        };
        save_unaccounted_request(&executor, &journal, &input).await?;
        let result = executor.model_step(&journal, &input, 0, &[]).await;
        assert_eq!(result.is_ok(), admitted, "{result:?}");
        assert_eq!(counts.load(Ordering::SeqCst), 1);
        assert_eq!(provider.requests().len(), usize::from(admitted));
        if admitted {
            executor.model_step(&journal, &input, 0, &[]).await?;
            assert_eq!(counts.load(Ordering::SeqCst), 1);
            assert_eq!(provider.requests().len(), 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn saved_started_request_reconciles_and_replays_without_fresh_admission() -> Result<()> {
    let journal = Journal::default();
    let (provider, counts) = counted_model(
        4_096,
        Some(AccountingFault::Digest),
        [],
        [ScriptedReconciliation {
            orphan: false,
            observed: vec![],
            continuation: Some(vec![
                ModelEvent::Content {
                    delta: "recovered".into(),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]),
        }],
    );
    let executor = uncompacted_executor(
        Model::new("test".into(), "saved".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(1_024)?;
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("x".repeat(5_000)),
        selected_context: None,
    };
    let prepared = save_unaccounted_request(&executor, &journal, &input).await?;
    let reference = transcript::stage_request(
        &journal,
        input.operation_id,
        "model:0:request",
        &prepared,
        None,
        None,
    )
    .await?;
    journal
        .append(
            input.operation_id,
            "model:0:claim",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: prepared.request_digest(),
                request: reference,
            },
            None,
        )?
        .await?;
    let output = executor.execute(input.clone(), &journal).await?;
    assert_eq!(output.text, "recovered");
    assert_eq!(executor.execute(input.clone(), &journal).await?, output);
    let changed = TurnInput {
        input: ModelContent::Text("changed".into()),
        ..input
    };
    assert!(matches!(
        executor.execute(changed, &journal).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(counts.load(Ordering::SeqCst), 0);
    assert!(provider.requests().is_empty());
    assert_eq!(provider.reconciliations().len(), 1);
    Ok(())
}
