use super::*;

/// A saved disabled-compaction turn binds no capacity and stages no count.
async fn save_unaccounted_request(
    executor: &StockExecutor,
    journal: &Journal,
    input: &TurnInput,
    single_request: bool,
) -> Result<crate::model::PreparedModelRequest> {
    let execution = StockExecution {
        single_request,
        ..StockExecution::ordinary(executor)
    };
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
    if single_request {
        binding["model_capacity"] =
            serde_json::to_value(executor.provider.context_capacity(&executor.model)?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
    }
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
async fn saved_started_request_reconciles_and_replays_without_fresh_admission() -> Result<()> {
    let journal = Journal::default();
    let (provider, counts) = counted_model(
        4_096,
        Some(AccountingFault::Digest),
        [],
        [ScriptedReconciliation {
            // A restarted provider has not observed the retained dispatch.
            orphan: true,
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
    let provider = Arc::new(
        Arc::into_inner(provider)
            .expect("unique fixture model")
            .without_accounting(),
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
    let prepared = save_unaccounted_request(&executor, &journal, &input, false).await?;
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
    let replayed = executor.model_step(&journal, &input, 0, &[]).await?;
    assert!(matches!(
        replayed.last(),
        Some(ModelEvent::Completed { .. })
    ));
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

#[tokio::test]
async fn saved_single_inference_keeps_its_capacity_binding() -> Result<()> {
    let journal = Journal::default();
    let (provider, counts) =
        counted_model(4_096, None, [ScriptedTurn::Text("inferred".into())], []);
    let executor = uncompacted_executor(
        Model::new("test".into(), "saved-inference".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    )
    .with_max_output_tokens(1_024)?;
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: Some(1),
        input: ModelContent::Text("input".into()),
        selected_context: None,
    };
    save_unaccounted_request(&executor, &journal, &input, true).await?;
    for _ in 0..2 {
        assert_eq!(
            executor.infer(&journal, &input, None).await?.text(),
            "inferred"
        );
    }
    assert_eq!(counts.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
    assert!(matches!(
        executor.model_step(&journal, &input, 0, &[]).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}
