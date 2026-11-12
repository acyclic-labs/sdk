use super::*;

/// A saved disabled-compaction turn binds no capacity and stages no count.
async fn save_unaccounted_request(
    executor: &StockExecutor,
    journal: &Journal,
    input: &TurnInput,
    single_request: bool,
    version: &str,
) -> Result<crate::model::PreparedModelRequest> {
    let execution = StockExecution {
        single_request,
        ..StockExecution::ordinary(executor)
    };
    let mut binding = json!({
        "executor": version,
        "input": input,
        "model": executor.model,
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
    let projection = transcript::stage_projection(
        journal,
        input.operation_id,
        "context:0",
        (
            &ResponseProjection {
                context,
                steering: None,
                canonical: None,
                checkpoint: None,
                working: None,
                accounting: None,
            },
            &prepared,
        ),
        None,
        None,
        None,
    )
    .await?;
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
async fn disabled_compaction_dispatches_fresh_and_saved_requests_without_accounting() -> Result<()>
{
    for saved in [false, true] {
        for (capacity, length, fault) in [
            (100, 10, None),
            (131_072, 10, None),
            (4_096, 5_000, None),
            (131_072, 10, Some(AccountingFault::Digest)),
        ] {
            let journal = Journal::default();
            let (provider, counts) =
                counted_model(capacity, fault, [ScriptedTurn::Text("done".into())], []);
            let executor = uncompacted_executor(
                Model::new("test".into(), "unchecked".into(), "1".into())?,
                provider.clone(),
                ContextPipeline::default(),
                ToolRegistry::default(),
            );
            let input = TurnInput {
                operation_id: OperationId::new(),
                max_steps: None,
                input: ModelContent::Text("x".repeat(length)),
                selected_context: None,
            };
            if saved {
                save_unaccounted_request(&executor, &journal, &input, false, "acyclic.stock.v9")
                    .await?;
            }
            let result = executor.execute(input.clone(), &journal).await?;
            assert_eq!(result.text, "done");
            assert_eq!(executor.execute(input, &journal).await?, result);
            assert_eq!(counts.load(Ordering::SeqCst), 0);
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
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("x".repeat(5_000)),
        selected_context: None,
    };
    let prepared =
        save_unaccounted_request(&executor, &journal, &input, false, "acyclic.stock.v9").await?;
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
                context: None,
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
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: Some(1),
        input: ModelContent::Text("input".into()),
        selected_context: None,
    };
    save_unaccounted_request(&executor, &journal, &input, true, "acyclic.stock.v9").await?;
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

#[tokio::test]
async fn old_inline_projection_binding_is_refused_before_dispatch() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(ScriptedModelProvider::new([done_turn()]));
    let executor = uncompacted_executor(
        Model::new("test".into(), "binding".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("saved".into()),
        selected_context: None,
    };
    save_unaccounted_request(&executor, &journal, &input, false, "acyclic.stock.v8").await?;
    assert!(matches!(
        executor.execute(input, &journal).await,
        Err(Error::Conflict(_))
    ));
    assert!(provider.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn independent_nonzero_model_step_stages_a_new_prefix_and_replays() -> Result<()> {
    let journal = Journal::default();
    let provider = Arc::new(ScriptedModelProvider::new([done_turn()]));
    let executor = uncompacted_executor(
        Model::new("test".into(), "independent-step".into(), "1".into())?,
        provider.clone(),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let input = TurnInput {
        operation_id: OperationId::new(),
        max_steps: None,
        input: ModelContent::Text("current".into()),
        selected_context: None,
    };
    let prior = vec![ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Text("caller supplied history".into()),
    }];
    let first = executor.model_step(&journal, &input, 7, &prior).await?;
    assert_eq!(
        first,
        executor.model_step(&journal, &input, 7, &prior).await?
    );
    assert_eq!(provider.requests().len(), 1);
    assert!(provider.requests()[0].messages.contains(&prior[0]));
    let records = journal
        .replay(input.operation_id, 0, u32::MAX, None)?
        .await?;
    let projection = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ContextPrepared { projection, .. } => Some(projection),
            _ => None,
        })
        .unwrap();
    let request = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ModelStarted { request, .. } => Some(request),
            _ => None,
        })
        .unwrap();
    let bytes = journal.load(projection, None)?.await?;
    let stored: Value =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        stored["request"],
        serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?
    );
    Ok(())
}
