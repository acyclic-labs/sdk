use super::*;

#[tokio::test]
async fn durable_tool_replay_is_bound_to_its_admitting_task() -> Result<()> {
    let agent = AgentId::from_bytes([58; 16]);
    let (host, private, issuer) = filesystem("tool-owner-e2e", "tool-owner", agent, 58).await?;
    let scope = scope(&issuer, &private, agent, &[])?;
    let journal = Arc::new(journal(
        Arc::new(MemoryStream::default()),
        host,
        &private,
        &issuer,
        &scope,
    )?);
    let definition = ToolDefinition {
        name: "example.noop".into(),
        revision: "1".into(),
        description: "No-op".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({})),
    };
    let executions = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let counted = Arc::new(CountingNoopTool {
        executions: executions.clone(),
        reconciliations: reconciliations.clone(),
    });
    let mut registry = ToolRegistry::default();
    registry.register(
        (
            Tool {
                hook_names: &[],
                definition: definition.clone(),
                executor: counted.clone(),
                projection: Arc::new(acyclic_harness::tool::JsonToolProjection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let invalid_output = ToolDefinition {
        name: "example.invalid-output".into(),
        revision: "1".into(),
        description: "Pinned invalid result".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"string"}),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({})),
    };
    registry.register(
        (
            Tool {
                hook_names: &[],
                definition: invalid_output.clone(),
                executor: counted.clone(),
                projection: Arc::new(acyclic_harness::tool::JsonToolProjection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let invalid_projection = ToolDefinition {
        name: "example.invalid-projection".into(),
        revision: "1".into(),
        description: "Valid canonical result with an invalid model projection".into(),
        input_schema: json!({"type":"object"}),
        output_schema: json!({}),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({"type":"string"})),
    };
    registry.register(
        (
            Tool {
                hook_names: &[],
                definition: invalid_projection.clone(),
                executor: counted,
                projection: Arc::new(acyclic_harness::tool::JsonToolProjection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let large_definition = ToolDefinition {
        name: "example.large-projection".into(),
        projection_schema: acyclic_harness::tool::json_projection_schema(json!({"type":"string"})),
        ..definition.clone()
    };
    let large_executions = Arc::new(AtomicUsize::new(0));
    let large_reconciliations = Arc::new(AtomicUsize::new(0));
    let large_projection = json!({"kind":"json","value":"x".repeat(500)});
    registry.register(
        (
            Tool {
                hook_names: &[],
                definition: large_definition.clone(),
                executor: Arc::new(CountingNoopTool {
                    executions: large_executions.clone(),
                    reconciliations: large_reconciliations.clone(),
                }),
                projection: Arc::new(FixedProjection(large_projection.clone())),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let limits = Limits {
        file_bytes: 256,
        render_bytes: 1_024,
        ..Limits::default()
    };
    let racing_journal = Arc::new(TerminalCasLoser {
        inner: journal.clone(),
        losses: AtomicUsize::new(0),
    });
    let runner =
        DurableToolRunner::new(&registry, racing_journal.clone(), None)?.with_limits(limits)?;
    let operation = OperationId::from_bytes([59; 16]);
    let first_task = TaskId::from_bytes([60; 16]);
    let other_task = TaskId::from_bytes([61; 16]);
    let task_scope = RuntimeScope::new(
        Capabilities::new([
            "tool:call:example.noop",
            "tool:call:example.invalid-output",
            "tool:call:example.invalid-projection",
            "tool:call:example.large-projection",
        ]),
        limits,
    )?;
    let mut bindings = Bindings::local();
    bindings.scope = task_scope.clone();
    bindings.state = Some(Arc::new(TestTaskState(task_scope)));
    let runtime = bindings.build(None)?;
    let first_context = runtime
        .durable_context(first_task, OperationId::from_bytes([63; 16]), None)
        .await?;
    let other_context = runtime
        .durable_context(other_task, OperationId::from_bytes([64; 16]), None)
        .await?;
    let projection_context = first_context.clone();
    for _ in 0..2 {
        assert!(matches!(
            runner
                .run(
                    first_task,
                    &definition,
                    &acyclic_harness::tool::ToolInvocation {
                        operation_id: operation,
                        call_id: operation.to_string(),
                        name: definition.name.clone(),
                        arguments: json!({}),
                    },
                    &ToolContext::new(
                        first_context.clone(),
                        operation,
                        &(operation.to_string()),
                        None
                    )?,
                    None,
                    false,
                )?
                .await?
                .0,
            Outcome::Succeeded(Value::Null)
        ));
    }
    let records = journal.replay(operation, 0, 64, None)?.await?;
    let invocation = records
        .iter()
        .find_map(|record| {
            if let ExecutionEvent::ToolStarted { invocation, .. } = &record.event {
                Some(invocation.clone())
            } else {
                None
            }
        })
        .ok_or_else(|| Error::NotFound("tool invocation artifact".into()))?;
    let completed = records
        .iter()
        .find_map(|record| {
            if let ExecutionEvent::ToolCompleted {
                result, projection, ..
            } = &record.event
            {
                Some(vec![result.clone(), projection.clone()])
            } else {
                None
            }
        })
        .ok_or_else(|| Error::NotFound("completed tool artifacts".into()))?;
    for target in [
        OversizedArtifact::Invocation,
        OversizedArtifact::Result,
        OversizedArtifact::Projection,
    ] {
        let maximum = match target {
            OversizedArtifact::Projection => limits.render_bytes,
            _ => limits.file_bytes,
        };
        let spy = Arc::new(CompletedDescriptorSpy {
            inner: journal.clone(),
            completed: completed.clone(),
            invocation: invocation.clone(),
            target,
            oversized_bytes: maximum + 1,
            completed_reads: AtomicUsize::new(0),
            invocation_reads: AtomicUsize::new(0),
        });
        let guarded = DurableToolRunner::new(&registry, spy.clone(), None)?.with_limits(limits)?;
        assert!(matches!(
            guarded
                .run(
                    first_task,
                    &definition,
                    &acyclic_harness::tool::ToolInvocation {
                        operation_id: operation,
                        call_id: operation.to_string(),
                        name: definition.name.clone(),
                        arguments: json!({}),
                    },
                    &ToolContext::new(
                        first_context.clone(),
                        operation,
                        &(operation.to_string()),
                        None
                    )?,
                    None,
                    false,
                )?
                .await
                .map(|result| result.0),
            Err(Error::Conflict(_))
        ));
        assert_eq!(spy.completed_reads.load(Ordering::SeqCst), 0);
        let expected_invocation_reads = if matches!(target, OversizedArtifact::Invocation) {
            0
        } else {
            1
        };
        assert_eq!(
            spy.invocation_reads.load(Ordering::SeqCst),
            expected_invocation_reads
        );
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    }
    // The bare invocation fits exactly, but its saved metadata wrapper does not.
    // Reject before claiming/executing so a completed call can always be replayed.
    let wrapped_operation = OperationId::new();
    let mut wrapped_invocation = acyclic_harness::tool::ToolInvocation {
        operation_id: wrapped_operation,
        call_id: "wrapped-boundary".into(),
        name: definition.name.clone(),
        arguments: json!({"data":""}),
    };
    let bare_bytes = serde_json::to_vec(&wrapped_invocation)
        .expect("fixture serialization")
        .len();
    wrapped_invocation.arguments["data"] =
        json!("x".repeat(limits.file_bytes as usize - bare_bytes));
    assert_eq!(
        serde_json::to_vec(&wrapped_invocation)
            .expect("fixture serialization")
            .len() as u64,
        limits.file_bytes
    );
    let rejected = runner
        .run(
            first_task,
            &definition,
            &wrapped_invocation,
            &ToolContext::new(
                first_context.clone(),
                wrapped_operation,
                "wrapped-boundary",
                None,
            )?,
            None,
            false,
        )?
        .await;
    assert!(matches!(rejected, Err(Error::Invalid(_))));
    assert!(
        journal
            .replay(wrapped_operation, 0, 64, None)?
            .await?
            .is_empty()
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    let deep_operation = OperationId::new();
    let mut deep = json!(0);
    for _ in 0..126 {
        deep = json!([deep]);
    }
    let deep_invocation = acyclic_harness::tool::ToolInvocation {
        operation_id: deep_operation,
        call_id: "deep-record".into(),
        name: definition.name.clone(),
        arguments: json!({"nested":deep}),
    };
    let rejected = runner
        .run(
            first_task,
            &definition,
            &deep_invocation,
            &ToolContext::new(first_context.clone(), deep_operation, "deep-record", None)?,
            None,
            false,
        )?
        .await;
    assert!(matches!(rejected, Err(Error::Invalid(_))));
    assert!(
        journal
            .replay(deep_operation, 0, 64, None)?
            .await?
            .is_empty()
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    let fresh_operation = OperationId::new();
    let bounded =
        DurableToolRunner::new(&registry, journal.clone(), None)?.with_limits(Limits {
            file_bytes: 1_024,
            render_bytes: 1_024,
            ..Limits::default()
        })?;
    assert!(matches!(
        bounded
            .run(
                first_task,
                &definition,
                &acyclic_harness::tool::ToolInvocation {
                    operation_id: fresh_operation,
                    call_id: fresh_operation.to_string(),
                    name: definition.name.clone(),
                    arguments: json!({"data":"x".repeat(1_024)}),
                },
                &ToolContext::new(
                    first_context.clone(),
                    fresh_operation,
                    &(fresh_operation.to_string()),
                    None
                )?,
                None,
                false,
            )?
            .await
            .map(|result| result.0),
        Err(Error::Invalid(_))
    ));
    assert!(
        journal
            .replay(fresh_operation, 0, 64, None)?
            .await?
            .is_empty()
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    drop(runner);
    registry.remove_from_model("example.noop")?;
    assert!(registry.get("example.noop", None, None)?.is_none());
    let runner =
        DurableToolRunner::new(&registry, racing_journal.clone(), None)?.with_limits(limits)?;
    assert!(matches!(
        runner
            .run(
                first_task,
                &definition,
                &acyclic_harness::tool::ToolInvocation {
                    operation_id: operation,
                    call_id: operation.to_string(),
                    name: definition.name.clone(),
                    arguments: json!({}),
                },
                &ToolContext::new(
                    first_context.clone(),
                    operation,
                    &(operation.to_string()),
                    None
                )?,
                None,
                false,
            )?
            .await?
            .0,
        Outcome::Succeeded(Value::Null)
    ));
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    assert!(
        runner
            .run(
                other_task,
                &definition,
                &acyclic_harness::tool::ToolInvocation {
                    operation_id: operation,
                    call_id: operation.to_string(),
                    name: definition.name.clone(),
                    arguments: json!({}),
                },
                &ToolContext::new(other_context, operation, &(operation.to_string()), None)?,
                None,
                false,
            )?
            .await
            .map(|result| result.0)
            .is_err()
    );
    for (index, (definition, operation, reason)) in [
        (
            invalid_output,
            OperationId::from_bytes([62; 16]),
            ToolFailureKind::InvalidOutput,
        ),
        (
            invalid_projection,
            OperationId::from_bytes([65; 16]),
            ToolFailureKind::ProjectionRejected,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let failure = runner
            .run(
                first_task,
                &definition,
                &acyclic_harness::tool::ToolInvocation {
                    operation_id: operation,
                    call_id: operation.to_string(),
                    name: definition.name.clone(),
                    arguments: json!({}),
                },
                &ToolContext::new(
                    first_context.clone(),
                    operation,
                    &(operation.to_string()),
                    None,
                )?,
                None,
                false,
            )?
            .await?
            .0;
        assert!(matches!(&failure, Outcome::Failed { .. }));
        assert_eq!(
            failure,
            runner
                .run(
                    first_task,
                    &definition,
                    &acyclic_harness::tool::ToolInvocation {
                        operation_id: operation,
                        call_id: operation.to_string(),
                        name: definition.name.clone(),
                        arguments: json!({}),
                    },
                    &ToolContext::new(
                        first_context.clone(),
                        operation,
                        &(operation.to_string()),
                        None
                    )?,
                    None,
                    false,
                )?
                .await?
                .0
        );
        assert_eq!(racing_journal.losses.load(Ordering::SeqCst), 2);
        assert_eq!(executions.load(Ordering::SeqCst), index + 2);
        assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
        assert!(
            matches!(journal.replay(operation, 0, 64, None)?.await?.last().map(|record| &record.event),
            Some(ExecutionEvent::ToolFailed { reason: actual, .. }) if actual == &reason)
        );
    }
    let large_operation = OperationId::from_bytes([66; 16]);
    for _ in 0..2 {
        assert!(matches!(
            runner
                .run(
                    first_task,
                    &large_definition,
                    &acyclic_harness::tool::ToolInvocation {
                        operation_id: large_operation,
                        call_id: large_operation.to_string(),
                        name: large_definition.name.clone(),
                        arguments: json!({}),
                    },
                    &ToolContext::new(
                        projection_context.clone(),
                        large_operation,
                        &(large_operation.to_string()),
                        None
                    )?,
                    None,
                    false,
                )?
                .await?
                .0,
            Outcome::Succeeded(Value::Null)
        ));
    }
    let records = journal.replay(large_operation, 0, 64, None)?.await?;
    let Some(ExecutionEvent::ToolCompleted {
        result, projection, ..
    }) = records.last().map(|record| &record.event)
    else {
        return Err(Error::NotFound("large projection completion".into()));
    };
    assert!(result.descriptor().byte_length() <= limits.file_bytes);
    assert!(projection.descriptor().byte_length() > limits.file_bytes);
    assert!(projection.descriptor().byte_length() <= limits.render_bytes);
    assert_eq!(
        serde_json::from_slice::<Value>(&journal.load(projection, None)?.await?)
            .map_err(|error| Error::Invalid(error.to_string()))?,
        large_projection
    );
    assert_eq!(large_executions.load(Ordering::SeqCst), 1);
    assert_eq!(large_reconciliations.load(Ordering::SeqCst), 0);
    Ok(())
}
