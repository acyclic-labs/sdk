use super::*;

const MAX_FUTURE_BYTES: usize = 4096;

fn assert_bounded<F: std::future::Future>(name: &str, future: F) {
    let bytes = std::mem::size_of_val(&future);
    eprintln!("{name} future: {bytes} bytes");
    assert!(bytes <= MAX_FUTURE_BYTES, "{name} future is {bytes} bytes");
}

#[tokio::test]
async fn execution_and_worker_futures_stay_bounded() -> Result<()> {
    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease, &mut None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let fence = LeaseFence::from(&lease.reservation);
    let turn = OperationId::from_bytes([106; 16]);
    let model = Model::new("synthetic".into(), "size".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    let execution = runtime
        .stock_execution(
            task,
            fence.clone(),
            turn,
            model.clone(),
            provider.clone(),
            ContextPipeline::default(),
            fixture.tools.clone(),
            None,
        )
        .await?;
    let input = TurnInput {
        operation_id: execution.operation_id(),
        input: crate::model::ModelContent::Text("question".into()),
        selected_context: None,
        max_steps: 1,
    };
    assert_bounded(
        "deadline",
        deadline::run(
            runtime.task_host(),
            task,
            Some(&fence),
            Some(20),
            input.operation_id,
            || async { Ok(()) },
            None,
            |_| false,
        ),
    );
    assert_bounded("execution", execution.execute(input.clone()));
    assert_bounded(
        "execution progress",
        execution.execute_progress(input.clone()),
    );
    assert_bounded("model step", execution.model_step(&input, 0, &[]));
    let commands = runtime.commands();
    assert_bounded("run task", runtime.run_task(lease.clone(), &commands, 2));
    let worker_context =
        WorkerContext::Ordinary(Arc::new(runtime.task_context(task, fence.clone()).await?));
    assert_bounded(
        "worker command dispatch",
        runtime.dispatch_commands(task, &worker_context, &fence, &[], &commands, None),
    );
    assert_bounded(
        "run task observed",
        runtime.run_task_observed(&lease, &commands, 2, None),
    );
    assert_bounded(
        "resume task",
        runtime.resume_task(lease.clone(), &commands, 2, None),
    );
    assert_bounded(
        "execution reconciliation",
        execution.reconcile(input.clone(), 1),
    );
    let execution = runtime
        .stock_execution(
            task,
            fence,
            turn,
            model,
            provider,
            ContextPipeline::default(),
            fixture.tools.clone(),
            None,
        )
        .await?;
    assert_bounded(
        "execution reconciliation progress",
        execution.reconcile_progress(input),
    );
    Ok(())
}

#[tokio::test]
async fn reconciliation_has_only_its_progress_allocation() -> Result<()> {
    use futures::FutureExt as _;

    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime(None).await?;
    let lease = fixture.admit(&runtime).await?;
    runtime.task_host().start_task(&lease, &mut None).await?;
    let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
    let model = Model::new("synthetic".into(), "size".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    for (valid, allowance) in [(true, 1_000), (false, 1_000), (true, 0)] {
        let execution = runtime
            .stock_execution(
                task,
                LeaseFence::from(&lease.reservation),
                OperationId::from_bytes([106; 16]),
                model.clone(),
                provider.clone(),
                ContextPipeline::default(),
                fixture.tools.clone(),
                None,
            )
            .await?;
        let input = TurnInput {
            operation_id: if valid {
                execution.operation_id()
            } else {
                OperationId::from_bytes([107; 16])
            },
            input: crate::model::ModelContent::Text("question".into()),
            selected_context: None,
            max_steps: 1,
        };
        let allocations = acyclic_test_support::measure(|| {
            let future = execution.reconcile(input, allowance);
            if valid && allowance != 0 {
                // Valid construction owns exactly one funded progress future.
                drop(future);
            } else {
                // Poll through observation into input validation, before store I/O.
                // Nested observation boxes would be allocated during this poll.
                assert!(matches!(future.now_or_never(), Some(Err(_))));
            }
        });
        // Rejected inputs also allocate their owned diagnostic. A zero
        // allowance allocates that diagnostic without a progress frame.
        let expected = u64::from(allowance != 0) + u64::from(!valid || allowance == 0);
        assert_eq!(allocations.count_total, expected);
    }
    Ok(())
}
