use super::*;

#[tokio::test]
async fn pre_dispatch_failures_settle_and_close_without_provider_requests() -> Result<()> {
    for (name, prompt, policy) in [
        (
            "no older source",
            "x".repeat(70_000),
            crate::context::ThresholdCompaction {
                response_reserve_tokens: 4096,
                recent_tokens: 100,
                ..Default::default()
            },
        ),
        (
            "mandatory source",
            "x".repeat(40_000),
            crate::context::ThresholdCompaction {
                response_reserve_tokens: 32_000,
                recent_tokens: 1,
                ..Default::default()
            },
        ),
    ] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider = Arc::new(ScriptedModelProvider::new([]).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 65_536,
                output_tokens: Some(32_000),
            },
        ));
        let harness = LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::Threshold(policy)),
        )
        .await?;
        let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
            harness.submit(OperationId::new(), &prompt).await?
        else {
            panic!("{name}: admission rejected")
        };
        assert!(
            matches!(harness.resume(task_id).await,
            Err(Error::TaskFailed { task, .. }) if task == task_id),
            "{name}: preparation did not settle as Failed"
        );
        assert!(
            provider.requests().is_empty(),
            "{name}: provider dispatched"
        );
        harness.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn post_dispatch_failure_keeps_the_turn_indeterminate() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([ScriptedTurn::Events(vec![ScriptedEvent::Error {
        error: "response interrupted after dispatch".into(),
    }])]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?.without_default_tools(),
    )
    .await?;
    let result = harness.run("answer").await;
    assert!(
        matches!(
            result,
            Err(Error::Indeterminate(_) | Error::CommandIndeterminate { .. })
        ),
        "post-dispatch failure became terminal: {result:?}"
    );
    assert_eq!(provider.requests().len(), 1);
    harness.disconnect_unfinished().await?;
    Ok(())
}
