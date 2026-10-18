use super::*;

struct LargeOutput(usize);
impl ToolExecutor for LargeOutput {
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Ok(ToolResult {
                value: json!({"text":"x".repeat(self.0)}),
            })
        })
    }
}

#[tokio::test]
async fn automatic_compaction_reads_its_exact_summary_without_widening_task_grants() -> Result<()> {
    automatic_compaction_case(capacity(), 18_000).await
}

#[tokio::test]
async fn automatic_compaction_fits_equal_small_context_and_output_capacities() -> Result<()> {
    automatic_compaction_case(
        ModelContextCapacity {
            context_tokens: 8192,
            output_tokens: 8192,
        },
        2000,
    )
    .await
}

async fn automatic_compaction_case(capacity: ModelContextCapacity, bytes: usize) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let call = |id: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                id.into(),
                "test.large".into(),
                json!({}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ])
    };
    let provider = Arc::new(
        ScriptedModelProvider::new([
            call("a"),
            call("b"),
            ScriptedTurn::Text("compressed history".into()),
            ScriptedTurn::Text("done".into()),
        ])
        .with_context_capacity(capacity),
    );
    let mut large = tool("test.large");
    large.executor = Arc::new(LargeOutput(bytes));
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider.clone())?
            .without_default_tools()
            .tool(large),
    )
    .await?;
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, "perform two operations").await?
    else {
        panic!("prompt rejected")
    };
    let original_grants = harness
        .runtime
        .task_host()
        .observe_admission(task_id)
        .await?
        .grants;
    assert_eq!(harness.resume(task_id).await?.text, "done");
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    for request in &requests {
        let request =
            crate::model::PreparedModelRequest::prepare(request.clone(), harness.limits, None)?;
        let count = provider.count_tokens(&request).await?;
        let input = count.validate(&request, &mut None)?;
        assert!(
            input + u64::from(request.request().max_output_tokens.unwrap())
                <= u64::from(capacity.context_tokens)
        );
    }
    let summary = requests[3]
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .find(|file| file.path().starts_with("summaries/"))
        .expect("compaction summary missing");
    let admission = harness
        .runtime
        .task_host()
        .observe_admission(task_id)
        .await?;
    assert!(
        crate::runtime::read_granted(&admission.grants, summary)?,
        "the summary must use the task's existing private read authority"
    );
    assert_eq!(admission.grants, original_grants);
    assert_ne!(summary.volume(), &harness.journal);
    // The actual model request already passed production content preflight.
    // Removing its private read grant must reject that same summary; journal
    // provenance cannot confer authority over a different private volume.
    let journal = crate::filesystem::FilesystemExecutionJournal::new(
        harness.task_stream(),
        harness.files.clone(),
        &crate::core::SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &harness.journal,
            verifier: &harness.verifier,
            scope: &harness.signed,
            maximum_bytes: harness.limits.file_bytes,
            io: None,
        },
    )?;
    let scope =
        crate::runtime::RuntimeScope::new(crate::Capabilities::default(), admission.limits)?;
    let content = crate::model::ModelContent::Part(crate::model::ModelContentPart::File {
        file: summary.clone(),
        policy: crate::model::FileProjectionPolicy::BoundedFull,
    });
    let denied = crate::executor::verify_model_contents_with_summary_authorization::<true, true>(
        &journal,
        std::iter::once(&content),
        harness.limits,
        Some(&scope),
        None,
        None,
    )
    .await
    .expect_err("a scope without private read authority must reject the compacted summary");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
    assert_eq!(harness.resume(task_id).await?.text, "done");
    assert_eq!(
        provider.requests().len(),
        4,
        "completed recovery resent the model request"
    );
    Ok(())
}

#[tokio::test]
async fn defaults_leave_input_room_when_output_capacity_equals_the_context() -> Result<()> {
    let capacity = ModelContextCapacity {
        context_tokens: 8192,
        output_tokens: 8192,
    };
    for legacy in [false, true] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())])
                .with_context_capacity(capacity),
        );
        let mut config =
            volume_config(directory.path(), project, provider.clone())?.without_default_tools();
        if legacy {
            config = config.compaction(crate::context::CompactionPolicy::Threshold(
                crate::context::ThresholdCompaction {
                    response_reserve_tokens: 8190,
                    recent_tokens: 1,
                    ..Default::default()
                },
            ));
        }
        let harness = LocalHarness::open(config).await?;
        let result = harness.run("complete an ordinary prompt").await;
        // Both presets must pass the same real admission/content preflight.
        // Restoring the former output-first preset prevents generation entirely.
        if legacy {
            assert!(result.is_err(), "output-first control unexpectedly fit");
            assert!(
                provider.requests().is_empty(),
                "an unfunded legacy budget reached the provider"
            );
        } else {
            assert_eq!(result?.text, "done");
            let requests = provider.requests();
            assert_eq!(requests.len(), 1);
            let request = crate::model::PreparedModelRequest::prepare(
                requests[0].clone(),
                harness.limits,
                None,
            )?;
            let count = provider.count_tokens(&request).await?;
            let input = count.validate(&request, &mut None)?;
            let output = request.request().max_output_tokens.unwrap();
            assert!(input > 2);
            assert!(output > 0 && input + u64::from(output) <= u64::from(capacity.context_tokens));
        }
        // The output-first control settles its admitted turn as failed without
        // generating, so both presets close a settled HEAD.
        harness.close().await?;
    }
    Ok(())
}

// Negative control for the checked test teardown: a settled HEAD must close,
// so disconnecting it as unfinished is refused.
#[tokio::test]
async fn a_settled_turn_cannot_be_torn_down_as_unfinished() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([ScriptedTurn::Text("done".into())]);
    let harness = LocalHarness::open(
        volume_config(directory.path(), project, provider)?.without_default_tools(),
    )
    .await?;
    assert_eq!(harness.run("settle one prompt").await?.text, "done");
    assert!(matches!(
        harness.disconnect_unfinished().await,
        Err(Error::Conflict(reason)) if reason.contains("must close")
    ));
    Ok(())
}
