use super::*;

struct LargeOutput;
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
                value: json!({"text":"x".repeat(18_000)}),
            })
        })
    }
}

#[tokio::test]
async fn automatic_compaction_reads_its_exact_summary_without_widening_task_grants() -> Result<()> {
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
    let provider = scripted([
        call("a"),
        call("b"),
        ScriptedTurn::Text("compressed history".into()),
        ScriptedTurn::Text("done".into()),
    ]);
    let mut large = tool("test.large");
    large.executor = Arc::new(LargeOutput);
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
    assert_eq!(harness.resume(task_id).await?.text, "done");
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    let summary = requests[3]
        .messages
        .iter()
        .flat_map(|message| message.content.file_refs())
        .find(|file| file.path().starts_with(".system/execution/"))
        .expect("compaction summary missing");
    let admission = harness
        .runtime
        .task_host()
        .observe_admission(task_id)
        .await?;
    assert!(
        !crate::runtime::read_granted(&admission.grants, summary)?,
        "model projection widened task tool authority"
    );
    // Run the exact model preflight on the summary captured from the real
    // automatic compaction. Removing the summary exception recreates main's
    // Unauthorized failure, before attempting any content read.
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
    let scope = crate::runtime::RuntimeScope::new(admission.grants, admission.limits)?;
    let content = crate::model::ModelContent::Part(crate::model::ModelContentPart::File {
        file: summary.clone(),
        policy: crate::model::FileProjectionPolicy::BoundedFull,
    });
    crate::executor::verify_model_contents_with_summary_authorization::<true, true>(
        &journal,
        std::iter::once(&content),
        harness.limits,
        Some(&scope),
        None,
        None,
    )
    .await?;
    let main = crate::executor::verify_model_contents_with_summary_authorization::<false, true>(
        &journal,
        std::iter::once(&content),
        harness.limits,
        Some(&scope),
        None,
        None,
    )
    .await
    .expect_err("main's missing authorization must reject the actual compacted summary");
    assert_eq!(main.code(), crate::wire::ErrorCode::Unauthorized);
    assert_eq!(harness.resume(task_id).await?.text, "done");
    assert_eq!(
        provider.requests().len(),
        4,
        "completed recovery resent the model request"
    );
    Ok(())
}
