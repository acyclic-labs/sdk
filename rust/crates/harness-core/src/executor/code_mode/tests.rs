use super::*;
use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemJournalBinding,
};

#[tokio::test]
async fn nested_files_use_original_journal_startup_and_replay_without_republication() -> Result<()>
{
    let provider = crate::resources::ProviderRef::new(
        "nested-files".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = crate::AgentId::from_bytes([91; 16]);
    let volume = VolumeRef::new(
        provider,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityRoot::new(
        "nested-files",
        [92; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "nested-files".into(),
        },
    );
    let grants = crate::Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
        "tool:call:read".into(),
        "tool:call:write".into(),
    ]);
    let signed = issuer.bootstrap_scope("owner", &grants, Some(agent), &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let journal = FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host.clone(),
        FilesystemJournalBinding {
            volume: &volume,
            verifier: &verifier,
            scope: &signed,
            io: None,
        },
    )?;
    let scope = crate::runtime::RuntimeScope::new(grants, Limits::default())?;
    let content = crate::runtime::ContentBindings {
        volume: volume.clone(),
        reader: FilesystemContentVerifier::new(host.clone(), &verifier, &signed, 65_536, None)?,
        writer: Some(FilesystemContentPublisher::new(
            host, &volume, &verifier, &signed, 65_536, None,
        )?),
    };
    let task = crate::runtime::Bindings {
        scope: scope.clone(),
        content: Some(content.clone()),
        ..crate::runtime::Bindings::local()
    }
    .build()?
    .agent_context(&crate::Capabilities::default(), content)?;
    let stock = StockExecutor::new(
        Model::new("test".into(), "model".into(), "1".into())?,
        Arc::new(crate::test_support::ScriptedModelProvider::new(Vec::new())),
        ContextPipeline::default(),
        crate::tools::defaults()?,
    )
    .with_compaction_policy(crate::context::CompactionPolicy::Disabled)
    .with_tool_authority(
        scope,
        Arc::new(crate::test_support::FixedToolPolicy::new(
            ToolPolicyDecision::Allow,
        )?),
    )?;
    let mut execution = StockExecution::ordinary(&stock);
    execution.context = Some((&task, OperationId::new()));
    let write = ToolInvocation::for_model_call(
        OperationId::new(),
        0,
        "write".into(),
        "write".into(),
        serde_json::json!({"path":"note","content":"first"}),
        None,
    )?;
    let metadata = serde_json::json!({"revision":"original"});
    let result = execution
        .nested_dispatch(&journal, write.clone(), &metadata, false)
        .await?;
    let ProfileDispatch::Settled(first, _) = result else {
        panic!("nested write did not settle")
    };
    let (_, before) = execution
        .step_records(&journal, write.operation_id, 0, true, 8, |_| true)
        .await?;
    assert!(matches!(
        before.first().map(|record| &record.event),
        Some(ExecutionEvent::Started { .. })
    ));
    assert!(
        before
            .iter()
            .any(|record| matches!(record.event, ExecutionEvent::ToolCompleted { .. }))
    );
    let replay = execution
        .nested_dispatch(&journal, write.clone(), &metadata, false)
        .await?;
    let ProfileDispatch::Settled(replayed, _) = replay else {
        panic!("nested replay did not settle")
    };
    assert_eq!(first, replayed);
    let (_, after) = execution
        .step_records(&journal, write.operation_id, 0, true, 8, |_| true)
        .await?;
    assert_eq!(
        before, after,
        "replay must not publish another start or effect"
    );
    let mut changed_arguments = write.clone();
    changed_arguments.arguments["content"] = serde_json::json!("changed");
    for (changed, changed_metadata) in [
        (write.clone(), serde_json::json!({"revision":"changed"})),
        (changed_arguments, metadata.clone()),
    ] {
        let error = match execution
            .nested_dispatch(&journal, changed, &changed_metadata, false)
            .await
        {
            Ok(_) => panic!("changed nested request reused an execution"),
            Err(error) => error,
        };
        assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
    }
    let read = ToolInvocation::for_model_call(
        OperationId::new(),
        0,
        "read".into(),
        "read".into(),
        serde_json::json!({"path":"note"}),
        None,
    )?;
    let ProfileDispatch::Settled(read, _) = execution
        .nested_dispatch(&journal, read, &metadata, false)
        .await?
    else {
        panic!("nested read did not settle")
    };
    assert_eq!(read.value["text"], "first");
    Ok(())
}
