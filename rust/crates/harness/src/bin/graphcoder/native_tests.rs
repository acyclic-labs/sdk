use super::*;
use acyclic_harness::{AgentId, Capabilities, Outcome,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner}, core::AggregateKind};
use std::collections::BTreeSet;

async fn composition(root: &std::path::Path) -> Result<Config> {
    let provider = ProviderRef::new("terminal-native-regression", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("terminal-native-regression", "stream", "2")?;
    let agent = AgentId::from_bytes([21; 16]);
    let volume = VolumeRef::new(provider.clone(), "private", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
    let authority = Authority { kind: AggregateKind::Task, id: "terminal-native-owner".into() };
    let issuer = AuthorityIssuer::new("terminal-native-regression", [22; 32], authority.clone());
    let scope = issuer.root_for_agent(agent, "owner", Capabilities::new([
        "operation:declare".to_owned(), "operation:observe".to_owned(),
        "operation:cancel".to_owned(), "operation:wake".to_owned(),
        "model:generate".to_owned(), "task:spawn:acyclic.stock_turn@1".to_owned(),
        volume.capability(VolumeOperation::Read)?, volume.capability(VolumeOperation::Write)?,
    ]));
    let filesystem_root = root.join("filesystem");
    let files = FilesystemHost::new(
        acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&filesystem_root)).await
            .map_err(|error| Error::Storage(error.to_string()))?, provider.clone(),
    )?;
    files.create_volume(&volume).await?;
    drop(files);
    Ok(Config {
        filesystem_root, stream_root: root.join("stream"), filesystem_provider: provider,
        stream_provider, private_volume: volume,
        owner: AuthorityBinding { authority, issuer: "terminal-native-regression".into(), key: [22; 32], scope },
        observed_authorities: vec![], generation: "terminal-native-generation-1".into(),
        limits: Limits::default(), run_limits: TaskRunLimits::default(),
        session_limits: SessionLimits { active_tasks: 4, total_tasks: 4, depth: 3, model_steps: 4 },
        concurrency: 4, deterministic_delay_ms: 0,
    })
}

async fn queue(session: &NativeSession, index: u8, parent: Option<TaskId>) -> Result<TaskId> {
    let operation = OperationId::from_bytes([index; 16]);
    let command_operation = OperationId::from_bytes([index + 20; 16]);
    let command = NativeSession::prompt_command(&format!("input-{index}"), command_operation)?;
    match session.queue(operation, command, parent).await? {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        Admission::Accepted(RuntimeTask::Live(_)) => Err(Error::Conflict("native task was not durable".into())),
        Admission::Rejected { reason } => Err(Error::Conflict(reason)),
        Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
    }
}

#[tokio::test]
async fn actual_stock_root_children_grandchild_reopen_without_effect_repetition() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let config = composition(directory.path()).await?;
    let session = NativeSession::open(config.clone()).await?;
    let root = queue(&session, 1, None).await?;
    let left = queue(&session, 2, Some(root)).await?;
    let right = queue(&session, 3, Some(root)).await?;
    let grandchild = queue(&session, 4, Some(left)).await?;
    let tasks = [root, left, right, grandchild];
    assert_eq!(tasks.into_iter().collect::<BTreeSet<_>>().len(), 4);
    let children = session.runtime.harness().children(root, None, None, 1).await?;
    assert_eq!(children.entries.len(), 1);
    assert!(children.next_after.is_some());
    let next = session.runtime.harness().children(root, Some(children.revision), children.next_after, 1).await?;
    assert_eq!(next.entries.len(), 1);
    assert_ne!(children.entries.first(), next.entries.first());
    let grandchildren = session.runtime.harness().children(left, None, None, 16).await?;
    assert_eq!(grandchildren.entries.first().map(|entry| entry.task_id), Some(grandchild));
    for task in tasks { assert_eq!(session.runtime.task_host().outcome(task).await?, None); }
    drop(session);
    let session = NativeSession::open(config.clone()).await?;
    // Reopening itself does not execute any queued model task.
    for task in tasks { assert_eq!(session.runtime.task_host().outcome(task).await?, None); }
    let worker = Worker { id: "terminal-native-worker".into(), available: Default::default(), labels: Default::default() };
    let mut observed = Vec::new();
    let mut claims = Vec::new();
    for task in tasks {
        session.run(task, &worker, &mut claims).await?;
        let outcome = session.runtime.task_host().outcome(task).await?;
        assert!(matches!(&outcome, Some(Outcome::Succeeded(value)) if value.get("text").and_then(serde_json::Value::as_str).is_some_and(|text| text.starts_with("deterministic request="))));
        observed.push(outcome);
    }
    assert_eq!(String::from_utf8(claims).map_err(|error| Error::Invalid(error.to_string()))?.matches("lease-json-hex=").count(), 4);
    drop(session);
    let session = NativeSession::open(config.clone()).await?;
    for (index, task) in tasks.into_iter().enumerate() {
        assert_eq!(session.runtime.task_host().outcome(task).await?, observed.get(index).cloned().flatten());
        let mut claims = Vec::new();
        assert!(session.run(task, &worker, &mut claims).await?.contains("worker idle"));
        assert!(claims.is_empty(), "terminal tasks must not be redispatched after reopen");
    }
    let mut forged = config;
    forged.owner.key = [99; 32];
    assert!(matches!(NativeSession::open(forged).await, Err(Error::Unauthorized(_))));
    Ok(())
}
