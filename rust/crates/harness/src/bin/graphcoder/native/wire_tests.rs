use super::*;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey,
    conversation::{VolumeClass, VolumeOwner},
    core::{Action, AggregateKind, Command, LifecycleState},
};
use futures::FutureExt;

#[tokio::test]
async fn replay_follows_owner_waiting_without_scheduler_changes() -> Result<()> {
    let root = tempfile::tempdir().map_err(storage)?;
    let authority = Authority {
        kind: AggregateKind::Task,
        id: "live-replay".into(),
    };
    let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
    let scope = issuer.issue(
        "owner",
        &Capabilities::new(["lifecycle:manage"]),
        None,
        None,
        &mut None,
    )?;
    let provider = ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?;
    let private = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::new()),
        &mut None,
    )?;
    let host = NativeSession::open(Config {
        filesystem_root: root.path().join("filesystem"),
        stream_root: root.path().join("stream"),
        filesystem_provider: provider,
        stream_provider: ProviderRef::new("test".into(), "stream".into(), "2".into(), &mut None)?,
        private_volume: private,
        owner: AuthorityBinding {
            authority: authority.clone(),
            issuer: "test".into(),
            key: [7; 32],
            scope: scope.clone(),
        },
        generation: "generation".into(),
        model_invocation_ledger: root.path().join("model.jsonl"),
        limits: Limits::default(),
        run_limits: TaskRunLimits::default(),
        session_limits: SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        concurrency: 1,
        deterministic_delay_ms: 0,
        fork_sources: Vec::new(),
        tools: Default::default(),
        context: Default::default(),
        native_publications: Vec::new(),
        project_source_root: None,
    })
    .await?;
    let resume = |revision| wire::ResumeRequest {
        protocol: Some(acyclic_harness::wire_api::current_protocol()),
        cursors: vec![wire::ReplayCursor {
            revision,
            ..host.start_cursor()
        }],
    };
    assert!(host.replay(resume(0)).await?.next().await.is_none());
    let transition = |revision, to| -> Result<Command> {
        let operation_id = OperationId::from_bytes([revision as u8 + 1; 16]);
        Ok(Command {
            operation_id,
            idempotency_key: IdempotencyKey::new(operation_id.to_string())?,
            expected_revision: revision,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::TransitionLifecycle { to, reason: None },
        })
    };
    let mut writer = open_aggregate(
        &host.stream,
        &host.files,
        &host.config.stream_provider,
        &host.config.owner,
    )
    .await?;
    writer
        .execute(transition(0, LifecycleState::Active)?, None, &mut None)
        .await?;
    let mut replay = host.replay(resume(0)).await?;
    let first = replay
        .next()
        .await
        .transpose()?
        .ok_or_else(|| Error::Storage("missing active".into()))?;
    assert_eq!(first.through_revision, 1);
    assert!(
        replay.next().now_or_never().is_none(),
        "must remain live at the current cut"
    );
    writer
        .execute(transition(1, LifecycleState::Waiting)?, None, &mut None)
        .await?;
    // No scheduler exists in this fixture: only the owner's stream changes.
    let waiting = tokio::time::timeout(std::time::Duration::from_secs(1), replay.next())
        .await
        .map_err(storage)?
        .transpose()?
        .ok_or_else(|| Error::Storage("missing waiting".into()))?;
    assert_eq!((waiting.from_revision, waiting.through_revision), (1, 2));
    assert!(waiting.live);
    assert_eq!(waiting.events[0].event_type, "lifecycle_transitioned");
    assert_eq!(
        waiting.events[0].operation_id,
        OperationId::from_bytes([2; 16]).to_string()
    );
    let mut resumed = host.replay(resume(1)).await?;
    assert_eq!(resumed.next().await.transpose()?, Some(waiting));
    Ok(())
}
