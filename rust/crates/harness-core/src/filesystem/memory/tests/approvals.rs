//! Memory storage opens approvals through the one harness approval owner.
use super::*;
use crate::{
    InteractionId,
    interaction::{ApprovalRoute, ApprovalSubject, Interaction, InteractionOutcome},
};

fn approval() -> Interaction {
    Interaction::Approval {
        prompt: "write approved".into(),
        operation_id: OperationId::new(),
        action_digest: [7; 32],
        subject: ApprovalSubject::Tool {
            route: ApprovalRoute::Policy,
            tool: "write".into(),
            call_id: "call".into(),
            arguments: json!({"path": "approved"}),
            target: None,
        },
    }
}

async fn open(
    storage: &MemoryHarnessStorage,
) -> Result<(InteractionId, Option<InteractionOutcome>)> {
    let journal = storage.journal();
    let id = InteractionId::new();
    let request = approval();
    journal.open_interaction(id, &request, None)?.await?;
    // Opening is exact-once: the same request reopens, a different one conflicts.
    journal.open_interaction(id, &request, None)?.await?;
    assert!(
        journal
            .open_interaction(id, &approval(), None)?
            .await
            .is_err()
    );
    Ok((id, journal.interaction_outcome(id, None)?.await?))
}

#[tokio::test]
async fn default_policy_opens_an_approval_that_waits_for_its_decider() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    assert_eq!(open(&storage).await?.1, None);
    Ok(())
}

#[tokio::test]
async fn human_decision_settles_a_waiting_approval() -> Result<()> {
    for (approved, settled) in [
        (true, InteractionOutcome::Approved {}),
        (false, InteractionOutcome::Declined {}),
    ] {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let (id, waiting) = open(&storage).await?;
        assert_eq!(waiting, None);
        // Only the policy's own principal may decide.
        assert!(
            storage
                .decide_approval(id, ApprovalResponder::Human("intruder"), approved, None)
                .await
                .is_err()
        );
        let human = ApprovalResponder::Human("human");
        assert_eq!(
            storage.decide_approval(id, human, approved, None).await?,
            Some(settled.clone())
        );
        // The same decision replays; the journal sees the settled outcome.
        assert_eq!(
            storage.decide_approval(id, human, approved, None).await?,
            Some(settled.clone())
        );
        assert!(
            storage
                .decide_approval(id, human, !approved, None)
                .await
                .is_err()
        );
        assert_eq!(
            storage.journal().interaction_outcome(id, None)?.await?,
            Some(settled)
        );
    }
    Ok(())
}

#[tokio::test]
async fn approval_dispatch_and_verification_retain_one_original_request() -> Result<()> {
    use crate::agent::{AgentPath, TurnEffects as _};
    use futures::{FutureExt as _, channel::oneshot};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    // Forward to the actual private-volume files. Count only the immutable
    // request, independently of the policy's retained review reads/writes.
    struct Reads {
        files: Arc<dyn ApprovalFiles>,
        requests: AtomicUsize,
        release: Mutex<Option<oneshot::Receiver<()>>>,
    }
    impl ApprovalFiles for Reads {
        fn read<'a>(
            &'a self,
            path: &'a str,
            maximum: u64,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
            Box::pin(async move {
                if path.ends_with("/request.json") {
                    self.requests.fetch_add(1, Ordering::SeqCst);
                    let release = self.release.lock().unwrap().take();
                    if let Some(release) = release {
                        release.await.map_err(|_| {
                            Error::Storage("approval request producer dropped".into())
                        })?;
                    }
                }
                self.files.read(path, maximum).await
            })
        }
        fn create<'a>(
            &'a self,
            path: &'a str,
            bytes: Vec<u8>,
            maximum: u64,
        ) -> BoxFuture<'a, Result<crate::approvals::ApprovalCreate>> {
            self.files.create(path, bytes, maximum)
        }
    }

    let agent = crate::agent::AgentId::from_bytes([13; 16]);
    let storage = MemoryHarnessStorage::new(AgentId::from_bytes(agent.to_bytes()), 65_536).await?;
    let files = Arc::new(AgentInternalFiles::new(
        storage.host.clone(),
        storage.volume.clone(),
        ContentGrant::verify(
            &storage.verifier(),
            &storage.scope,
            &storage.volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?,
    ));
    let effects = Arc::new(
        crate::filesystem::agent_effects::FilesystemTurnEffects::new(
            files,
            storage.stream.clone(),
            AgentPath::root(&agent),
            (OperationId::new(), 0),
            65_536,
        ),
    );
    let reads = Arc::new(Reads {
        files: storage.approval_files.clone(),
        requests: AtomicUsize::new(0),
        release: Mutex::new(None),
    });
    let policy = AgentApprovalPolicy::new(
        crate::preset::Preset::graphcoder().approval_policy,
        None,
        Default::default(),
        Limits {
            file_bytes: 65_536,
            ..Limits::default()
        },
    )?;
    let owner = AgentApprovals::new(reads.clone(), Arc::new(policy)).yielding(effects.clone());
    let id = InteractionId::new();
    let request = approval();
    let Interaction::Approval {
        operation_id,
        action_digest,
        ..
    } = &request
    else {
        unreachable!()
    };
    owner.open(id, &request).await?;
    assert_eq!(
        owner.dispatch(id, &*storage.journal(), None).await?,
        Some(InteractionOutcome::Approved {})
    );
    assert_eq!(
        reads.requests.swap(0, Ordering::SeqCst),
        1,
        "dispatch reread its original request"
    );
    for (operation, digest, accepted) in [
        (*operation_id, *action_digest, true),
        (OperationId::new(), *action_digest, false),
        (*operation_id, [8; 32], false),
    ] {
        let verified = owner.verify(id, operation, digest).await;
        assert!(
            matches!(
                (&verified, accepted),
                (Ok(()), true) | (Err(Error::Unauthorized(_)), false)
            ),
            "verification lost the original action binding: {verified:?}"
        );
        assert_eq!(
            reads.requests.swap(0, Ordering::SeqCst),
            1,
            "verification reread its original request"
        );
    }
    // Reopen the same real volume through a second owner, as after a restart.
    let reopened = agent_approvals(
        &storage.approval_files,
        crate::preset::Preset::graphcoder().approval_policy,
        65_536,
    )?;
    reopened.verify(id, *operation_id, *action_digest).await?;
    assert!(!owner.yielded());
    assert!(
        effects.recorded().await?.is_empty(),
        "approval resolution executed an effect"
    );

    let journal = storage.journal();
    for cancel in [false, true] {
        let (producer, release) = oneshot::channel();
        *reads.release.lock().unwrap() = Some(release);
        let mut pending = Box::pin(owner.dispatch(id, &*journal, None));
        assert!(pending.as_mut().now_or_never().is_none());
        if cancel {
            drop(producer);
            assert!(
                matches!(pending.now_or_never(),
        Some(Err(Error::Storage(reason))) if reason == "approval request producer dropped"),
                "request producer loss did not end dispatch"
            );
        } else {
            producer
                .send(())
                .expect("original request read remains active");
            assert!(
                matches!(
                    pending.now_or_never(),
                    Some(Ok(Some(InteractionOutcome::Approved {})))
                ),
                "request producer release did not end dispatch"
            );
        }
        assert_eq!(
            reads.requests.swap(0, Ordering::SeqCst),
            1,
            "dispatch repeated its original producer"
        );
    }
    Ok(())
}

#[tokio::test]
async fn approval_policy_binds_before_the_journal_is_shared() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let _shared = storage.journal();
    assert!(
        storage
            .with_approval_policy(ApprovalPolicy::default())
            .is_err()
    );
    Ok(())
}
