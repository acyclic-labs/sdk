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
                .decide_approval(
                    id,
                    ApprovalResponder::Human("intruder"),
                    approved.into(),
                    None
                )
                .await
                .is_err()
        );
        let human = ApprovalResponder::Human("human");
        assert_eq!(
            storage
                .decide_approval(id, human, approved.into(), None)
                .await?,
            Some(settled.clone())
        );
        // The same decision replays; the journal sees the settled outcome.
        assert_eq!(
            storage
                .decide_approval(id, human, approved.into(), None)
                .await?,
            Some(settled.clone())
        );
        assert!(
            storage
                .decide_approval(id, human, (!approved).into(), None)
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
            &mut None,
        )?,
    ));
    let effects = Arc::new(
        crate::filesystem::agent_effects::FilesystemTurnEffects::new(
            files,
            storage.stream.clone(),
            acyclic_actors::addressing::ActorReference::new(b"test", b"agents", b"owner")
                .expect("approval actor namespace"),
            AgentPath::root(&agent),
            (OperationId::new(), 0),
            65_536,
        ),
    );
    let reads = Arc::new(Reads {
        files: storage.internal_files.clone(),
        requests: AtomicUsize::new(0),
        release: Mutex::new(None),
    });
    let VolumeOwner::Agent(owner) = storage.volume.owner() else {
        return Err(Error::Invalid("expected agent owner".into()));
    };
    let policy = AgentApprovalPolicy::new(
        crate::agent::AgentId::from_bytes(owner.into_bytes()),
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
        owner.outcome(id, &*storage.journal(), None).await?,
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
        &storage.volume,
        storage.internal_files.clone(),
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
        let mut pending = Box::pin(owner.outcome(id, &*journal, None));
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

#[tokio::test]
async fn an_exact_human_rule_replays_and_keeps_agent_policy_and_action_scope() -> Result<()> {
    use crate::interaction::{ApprovalTarget, ApprovalVerdict};
    let owner = AgentId::new();
    let storage = MemoryHarnessStorage::new(owner.clone(), 65_536).await?;
    let journal = storage.journal();
    let denied = InteractionId::new();
    journal.open_interaction(denied, &approval(), None)?.await?;
    assert_eq!(
        storage
            .decide_approval(
                denied,
                ApprovalResponder::Human("human"),
                ApprovalVerdict::Deny,
                None
            )
            .await?,
        Some(InteractionOutcome::Declined {})
    );
    let id = InteractionId::new();
    let original = approval();
    journal.open_interaction(id, &original, None)?.await?;
    assert_eq!(journal.interaction_outcome(id, None)?.await?, None);
    assert!(
        storage
            .decide_approval(
                id,
                ApprovalResponder::Human("intruder"),
                ApprovalVerdict::ApproveRule,
                None
            )
            .await
            .is_err()
    );
    assert!(
        storage
            .decide_approval(
                id,
                ApprovalResponder::Parent(&crate::agent::AgentId::from_bytes(
                    AgentId::new().into_bytes(),
                )),
                ApprovalVerdict::ApproveRule,
                None
            )
            .await
            .is_err()
    );
    for _ in 0..2 {
        assert_eq!(
            storage
                .decide_approval(
                    id,
                    ApprovalResponder::Human("human"),
                    ApprovalVerdict::ApproveRule,
                    None
                )
                .await?,
            Some(InteractionOutcome::Approved {})
        );
    }
    // A retry cannot change an already-recorded rule decision into once/deny.
    assert!(
        storage
            .decide_approval(
                id,
                ApprovalResponder::Human("human"),
                ApprovalVerdict::Approve,
                None
            )
            .await
            .is_err()
    );
    // New operation/call identities are not part of the standing action rule.
    let mut same = approval();
    if let Interaction::Approval {
        subject: ApprovalSubject::Tool { call_id, .. },
        ..
    } = &mut same
    {
        *call_id = "another-call".into();
    }
    let repeat = InteractionId::new();
    journal.open_interaction(repeat, &same, None)?.await?;
    assert_eq!(
        journal.interaction_outcome(repeat, None)?.await?,
        Some(InteractionOutcome::Approved {})
    );
    for variant in 0..5 {
        let mut changed = approval();
        let Interaction::Approval { subject, .. } = &mut changed else {
            unreachable!()
        };
        let ApprovalSubject::Tool {
            arguments,
            tool,
            target,
            route,
            ..
        } = subject
        else {
            unreachable!()
        };
        match variant {
            0 => arguments["path"] = json!("other"),
            1 => arguments["extra"] = json!(true),
            2 => *tool = "shell".into(),
            3 => {
                *target = Some(ApprovalTarget {
                    device: "other".into(),
                    owner: "someone".into(),
                })
            }
            _ => *route = ApprovalRoute::Human,
        }
        let request = InteractionId::new();
        journal.open_interaction(request, &changed, None)?.await?;
        assert_eq!(journal.interaction_outcome(request, None)?.await?, None);
        if variant == 4 {
            assert!(
                storage
                    .decide_approval(
                        request,
                        ApprovalResponder::Human("human"),
                        ApprovalVerdict::ApproveRule,
                        None
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                storage
                    .decide_approval(
                        request,
                        ApprovalResponder::Human("human"),
                        ApprovalVerdict::Approve,
                        None
                    )
                    .await?,
                Some(InteractionOutcome::Approved {})
            );
        }
    }
    assert_eq!(
        journal.interaction_outcome(denied, None)?.await?,
        Some(InteractionOutcome::Declined {}),
        "a later rule must not rewrite an original denial"
    );
    let opaque = Interaction::Approval {
        prompt: "private action".into(),
        operation_id: OperationId::new(),
        action_digest: [9; 32],
        subject: ApprovalSubject::Opaque {},
    };
    let opaque_id = InteractionId::new();
    journal.open_interaction(opaque_id, &opaque, None)?.await?;
    assert!(
        storage
            .decide_approval(
                opaque_id,
                ApprovalResponder::Human("human"),
                ApprovalVerdict::ApproveRule,
                None
            )
            .await
            .is_err()
    );
    assert_eq!(journal.interaction_outcome(opaque_id, None)?.await?, None);
    // Even an inherited protected file set does not delegate to another agent
    // or a different immutable policy. The original rule owner is explicit.
    let mut changed_policy = ApprovalPolicy::default();
    changed_policy
        .approvers
        .push(crate::approvals::Approver::Parent);
    for (agent, policy, scope_conflict) in [
        (AgentId::new(), ApprovalPolicy::default(), true),
        (owner, ApprovalPolicy::deny_all("deny"), false),
        (owner, changed_policy, true),
    ] {
        let policy = AgentApprovalPolicy::new(
            crate::agent::AgentId::from_bytes(agent.into_bytes()),
            policy,
            None,
            Default::default(),
            Limits {
                file_bytes: 65_536,
                ..Limits::default()
            },
        )?;
        let approvals = AgentApprovals::new(storage.internal_files.clone(), Arc::new(policy));
        let retained = approvals.outcome(id, journal.as_ref(), None).await;
        if scope_conflict {
            assert!(
                matches!(retained, Err(Error::Conflict(_))),
                "a copied rule receipt authorized another scope: {retained:?}"
            );
        } else {
            assert_eq!(retained?, Some(InteractionOutcome::Denied {}));
        }
        assert!(
            matches!(
                approvals
                    .decide(
                        id,
                        ApprovalResponder::Human("human"),
                        ApprovalVerdict::ApproveRule,
                        None
                    )
                    .await,
                Err(Error::Conflict(_))
            ),
            "an old decision minted a grant under a new owner or policy"
        );
        let request = InteractionId::new();
        approvals.open(request, &approval()).await?;
        assert_ne!(
            approvals.outcome(request, journal.as_ref(), None).await?,
            Some(InteractionOutcome::Approved {})
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_interrupted_rule_publication_replays_the_original_decision() -> Result<()> {
    use crate::{approvals::ApprovalCreate, interaction::ApprovalVerdict};
    use std::sync::atomic::{AtomicBool, Ordering};
    struct FailGrantOnce {
        files: Arc<dyn ApprovalFiles>,
        fail: AtomicBool,
    }
    impl ApprovalFiles for FailGrantOnce {
        fn read<'a>(
            &'a self,
            path: &'a str,
            maximum: u64,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
            self.files.read(path, maximum)
        }
        fn create<'a>(
            &'a self,
            path: &'a str,
            bytes: Vec<u8>,
            maximum: u64,
        ) -> BoxFuture<'a, Result<ApprovalCreate>> {
            Box::pin(async move {
                if path.starts_with("approvals/rules/") && self.fail.swap(false, Ordering::AcqRel) {
                    return Err(Error::Storage("injected rule publication failure".into()));
                }
                self.files.create(path, bytes, maximum).await
            })
        }
    }
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent.clone(), 65_536).await?;
    let files = Arc::new(FailGrantOnce {
        files: storage.internal_files.clone(),
        fail: AtomicBool::new(true),
    });
    let policy = AgentApprovalPolicy::new(
        crate::agent::AgentId::from_bytes(agent.into_bytes()),
        ApprovalPolicy::default(),
        None,
        Default::default(),
        Limits {
            file_bytes: 65_536,
            ..Limits::default()
        },
    )?;
    let approvals = AgentApprovals::new(files, Arc::new(policy));
    let original = InteractionId::new();
    approvals.open(original, &approval()).await?;
    assert!(
        approvals
            .decide(
                original,
                ApprovalResponder::Human("human"),
                ApprovalVerdict::ApproveRule,
                None
            )
            .await
            .is_err()
    );
    // The real protected volume retained the first decision; an error neither
    // hides it nor silently retries its standing-rule publication.
    let path = format!(
        "approvals/{}/decisions/{}.json",
        crate::approvals::agent_request_id(original),
        crate::interaction::ApprovalDecisionFile::principal_file("human")
    );
    let bytes = storage
        .internal_files
        .read(&path, 65_536)
        .await?
        .ok_or_else(|| Error::NotFound("original rule decision".into()))?;
    assert_eq!(
        crate::interaction::ApprovalDecisionFile::decode(&bytes, 65_536)?.verdict,
        ApprovalVerdict::ApproveRule
    );
    let later = InteractionId::new();
    approvals.open(later, &approval()).await?;
    assert_eq!(
        approvals
            .outcome(later, storage.journal.as_ref(), None)
            .await?,
        None
    );
    approvals
        .decide(
            original,
            ApprovalResponder::Human("human"),
            ApprovalVerdict::ApproveRule,
            None,
        )
        .await?;
    assert_eq!(
        approvals
            .outcome(later, storage.journal.as_ref(), None)
            .await?,
        Some(InteractionOutcome::Approved {})
    );
    Ok(())
}
