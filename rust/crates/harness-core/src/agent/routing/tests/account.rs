//! Signed account calls exercise the shared peer admission through real serving.
use super::*;
use acyclic_auth::contract::{
    Grant, GrantKind, GrantSet, PrincipalKind, ResourceGrant, ResourcePattern,
};
use acyclic_stream::{
    BoxProviderFuture, StreamError,
    session::{CallAuthorization, Client},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

type Provider = acyclic_stream::testing::Counted<MemoryStream>;

// The fixture's signed per-session credential conveys its root's label and the
// node relay scope together. There is no default grant or directory authority.
struct FundingPolicy {
    debit: StreamPath,
    denied: AtomicBool,
    pause: Mutex<Option<FundingPause>>,
}

struct FundingPause {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl route::AdmissionPolicy for FundingPolicy {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &route::Envelope,
    ) -> Result<(), StreamError> {
        let principal = authorization.principal()?;
        if self.denied.load(Ordering::SeqCst)
            || principal.kind != PrincipalKind::Agent
            || principal.id != acyclic_auth::encode_component(&message.sender.encode())
            || !principal
                .roles
                .iter()
                .any(|role| role == crate::agent::SEND_PEER)
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        _: &'a CallAuthorization,
        message: &'a route::Envelope,
        _: Option<&'a route::Placement>,
    ) -> BoxProviderFuture<'a, Result<route::Admission, route::Error>> {
        Box::pin(async move {
            let pause = self
                .pause
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take();
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|_| StreamError::Unavailable)?;
                pause.release.await.map_err(|_| StreamError::Unavailable)?;
            }
            // A real companion append proves that preparation is not re-run
            // for an accepted intent or detached from its relay publication.
            Ok(route::Admission::Allow(vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: self.debit.clone(),
                    records: vec![message.request.idempotency_key.as_bytes().to_vec().into()],
                }],
            )]))
        })
    }
}

fn credential(sender: &ActorReference, node: &route::NodeId, peer: bool) -> bytes::Bytes {
    let resource = route::relay_resource(sender.account(), node);
    acyclic_auth::testing::root(|block| {
        let key = &mut block.certificates.as_mut().expect("signed fixture").keys[0].key;
        key.principal.kind = PrincipalKind::Agent;
        key.principal.id = acyclic_auth::encode_component(&sender.encode());
        key.principal.lineage = Vec::new();
        key.principal.roles = if peer {
            vec![crate::agent::SEND_PEER.into()]
        } else {
            Vec::new()
        };
        key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: resource.clone(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }),
            }],
        };
    })
    .into()
}

#[tokio::test]
async fn signed_account_admission_preserves_membership_and_original_outcomes()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(Provider::new(MemoryStream::default()));
    let paths = route::Paths::new(StreamPath::new("routing")?);
    let node = route::NodeId::new(b"account")?;
    let destination = route::NodeId::new(b"destination")?;
    let debit = StreamPath::new("funding")?;
    let roots = [1, 2, 3, 4].map(|id| AgentPath::root(&AgentId::from_bytes([id; 16])));
    let witness = PeerMembershipHead {
        tail: 1,
        commit: [0; 32],
    };
    let admission = PeerAdmission {
        sender: witness.clone(),
        recipient: witness,
    };
    let first = peer_envelope(&roots[0], &roots[2], "first", admission.clone())?;
    let second = peer_envelope(&roots[1], &roots[2], "second", admission.clone())?;
    let profile = SessionProfile {
        phase: SessionPhase::Idle,
        title: "Session".into(),
        workspaces: Vec::new(),
        goal_preview: String::new(),
    };
    let mut publications = Vec::new();
    for (actor, location) in [
        (&first.sender, &first.source),
        (&second.sender, &second.source),
        (&first.to, &destination),
    ] {
        let publication = SessionPublication {
            actor: actor.clone(),
            row: SessionRow::new(location.clone(), profile.clone()),
            intent: acyclic_stream::IdempotencyKey::new(actor.encode())?,
        };
        let mut effects = vec![
            route::Placement {
                actor: actor.clone(),
                node: location.clone(),
            }
            .effect(&paths)?,
        ];
        effects.push(publication.effect()?);
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: publication.intent.clone(),
                    effects
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        publications.push(publication);
    }
    let funding = Arc::new(FundingPolicy {
        debit: debit.clone(),
        denied: AtomicBool::new(false),
        pause: Mutex::new(None),
    });
    let service = Arc::new(route::AccountService::new(
        first.sender.account().clone(),
        provider.clone(),
        paths.clone(),
        Arc::new(super::super::PeerAccountPolicy::new(
            provider.clone(),
            paths.clone(),
            node,
            funding.clone(),
        )),
    ));
    let client = Client::in_process_with_products(
        provider.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        service,
        None,
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(&first.sender, &first.source, true))?;
    client.register_credential(1, credential(&first.sender, &first.source, false))?;
    client.register_credential(2, credential(&second.sender, &second.source, true))?;
    // The node scope alone, another root's session, and control-shaped bytes
    // are all denied before any account metadata or storage is consulted.
    let before = provider.requests();
    for (slot, message) in [(1, &first), (0, &second)] {
        assert!(
            matches!(route::admit(&client, slot, message, Some(4096)).await, Err(route::Error::Stream(cause)) if cause.category() == StreamError::AccessDenied)
        );
    }
    let mut control = first.clone();
    control.request.effects[0].mutations[0] = CommitMutation::Append {
        path: StreamPath::new("private/control")?,
        records: vec![b"control".to_vec().into()],
    };
    assert!(
        matches!(route::admit(&client, 0, &control, Some(4096)).await, Err(route::Error::Stream(cause)) if cause.category() == StreamError::AccessDenied)
    );
    let denied = provider.requests().since(before);
    assert_eq!(denied.control(), 0);
    assert_eq!(denied.data(), 0);
    let accepted = route::admit(&client, 0, &first, Some(4096)).await?;
    assert!(matches!(
        accepted,
        route::Routed::Relayed { relay_seq: 0, .. }
    ));
    assert!(
        matches!(
            route::admit(&client, 2, &second, Some(4096)).await?,
            route::Routed::Relayed { relay_seq: 1, .. }
        ),
        "one node must serve multiple independently granted roots"
    );
    for sequence in 0..2 {
        let relayed = provider
            .record_at(paths.relay(&destination)?, sequence)
            .await?
            .ok_or("relay missing")?;
        let funded = provider
            .record_at(debit.clone(), sequence)
            .await?
            .ok_or("funding missing")?;
        assert_eq!(relayed.commit_id, funded.commit_id);
    }
    let current = &publications[2];
    let removal = SessionPublication {
        actor: current.actor.clone(),
        row: current.row.changed(None)?.ok_or("removal missing")?,
        intent: crate::agent::intent_key("remove", &[])?,
    };
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: removal.intent.clone(),
                effects: vec![removal.effect()?]
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(
        route::admit(&client, 0, &first, Some(4096)).await?,
        accepted,
        "current opt-out must not rewrite an original admission"
    );
    assert_eq!(
        provider.tail(debit.clone()).await?,
        2,
        "exact replay must not spend another storage admission"
    );
    let before = provider.requests();
    assert!(
        matches!(route::admit(&client, 1, &first, Some(4096)).await,
        Err(route::Error::Stream(cause)) if cause.category() == StreamError::AccessDenied),
        "a retained outcome must still require the sender grant"
    );
    let denied = provider.requests().since(before);
    assert_eq!(denied.control(), 0);
    assert_eq!(denied.data(), 0);
    let mut changed = first.clone();
    let CommitMutation::Append { records, .. } = &mut changed.request.effects[0].mutations[0]
    else {
        return Err("message append missing".into());
    };
    let mut input = InboxRecord::decode(&records[0].value)?;
    let InboxRecord::Message(message) = &mut input else {
        return Err("message missing".into());
    };
    message.re = Some(IntentId::try_from("different-thread")?);
    records[0].value = input.encode()?.into();
    assert!(
        matches!(route::admit(&client, 0, &changed, Some(4096)).await,
        Err(route::Error::Stream(cause)) if cause.category() == StreamError::IdempotencyMismatch),
        "canonical changed bytes must not reuse an accepted intent"
    );
    let fresh = peer_envelope(&roots[0], &roots[2], "after-off", admission.clone())?;
    assert_eq!(
        route::admit(&client, 0, &fresh, Some(4096)).await?,
        route::Routed::Rejected(route::Rejection::Closed)
    );
    assert_eq!(provider.tail(paths.relay(&destination)?).await?, 2);
    let missing = peer_envelope(&roots[0], &roots[3], "missing", admission)?;
    assert_eq!(
        route::admit(&client, 0, &missing, Some(4096)).await?,
        route::Routed::Rejected(route::Rejection::NoSuchActor)
    );
    for (sequence, original) in [&fresh, &missing].into_iter().enumerate() {
        let record = provider
            .record_at(paths.relay(&first.source)?, sequence as u64)
            .await?
            .ok_or("refusal missing")?;
        let refused = route::RelayRecord {
            envelope: route::Envelope::decode(&record.value)?,
            receipt: route::RelayReceipt {
                commit_id: record.commit_id,
                sequence: record.sequence,
            },
            headers: record.headers,
        };
        assert!(crate::agent::PeerRejectionRoute::parse(&refused.envelope).is_ok());
        assert_eq!(
            refused.rejection(original)?,
            Some(if sequence == 0 {
                route::Rejection::Closed
            } else {
                route::Rejection::NoSuchActor
            })
        );
        let funded = provider
            .record_at(debit.clone(), sequence as u64 + 2)
            .await?
            .ok_or("refusal funding missing")?;
        assert_eq!(funded.commit_id, refused.receipt.commit_id);
    }
    let later = route::Placement {
        actor: missing.to.clone(),
        node: destination.clone(),
    };
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: acyclic_stream::IdempotencyKey::new(missing.to.encode())?,
                effects: vec![later.effect(&paths)?]
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(
        route::admit(&client, 0, &missing, Some(4096)).await?,
        route::Routed::Rejected(route::Rejection::NoSuchActor)
    );
    assert_eq!(provider.tail(paths.relay(&first.source)?).await?, 2);
    assert_eq!(provider.tail(debit.clone()).await?, 4);
    for root in &roots {
        assert!(
            matches!(
                provider.tail(root.inbox()?).await,
                Err(StreamError::NotFound)
            ),
            "the account router must not host a session inbox"
        );
    }
    // A receiver that opted out must still settle its previously admitted input.
    client.register_credential(3, credential(&first.to, &destination, true))?;
    let original = provider
        .record_at(paths.relay(&destination)?, 0)
        .await?
        .ok_or("original relay missing")?;
    let receipt = route::RelayReceipt {
        commit_id: original.commit_id,
        sequence: original.sequence,
    };
    let peer = PeerRoute::parse(&first)?;
    let status = crate::agent::SentReplyStatus::Answered { sequence: 1 };
    let second_record = provider
        .record_at(paths.relay(&destination)?, 1)
        .await?
        .ok_or("second relay missing")?;
    let unrelated = route::RelayReceipt {
        commit_id: second_record.commit_id,
        sequence: second_record.sequence,
    };
    let forged = peer.result(destination.clone(), &unrelated, status.clone())?;
    assert!(
        route::admit(&client, 3, &forged, Some(4096)).await.is_err(),
        "a valid receipt for another send authorized this settlement"
    );
    assert_eq!(provider.tail(debit.clone()).await?, 4);
    let terminal = peer.result(destination.clone(), &receipt, status)?;
    let before = provider.named_head_selections();
    let outcome = route::admit(&client, 3, &terminal, Some(4096)).await?;
    assert!(matches!(
        outcome,
        route::Routed::Relayed { relay_seq: 2, .. }
    ));
    assert_eq!(
        provider.named_head_selections() - before,
        0,
        "terminal admission must not select current membership heads"
    );
    assert_eq!(
        route::admit(&client, 3, &terminal, Some(4096)).await?,
        outcome
    );
    assert_eq!(
        provider.tail(debit).await?,
        5,
        "exact terminal replay debited storage twice"
    );
    let revoked = peer_envelope(
        &roots[0],
        &roots[2],
        "revoked-during-funding",
        PeerRoute::parse(&first)?.admission.clone(),
    )?;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *funding.pause.lock().map_err(|_| "funding lock poisoned")? = Some(FundingPause {
        entered,
        release: resume,
    });
    let revoke = async {
        ready.await?;
        let before = provider.requests();
        funding.denied.store(true, Ordering::SeqCst);
        release.send(()).map_err(|_| "funding pause closed")?;
        Ok::<_, Box<dyn std::error::Error>>(before)
    };
    let (result, before) = futures::join!(route::admit(&client, 0, &revoked, Some(4096)), revoke);
    assert!(
        matches!(result, Err(route::Error::Stream(cause)) if cause.category() == StreamError::AccessDenied)
    );
    let after_revocation = provider.requests().since(before?);
    assert_eq!(
        after_revocation.control(),
        0,
        "revoked grant read another membership cut"
    );
    assert_eq!(
        after_revocation.data(),
        0,
        "revoked grant published a storage debit or relay"
    );
    Ok(())
}
