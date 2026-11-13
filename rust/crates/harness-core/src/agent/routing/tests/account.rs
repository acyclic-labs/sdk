//! Signed account calls exercise the shared peer admission through real serving.
use super::*;
use acyclic_auth::contract::{
    Grant, GrantKind, GrantSet, PrincipalKind, ResourceGrant, ResourcePattern,
};
use acyclic_stream::{
    BoxProviderFuture, StreamError,
    session::{CallAuthorization, Client},
};
use std::sync::Arc;

type Provider = acyclic_stream::testing::Counted<MemoryStream>;

// The fixture's signed per-session credential conveys its root's label and the
// node relay scope together. There is no default grant or directory authority.
struct PeerPolicy {
    provider: Arc<Provider>,
    node: route::NodeId,
    debit: StreamPath,
}

impl route::AdmissionPolicy for PeerPolicy {
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &route::Envelope,
    ) -> Result<(), StreamError> {
        let principal = authorization.principal()?;
        if principal.kind != PrincipalKind::Agent
            || principal.id != acyclic_auth::encode_component(&message.sender.encode())
            || !principal
                .roles
                .iter()
                .any(|role| role == crate::agent::SEND_PEER)
            || PeerRoute::parse(message).is_err()
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(())
    }

    fn prepare<'a>(
        &'a self,
        _: &'a CallAuthorization,
        message: &'a route::Envelope,
        destination: Option<&'a route::Placement>,
    ) -> BoxProviderFuture<'a, Result<route::Admission, route::Error>> {
        Box::pin(async move {
            // A real companion append proves that preparation is not re-run
            // for an accepted intent or detached from its relay publication.
            let funded = route::Admission::Allow(vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: self.debit.clone(),
                    records: vec![message.request.idempotency_key.as_bytes().to_vec().into()],
                }],
            )]);
            PeerRoute::parse(message)
                .map_err(|_| route::Error::InvalidRecord)?
                .account_admission(self.provider.as_ref(), destination, &self.node, funded)
                .await
                .map_err(|error| match error {
                    Error::Stream(cause) => route::Error::Stream(cause),
                    _ => route::Error::InvalidRecord,
                })
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
    let service = Arc::new(route::AccountService::new(
        first.sender.account().clone(),
        provider.clone(),
        paths.clone(),
        Arc::new(PeerPolicy {
            provider: provider.clone(),
            node,
            debit: debit.clone(),
        }),
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
            matches!(route::admit(&client, slot, message, 4096).await, Err(route::Error::Stream(cause)) if cause.category() == StreamError::AccessDenied)
        );
    }
    let mut control = first.clone();
    control.request.effects[0].mutations[0] = CommitMutation::Append {
        path: StreamPath::new("private/control")?,
        records: vec![b"control".to_vec().into()],
    };
    assert!(
        matches!(route::admit(&client, 0, &control, 4096).await, Err(route::Error::Stream(cause)) if cause.category() == StreamError::AccessDenied)
    );
    let denied = provider.requests().since(before);
    assert_eq!(denied.control(), 0);
    assert_eq!(denied.data(), 0);
    let accepted = route::admit(&client, 0, &first, 4096).await?;
    assert!(matches!(
        accepted,
        route::Routed::Relayed { relay_seq: 0, .. }
    ));
    assert!(
        matches!(
            route::admit(&client, 2, &second, 4096).await?,
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
        route::admit(&client, 0, &first, 4096).await?,
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
        matches!(route::admit(&client, 1, &first, 4096).await,
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
        matches!(route::admit(&client, 0, &changed, 4096).await,
        Err(route::Error::Stream(cause)) if cause.category() == StreamError::IdempotencyMismatch),
        "canonical changed bytes must not reuse an accepted intent"
    );
    let fresh = peer_envelope(&roots[0], &roots[2], "after-off", admission.clone())?;
    assert_eq!(
        route::admit(&client, 0, &fresh, 4096).await?,
        route::Routed::Rejected(route::Rejection::Closed)
    );
    assert_eq!(provider.tail(paths.relay(&destination)?).await?, 2);
    let missing = peer_envelope(&roots[0], &roots[3], "missing", admission)?;
    assert_eq!(
        route::admit(&client, 0, &missing, 4096).await?,
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
        route::admit(&client, 0, &missing, 4096).await?,
        route::Routed::Rejected(route::Rejection::NoSuchActor)
    );
    assert_eq!(provider.tail(paths.relay(&first.source)?).await?, 2);
    assert_eq!(provider.tail(debit).await?, 4);
    for root in &roots {
        assert!(
            matches!(
                provider.tail(root.inbox()?).await,
                Err(StreamError::NotFound)
            ),
            "the account router must not host a session inbox"
        );
    }
    Ok(())
}
