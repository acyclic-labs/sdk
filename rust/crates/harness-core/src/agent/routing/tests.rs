//! Original Stream publications prove the absence fence and its negative control.
use super::*;
use crate::agent::{
    AgentId, ForkAdopted, InboxRecord, IntentId, Message, PeerMembershipHead, Sender, SessionPhase,
    SessionProfile, SessionPublication, SessionRow, create_root_with_peer,
};
use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, MemoryStream,
    RecordHeaders, StreamPath, StreamProvider as _, StreamProviderExt as _,
};

mod account;

#[tokio::test]
async fn account_membership_uses_one_fenced_view_for_both_roots()
-> Result<(), Box<dyn std::error::Error>> {
    for remove_sender in [false, true] {
        let provider = MemoryStream::default();
        let sender = AgentPath::root(&AgentId::from_bytes([1; 16]));
        let recipient = AgentPath::root(&AgentId::from_bytes([2; 16]));
        let witness = PeerMembershipHead {
            tail: 1,
            commit: [0; 32],
        };
        let envelope = peer_envelope(
            &sender,
            &recipient,
            "message",
            PeerAdmission {
                sender: witness.clone(),
                recipient: witness,
            },
        )?;
        let peer = PeerRoute::parse(&envelope)?;
        let paths = route::Paths::new(StreamPath::new("routing")?);
        let account_node = route::NodeId::new(b"account")?;
        let destination = route::Placement {
            actor: envelope.to.clone(),
            node: route::NodeId::new(b"destination")?,
        };
        let no_credit = acyclic_stream::testing::Counted::new(provider.clone());
        let (drained, changed) = futures::channel::oneshot::channel();
        let waiting = peer
            .account_admission(
                &no_credit,
                Some(&destination),
                &account_node,
                route::Admission::Credit {
                    changed: Box::pin(async move {
                        changed.await.map_err(|_| route::Error::InvalidRecord)?;
                        Ok(())
                    }),
                },
            )
            .await?;
        assert_eq!(
            no_credit.requests().control(),
            0,
            "exhausted storage must wait without reading directory rows"
        );
        assert_eq!(no_credit.requests().data(), 0);
        let route::Admission::Credit { changed } = waiting else {
            return Err("credit wait was replaced".into());
        };
        drained
            .send(())
            .map_err(|_| "credit listener disappeared")?;
        changed.await?;
        let mut altered = peer.rejection(account_node.clone(), route::Rejection::Closed)?;
        altered.request.deadline_unix_millis = Some(1);
        assert!(
            peer.account_admission(
                &no_credit,
                Some(&destination),
                &account_node,
                route::Admission::Reject {
                    reason: route::Rejection::Closed,
                    notice: altered,
                    effects: Vec::new(),
                }
            )
            .await
            .is_err(),
            "a host policy must not smuggle a changed Notice through membership admission"
        );
        assert_eq!(no_credit.requests().control(), 0);
        assert_eq!(
            peer.account_membership(&provider, None).await?.0,
            Some(route::Rejection::NoSuchActor)
        );
        assert_eq!(
            peer.account_membership(&provider, Some(&destination))
                .await?
                .0,
            Some(route::Rejection::Closed)
        );
        let profile = SessionProfile {
            phase: SessionPhase::Idle,
            title: "Session".into(),
            workspaces: Vec::new(),
            goal_preview: String::new(),
        };
        let publications = [
            SessionPublication {
                actor: envelope.sender.clone(),
                row: SessionRow::new(envelope.source.clone(), profile.clone(), [7; 32]),
                intent: crate::agent::intent_key("sender-row", &[])?,
            },
            SessionPublication {
                actor: envelope.to.clone(),
                row: SessionRow::new(destination.node.clone(), profile, [7; 32]),
                intent: crate::agent::intent_key("recipient-row", &[])?,
            },
        ];
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: crate::agent::intent_key("rows", &[])?,
                    effects: publications
                        .iter()
                        .map(SessionPublication::effect)
                        .collect::<Result<_, _>>()?,
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        let counted = acyclic_stream::testing::Counted::new(provider.clone());
        let debit = StreamPath::new("storage-debit")?;
        let funded = CommitEffect::hard(
            vec![CommitCondition::Absent {
                path: debit.clone(),
            }],
            vec![CommitMutation::Append {
                path: debit.clone(),
                records: vec![b"debit".to_vec().into()],
            }],
        );
        let decision = peer
            .account_admission(
                &counted,
                Some(&destination),
                &account_node,
                route::Admission::Allow(vec![funded]),
            )
            .await?;
        let route::Admission::Allow(effects) = decision else {
            return Err("enabled peers were refused".into());
        };
        assert_eq!(
            counted
                .requests()
                .class(acyclic_stream::testing::RequestClass::Head),
            1
        );
        assert_eq!(counted.requests().control(), 1);
        assert_eq!(
            counted.requests().data(),
            0,
            "account admission must not load either remote root"
        );
        let removed = &publications[usize::from(!remove_sender)];
        let removal = SessionPublication {
            actor: removed.actor.clone(),
            row: removed.row.changed(None)?.ok_or("removal missing")?,
            intent: crate::agent::intent_key("remove-row", &[])?,
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
        let mut admission = envelope.relay_request(&paths, &destination.node)?;
        admission.effects.extend(effects);
        assert!(
            matches!(
                provider.commit(admission.clone()).await?,
                CommitOutcome::Conflict(_)
            ),
            "either root's opt-out must fence a previously prepared relay"
        );
        assert!(
            matches!(
                provider.tail(debit.clone()).await,
                Err(acyclic_stream::StreamError::NotFound)
            ),
            "a refused publication must not spend its storage debit"
        );
        assert!(
            provider
                .inspect_idempotency(admission.idempotency_key.clone())
                .await?
                .is_none()
        );
        assert_eq!(
            peer.account_membership(&provider, Some(&destination))
                .await?
                .0,
            Some(route::Rejection::Closed)
        );
        let refused = peer
            .account_admission(
                &provider,
                Some(&destination),
                &account_node,
                route::Admission::Allow(Vec::new()),
            )
            .await?;
        let route::Admission::Reject { reason, notice, .. } = refused else {
            return Err("removed peer was admitted".into());
        };
        assert_eq!(reason, route::Rejection::Closed);
        assert_eq!(notice, peer.rejection(account_node.clone(), reason)?);
        assert!(crate::agent::PeerRejectionRoute::parse(&notice).is_ok());
        // Removing only the membership guard reproduces relay admission after
        // opt-out on the same native store.
        admission.effects.pop().ok_or("guard missing")?;
        assert!(matches!(
            provider.commit(admission).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(provider.tail(paths.relay(&destination.node)?).await?, 1);
        let relay = provider
            .record_at(paths.relay(&destination.node)?, 0)
            .await?
            .ok_or("relay missing")?;
        let spent = provider
            .record_at(debit, 0)
            .await?
            .ok_or("funded effect was omitted")?;
        assert_eq!(
            spent.commit_id, relay.commit_id,
            "the host's funding effects must join relay publication atomically"
        );
        let mut wrong = destination.clone();
        wrong.node = route::NodeId::new(b"wrong-node")?;
        assert!(
            peer.account_membership(&provider, Some(&wrong))
                .await
                .is_err()
        );

        let inherited = MemoryStream::default();
        let mut copied = publications[1].effect()?;
        let CommitMutation::Append { path, .. } = &mut copied.mutations[0] else {
            return Err("row append missing".into());
        };
        let original_path = path.clone();
        let other_path = StreamPath::new("other-row")?;
        *path = other_path.clone();
        assert!(matches!(
            inherited
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: crate::agent::intent_key("copy-row", &[])?,
                    effects: vec![publications[0].effect()?, copied]
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        assert!(matches!(
            inherited
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: crate::agent::intent_key("inherit-row", &[])?,
                    effects: vec![CommitEffect::hard(
                        vec![CommitCondition::Absent {
                            path: original_path.clone()
                        }],
                        vec![CommitMutation::Fork {
                            source: other_path,
                            destination: original_path,
                            at_tail: 1,
                            exclude: Vec::new(),
                            records: Vec::new()
                        }]
                    )]
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        assert!(
            peer.account_membership(&inherited, Some(&destination))
                .await
                .is_err(),
            "an inherited enabled row must not certify another root's membership"
        );
    }
    Ok(())
}

#[tokio::test]
async fn absent_refusal_fences_creation_of_a_disabled_recipient()
-> Result<(), Box<dyn std::error::Error>> {
    let reference = MemoryStream::default();
    let destination = MemoryStream::default();
    let recipient = AgentId::from_bytes([1; 16]);
    let sender = AgentPath::root(&AgentId::from_bytes([2; 16]));
    let created = create_root_with_peer(
        &IntentId::try_from("create")?,
        ForkAdopted {
            agent: recipient,
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        },
        false,
    )?;
    assert!(matches!(
        reference.commit(created.request.clone()).await?,
        CommitOutcome::Committed(_)
    ));
    let membership = reference
        .head(created.child.peer_membership()?)
        .await?
        .last
        .ok_or("membership missing")?;
    let witness = PeerMembershipHead {
        tail: 1,
        commit: *membership.commit_id.as_bytes(),
    };
    let envelope = peer_envelope(
        &sender,
        &created.child,
        "message",
        PeerAdmission {
            sender: witness.clone(),
            recipient: witness,
        },
    )?;
    let peer = PeerRoute::parse(&envelope)?;
    let routing = Routing {
        paths: route::Paths::new(StreamPath::new("routing")?),
        node: route::NodeId::new(b"destination")?,
    };
    let account = MemoryStream::default();
    envelope
        .publish_relay(&account, &routing.paths, &routing.node, Vec::new())
        .await?;
    let relay = account
        .record_at(routing.paths.relay(&routing.node)?, 0)
        .await?
        .ok_or("relay missing")?;
    let receipt = route::RelayReceipt {
        commit_id: relay.commit_id,
        sequence: relay.sequence,
    };
    assert!(
        !peer
            .admission
            .recipient
            .retained(&destination, &created.child)
            .await?
    );
    let refusal = peer.absent_request(&routing, &receipt, peer.delivery_request(&receipt)?)?;

    // Creation wins after the missing-witness read but before the conditional
    // refusal. The adversarial future head now matches, but it is disabled.
    assert!(matches!(
        destination.commit(created.request).await?,
        CommitOutcome::Committed(_)
    ));
    assert!(
        peer.admission
            .recipient
            .validate(&destination, &created.child)
            .await
            .is_err()
    );
    assert!(
        matches!(
            destination.commit(refusal.clone()).await?,
            CommitOutcome::Conflict(_)
        ),
        "absence must be established at the original refusal commit"
    );
    assert!(
        destination
            .inspect_idempotency(refusal.idempotency_key.clone())
            .await?
            .is_none()
    );
    assert_eq!(destination.tail(created.child.inbox()?).await?, 1);

    // Removing only the hard absence condition reproduces the bypass: the
    // ordinary soft effect sees a matching disabled head and admits the input.
    let mut unguarded: CommitRequest = refusal;
    for effect in &mut unguarded.effects {
        effect.conditions.retain(|condition| !matches!(condition, CommitCondition::Absent { path } if path == &created.child.inbox().expect("valid inbox")));
    }
    assert!(matches!(
        destination.commit(unguarded).await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(
        destination.tail(created.child.inbox()?).await?,
        2,
        "negative control must admit input to the disabled root"
    );
    Ok(())
}

fn peer_envelope(
    sender: &AgentPath,
    recipient: &AgentPath,
    id: &str,
    admission: PeerAdmission,
) -> Result<route::Envelope, Box<dyn std::error::Error>> {
    let input = InboxRecord::Message(Message {
        from: Sender::Agent {
            path: sender.clone(),
        },
        id: IntentId::try_from(id)?,
        re: None,
        body: Box::new(FileRef::new(
            VolumeRef::new(
                crate::resources::ProviderRef::new(
                    "test".into(),
                    "filesystem".into(),
                    "1".into(),
                    &mut None,
                )?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes(sender.id().to_bytes())),
                &mut None,
            )?,
            "message.md".into(),
            "version-1".into(),
            FileDescriptor::from_bytes(b"message", "text/plain", &mut None)?,
            "message.md".into(),
            &mut None,
        )?),
        attachments: None,
    });
    let mut request = crate::agent::send(
        recipient,
        &Sender::Agent {
            path: sender.clone(),
        },
        &input,
    )?;
    let CommitMutation::Append { records, .. } = &mut request.effects[0].mutations[0] else {
        return Err("inbox append missing".into());
    };
    records[0].headers = RecordHeaders::new(
        Vec::new(),
        vec![acyclic_stream::Attribute {
            name: ADMISSION.into(),
            value: admission.encode()?.into(),
        }],
    )?;
    let actor = |path: &AgentPath| {
        ActorReference::new(b"account", b"agents", path.id().as_str().as_bytes())
    };
    Ok(route::Envelope {
        source: route::NodeId::new(b"source")?,
        sender: actor(sender)?,
        to: actor(recipient)?,
        request,
    })
}
