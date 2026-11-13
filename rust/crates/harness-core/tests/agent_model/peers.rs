//! Peer membership uses the same atomic root creation and archive publications.
use super::*;
use acyclic_harness_core::Capabilities;
use proptest::prelude::*;

#[tokio::test]
async fn routed_peer_delivery_uses_only_owning_node_membership_and_retains_refusal() -> Result {
    use acyclic_actors::{
        addressing::ActorReference,
        domain::ActorLimits,
        local::SystemClock,
        route::{self, NodeId, Paths},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Recipient(Arc<AtomicUsize>);
    impl TurnDriver for Recipient {
        fn step<'a>(
            &'a self,
            _: &'a AgentState,
            _: u64,
            _: Option<Queued>,
            _: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(TurnStep {
                    checkpoint: None,
                    outs: Vec::new(),
                    wait: Wait::Input,
                    drops: None,
                })
            })
        }
    }
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        for opt_out_before_delivery in [false, true] {
            let source = MemoryStream::default();
            let destination = Arc::new(MemoryStream::default());
            let sender = create(&source, id(60)).await?;
            let recipient = AgentPath::root(&id(61));
            let within = ActorReference::new(b"account", b"agents", b"owner")?;
            let calls = Arc::new(AtomicUsize::new(0));
            let host = AgentHost::new(
                destination.clone(),
                Recipient(calls.clone()),
                within.clone(),
                ActorLimits::default(),
                Arc::new(SystemClock),
            )?;
            host.create(&intent("recipient"), adopted(recipient.id()))
                .await?;
            let head =
                async |provider: &MemoryStream, root: &AgentPath| -> Result<PeerMembershipHead> {
                    let record = provider
                        .head(root.peer_membership()?)
                        .await?
                        .last
                        .ok_or("membership missing")?;
                    Ok(PeerMembershipHead {
                        tail: record.sequence + 1,
                        commit: *record.commit_id.as_bytes(),
                    })
                };
            let admission = PeerAdmission {
                sender: head(&source, &sender).await?,
                recipient: head(&destination, &recipient).await?,
            };
            let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
            let step = Step {
                outs: effect_outs(
                    &state,
                    vec![TurnEffect::Send {
                        peer: Some(admission),
                        delivery,
                        to: recipient.clone(),
                        id: intent("request"),
                        re: None,
                        body: Box::new(super::driver::file("message.md")?),
                        attachments: None,
                    }],
                ),
                state,
                status: None,
                alarm: Alarm::Clear,
            };
            let routing = Routing {
                paths: Paths::new(StreamPath::new("routing")?),
                node: NodeId::new(b"source")?,
            };
            submit(
                &source,
                routing.lower(
                    &within,
                    &BTreeSet::from([recipient.clone()]),
                    &step,
                    1,
                    intent_key("source", &[])?,
                )?,
            )
            .await?;
            let record = source
                .record_at(routing.paths.outbox(&routing.node)?, 0)
                .await?
                .ok_or("outbox missing")?;
            let envelope = route::Publication::decode(&record.value)?.delivery()?;
            drop(source);
            let peer = PeerRoute::parse(&envelope)?;
            if opt_out_before_delivery {
                host.set_peer_membership(&recipient, &user(), &intent("off"), false)
                    .await?;
            }
            let expected = if opt_out_before_delivery {
                Delivered::Closed
            } else {
                Delivered::Committed { inbox: 1 }
            };
            let activation = actor_store(&destination)?.activation(&host.actor(&recipient)?);
            let activation_path = activation.mutations[0].path().clone();
            let before_activation = destination.tail(activation_path.clone()).await?;
            assert_eq!(peer.deliver(&host).await?, expected);
            assert_eq!(
                destination.tail(activation_path).await?,
                before_activation + u64::from(!opt_out_before_delivery),
                "a skipped inbox append published a wake"
            );
            host.set_peer_membership(
                &recipient,
                &user(),
                &intent("change-membership"),
                opt_out_before_delivery,
            )
            .await?;
            assert_eq!(
                peer.deliver(&host).await?,
                expected,
                "exact replay changed its original outcome"
            );
            assert_eq!(
                destination.tail(recipient.inbox()?).await?,
                if opt_out_before_delivery { 1 } else { 2 }
            );
            host.drain(&recipient).await?;
            assert_eq!(
                calls.load(Ordering::SeqCst),
                usize::from(!opt_out_before_delivery)
            );
            assert!(
                matches!(
                    destination.tail(sender.peer_membership()?).await,
                    Err(StreamError::NotFound)
                ),
                "sender state must remain on its own node"
            );
            if !opt_out_before_delivery {
                let records = values(&*destination, recipient.inbox()?).await?;
                let input = InboxRecord::decode(&records[1])?;
                match (input, delivery) {
                    (InboxRecord::Message(message), MessageDelivery::Steer)
                    | (InboxRecord::FollowUp(message), MessageDelivery::FollowUp) => {
                        assert_eq!(message.from, Sender::Agent { path: sender });
                    }
                    _ => return Err("routed delivery changed receiver semantics".into()),
                }
            }
            let mut altered = envelope.clone();
            altered.request.effects[0]
                .conditions
                .push(CommitCondition::Present {
                    path: StreamPath::new("foreign/private")?,
                });
            assert!(
                PeerRoute::parse(&altered).is_err(),
                "caller-selected reads were admitted"
            );
            let mut altered = envelope.clone();
            altered.request.effects.push(CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: StreamPath::new("foreign/private")?,
                    records: vec!["write".into()],
                }],
            ));
            assert!(
                PeerRoute::parse(&altered).is_err(),
                "foreign writes were admitted"
            );
            let mut altered = envelope.clone();
            altered.request.idempotency_key = intent_key("substituted", &[])?;
            assert!(
                PeerRoute::parse(&altered).is_err(),
                "substituted intent was admitted"
            );
            let mut altered = envelope.clone();
            altered.to =
                ActorReference::new(b"account", b"other", recipient.id().as_str().as_bytes())?;
            assert!(
                PeerRoute::parse(&altered).is_err(),
                "foreign namespace was admitted"
            );
            let mut altered = envelope.clone();
            altered.sender =
                ActorReference::new(b"other-account", b"agents", envelope.sender.id().as_bytes())?;
            altered.to =
                ActorReference::new(b"other-account", b"agents", envelope.to.id().as_bytes())?;
            assert!(
                PeerRoute::parse(&altered)?.deliver(&host).await.is_err(),
                "foreign account reached an owning-node writer"
            );
            for input in [
                InboxRecord::Archive(Control {
                    by: user(),
                    id: intent("archive"),
                }),
                InboxRecord::Message(Message {
                    from: user(),
                    id: intent("request"),
                    re: None,
                    body: Box::new(super::driver::file("message.md")?),
                    attachments: None,
                }),
            ] {
                let mut altered = envelope.clone();
                let CommitMutation::Append { records, .. } =
                    &mut altered.request.effects[0].mutations[0]
                else {
                    unreachable!()
                };
                records[0].value = input.encode()?.into();
                assert!(
                    PeerRoute::parse(&altered).is_err(),
                    "control or user input was admitted"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn remote_send_recovers_original_admission_from_its_source_publication() -> Result {
    use acyclic_actors::{
        addressing::ActorReference,
        route::{self, NodeId, Paths, Routed},
    };
    use acyclic_stream::{FollowRequest, WhenAbsent};
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        let local = MemoryStream::default();
        let account = MemoryStream::default();
        let sender = create(&local, id(50)).await?;
        let recipient = create(&local, id(51)).await?;
        let within = ActorReference::new(b"account", b"agents", sender.id().as_str().as_bytes())?;
        let routing = Routing {
            paths: Paths::new(StreamPath::new("routing")?),
            node: NodeId::new(b"source")?,
        };
        let destination = NodeId::new(b"destination")?;
        let admission =
            session_peer_admission(&local, &Capabilities::new([SEND_PEER]), &sender, &recipient)
                .await?
                .ok_or("peer admission missing")?;
        let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
        let names = ["first", "second"];
        let step = Step {
            outs: effect_outs(
                &state,
                names
                    .iter()
                    .map(|name| TurnEffect::Send {
                        peer: Some(admission.clone()),
                        delivery,
                        to: recipient.clone(),
                        id: intent(name),
                        re: None,
                        body: Box::new(super::driver::file("message.md").expect("valid message")),
                        attachments: None,
                    })
                    .collect(),
            ),
            state,
            status: None,
            alarm: Alarm::Clear,
        };
        let request = routing.lower(
            &within,
            &BTreeSet::from([recipient.clone()]),
            &step,
            1,
            intent_key("source-step", &[])?,
        )?;
        // Recovery cannot discover an outbox record without its sender marker.
        assert!(sent(&local, &sender, &intent("first")).await?.is_none());
        let receipt = match local.commit(request.clone()).await? {
            CommitOutcome::Committed(receipt) => receipt,
            _ => return Err("source publication conflicted".into()),
        };
        submit(&local, request).await?;
        let outbox = routing.paths.outbox(&routing.node)?;
        assert_eq!(local.tail(outbox.clone()).await?, 2);
        assert_eq!(
            local.tail(recipient.inbox()?).await?,
            1,
            "source never writes a foreign inbox"
        );
        for (sequence, name) in names.iter().enumerate() {
            let marker = local
                .head(sender.sent_message(&intent(name))?)
                .await?
                .last
                .ok_or("marker missing")?;
            let record = local
                .record_at(outbox.clone(), sequence as u64)
                .await?
                .ok_or("outbox missing")?;
            assert_eq!(record.commit_id, marker.commit_id);
            assert_eq!(record.commit_id, receipt.commit_id);
            let envelope = route::Publication::decode(&record.value)?.delivery()?;
            let CommitMutation::Append { records, .. } = &envelope.request.effects[0].mutations[0]
            else {
                return Err("message append missing".into());
            };
            let original = InboxRecord::decode(&records[0].value)?;
            let (InboxRecord::Message(message) | InboxRecord::FollowUp(message)) = &original else {
                return Err("not a message".into());
            };
            assert_eq!(
                message.from,
                Sender::Agent {
                    path: sender.clone()
                }
            );
            assert_eq!(
                envelope.request.idempotency_key,
                send(&recipient, &message.from, &original)?.idempotency_key
            );
            assert_eq!(
                sent(&local, &sender, &intent(name))
                    .await?
                    .ok_or("receipt missing")?
                    .delivery,
                SentDelivery::Pending {
                    source: routing.paths.outbox_cursor(&routing.node)?,
                    from: sequence as u64
                }
            );
        }
        let cursor = routing.paths.outbox_cursor(&routing.node)?;
        let mut changes = acyclic_stream::records_only(
            local
                .follow(FollowRequest {
                    when_absent: WhenAbsent::Wait,
                    ..FollowRequest::new(cursor, 1)
                })
                .await?,
        );
        let forward = route::forward(
            &local,
            &routing.paths,
            &routing.node,
            async |_| Err(route::Error::InvalidRecord),
            async |envelope| {
                envelope
                    .publish_relay(&account, &routing.paths, &destination, Vec::new())
                    .await
            },
        );
        let acknowledged = changes.try_next();
        futures::pin_mut!(forward, acknowledged);
        match futures::future::select(forward, acknowledged).await {
            futures::future::Either::Right((record, _)) => assert!(record?.is_some()),
            futures::future::Either::Left((result, _)) => {
                return Err(format!("forward stopped: {result:?}").into());
            }
        }
        for (sequence, name) in names.iter().enumerate() {
            let record = local
                .record_at(outbox.clone(), sequence as u64)
                .await?
                .ok_or("outbox missing")?;
            let envelope = route::Publication::decode(&record.value)?.delivery()?;
            let expected = Routed::Relayed {
                key: envelope.request.idempotency_key.clone(),
                relay_seq: sequence as u64,
            };
            assert_eq!(
                envelope
                    .publish_relay(&account, &routing.paths, &destination, Vec::new())
                    .await?,
                expected
            );
            assert_eq!(
                sent(&local, &sender, &intent(name))
                    .await?
                    .ok_or("receipt missing")?
                    .delivery,
                SentDelivery::Complete(expected)
            );
        }
        assert_eq!(account.tail(routing.paths.relay(&destination)?).await?, 2);
        assert_eq!(local.tail(recipient.inbox()?).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn archive_closes_a_root_without_peer_membership() -> Result {
    let provider = MemoryStream::default();
    let mut root = create_root(&intent("pre-peer-root"), adopted(id(42)))?;
    let membership = root.child.peer_membership()?;
    // Publish the original root shape, without the optional peer admission.
    for effect in &mut root.request.effects {
        effect
            .conditions
            .retain(|condition| condition.path() != &membership);
        effect
            .mutations
            .retain(|mutation| mutation.path() != &membership);
    }
    submit(&provider, root.request).await?;
    let request = archive(&root.child, &user(), &intent("archive"))?;
    assert!(matches!(
        submit(&provider, request.clone()).await?,
        Submitted::Committed(_)
    ));
    submit(&provider, request).await?;
    assert_eq!(provider.tail(root.child.inbox()?).await?, 2);
    assert_eq!(provider.tail(root.child.closed()?).await?, 1);
    assert!(matches!(
        provider.tail(membership).await,
        Err(StreamError::NotFound)
    ));
    Ok(())
}

#[tokio::test]
async fn either_opt_out_fences_prepared_peer_delivery_and_replay() -> Result {
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        for opt_out_sender in [false, true] {
            let provider = MemoryStream::default();
            let sender = create(&provider, id(40)).await?;
            let recipient = create(&provider, id(41)).await?;
            let grants = Capabilities::new([SEND_PEER]);
            let admission = session_peer_admission(&provider, &grants, &sender, &recipient)
                .await?
                .ok_or("enabled roots not admitted")?;
            let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
            let effect = |peer, name| TurnEffect::Send {
                peer: Some(peer),
                delivery,
                to: recipient.clone(),
                id: intent(name),
                re: None,
                body: Box::new(super::driver::file("peer.md").expect("valid file")),
                attachments: None,
            };
            let step = Step {
                outs: effect_outs(&state, vec![effect(admission, "before-opt-out")]),
                state: state.clone(),
                status: None,
                alarm: Alarm::Clear,
            };
            let request = lower(&step, 1, acyclic_stream::IdempotencyKey::unique())?;
            let changed = if opt_out_sender { &sender } else { &recipient };
            submit(
                &provider,
                set_peer_membership(changed, &user(), &intent("off"), false)?,
            )
            .await?;
            assert!(matches!(
                submit(&provider, request.clone()).await?,
                Submitted::Committed(_)
            ));
            assert_eq!(
                provider.tail(recipient.inbox()?).await?,
                1,
                "no input after opt-out"
            );
            assert_eq!(
                sent(&provider, &sender, &intent("before-opt-out"))
                    .await?
                    .ok_or("missing rejection receipt")?
                    .delivery,
                SentDelivery::Complete(acyclic_actors::route::Routed::Rejected(
                    acyclic_actors::route::Rejection::Closed
                ))
            );
            submit(
                &provider,
                set_peer_membership(changed, &user(), &intent("on"), true)?,
            )
            .await?;
            submit(&provider, request).await?;
            assert_eq!(
                provider.tail(recipient.inbox()?).await?,
                1,
                "replay cannot undo refusal"
            );
            let admission = session_peer_admission(&provider, &grants, &sender, &recipient)
                .await?
                .ok_or("re-enabled roots not admitted")?;
            let step = Step {
                outs: effect_outs(&state, vec![effect(admission, "after-opt-in")]),
                state,
                status: None,
                alarm: Alarm::Clear,
            };
            submit(
                &provider,
                lower(&step, 1, acyclic_stream::IdempotencyKey::unique())?,
            )
            .await?;
            assert_eq!(provider.tail(recipient.inbox()?).await?, 2);
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn peer_send_requires_label_and_both_member_roots(
        granted in any::<bool>(),
        sender_member in any::<bool>(),
        recipient_member in any::<bool>(),
        sender_child in any::<bool>(),
        recipient_child in any::<bool>(),
        self_send in any::<bool>(),
    ) {
        let observed = futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let sender = create(&provider, id(20)).await?;
            let recipient = create(&provider, id(21)).await?;
            let recipient = if self_send { sender.clone() } else { recipient };
            for (root, enabled) in [(&sender, sender_member), (&recipient, recipient_member)] {
                if !enabled {
                    assert!(matches!(submit(&provider, set_peer_membership(
                        root, &user(), &intent("opt-out"), false,
                    )?).await?, Submitted::Committed(_)));
                }
            }
            let sender = if sender_child { sender.child(&id(22))? } else { sender };
            let recipient = if recipient_child { recipient.child(&id(23))? } else { recipient };
            let grants = Capabilities::new(granted.then_some(SEND_PEER));
            Ok::<_, Box<dyn std::error::Error>>(session_peer_allowed(
                &provider, &grants, &sender, &recipient,
            ).await?)
        }).expect("membership publication");
        prop_assert_eq!(observed,
            granted && sender_member && recipient_member && !sender_child && !recipient_child && !self_send);
    }
}

#[tokio::test]
async fn peer_opt_out_is_symmetric_user_only_and_archive_fences_rejoin() -> Result {
    let provider = MemoryStream::default();
    let a = create(&provider, id(30)).await?;
    let b = create(&provider, id(31)).await?;
    let grants = Capabilities::new([SEND_PEER]);
    assert!(session_peer_allowed(&provider, &grants, &a, &b).await?);
    let mut launch = adopted(id(32));
    launch.profile = Some(acyclic_harness_core::templates::root()?);
    let born_off = create_root_with_peer(&intent("create-off"), launch, false)?;
    let opted_out = born_off.child.clone();
    assert!(matches!(
        submit(&provider, born_off.request).await?,
        Submitted::Committed(_)
    ));
    assert!(!session_peer_allowed(&provider, &grants, &opted_out, &a).await?);
    assert!(!session_peer_allowed(&provider, &grants, &a, &opted_out).await?);
    let records = values(&provider, opted_out.inbox()?).await?;
    let InboxRecord::ForkAdopted(adopted) = InboxRecord::decode(&records[0])? else {
        return Err("root adoption missing".into());
    };
    assert!(
        adopted
            .profile
            .as_ref()
            .is_some_and(|profile| profile.peers.contains(SEND_PEER))
    );
    for by in [
        Sender::Agent { path: a.clone() },
        Sender::Host {
            host: "model".into(),
        },
    ] {
        assert!(set_peer_membership(&a, &by, &intent("forbidden"), false).is_err());
    }
    let off = set_peer_membership(&a, &user(), &intent("off"), false)?;
    submit(&provider, off.clone()).await?;
    submit(&provider, off).await?;
    assert_eq!(provider.tail(a.peer_membership()?).await?, 2);
    assert!(!session_peer_allowed(&provider, &grants, &a, &b).await?);
    assert!(!session_peer_allowed(&provider, &grants, &b, &a).await?);
    submit(
        &provider,
        set_peer_membership(&a, &user(), &intent("on"), true)?,
    )
    .await?;
    assert!(session_peer_allowed(&provider, &grants, &a, &b).await?);
    let rejoin = set_peer_membership(&a, &user(), &intent("stale-on"), true)?;
    submit(&provider, archive(&a, &user(), &intent("archive"))?).await?;
    assert!(!session_peer_allowed(&provider, &grants, &a, &b).await?);
    assert!(!session_peer_allowed(&provider, &grants, &b, &a).await?);
    assert!(matches!(
        submit(&provider, rejoin).await?,
        Submitted::RecipientClosed(_)
    ));
    Ok(())
}
