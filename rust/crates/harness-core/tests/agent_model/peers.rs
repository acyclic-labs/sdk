//! Peer membership uses the same atomic root creation and archive publications.
use super::*;
use acyclic_harness_core::Capabilities;
use proptest::prelude::*;

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
            let envelope = route::Envelope::decode(&record.value)?;
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
        let forward = route::forward(&local, &routing.paths, &routing.node, async |envelope| {
            envelope
                .publish_relay(&account, &routing.paths, &destination, Vec::new())
                .await
        });
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
            let envelope = route::Envelope::decode(&record.value)?;
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
