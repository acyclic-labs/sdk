//! Peer membership uses the same atomic root creation and archive publications.
use super::*;
use acyclic_harness_core::Capabilities;
use proptest::prelude::*;

#[tokio::test]
async fn message_intent_cannot_be_reused_at_a_different_turn_boundary() -> Result {
    let provider = MemoryStream::default();
    let sender = create(&provider, id(94)).await?;
    let recipient = create(&provider, id(95)).await?;
    let from = Sender::Agent {
        path: sender.clone(),
    };
    let message = Message {
        from: from.clone(),
        id: intent("one-message"),
        re: None,
        body: Box::new(super::driver::file("body.md")?),
        attachments: None,
    };
    let original = send(&recipient, &from, &InboxRecord::Message(message.clone()))?;
    let committed = provider.commit(original.clone()).await?;
    assert!(matches!(&committed, CommitOutcome::Committed(_)));
    let changed = send(&recipient, &from, &InboxRecord::FollowUp(message.clone()))?;
    let error = provider
        .commit(changed.clone())
        .await
        .expect_err("changing a boundary must not create another message intent");
    assert_eq!(error.category(), StreamError::IdempotencyMismatch);
    assert_eq!(provider.commit(original).await?, committed);
    assert_eq!(provider.tail(recipient.inbox()?).await?, 2);

    // Negative control restores the separate follow-up key, reproducing two
    // real inbox inputs with the same sender and caller-owned message intent.
    let mut separate_boundary = changed;
    separate_boundary.idempotency_key = intent_key(
        "follow-up",
        &[
            recipient.as_str(),
            &format!("agent:{sender}"),
            message.id.as_str(),
        ],
    )?;
    assert!(matches!(
        provider.commit(separate_boundary).await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(provider.tail(recipient.inbox()?).await?, 3);
    Ok(())
}

#[tokio::test]
async fn membership_epochs_close_atomically_and_cannot_be_revived() -> Result {
    let provider = MemoryStream::default();
    let sender = create(&provider, id(90)).await?;
    let recipient = create(&provider, id(91)).await?;
    let member_path = recipient.peer_membership()?;
    let first = provider
        .head(member_path.clone())
        .await?
        .last
        .ok_or("membership missing")?;
    let original = PeerMembership::decode(&first.value)?;
    let epoch = original.epoch.ok_or("initial grant missing")?;
    let created = provider
        .head(recipient.peer_epoch(&epoch)?)
        .await?
        .last
        .ok_or("grant missing")?;
    assert_eq!(
        created.commit_id, first.commit_id,
        "grant was detached from root creation"
    );
    let guarded = |member: PeerMembership, name: &str| -> Result<CommitRequest> {
        let from = Sender::Agent {
            path: sender.clone(),
        };
        let mut request = send(
            &recipient,
            &from,
            &InboxRecord::Message(Message {
                from: from.clone(),
                id: intent(name),
                re: None,
                body: Box::new(super::driver::file("body.txt")?),
                attachments: None,
            }),
        )?;
        request.effects[0]
            .conditions
            .extend(member.conditions(&recipient)?);
        request.effects[0].soft = true;
        Ok(request)
    };
    let held = guarded(original, "held")?;
    let unchanged_interval = guarded(original, "unchanged-interval")?;
    submit(
        &provider,
        set_peer_membership(&provider, &recipient, &user(), &intent("same-on"), true).await?,
    )
    .await?;
    let same = provider
        .head(member_path.clone())
        .await?
        .last
        .ok_or("membership missing")?;
    assert_eq!(PeerMembership::decode(&same.value)?.epoch, Some(epoch));
    assert_eq!(provider.tail(recipient.peer_epoch(&epoch)?).await?, 1);
    assert!(
        matches!(provider.commit(unchanged_interval).await?,
        CommitOutcome::Committed(receipt) if receipt.skipped.is_empty()),
        "a duplicate opt-in must not invalidate an already prepared send"
    );

    submit(
        &provider,
        set_peer_membership(&provider, &recipient, &user(), &intent("off"), false).await?,
    )
    .await?;
    let off = provider
        .head(member_path.clone())
        .await?
        .last
        .ok_or("membership missing")?;
    let closed = provider
        .head(recipient.peer_epoch_closed(&epoch)?)
        .await?
        .last
        .ok_or("revocation missing")?;
    assert!(!PeerMembership::decode(&off.value)?.enabled());
    assert_eq!(
        off.commit_id, closed.commit_id,
        "opt-out did not revoke atomically"
    );
    let refused = provider.commit(held.clone()).await?;
    assert!(matches!(&refused, CommitOutcome::Committed(receipt) if receipt.skipped == [0]));
    assert_eq!(provider.tail(recipient.inbox()?).await?, 2);

    submit(
        &provider,
        set_peer_membership(&provider, &recipient, &user(), &intent("rejoin"), true).await?,
    )
    .await?;
    let on = provider
        .head(member_path)
        .await?
        .last
        .ok_or("membership missing")?;
    let renewed = PeerMembership::decode(&on.value)?;
    assert_ne!(renewed.epoch, Some(epoch));
    let new_epoch = renewed.epoch.ok_or("renewed grant missing")?;
    assert_eq!(
        provider
            .head(recipient.peer_epoch(&new_epoch)?)
            .await?
            .last
            .ok_or("grant missing")?
            .commit_id,
        on.commit_id
    );
    assert_eq!(
        provider.commit(held).await?,
        refused,
        "rejoin changed an original refusal"
    );
    assert!(
        matches!(provider.commit(guarded(original, "stale-epoch")?).await?,
        CommitOutcome::Committed(receipt) if receipt.skipped == [0])
    );
    assert!(
        matches!(provider.commit(guarded(renewed, "current-epoch")?).await?,
        CommitOutcome::Committed(receipt) if receipt.skipped.is_empty())
    );
    assert_eq!(provider.tail(recipient.inbox()?).await?, 3);

    // Negative control: deleting only revocation admits the stale epoch.
    let mut bypass = guarded(original, "without-revocation")?;
    let revocation = recipient.peer_epoch_closed(&epoch)?;
    bypass.effects[0]
        .conditions
        .retain(|condition| condition.path() != &revocation);
    assert!(matches!(provider.commit(bypass).await?,
        CommitOutcome::Committed(receipt) if receipt.skipped.is_empty()));
    assert_eq!(provider.tail(recipient.inbox()?).await?, 4);
    submit(&provider, archive(&recipient, &user(), &intent("archive"))?).await?;
    assert!(
        matches!(provider.commit(guarded(renewed, "after-archive")?).await?,
        CommitOutcome::Committed(receipt) if receipt.skipped == [0])
    );
    Ok(())
}

#[tokio::test]
async fn revoked_remote_send_does_not_stall_its_source_publication() -> Result {
    use acyclic_actors::{
        addressing::ActorReference,
        route::{NodeId, Paths},
    };
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        let provider = MemoryStream::default();
        let sender = create(&provider, id(92)).await?;
        let recipient = create(&provider, id(93)).await?;
        let admission = session_peer_admission(
            &provider,
            &Capabilities::new([SEND_PEER]),
            &sender,
            &recipient,
        )
        .await?
        .ok_or("admission missing")?;
        let routing = Routing {
            paths: Paths::new(StreamPath::new("routing")?),
            node: NodeId::new(b"source")?,
        };
        let within = ActorReference::new(b"account", b"agents", sender.id().as_str().as_bytes())?;
        let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
        let step = Step {
            outs: effect_outs(
                &state,
                vec![TurnEffect::Send {
                    peer: Some(admission.clone()),
                    delivery,
                    to: recipient.clone(),
                    id: intent("held-send"),
                    re: None,
                    body: Box::new(super::driver::file("message.md")?),
                    attachments: None,
                }],
            ),
            state,
            status: None,
            alarm: Alarm::Clear,
        };
        let request = routing.lower(
            &within,
            &BTreeSet::from([recipient]),
            &step,
            1,
            intent_key("held-publication", &[])?,
        )?;
        submit(
            &provider,
            set_peer_membership(&provider, &sender, &user(), &intent("opt-out"), false).await?,
        )
        .await?;
        // Negative control: a source-only hard revocation fence prevents this
        // already recorded turn from ever completing, even after rejoin.
        let mut stalled = request.clone();
        stalled.effects[0]
            .conditions
            .extend(admission.sender.conditions(&sender)?);
        assert!(matches!(
            provider.commit(stalled).await?,
            CommitOutcome::Conflict(_)
        ));
        assert!(matches!(
            provider.commit(request.clone()).await?,
            CommitOutcome::Committed(_)
        ));
        let path = routing.paths.outbox(&routing.node)?;
        assert_eq!(provider.tail(path.clone()).await?, 1);
        let record = provider
            .record_at(path.clone(), 0)
            .await?
            .ok_or("outbox missing")?;
        let envelope = acyclic_actors::route::Publication::decode(&record.value)?.delivery()?;
        assert_eq!(
            PeerRoute::parse(&envelope)?.admission(),
            &admission,
            "the account must receive the original interval, not a new grant"
        );
        assert!(matches!(
            provider.commit(request).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(provider.tail(path).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn absent_recipient_refusal_and_notice_share_the_original_commit() -> Result {
    use acyclic_actors::{
        addressing::ActorReference,
        route::{self, NodeId, Paths},
    };
    use std::sync::Arc;
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        let source = MemoryStream::default();
        let account = MemoryStream::default();
        let destination = Arc::new(MemoryStream::default());
        let sender = create(&source, id(70)).await?;
        let recipient = create(&source, id(71)).await?;
        let within = ActorReference::new(b"account", b"agents", sender.id().as_str().as_bytes())?;
        let sending = Routing {
            paths: Paths::new(StreamPath::new("routing")?),
            node: NodeId::new(b"source")?,
        };
        let receiving = Routing {
            paths: sending.paths.clone(),
            node: NodeId::new(b"destination")?,
        };
        let admission = session_peer_admission(
            &source,
            &Capabilities::new([SEND_PEER]),
            &sender,
            &recipient,
        )
        .await?
        .ok_or("admission missing")?;
        let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
        let step = Step {
            outs: effect_outs(
                &state,
                vec![TurnEffect::Send {
                    peer: Some(admission),
                    delivery,
                    to: recipient.clone(),
                    id: intent("original"),
                    re: None,
                    body: Box::new(super::driver::file("message.md")?),
                    attachments: None,
                }],
            ),
            state,
            status: None,
            alarm: Alarm::Clear,
        };
        submit(
            &source,
            sending.lower(
                &within,
                &BTreeSet::from([recipient.clone()]),
                &step,
                1,
                intent_key("source-step", &[])?,
            )?,
        )
        .await?;
        let publication = source
            .record_at(sending.paths.outbox(&sending.node)?, 0)
            .await?
            .ok_or("source missing")?;
        let original = route::Publication::decode(&publication.value)?.delivery()?;
        original
            .publish_relay(&account, &sending.paths, &receiving.node, Vec::new())
            .await?;
        let admitted = account
            .record_at(sending.paths.relay(&receiving.node)?, 0)
            .await?
            .ok_or("relay missing")?;
        let receipt = route::RelayReceipt {
            commit_id: admitted.commit_id,
            sequence: admitted.sequence,
        };
        let relayed = route::RelayRecord {
            envelope: route::Envelope::decode(&admitted.value)?,
            receipt,
            headers: admitted.headers,
        };
        let peer = PeerRoute::parse(&original)?;
        let host = AgentHost::new(
            destination.clone(),
            Scripted(Mutex::new(VecDeque::new())),
            within.clone(),
            acyclic_actors::domain::ActorLimits::default(),
            Arc::new(acyclic_actors::local::SystemClock),
        )?;
        assert_eq!(
            host.deliver_routed_peer(&receiving, &relayed).await?,
            Delivered::Closed
        );
        assert!(
            matches!(
                destination.tail(recipient.inbox()?).await,
                Err(StreamError::NotFound)
            ),
            "refusal must not create an inbox"
        );
        let CommitMutation::Append { records, .. } = &original.request.effects[0].mutations[0]
        else {
            return Err("message append missing".into());
        };
        let mut user_input = InboxRecord::decode(&records[0].value)?;
        let (InboxRecord::Message(message) | InboxRecord::FollowUp(message)) = &mut user_input
        else {
            return Err("message missing".into());
        };
        message.from = Sender::User {
            principal: "owner".into(),
        };
        for input in [
            user_input,
            InboxRecord::Archive(Control {
                by: Sender::User {
                    principal: "owner".into(),
                },
                id: intent("forged-archive"),
            }),
        ] {
            let mut forbidden = relayed.clone();
            let CommitMutation::Append { records, .. } =
                &mut forbidden.envelope.request.effects[0].mutations[0]
            else {
                return Err("message append missing".into());
            };
            records[0].value = input.encode()?.into();
            assert!(
                host.deliver_routed_peer(&receiving, &forbidden)
                    .await
                    .is_err()
            );
        }
        let notice = destination
            .record_at(receiving.paths.outbox(&receiving.node)?, 0)
            .await?
            .ok_or("refusal Notice missing")?;
        let retained = destination
            .inspect_idempotency(original.request.idempotency_key.clone())
            .await?
            .ok_or("original refusal missing")?;
        let acyclic_stream::IdempotencyOutcome::Commit(committed) = retained.outcome else {
            return Err("refusal was not committed".into());
        };
        assert_eq!(
            committed.commit_id, notice.commit_id,
            "the return Notice and original refusal must be one publication"
        );
        assert!(committed.mutations.iter().all(|mutation| matches!(mutation,
            acyclic_stream::CommittedMutation::Append(append) if append.path == receiving.paths.outbox(&receiving.node).expect("valid outbox")
        )), "a skipped input must not publish an inbox or activation");
        let result = route::Publication::decode(&notice.value)?.delivery()?;
        PeerResultRoute::parse(&result)?
            .verify_original(&account, &sending.paths)
            .await?;
        assert_eq!(
            host.deliver_routed_peer(&receiving, &relayed).await?,
            Delivered::Closed
        );

        // A new root at the same address has a different creation commit. It
        // cannot revive the old intent, even though its membership is enabled.
        let created = create_root(&intent("later-creation"), adopted(recipient.id()))?;
        submit(destination.as_ref(), created.request.clone()).await?;
        assert_eq!(
            host.deliver_routed_peer(&receiving, &relayed).await?,
            Delivered::Closed
        );
        assert_eq!(destination.tail(recipient.inbox()?).await?, 1);
        assert_eq!(
            destination
                .tail(receiving.paths.outbox(&receiving.node)?)
                .await?,
            1
        );

        let mut changed = original.clone();
        let CommitMutation::Append { records, .. } = &mut changed.request.effects[0].mutations[0]
        else {
            return Err("message append missing".into());
        };
        let mut input = InboxRecord::decode(&records[0].value)?;
        let (InboxRecord::Message(message) | InboxRecord::FollowUp(message)) = &mut input else {
            return Err("message missing".into());
        };
        message.body = Box::new(super::driver::file("changed.md")?);
        records[0].value = input.encode()?.into();
        assert!(
            PeerRoute::parse(&changed)?
                .deliver(&host, &receiving, &receipt)
                .await
                .is_err(),
            "changed bytes must not recover the original refusal"
        );

        // The same invalid witness against a present root, without a retained
        // absence publication, must fail instead of spending its fresh intent.
        let present = Arc::new(MemoryStream::default());
        submit(present.as_ref(), created.request).await?;
        let present_host = AgentHost::new(
            present.clone(),
            Scripted(Mutex::new(VecDeque::new())),
            within,
            acyclic_actors::domain::ActorLimits::default(),
            Arc::new(acyclic_actors::local::SystemClock),
        )?;
        assert!(
            peer.deliver(&present_host, &receiving, &receipt)
                .await
                .is_err()
        );
        assert!(
            present
                .inspect_idempotency(original.request.idempotency_key.clone())
                .await?
                .is_none()
        );
        assert_eq!(present.tail(recipient.inbox()?).await?, 1);
        assert!(matches!(
            present.tail(receiving.paths.outbox(&receiving.node)?).await,
            Err(StreamError::NotFound)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn account_rejection_settles_only_its_original_owned_send() -> Result {
    use acyclic_actors::{
        addressing::ActorReference,
        route::{self, NodeId, Paths, Rejection, Routed},
    };
    use acyclic_stream::{FollowRequest, RecordHeaders, WhenAbsent};
    use std::sync::Arc;
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        for reason in [Rejection::Closed, Rejection::NoSuchActor] {
            let local = Arc::new(MemoryStream::default());
            let account = MemoryStream::default();
            let sender = create(local.as_ref(), id(60)).await?;
            let recipient = create(local.as_ref(), id(61)).await?;
            let within =
                ActorReference::new(b"account", b"agents", sender.id().as_str().as_bytes())?;
            let routing = Routing {
                paths: Paths::new(StreamPath::new("routing")?),
                node: NodeId::new(b"source")?,
            };
            let admission = session_peer_admission(
                local.as_ref(),
                &Capabilities::new([SEND_PEER]),
                &sender,
                &recipient,
            )
            .await?
            .ok_or("peer grant missing")?;
            let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
            let message_id = intent("original");
            let step = Step {
                outs: effect_outs(
                    &state,
                    vec![TurnEffect::Send {
                        peer: Some(admission),
                        delivery,
                        to: recipient.clone(),
                        id: message_id.clone(),
                        re: None,
                        body: Box::new(super::driver::file("message.md")?),
                        attachments: None,
                    }],
                ),
                state,
                status: None,
                alarm: Alarm::Clear,
            };
            submit(
                local.as_ref(),
                routing.lower(
                    &within,
                    &BTreeSet::from([recipient.clone()]),
                    &step,
                    1,
                    intent_key("original-step", &[])?,
                )?,
            )
            .await?;
            let published = local
                .record_at(routing.paths.outbox(&routing.node)?, 0)
                .await?
                .ok_or("source missing")?;
            let original = route::Publication::decode(&published.value)?.delivery()?;
            let peer = PeerRoute::parse(&original)?;
            let account_node = NodeId::new(b"account")?;
            let notice = peer.rejection(account_node.clone(), reason)?;
            assert!(
                PeerRoute::parse(&notice).is_err(),
                "a peer grant must not admit host refusals"
            );
            assert!(
                PeerResultRoute::parse(&notice).is_err(),
                "an account refusal must not invent a destination receipt"
            );
            assert_eq!(
                original
                    .publish_rejection(&account, &routing.paths, reason, &notice, Vec::new())
                    .await?,
                Routed::Rejected(reason)
            );
            assert_eq!(
                original
                    .publish_rejection(&account, &routing.paths, reason, &notice, Vec::new())
                    .await?,
                Routed::Rejected(reason)
            );
            assert_eq!(
                original
                    .publish_relay(
                        &account,
                        &routing.paths,
                        &NodeId::new(b"destination")?,
                        Vec::new()
                    )
                    .await?,
                Routed::Rejected(reason),
                "a later routing decision must retain the original refusal"
            );
            assert_eq!(account.tail(routing.paths.relay(&routing.node)?).await?, 1);
            let record = account
                .record_at(routing.paths.relay(&routing.node)?, 0)
                .await?
                .ok_or("rejection missing")?;
            let relayed = route::RelayRecord {
                envelope: route::Envelope::decode(&record.value)?,
                receipt: route::RelayReceipt {
                    commit_id: record.commit_id,
                    sequence: record.sequence,
                },
                headers: record.headers,
            };
            let host = AgentHost::new(
                local.clone(),
                Scripted(Mutex::new(VecDeque::new())),
                within.clone(),
                acyclic_actors::domain::ActorLimits::default(),
                Arc::new(acyclic_actors::local::SystemClock),
            )?;
            host.set_peer_membership(
                &sender,
                &Sender::User {
                    principal: "owner".into(),
                },
                &intent("off"),
                false,
            )
            .await?;
            let routed = PeerRejectionRoute::parse(&relayed.envelope)?;
            let mut unproven = relayed.clone();
            unproven.headers = RecordHeaders::default();
            assert!(
                routed.deliver(&host, &unproven).await.is_err(),
                "ordinary relay publication cannot authorize an account refusal"
            );
            let other_reason = match reason {
                Rejection::Closed => Rejection::NoSuchActor,
                Rejection::NoSuchActor => Rejection::Closed,
            };
            let mut changed = relayed.clone();
            changed.envelope = peer.rejection(account_node.clone(), other_reason)?;
            assert!(
                PeerRejectionRoute::parse(&changed.envelope)?
                    .deliver(&host, &changed)
                    .await
                    .is_err(),
                "a genuine binding must not authorize a changed reason"
            );
            // The binding must match the sender's original bytes, not just its
            // message id, target and a syntactically valid rejection Notice.
            let mut altered = original.clone();
            altered.source = NodeId::new(b"other-source")?;
            let other_account = MemoryStream::default();
            altered
                .publish_rejection(&other_account, &routing.paths, reason, &notice, Vec::new())
                .await?;
            let wrong = other_account
                .record_at(routing.paths.relay(&altered.source)?, 0)
                .await?
                .ok_or("negative binding missing")?;
            let wrong = route::RelayRecord {
                envelope: route::Envelope::decode(&wrong.value)?,
                receipt: route::RelayReceipt {
                    commit_id: wrong.commit_id,
                    sequence: wrong.sequence,
                },
                headers: wrong.headers,
            };
            assert!(
                PeerRejectionRoute::parse(&wrong.envelope)?
                    .deliver(&host, &wrong)
                    .await
                    .is_err(),
                "another original publication must not settle this send"
            );
            assert_eq!(
                local.tail(sender.inbox()?).await?,
                1,
                "failed proofs must write no inbox input"
            );
            let mut events = acyclic_stream::records_only(
                account
                    .follow(FollowRequest {
                        when_absent: WhenAbsent::Wait,
                        ..FollowRequest::new(routing.paths.relay_cursor(&routing.node)?, 0)
                    })
                    .await?,
            );
            let drain = route::drain(&account, &routing.paths, &routing.node, async |record| {
                host.deliver_routed_peer(&routing, &record)
                    .await
                    .map_err(|_| route::Error::InvalidRecord)?;
                Ok(())
            });
            let ack = events.try_next();
            futures::pin_mut!(drain, ack);
            match futures::future::select(drain, ack).await {
                futures::future::Either::Right((ack, _)) => assert!(ack?.is_some()),
                futures::future::Either::Left((outcome, _)) => {
                    return Err(format!("drain stopped: {outcome:?}").into());
                }
            }
            assert_eq!(local.tail(sender.inbox()?).await?, 2);
            let settled = sent_reply(&local, &within, &sender, &message_id)
                .await?
                .ok_or("send missing")?;
            assert_eq!(settled.status, SentReplyStatus::Dropped);
            assert!(settled.watch.is_none());
            assert_eq!(
                routed.deliver(&host, &relayed).await?,
                Delivered::Committed { inbox: 1 }
            );
            assert_eq!(
                local.tail(sender.inbox()?).await?,
                2,
                "a lost acknowledgment must replay one Notice"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn session_profiles_publish_only_public_metadata_changes() -> Result {
    use acyclic_actors::route::NodeId;
    let volume = super::driver::file("body.md")?.volume().clone();
    let private = WorkspaceEntry::initial_volume(
        WorkspaceRole::Private,
        "private-state".into(),
        volume.clone(),
    );
    let project = WorkspaceEntry::initial_volume(WorkspaceRole::User, "project".into(), volume);
    let table = WorkspaceTable::new(vec![private, project.clone()])?;
    let phase = Phase::WaitingUser {
        path: "private-input-fence".into(),
    };
    let profile = SessionProfile::project(&phase, "Review".into(), table.entries(), "é中 tail", 4);
    let provider = MemoryStream::default();
    let path = StreamPath::new("registry/account/sessions/root")?;
    let mut row = SessionRow::new(NodeId::new(b"owner")?, profile.clone(), [7; 32]);
    let request = CommitRequest {
        deadline_unix_millis: None,
        idempotency_key: intent_key("initial-session-row", &[])?,
        effects: vec![row.effect(path.clone())?],
    };
    assert!(matches!(
        provider.commit(request.clone()).await?,
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(
        provider.commit(request).await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(provider.tail(path.clone()).await?, 1);
    assert_eq!(profile.goal_preview, "é");
    let encoded = serde_json::to_string(&profile)?;
    // Negative controls: the original admitted values contain the data that
    // discovery must omit. This is not an empty-input redaction assertion.
    assert!(serde_json::to_string(&phase)?.contains("private-input-fence"));
    assert!(serde_json::to_string(&table)?.contains("private-state"));
    assert!(!encoded.contains("private-input-fence"));
    assert!(!encoded.contains("private-state"));
    assert_eq!(profile.workspaces.len(), 1);
    assert_eq!(profile.workspaces[0].id, project.id);

    let internal_change = SessionProfile::project(
        &Phase::WaitingUser {
            path: "another-private-fence".into(),
        },
        "Review".into(),
        table.entries(),
        "é中 changed after the preview",
        4,
    );
    assert!(row.changed(Some(internal_change))?.is_none());
    for changed in [
        SessionProfile::project(
            &Phase::Running,
            "Review".into(),
            table.entries(),
            "é中 tail",
            4,
        ),
        SessionProfile::project(&phase, "Updated".into(), table.entries(), "é中 tail", 4),
        SessionProfile::project(&phase, "Review".into(), table.entries(), "new goal", 4),
        {
            let mut rerooted = project.clone();
            rerooted.root = "subdirectory".into();
            SessionProfile::project(
                &phase,
                "Review".into(),
                WorkspaceTable::new(vec![rerooted])?.entries(),
                "é中 tail",
                4,
            )
        },
        {
            let mut renamed = project;
            renamed.name = "renamed".into();
            SessionProfile::project(
                &phase,
                "Review".into(),
                WorkspaceTable::new(vec![renamed])?.entries(),
                "é中 tail",
                4,
            )
        },
    ] {
        let next = row
            .changed(Some(changed.clone()))?
            .ok_or("missing change")?;
        assert_eq!(next.rev, row.rev + 1);
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: intent_key("session-row", &[&next.rev.to_string()])?,
                    effects: vec![next.effect(path.clone())?],
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        let mut stale = row.changed(Some(changed))?.ok_or("missing stale change")?;
        stale.profile.as_mut().ok_or("missing stale profile")?.title = "stale producer".into();
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: intent_key("stale-session-row", &[&next.rev.to_string()])?,
                    effects: vec![stale.effect(path.clone())?],
                })
                .await?,
            CommitOutcome::Conflict(_)
        ));
        let head = provider.head(path.clone()).await?;
        let record = head.last.ok_or("session row missing")?;
        assert_eq!(SessionRow::decode(&record)?, next);
        assert_eq!(
            next.membership, row.membership,
            "display changes must preserve the user's active interval"
        );
        // Removing only the public interval is an actual malformed live row,
        // even though all display metadata and its original commit remain.
        let mut missing_epoch = record.clone();
        missing_epoch.headers = acyclic_stream::RecordHeaders::default();
        assert!(SessionRow::decode(&missing_epoch).is_err());
        assert_eq!(head.tail, next.rev);
        row = next;
    }
    let removed = row.changed(None)?.ok_or("missing removal")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        idempotency_key: intent_key("remove-session-row", &[])?,
        effects: vec![removed.effect(path.clone())?],
    };
    for _ in 0..2 {
        assert!(matches!(
            provider.commit(request.clone()).await?,
            CommitOutcome::Committed(_)
        ));
    }
    assert!(removed.changed(None)?.is_none());
    let head = provider.head(path.clone()).await?;
    let record = head.last.ok_or("removal missing")?;
    assert_eq!(SessionRow::decode(&record)?, removed);
    assert!(!removed.membership.enabled());
    assert_eq!(head.tail, removed.rev);
    let encoded: serde_json::Value = serde_json::from_slice(&record.value)?;
    for field in ["phase", "title", "workspaces", "goal_preview"] {
        assert!(encoded.get(field).is_none(), "removed row retained {field}");
    }
    for (field, value) in [
        ("title", serde_json::json!("retained metadata")),
        ("version", serde_json::json!(2)),
        ("rev", serde_json::json!(0)),
    ] {
        let mut invalid = encoded.clone();
        invalid[field] = value;
        let mut invalid_record = record.clone();
        invalid_record.value = serde_json::to_vec(&invalid)?.into();
        assert!(SessionRow::decode(&invalid_record).is_err());
    }
    let late = row
        .changed(Some(profile.clone()))?
        .ok_or("missing late row")?;
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: intent_key("late-session-row", &[])?,
                effects: vec![late.effect(path.clone())?],
            })
            .await?,
        CommitOutcome::Conflict(_)
    ));
    assert!(removed.changed(Some(profile.clone())).is_err());
    let rejoined = removed
        .changed_membership(
            Some(profile),
            PeerMembership {
                epoch: Some([8; 32]),
            },
        )?
        .ok_or("missing rejoin")?;
    assert_ne!(rejoined.membership, row.membership);
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: intent_key("rejoin-session-row", &[])?,
                effects: vec![rejoined.effect(path.clone())?],
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    assert_eq!(provider.tail(path).await?, rejoined.rev);
    Ok(())
}

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
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct RefuseReturn(AtomicBool);
    impl acyclic_stream::CommitAdmission for RefuseReturn {
        fn not_before(&self, value: &[u8]) -> Option<u64> {
            (self.0.load(Ordering::SeqCst)
                && matches!(
                    route::Publication::decode(value),
                    Ok(route::Publication::Delivery(_))
                ))
            .then_some(u64::MAX)
        }
    }
    struct Recipient {
        calls: Arc<AtomicUsize>,
        answer: bool,
        goal: Option<acyclic_harness_core::OperationId>,
        steering: bool,
        parked: bool,
        output: acyclic_harness_core::conversation::FileRef,
    }
    impl TurnDriver for Recipient {
        fn step<'a>(
            &'a self,
            state: &'a AgentState,
            _: u64,
            input: Option<Queued>,
            _: acyclic_actors::guest::CancellationToken,
        ) -> TurnFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if self.parked {
                    return Ok(TurnStep {
                        checkpoint: Some(Vec::new()),
                        outs: Vec::new(),
                        wait: Wait::User("input".into()),
                        drops: None,
                    });
                }
                let outs = match input {
                    Some(input) if self.answer => {
                        let completed_goal = self.goal.filter(|_| matches!(&input.input,
                            TurnInput::Message { from: Sender::Agent { path }, .. } if path == &state.path));
                        let steering =
                            (self.steering && input.inbox <= 2).then(|| SteeringTranscript {
                                through: input.inbox + 3,
                                file: self.output.clone(),
                            });
                        let through = steering.as_ref().map(|cut| cut.through);
                        let mut outs = vec![Out::Conversation(
                            ConversationEntry {
                                observed_through: through.unwrap_or(input.inbox + 1),
                                steering,
                                inbox: input.inbox,
                                input: input.input,
                                output: self.output.clone(),
                                outcome: TurnOutcome::Completed,
                                goal: self.goal,
                                announcements: Vec::new(),
                                tool_results: None,
                                notices: None,
                            }
                            .encode()?,
                        )];
                        if let Some(through) = through {
                            outs.push(Out::Steered(through));
                        }
                        if let Some(run) = completed_goal {
                            outs.push(Out::GoalCompleted { run });
                        }
                        outs
                    }
                    Some(input) => vec![Out::InputCancelled {
                        input: input.inbox,
                        conversation: state.conversation_tail,
                    }],
                    None => Vec::new(),
                };
                Ok(TurnStep {
                    checkpoint: None,
                    outs,
                    wait: Wait::Input,
                    drops: None,
                })
            })
        }
    }
    for (delivery, answer, continuing, parked) in
        [MessageDelivery::Steer, MessageDelivery::FollowUp]
            .into_iter()
            .flat_map(|delivery| {
                [
                    (false, false, false),
                    (true, false, false),
                    (true, true, false),
                    (false, false, true),
                ]
                .map(|(answer, continuing, parked)| (delivery, answer, continuing, parked))
            })
    {
        for opt_out_before_delivery in [false, true] {
            let steering = answer && !continuing && !opt_out_before_delivery;
            let cancelled_steering = steering && delivery == MessageDelivery::FollowUp;
            let source = MemoryStream::default();
            let destination = Arc::new(MemoryStream::default());
            let sender = create(&source, id(60)).await?;
            let recipient = AgentPath::root(&id(61));
            let within = ActorReference::new(b"account", b"agents", b"owner")?;
            let calls = Arc::new(AtomicUsize::new(0));
            let receiving = Routing {
                paths: Paths::new(StreamPath::new("routing")?),
                node: NodeId::new(b"recipient-node")?,
            };
            let handler = AgentHandler::new(
                actor_store(&destination)?,
                Recipient {
                    calls: calls.clone(),
                    answer,
                    goal: continuing
                        .then(|| acyclic_harness_core::OperationId::from_bytes([4; 16])),
                    steering,
                    parked,
                    output: super::driver::file("answer.md")?,
                },
                ActorLimits::default(),
            )?
            .with_routing(receiving.clone());
            let host = AgentHost::hosting(
                acyclic_actors::guest::Requests::default(),
                destination.clone(),
                within.clone(),
                Arc::new(handler),
                Arc::new(SystemClock),
                None,
            )?;
            host.create(&intent("recipient"), adopted(recipient.id()))
                .await?;
            let head =
                async |provider: &MemoryStream, root: &AgentPath| -> Result<PeerMembership> {
                    let record = provider
                        .head(root.peer_membership()?)
                        .await?
                        .last
                        .ok_or("membership missing")?;
                    Ok(PeerMembership::decode(&record.value)?)
                };
            let admission = PeerAdmission {
                sender: head(&source, &sender).await?,
                recipient: head(&destination, &recipient).await?,
            };
            let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
            let mut sends = vec![("request", delivery)];
            if steering {
                sends.extend([
                    ("reserved", MessageDelivery::FollowUp),
                    ("earlier-steering", MessageDelivery::Steer),
                    ("later-steering", MessageDelivery::Steer),
                ]);
            }
            let body = super::driver::file("message.md")?;
            let step = Step {
                outs: effect_outs(
                    &state,
                    sends
                        .into_iter()
                        .map(|(name, delivery)| TurnEffect::Send {
                            peer: Some(admission.clone()),
                            delivery,
                            to: recipient.clone(),
                            id: intent(name),
                            re: None,
                            body: Box::new(body.clone()),
                            attachments: None,
                        })
                        .collect(),
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
            let mut additional = Vec::new();
            if steering {
                for sequence in 1..4 {
                    let record = source
                        .record_at(routing.paths.outbox(&routing.node)?, sequence)
                        .await?
                        .ok_or("additional outbox record missing")?;
                    additional.push(route::Publication::decode(&record.value)?.delivery()?);
                }
            }
            drop(source);
            let account = MemoryStream::default();
            let recipient_node = NodeId::new(b"recipient-node")?;
            envelope
                .publish_relay(&account, &routing.paths, &recipient_node, Vec::new())
                .await?;
            let original = account
                .head(routing.paths.relay(&recipient_node)?)
                .await?
                .last
                .ok_or("relay missing")?;
            let receipt = route::RelayReceipt {
                commit_id: original.commit_id,
                sequence: original.sequence,
            };
            let incoming = route::RelayRecord {
                envelope: route::Envelope::decode(&original.value)?,
                receipt,
                headers: original.headers,
            };
            let mut steered = Vec::new();
            for envelope in additional {
                envelope
                    .publish_relay(&account, &routing.paths, &recipient_node, Vec::new())
                    .await?;
                let record = account
                    .head(routing.paths.relay(&recipient_node)?)
                    .await?
                    .last
                    .ok_or("additional relay missing")?;
                steered.push((
                    envelope,
                    route::RelayReceipt {
                        commit_id: record.commit_id,
                        sequence: record.sequence,
                    },
                ));
            }
            drop(account);
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
            let activation = actor_store(&destination)?.activation(&host.actor(&recipient)?)?;
            let activation_path = activation.mutations[0].path().clone();
            let before_activation = destination.tail(activation_path.clone()).await?;
            if opt_out_before_delivery {
                // Stop exactly between the retained refusal and its return
                // publication. A rejoin before replay must not deliver input.
                let failure = Arc::new(RefuseReturn(AtomicBool::new(true)));
                destination.install_commit_admission(failure.clone())?;
                assert!(
                    host.deliver_routed_peer(&receiving, &incoming)
                        .await
                        .is_err()
                );
                assert!(
                    destination
                        .inspect_idempotency(envelope.request.idempotency_key.clone())
                        .await?
                        .is_some(),
                    "the refusal must already be retained at this failure cut"
                );
                assert!(matches!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await,
                    Err(StreamError::NotFound)
                ));
                host.set_peer_membership(&recipient, &user(), &intent("rejoin"), true)
                    .await?;
                failure.0.store(false, Ordering::SeqCst);
            }
            assert_eq!(
                host.deliver_routed_peer(&receiving, &incoming).await?,
                expected
            );
            assert_eq!(
                destination.tail(activation_path).await?,
                before_activation + u64::from(!opt_out_before_delivery),
                "a skipped inbox append published a wake"
            );
            for (envelope, receipt) in &steered {
                assert!(matches!(
                    PeerRoute::parse(envelope)?
                        .deliver(&host, &receiving, receipt)
                        .await?,
                    Delivered::Committed { .. }
                ));
            }
            if cancelled_steering {
                host.send(
                    &recipient,
                    &user(),
                    &InboxRecord::Cancel(Cancellation {
                        by: user(),
                        id: intent("cancel-steering"),
                        input: 3,
                        conversation: 0,
                    }),
                )
                .await?;
            }
            host.set_peer_membership(
                &recipient,
                &user(),
                &intent("change-membership"),
                opt_out_before_delivery,
            )
            .await?;
            assert_eq!(
                peer.deliver(&host, &receiving, &receipt).await?,
                expected,
                "exact replay changed its original outcome"
            );
            assert_eq!(
                destination.tail(recipient.inbox()?).await?,
                if opt_out_before_delivery {
                    1
                } else if steering {
                    5 + u64::from(cancelled_steering)
                } else {
                    2
                }
            );
            let return_outbox = receiving.paths.outbox(&receiving.node)?;
            if opt_out_before_delivery {
                assert_eq!(destination.tail(return_outbox.clone()).await?, 1);
                let result = destination
                    .record_at(return_outbox, 0)
                    .await?
                    .ok_or("refusal result missing")?;
                assert_eq!(
                    route::Publication::decode(&result.value)?.delivery()?,
                    peer.result(receiving.node.clone(), &receipt, SentReplyStatus::Dropped)?,
                    "replayed refusal must publish the original terminal Notice"
                );
            } else {
                assert!(
                    matches!(
                        destination.tail(return_outbox).await,
                        Err(StreamError::NotFound)
                    ),
                    "ordinary accepted delivery must not publish a refusal"
                );
            }
            host.drain(&recipient).await?;
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if steering {
                    2
                } else {
                    usize::from(!opt_out_before_delivery)
                }
            );
            if parked && !opt_out_before_delivery {
                assert!(matches!(
                    host.reply(&recipient, &intent("request"), 1).await?,
                    Reply::Pending
                ));
                assert!(matches!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await,
                    Err(StreamError::NotFound)
                ));
                host.archive(&recipient, &user(), &intent("archive-parked"))
                    .await?;
                host.drain(&recipient).await?;
                assert!(matches!(
                    host.reply(&recipient, &intent("request"), 1).await?,
                    Reply::Dropped
                ));
                assert_eq!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await?,
                    1
                );
                let result = destination
                    .record_at(receiving.paths.outbox(&receiving.node)?, 0)
                    .await?
                    .ok_or("archived input result missing")?;
                assert_eq!(
                    route::Publication::decode(&result.value)?.delivery()?,
                    peer.result(receiving.node.clone(), &receipt, SentReplyStatus::Dropped)?
                );
                let mut facts = destination
                    .read(ReadRequest {
                        path: StreamPath::new(format!("{recipient}/context/input-events"))?,
                        from: 0,
                        limit: 1,
                        key: Some(acyclic_stream::ReadKeySeek::exact(
                            1_u64.to_be_bytes().to_vec(),
                            u64::MAX,
                        )),
                        at_least: Default::default(),
                    })
                    .await?;
                assert_eq!(
                    facts
                        .try_next()
                        .await?
                        .ok_or("archive terminal fact missing")?
                        .commit_id,
                    result.commit_id,
                    "archive result must join its original actor publication"
                );
                host.archive(&recipient, &user(), &intent("archive-parked"))
                    .await?;
                host.drain(&recipient).await?;
                assert_eq!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await?,
                    1
                );
                continue;
            }
            if continuing && !opt_out_before_delivery {
                assert!(matches!(
                    host.reply(&recipient, &intent("request"), 1).await?,
                    Reply::Continuing { .. }
                ));
                assert!(
                    matches!(
                        destination
                            .tail(receiving.paths.outbox(&receiving.node)?)
                            .await,
                        Err(StreamError::NotFound)
                    ),
                    "a continuing goal must not publish an early Answered result"
                );
                assert_eq!(peer.deliver(&host, &receiving, &receipt).await?, expected);
                host.drain(&recipient).await?;
                assert!(matches!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await,
                    Err(StreamError::NotFound)
                ));
                let run = acyclic_harness_core::OperationId::from_bytes([4; 16]);
                let continuation = InboxRecord::Message(Message {
                    from: Sender::Agent {
                        path: recipient.clone(),
                    },
                    id: IntentId::try_from(format!("goal:{run}:1"))?,
                    re: None,
                    body: Box::new(super::driver::file("goal.md")?),
                    attachments: None,
                });
                let from = Sender::Agent {
                    path: recipient.clone(),
                };
                let Delivered::Committed { inbox } =
                    host.send(&recipient, &from, &continuation).await?
                else {
                    return Err("continuation was not accepted".into());
                };
                let dropped = delivery == MessageDelivery::FollowUp;
                if dropped {
                    host.send(
                        &recipient,
                        &user(),
                        &InboxRecord::Cancel(Cancellation {
                            by: user(),
                            id: intent("cancel-goal"),
                            input: inbox,
                            conversation: 1,
                        }),
                    )
                    .await?;
                }
                host.drain(&recipient).await?;
                let status = if dropped {
                    SentReplyStatus::Dropped
                } else {
                    SentReplyStatus::Answered { sequence: 1 }
                };
                assert_eq!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await?,
                    1
                );
                let terminal = destination
                    .record_at(receiving.paths.outbox(&receiving.node)?, 0)
                    .await?
                    .ok_or("goal terminal missing")?;
                assert_eq!(
                    route::Publication::decode(&terminal.value)?.delivery()?,
                    peer.result(receiving.node.clone(), &receipt, status)?
                );
                let reply = host.reply(&recipient, &intent("request"), 1).await?;
                if dropped {
                    assert!(matches!(reply, Reply::Dropped));
                } else {
                    assert!(matches!(reply, Reply::Answered { sequence: 1, .. }));
                }
                let mut facts = destination
                    .read(ReadRequest {
                        path: StreamPath::new(format!("{recipient}/context/input-events"))?,
                        from: 0,
                        limit: 1,
                        key: Some(acyclic_stream::ReadKeySeek::exact(
                            if dropped {
                                run.into_bytes().to_vec()
                            } else {
                                inbox.to_be_bytes().to_vec()
                            },
                            u64::MAX,
                        )),
                        at_least: Default::default(),
                    })
                    .await?;
                assert_eq!(
                    facts
                        .try_next()
                        .await?
                        .ok_or("goal terminal fact missing")?
                        .commit_id,
                    terminal.commit_id,
                    "goal result must share the original terminal commit"
                );
                assert_eq!(peer.deliver(&host, &receiving, &receipt).await?, expected);
                host.drain(&recipient).await?;
                assert_eq!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await?,
                    1
                );
                continue;
            }
            let terminal_count = if steering { 4 } else { 1 };
            assert_eq!(
                destination
                    .tail(receiving.paths.outbox(&receiving.node)?)
                    .await?,
                terminal_count
            );
            let terminal = destination
                .record_at(receiving.paths.outbox(&receiving.node)?, 0)
                .await?
                .ok_or("terminal Notice missing")?;
            assert_eq!(
                route::Publication::decode(&terminal.value)?.delivery()?,
                peer.result(
                    receiving.node.clone(),
                    &receipt,
                    if answer && !opt_out_before_delivery {
                        SentReplyStatus::Answered { sequence: 0 }
                    } else {
                        SentReplyStatus::Dropped
                    }
                )?,
            );
            if steering {
                for (index, (original, sequence, inbox)) in [(1_usize, 0, 3), (0, 1, 2), (2, 1, 4)]
                    .into_iter()
                    .enumerate()
                {
                    let (envelope, receipt) = &steered[original];
                    let dropped = cancelled_steering && original == 1;
                    let result = destination
                        .record_at(receiving.paths.outbox(&receiving.node)?, index as u64 + 1)
                        .await?
                        .ok_or("steering terminal missing")?;
                    assert_eq!(
                        route::Publication::decode(&result.value)?.delivery()?,
                        PeerRoute::parse(envelope)?.result(
                            receiving.node.clone(),
                            receipt,
                            if dropped {
                                SentReplyStatus::Dropped
                            } else {
                                SentReplyStatus::Answered { sequence }
                            }
                        )?
                    );
                    let id = PeerRoute::parse(envelope)?.id().clone();
                    let reply = host.reply(&recipient, &id, inbox).await?;
                    if dropped {
                        assert!(matches!(reply, Reply::Dropped));
                    } else {
                        assert!(
                            matches!(reply, Reply::Answered { sequence: actual, .. } if actual == sequence)
                        );
                    }
                }
            }
            if !opt_out_before_delivery {
                let mut facts = destination
                    .read(ReadRequest {
                        path: StreamPath::new(format!("{recipient}/context/input-events"))?,
                        from: 0,
                        limit: 1,
                        key: Some(acyclic_stream::ReadKeySeek::exact(
                            1_u64.to_be_bytes().to_vec(),
                            u64::MAX,
                        )),
                        at_least: Default::default(),
                    })
                    .await?;
                assert_eq!(
                    facts
                        .try_next()
                        .await?
                        .ok_or("settlement fact missing")?
                        .commit_id,
                    terminal.commit_id,
                    "the terminal Notice must join the original input settlement"
                );
                host.drain(&recipient).await?;
                assert_eq!(
                    destination
                        .tail(receiving.paths.outbox(&receiving.node)?)
                        .await?,
                    terminal_count,
                    "replaying an idle actor must not publish another terminal"
                );
                let local_peer = AgentPath::root(&id(62));
                host.create(&intent("local-peer"), adopted(local_peer.id()))
                    .await?;
                for (name, from) in [
                    ("ordinary-user", user()),
                    ("ordinary-peer", Sender::Agent { path: local_peer }),
                ] {
                    let message = InboxRecord::Message(Message {
                        from: from.clone(),
                        id: intent(name),
                        re: None,
                        body: Box::new(super::driver::file("ordinary.md")?),
                        attachments: None,
                    });
                    host.deliver(&recipient, send(&recipient, &from, &message)?)
                        .await?;
                    host.drain(&recipient).await?;
                    assert_eq!(
                        destination
                            .tail(receiving.paths.outbox(&receiving.node)?)
                            .await?,
                        terminal_count,
                        "ordinary user and same-node peer inputs must not emit routed results"
                    );
                }
            }
            assert!(
                matches!(
                    destination.tail(sender.peer_membership()?).await,
                    Err(StreamError::NotFound)
                ),
                "sender state must remain on its own node"
            );
            if !opt_out_before_delivery {
                let record = destination
                    .record_at(recipient.inbox()?, 1)
                    .await?
                    .ok_or("inbox input missing")?;
                assert_eq!(
                    route::RelayReceipt::from_headers(&record.headers)?,
                    Some(receipt)
                );
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
            let mut changed = receipt;
            let mut identity = *changed.commit_id.as_bytes();
            identity[0] ^= 1;
            changed.commit_id = acyclic_stream::CommitId::from_bytes(identity);
            assert!(
                peer.deliver(&host, &receiving, &changed).await.is_err(),
                "a retry replaced its original relay reference"
            );
            let mut altered = envelope.clone();
            let CommitMutation::Append { records, .. } =
                &mut altered.request.effects[0].mutations[0]
            else {
                unreachable!()
            };
            let mut attributes = vec![receipt.attribute()];
            attributes.extend(
                records[0]
                    .headers
                    .attributes()
                    .iter()
                    .map(|attribute| attribute.to_owned()),
            );
            records[0].headers = acyclic_stream::RecordHeaders::new(Vec::new(), attributes)?;
            assert!(
                PeerRoute::parse(&altered).is_err(),
                "a peer supplied host-authored receipt metadata"
            );
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
                "substituted destination namespace was admitted"
            );
            let mut altered = envelope.clone();
            altered.sender =
                ActorReference::new(b"other-account", b"agents", envelope.sender.id().as_bytes())?;
            altered.to =
                ActorReference::new(b"other-account", b"agents", envelope.to.id().as_bytes())?;
            let CommitMutation::Append { records, .. } = &envelope.request.effects[0].mutations[0]
            else {
                unreachable!()
            };
            let input = InboxRecord::decode(&records[0].value)?;
            let altered = PeerRoute::prepare(
                altered.source,
                altered.sender,
                altered.to,
                &input,
                peer.admission(),
            )?;
            assert!(
                PeerRoute::parse(&altered)?
                    .deliver(&host, &receiving, &receipt)
                    .await
                    .is_err(),
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
        let registration = route::Registration {
            placement: route::Placement {
                actor: within.clone(),
                node: routing.node.clone(),
            },
            intent: intent_key("root-placement", &[])?,
        };
        submit(
            &local,
            CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: registration.intent.clone(),
                effects: registration.effects(&routing.paths)?.into(),
            },
        )
        .await?;
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
        assert_eq!(local.tail(outbox.clone()).await?, 3);
        assert!(
            local
                .record_at(outbox.clone(), 0)
                .await?
                .ok_or("placement missing")?
                .headers
                .keys()
                .is_empty(),
            "metadata publication must not masquerade as a message index"
        );
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
                .record_at(outbox.clone(), sequence as u64 + 1)
                .await?
                .ok_or("outbox missing")?;
            assert_eq!(record.commit_id, marker.commit_id);
            assert_eq!(record.commit_id, receipt.commit_id);
            let envelope = route::Publication::decode(&record.value)?.delivery()?;
            let canonical = envelope.outbox_record(&routing.paths)?;
            assert_eq!(canonical.value, record.value);
            assert_eq!(
                canonical.headers, record.headers,
                "singleton and batched publication must share the same index encoder"
            );
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
                sent(&local, &sender, &intent(name))
                    .await?
                    .ok_or("receipt missing")?
                    .delivery,
                SentDelivery::Pending {
                    source: routing.paths.outbox_cursor(&routing.node)?,
                    from: sequence as u64 + 1
                }
            );
        }
        // A copied source key is not proof of the original journal publication.
        // Keep all route/message bytes genuine and change only the commit
        // relationship or the selected intent, so each guard is necessary.
        for separate_commit in [true, false] {
            let mirror = MemoryStream::default();
            let source = local
                .record_at(outbox.clone(), if separate_commit { 1 } else { 2 })
                .await?
                .ok_or("original source missing")?;
            let marker_path = sender.sent_message(&intent("first"))?;
            let marker = local
                .head(marker_path.clone())
                .await?
                .last
                .ok_or("original marker missing")?;
            let mut attributes = marker
                .headers
                .attributes()
                .iter()
                .map(|attribute| attribute.to_owned())
                .collect::<Vec<_>>();
            let source_key = source
                .headers
                .keys()
                .first()
                .ok_or("source index missing")?
                .clone();
            for attribute in &mut attributes {
                if attribute.name == "acyclic.actors.route.source-intent.v1" {
                    attribute.value = source_key.clone();
                }
            }
            let marker = acyclic_stream::AppendRecord {
                value: marker.value,
                headers: acyclic_stream::RecordHeaders::new(Vec::new(), attributes)?,
            };
            let mut effects = vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: outbox.clone(),
                    records: vec![acyclic_stream::AppendRecord {
                        value: source.value,
                        headers: source.headers,
                    }],
                }],
            )];
            if !separate_commit {
                effects[0].mutations.push(CommitMutation::Append {
                    path: marker_path.clone(),
                    records: vec![marker.clone()],
                });
            }
            submit(
                &mirror,
                CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: intent_key("copy-source", &[])?,
                    effects,
                },
            )
            .await?;
            if separate_commit {
                submit(
                    &mirror,
                    CommitRequest {
                        deadline_unix_millis: None,
                        idempotency_key: intent_key("copy-marker", &[])?,
                        effects: vec![CommitEffect::hard(
                            Vec::new(),
                            vec![CommitMutation::Append {
                                path: marker_path,
                                records: vec![marker],
                            }],
                        )],
                    },
                )
                .await?;
            }
            assert!(
                sent(&mirror, &sender, &intent("first")).await.is_err(),
                "a source locator must prove its original commit and exact message intent"
            );
        }
        let cursor = routing.paths.outbox_cursor(&routing.node)?;
        let mut changes = acyclic_stream::records_only(
            local
                .follow(FollowRequest {
                    when_absent: WhenAbsent::Wait,
                    ..FollowRequest::new(cursor, 2)
                })
                .await?,
        );
        let forward = route::forward(
            &local,
            &routing.paths,
            &routing.node,
            async |actual| {
                let route::Publication::Placement(actual) = actual else {
                    return Err(route::Error::InvalidRecord);
                };
                assert_eq!(actual, registration);
                let outcome = account
                    .commit(CommitRequest {
                        deadline_unix_millis: None,
                        idempotency_key: actual.intent.clone(),
                        effects: vec![actual.placement.effect(&routing.paths)?],
                    })
                    .await?;
                if !matches!(outcome, CommitOutcome::Committed(_)) {
                    return Err(route::Error::InvalidRecord);
                }
                Ok(())
            },
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
        assert!(
            route::receipt(
                &local,
                &routing.paths,
                &routing.node,
                0,
                &registration.intent,
            )
            .await
            .is_err(),
            "placement acknowledgment is not a message receipt"
        );
        for (sequence, name) in names.iter().enumerate() {
            let record = local
                .record_at(outbox.clone(), sequence as u64 + 1)
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
        let counted = std::sync::Arc::new(acyclic_stream::testing::Counted::new(local.clone()));
        assert_eq!(
            sent_reply(&counted, &within, &sender, &intent("first"))
                .await?
                .ok_or("send missing")?
                .status,
            SentReplyStatus::Pending
        );
        // One owned-marker head, its indexed publication, its admission receipt,
        // and one indexed result read. The read carries its own watch cut.
        assert_eq!(
            counted
                .requests()
                .class(acyclic_stream::testing::RequestClass::Head),
            1
        );
        assert_eq!(counted.requests().control(), 1);
        assert_eq!(counted.requests().data(), 3);
        let owned = std::sync::Arc::new(local.clone());
        let host = AgentHost::new(
            owned.clone(),
            Scripted(Mutex::new(VecDeque::new())),
            within.clone(),
            acyclic_actors::domain::ActorLimits::default(),
            std::sync::Arc::new(acyclic_actors::local::SystemClock),
        )?;
        let notices = MemoryStream::default();
        for (sequence, (name, expected)) in names
            .iter()
            .zip([
                SentReplyStatus::Answered { sequence: 0 },
                SentReplyStatus::Dropped,
            ])
            .enumerate()
        {
            let pending = sent_reply(&owned, &within, &sender, &intent(name))
                .await?
                .ok_or("send missing")?;
            assert_eq!(pending.status, SentReplyStatus::Pending);
            let Some(Watch::PeerReply {
                sender: watching,
                id,
                from,
                ..
            }) = pending.watch
            else {
                return Err("remote reply did not watch its own inbox".into());
            };
            assert_eq!(watching, sender);
            assert_eq!(id, intent(name));
            let mut changes = acyclic_stream::records_only(
                owned
                    .follow(FollowRequest::new(sender.inbox()?, from))
                    .await?,
            );
            let original = account
                .record_at(routing.paths.relay(&destination)?, sequence as u64)
                .await?
                .ok_or("relay missing")?;
            let envelope = route::Envelope::decode(&original.value)?;
            let peer = PeerRoute::parse(&envelope)?;
            let receipt = route::RelayReceipt {
                commit_id: original.commit_id,
                sequence: original.sequence,
            };
            assert!(
                peer.result(destination.clone(), &receipt, SentReplyStatus::Pending)
                    .is_err()
            );
            let result = peer.result(destination.clone(), &receipt, expected.clone())?;
            assert!(
                PeerRoute::parse(&result).is_err(),
                "peer grant admitted a host result"
            );
            let result_request = CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: result.request.idempotency_key.clone(),
                effects: vec![result.outbox_effect(&routing.paths)?],
            };
            submit(&notices, result_request.clone()).await?;
            submit(&notices, result_request).await?;
            let queued = notices
                .record_at(routing.paths.outbox(&destination)?, sequence as u64)
                .await?
                .ok_or("result outbox missing")?;
            let result = route::Publication::decode(&queued.value)?.delivery()?;
            let routed = PeerResultRoute::parse(&result)?;
            routed.verify_original(&account, &routing.paths).await?;
            let admitted = result
                .publish_relay(&account, &routing.paths, &routing.node, Vec::new())
                .await?;
            result
                .publish_relay(&account, &routing.paths, &routing.node, Vec::new())
                .await?;
            // An owned pending send may settle even after the user opts out.
            host.set_peer_membership(
                &sender,
                &Sender::User {
                    principal: "owner".into(),
                },
                &intent(&format!("off-{sequence}")),
                false,
            )
            .await?;
            let Routed::Relayed { relay_seq, .. } = admitted else {
                return Err("result was not relayed".into());
            };
            let record = account
                .record_at(routing.paths.relay(&routing.node)?, relay_seq)
                .await?
                .ok_or("result relay missing")?;
            let relayed = route::RelayRecord {
                envelope: route::Envelope::decode(&record.value)?,
                receipt: route::RelayReceipt {
                    commit_id: record.commit_id,
                    sequence: record.sequence,
                },
                headers: record.headers,
            };
            let delivered = host.deliver_routed_peer(&routing, &relayed).await?;
            assert_eq!(
                host.deliver_routed_peer(&routing, &relayed).await?,
                delivered
            );
            let event = changes.try_next().await?.ok_or("missing result event")?;
            assert!(matches!(
                InboxRecord::decode(&event.value)?,
                InboxRecord::Notice {
                    notice: NoticePayload::Quiet(_),
                    ..
                }
            ));
            let observed = sent_reply(&owned, &within, &sender, &intent(name))
                .await?
                .ok_or("send missing")?;
            assert_eq!(observed.status, expected);
            assert!(observed.watch.is_none());
            let mut unrelated = result.clone();
            unrelated.source = routing.node.clone();
            assert!(
                PeerResultRoute::parse(&unrelated)?
                    .verify_original(&account, &routing.paths)
                    .await
                    .is_err(),
                "another node claimed this result"
            );
            if sequence == 0 {
                let changed =
                    peer.result(destination.clone(), &receipt, SentReplyStatus::Dropped)?;
                assert!(
                    changed
                        .publish_relay(&account, &routing.paths, &routing.node, Vec::new())
                        .await
                        .is_err(),
                    "a terminal result changed under one intent"
                );
            }
        }
        assert_eq!(notices.tail(routing.paths.outbox(&destination)?).await?, 2);
        assert_eq!(account.tail(routing.paths.relay(&routing.node)?).await?, 2);
        assert_eq!(owned.tail(sender.inbox()?).await?, 3);
        // A history fork retains its parent's original result bytes. Reusing
        // that message id in the new root must still wait for a new result.
        let mut adoption = adopted(id(52));
        adoption.forked_at = 3;
        let mut fork = create_root(&intent("history-fork"), adoption.clone())?;
        let forked = fork.child.clone();
        for effect in &mut fork.request.effects {
            for mutation in &mut effect.mutations {
                if let CommitMutation::Append { path, .. } = mutation
                    && *path == forked.inbox()?
                {
                    *mutation = CommitMutation::Fork {
                        source: sender.inbox()?,
                        destination: forked.inbox()?,
                        at_tail: 3,
                        exclude: Vec::new(),
                        records: vec![
                            InboxRecord::ForkAdopted(Box::new(adoption.clone()))
                                .encode()?
                                .into(),
                        ],
                    };
                }
            }
        }
        submit(&*owned, fork.request).await?;
        let old = owned
            .record_at(sender.inbox()?, 1)
            .await?
            .ok_or("parent result missing")?;
        let mut inherited = owned
            .read(ReadRequest {
                path: forked.inbox()?,
                from: 0,
                limit: 1,
                key: Some(acyclic_stream::ReadKeySeek::exact(
                    old.headers.keys()[0].clone(),
                    4,
                )),
                at_least: Default::default(),
            })
            .await?;
        assert_eq!(
            inherited
                .try_next()
                .await?
                .ok_or("inherited control missing")?
                .commit_id,
            old.commit_id
        );
        let admission = session_peer_admission(
            &*owned,
            &Capabilities::new([SEND_PEER]),
            &forked,
            &recipient,
        )
        .await?
        .ok_or("fork grant missing")?;
        let state = AgentState::adopted(forked.clone(), &adoption)?;
        let step = Step {
            outs: effect_outs(
                &state,
                vec![TurnEffect::Send {
                    peer: Some(admission),
                    delivery,
                    to: recipient.clone(),
                    id: intent("first"),
                    re: None,
                    body: Box::new(super::driver::file("message.md")?),
                    attachments: None,
                }],
            ),
            state,
            status: None,
            alarm: Alarm::Clear,
        };
        let fork_actor =
            ActorReference::new(b"account", b"agents", forked.id().as_str().as_bytes())?;
        submit(
            &*owned,
            routing.lower(
                &fork_actor,
                &BTreeSet::from([recipient]),
                &step,
                4,
                intent_key("fork-send", &[])?,
            )?,
        )
        .await?;
        let pending = sent_reply(&owned, &fork_actor, &forked, &intent("first"))
            .await?
            .ok_or("fork send missing")?;
        assert_eq!(pending.status, SentReplyStatus::Pending);
        assert!(
            matches!(pending.watch, Some(Watch::PeerReply { sender, from: 4, .. }) if sender == forked)
        );
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
                set_peer_membership(&provider, changed, &user(), &intent("off"), false).await?,
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
                set_peer_membership(&provider, changed, &user(), &intent("on"), true).await?,
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
                    assert!(matches!(submit(&provider, set_peer_membership(&provider,
                        root, &user(), &intent("opt-out"), false,
                    ).await?).await?, Submitted::Committed(_)));
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
        assert!(
            set_peer_membership(&provider, &a, &by, &intent("forbidden"), false)
                .await
                .is_err()
        );
    }
    let off = set_peer_membership(&provider, &a, &user(), &intent("off"), false).await?;
    submit(&provider, off.clone()).await?;
    submit(&provider, off).await?;
    assert_eq!(provider.tail(a.peer_membership()?).await?, 2);
    assert!(!session_peer_allowed(&provider, &grants, &a, &b).await?);
    assert!(!session_peer_allowed(&provider, &grants, &b, &a).await?);
    submit(
        &provider,
        set_peer_membership(&provider, &a, &user(), &intent("on"), true).await?,
    )
    .await?;
    assert!(session_peer_allowed(&provider, &grants, &a, &b).await?);
    let rejoin = set_peer_membership(&provider, &a, &user(), &intent("stale-on"), true).await?;
    submit(&provider, archive(&a, &user(), &intent("archive"))?).await?;
    assert!(!session_peer_allowed(&provider, &grants, &a, &b).await?);
    assert!(!session_peer_allowed(&provider, &grants, &b, &a).await?);
    assert!(matches!(
        submit(&provider, rejoin).await?,
        Submitted::RecipientClosed(_)
    ));
    Ok(())
}
