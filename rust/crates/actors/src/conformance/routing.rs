//! Routing transaction invariants use the original backend, not a mock router.
use super::{Backend, Error, Failure, Fixture, RouteCommand};
use crate::{
    addressing::ActorReference,
    route::{Envelope, NodeId, Paths, Placement, Rejection, Routed},
};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    ReadRequest, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt as _;

/// Signed account admission and event-driven reservation backpressure.
pub mod account;

/// A destination restart after inbox commit but before relay acknowledgement
/// replays through ordinary activation without running the input twice. Every
/// backend executes its real package and scheduler, not just inbox appends.
pub async fn replayed_delivery<T: Backend + ?Sized>(target: &T, run: &str) -> Result<(), Failure> {
    let mut receiver = Fixture::create(target, run, "routing-replay").await?;
    let provider = target.store().provider();
    let root = StreamPath::new(format!("{}/routing-replay", target.store().root()))
        .map_err(Error::from)?;
    let paths = Paths::new(root.clone());
    let source = NodeId::new(b"source").map_err(violation)?;
    let destination = NodeId::new(b"destination").map_err(violation)?;
    let placement = Placement {
        actor: receiver.actor.clone(),
        node: destination.clone(),
    };
    receiver
        .creation
        .as_mut()
        .ok_or_else(|| violation("receiver creation already published"))?
        .push(placement.effect(&paths).map_err(violation)?);
    receiver.send("emit").await?;
    receiver.expect(&["r0"]).await?;
    let created = provider
        .record_at(receiver.inbox.clone(), 0)
        .await
        .map_err(Error::from)?
        .ok_or_else(|| violation("receiver creation input missing"))?;
    let located = provider
        .record_at(paths.placement(&receiver.actor).map_err(Error::from)?, 0)
        .await
        .map_err(Error::from)?
        .ok_or_else(|| violation("receiver placement missing"))?;
    require(
        created.commit_id == located.commit_id,
        "placement did not share the original actor creation",
    )?;
    let mut sender = Fixture::create(target, run, "routing-source").await?;
    let mut messages = Vec::new();
    for index in 0..2 {
        let message = Envelope {
            source: source.clone(),
            sender: sender.actor.clone(),
            to: receiver.actor.clone(),
            request: CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: key(&root, &format!("input-{index}"))?,
                effects: vec![CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: receiver.inbox.clone(),
                        records: vec!["emit".into()],
                    }],
                )],
            },
        };
        let command = serde_json::to_string(&RouteCommand {
            root: root.clone(),
            envelope: message.encode().to_vec(),
        })
        .map_err(|_| Error::Invalid("route command cannot encode"))?;
        sender.send(&format!("route {command}")).await?;
        messages.push(message);
    }
    sender.expect(&["r0", "r1"]).await?;
    for sequence in 0..2 {
        let publication = provider
            .record_at(paths.outbox(&source).map_err(Error::from)?, sequence)
            .await
            .map_err(Error::from)?
            .ok_or_else(|| violation("source publication missing"))?;
        let output = sender.input(sequence).await?;
        let journal_output = provider
            .record_at(sender.outbox.clone(), sequence)
            .await
            .map_err(Error::from)?
            .ok_or_else(|| violation("sender output missing"))?;
        require(
            publication.commit_id == journal_output.commit_id
                && publication.commit_id != output.commit_id,
            "route outbox was detached from the sender transition",
        )?;
    }
    receiver.expect(&["r0"]).await?;
    let interrupted_forward = crate::route::forward(
        provider.as_ref(),
        &paths,
        &source,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |message| {
            if message != messages[0] {
                return Err(crate::route::Error::InvalidRecord);
            }
            let destination = Placement::read(provider.as_ref(), &paths, &message.to)
                .await?
                .ok_or(crate::route::Error::InvalidRecord)?;
            message
                .publish_relay(provider.as_ref(), &paths, &destination.node, Vec::new())
                .await?;
            // Lose the account response after durable admission, before the
            // sender can record it. Reconnect must resubmit this same intent.
            Err(StreamError::Unavailable.into())
        },
    )
    .await;
    require(
        matches!(
            interrupted_forward,
            Err(crate::route::Error::Stream(StreamError::Unavailable))
        ),
        "forwarder did not stop after losing its admission response",
    )?;
    require(
        matches!(
            provider
                .tail(paths.outbox_cursor(&source).map_err(Error::from)?)
                .await,
            Err(StreamError::NotFound)
        ) && provider
            .tail(paths.relay(&destination).map_err(Error::from)?)
            .await
            .map_err(Error::from)?
            == 1,
        "lost response either acknowledged or lost the admitted input",
    )?;
    target.restart().await?;
    sender.expect(&["r0", "r1"]).await?;
    let moved = Placement {
        actor: receiver.actor.clone(),
        node: source.clone(),
    };
    require(
        matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: key(&root, "move-recipient")?,
                    effects: vec![moved.effect(&paths).map_err(violation)?],
                })
                .await
                .map_err(Error::from)?,
            CommitOutcome::Conflict(_)
        ) && Placement::read(provider.as_ref(), &paths, &receiver.actor)
            .await
            .map_err(violation)?
            .as_ref()
            == Some(&placement),
        "restart allowed the recipient to move to another node",
    )?;
    // The lost response must not let a restarted sender replace the admitted
    // bytes under the original intent. The unchanged retry below is the
    // positive control and must still reach the real receiver exactly once.
    let mut changed = messages[0].clone();
    let CommitMutation::Append { records, .. } = &mut changed.request.effects[0].mutations[0]
    else {
        return Err(violation("route fixture has no inbox append"));
    };
    records[0].value = "changed input".into();
    require(
        matches!(
            changed
                .publish_relay(provider.as_ref(), &paths, &destination, Vec::new())
                .await,
            Err(crate::route::Error::Stream(
                StreamError::IdempotencyMismatch
            ))
        ),
        "restart allowed changed bytes under an admitted intent",
    )?;
    require(
        provider
            .tail(paths.relay(&destination).map_err(Error::from)?)
            .await
            .map_err(Error::from)?
            == 1,
        "a changed retry appended a relay record",
    )?;
    let forward = crate::route::forward(
        provider.as_ref(),
        &paths,
        &source,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |message| {
            let destination = Placement::read(provider.as_ref(), &paths, &message.to)
                .await?
                .ok_or(crate::route::Error::InvalidRecord)?;
            message
                .publish_relay(provider.as_ref(), &paths, &destination.node, Vec::new())
                .await
        },
    );
    until_ack(
        provider.as_ref(),
        paths.outbox_cursor(&source).map_err(Error::from)?,
        1,
        forward,
    )
    .await?;
    require(
        provider
            .tail(paths.relay(&destination).map_err(Error::from)?)
            .await
            .map_err(Error::from)?
            == 2,
        "sender resubmission duplicated its relay input",
    )?;
    let interrupted =
        crate::route::drain(provider.as_ref(), &paths, &destination, async |record| {
            if record.envelope != messages[0] {
                return Err(crate::route::Error::InvalidRecord);
            }
            receiver.deliver(record.envelope.request).await?;
            // The inbox publication is durable; the drain has not committed
            // its acknowledgement. End this consumer at exactly that boundary.
            Err(StreamError::Unavailable.into())
        })
        .await;
    require(
        matches!(
            interrupted,
            Err(crate::route::Error::Stream(StreamError::Unavailable))
        ),
        "drain did not stop at the pre-ack boundary",
    )?;
    require(
        matches!(
            provider
                .tail(paths.relay_cursor(&destination).map_err(Error::from)?)
                .await,
            Err(StreamError::NotFound)
        ),
        "interrupted delivery was acknowledged",
    )?;
    receiver.expect(&["r0", "r1"]).await?;
    target.restart().await?;
    let mut replayed = 0;
    let drain = crate::route::drain(provider.as_ref(), &paths, &destination, async |record| {
        if messages.get(replayed) != Some(&record.envelope) {
            return Err(crate::route::Error::InvalidRecord);
        }
        receiver.deliver(record.envelope.request).await?;
        replayed += 1;
        Ok(())
    });
    // Wait for both durable ack records, rather than cancelling the drain at
    // its first (replayed) record or relying on a readiness delay.
    until_ack(
        provider.as_ref(),
        paths.relay_cursor(&destination).map_err(Error::from)?,
        1,
        drain,
    )
    .await?;
    require(replayed == 2, "reconnect skipped a relay input")?;
    receiver.expect(&["r0", "r1", "r2"]).await?;
    require(
        provider
            .tail(receiver.inbox.clone())
            .await
            .map_err(Error::from)?
            == 3,
        "replay appended another inbox record",
    )?;

    // Resume normal traffic with both original pumps running concurrently.
    // The actor publishes the next outbox record as forwarding and draining
    // resume their original prefixes; no reconnect or readiness timer helps it.
    let mut live = messages[1].clone();
    live.request.idempotency_key = key(&root, "live-input")?;
    let command = serde_json::to_string(&RouteCommand {
        root,
        envelope: live.encode().to_vec(),
    })
    .map_err(|_| Error::Invalid("route command cannot encode"))?;
    let publish = async {
        sender.send(&format!("route {command}")).await?;
        sender.expect(&["r0", "r1", "r2"]).await
    };
    let forward = crate::route::forward(
        provider.as_ref(),
        &paths,
        &source,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |message| {
            let destination = Placement::read(provider.as_ref(), &paths, &message.to)
                .await?
                .ok_or(crate::route::Error::InvalidRecord)?;
            message
                .publish_relay(provider.as_ref(), &paths, &destination.node, Vec::new())
                .await
        },
    );
    let drain = crate::route::drain(provider.as_ref(), &paths, &destination, async |record| {
        if record.envelope != live {
            return Err(crate::route::Error::InvalidRecord);
        }
        receiver.deliver(record.envelope.request).await?;
        Ok(())
    });
    futures::try_join!(
        publish,
        until_ack(
            provider.as_ref(),
            paths.outbox_cursor(&source).map_err(Error::from)?,
            2,
            forward
        ),
        until_ack(
            provider.as_ref(),
            paths.relay_cursor(&destination).map_err(Error::from)?,
            2,
            drain
        ),
    )?;
    receiver.expect(&["r0", "r1", "r2", "r3"]).await?;
    require(
        provider
            .tail(receiver.inbox.clone())
            .await
            .map_err(Error::from)?
            == 4,
        "live routing duplicated or lost an inbox record",
    )
}

async fn drain_one<P: StreamProvider + ?Sized>(
    provider: &P,
    paths: &Paths,
    node: &NodeId,
    expected: &IdempotencyKey,
    acknowledged: u64,
    rejected: Option<(&Envelope, Rejection)>,
) -> Result<(), Failure> {
    let drain = crate::route::drain(provider, paths, node, async |record| {
        if let Some((original, reason)) = rejected {
            if record.rejection(original)? != Some(reason) {
                return Err(crate::route::Error::InvalidRecord);
            }
            let mut changed = original.clone();
            changed.request.idempotency_key = expected.clone();
            if record.rejection(&changed).is_ok() {
                return Err(crate::route::Error::InvalidRecord);
            }
            let mut wrong_target = record.clone();
            wrong_target.envelope.to = original.to.clone();
            if wrong_target.rejection(original).is_ok() {
                return Err(crate::route::Error::InvalidRecord);
            }
        } else if record.rejection(&record.envelope)?.is_some() {
            return Err(crate::route::Error::InvalidRecord);
        }
        if record.receipt.read(provider, paths, node).await? != record.envelope {
            return Err(crate::route::Error::InvalidRecord);
        }
        let mut forged = *record.receipt.commit_id.as_bytes();
        forged[0] ^= 1;
        if (crate::route::RelayReceipt {
            commit_id: acyclic_stream::CommitId::from_bytes(forged),
            ..record.receipt
        })
        .read(provider, paths, node)
        .await
        .is_ok()
        {
            return Err(crate::route::Error::InvalidRecord);
        }
        let envelope = record.envelope;
        if &envelope.request.idempotency_key != expected {
            return Err(crate::route::Error::InvalidRecord);
        }
        match provider.commit(envelope.request).await? {
            CommitOutcome::Committed(_) => Ok(()),
            CommitOutcome::Conflict(_) => Err(crate::route::Error::InvalidRecord),
        }
    });
    until_ack(
        provider,
        paths.relay_cursor(node).map_err(Error::from)?,
        acknowledged,
        drain,
    )
    .await
}

async fn until_ack<P, F>(
    provider: &P,
    cursor: StreamPath,
    acknowledged: u64,
    drain: F,
) -> Result<(), Failure>
where
    P: StreamProvider + ?Sized,
    F: std::future::Future<Output = Result<std::convert::Infallible, crate::route::Error>>,
{
    let mut changes = acyclic_stream::records_only(
        provider
            .follow(acyclic_stream::FollowRequest {
                when_absent: acyclic_stream::WhenAbsent::Wait,
                ..acyclic_stream::FollowRequest::new(cursor, acknowledged)
            })
            .await
            .map_err(Error::from)?,
    );
    let next = changes.try_next();
    futures::pin_mut!(drain, next);
    match futures::future::select(drain, next).await {
        futures::future::Either::Left((result, _)) => Err(violation(format!(
            "drain stopped before acknowledging delivery: {result:?}"
        ))),
        futures::future::Either::Right((record, _)) => {
            require(record.map_err(Error::from)?.is_some(), "ack follow ended")
        }
    }
}

fn violation(detail: impl std::fmt::Display) -> Failure {
    Failure::Violation {
        case: "routing-storage",
        detail: detail.to_string(),
    }
}

fn require(condition: bool, detail: &str) -> Result<(), Failure> {
    if condition {
        Ok(())
    } else {
        Err(violation(detail))
    }
}

fn key(root: &StreamPath, label: &str) -> Result<IdempotencyKey, Failure> {
    use sha2::{Digest as _, Sha256};
    IdempotencyKey::new(Sha256::digest(format!("{root}/{label}").as_bytes()).to_vec())
        .map_err(Error::from)
        .map_err(Into::into)
}

async fn values<P: StreamProvider + ?Sized>(
    provider: &P,
    path: StreamPath,
) -> Result<Vec<acyclic_stream::Record>, Failure> {
    match provider
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 2,
            at_least: Default::default(),
        })
        .await
    {
        Ok(records) => records
            .try_collect()
            .await
            .map_err(Error::from)
            .map_err(Into::into),
        Err(StreamError::NotFound) => Ok(Vec::new()),
        Err(error) => Err(Error::from(error).into()),
    }
}

/// Placement is immutable and outbox publication is atomic with the sender's
/// journal. Called by the shared suite for every actual storage backend.
pub async fn storage<P: StreamProvider + ?Sized>(
    provider: &P,
    root: StreamPath,
) -> Result<(), Failure> {
    let paths = Paths::new(root.clone());
    let sender = ActorReference::new(b"routing", b"sessions", b"sender").map_err(violation)?;
    let to = ActorReference::new(b"routing", b"sessions", b"recipient").map_err(violation)?;
    let source = NodeId::new(b"source").map_err(violation)?;
    let destination = NodeId::new(b"destination").map_err(violation)?;
    let placement = Placement {
        actor: to.clone(),
        node: destination,
    };
    require(
        Placement::read(provider, &paths, &to)
            .await
            .map_err(violation)?
            .is_none(),
        "missing placement must remain unknown",
    )?;
    let create = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![placement.effect(&paths).map_err(violation)?],
        idempotency_key: key(&root, "place")?,
    };
    let first = provider.commit(create.clone()).await.map_err(Error::from)?;
    require(
        matches!(first, CommitOutcome::Committed(_)),
        "initial placement did not commit",
    )?;
    require(
        provider.commit(create.clone()).await.map_err(Error::from)? == first,
        "placement retry changed its receipt",
    )?;
    require(
        Placement::read(provider, &paths, &to)
            .await
            .map_err(violation)?
            .as_ref()
            == Some(&placement),
        "placement did not resolve to its original node",
    )?;
    let mut replace = create.clone();
    replace.idempotency_key = key(&root, "replace-placement")?;
    for node in [&placement.node, &source] {
        replace.effects = vec![
            Placement {
                actor: to.clone(),
                node: node.clone(),
            }
            .effect(&paths)
            .map_err(violation)?,
        ];
        require(
            matches!(
                provider
                    .commit(replace.clone())
                    .await
                    .map_err(Error::from)?,
                CommitOutcome::Conflict(_)
            ),
            "a second placement was accepted",
        )?;
    }
    let inbox = StreamPath::new(format!("{root}/recipient-inbox")).map_err(Error::from)?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: inbox.clone(),
                records: vec!["peer input".into()],
            }],
        )],
        idempotency_key: key(&root, "original-send")?,
    };
    let envelope = Envelope {
        source: source.clone(),
        sender,
        to,
        request,
    };
    let journal = StreamPath::new(format!("{root}/sender-journal")).map_err(Error::from)?;
    let missing = StreamPath::new(format!("{root}/missing-admission")).map_err(Error::from)?;
    let mut publish = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![
            envelope.outbox_effect(&paths).map_err(violation)?,
            CommitEffect::hard(
                vec![CommitCondition::Present { path: missing }],
                vec![CommitMutation::Append {
                    path: journal.clone(),
                    records: vec!["sender effect".into()],
                }],
            ),
        ],
        idempotency_key: key(&root, "sender-step")?,
    };
    require(
        matches!(
            provider
                .commit(publish.clone())
                .await
                .map_err(Error::from)?,
            CommitOutcome::Conflict(_)
        ),
        "sender's failed guard was ignored",
    )?;
    require(
        values(provider, paths.outbox(&source).map_err(Error::from)?)
            .await?
            .is_empty(),
        "failed sender step leaked a forwardable message",
    )?;
    publish.effects[1].conditions.clear();
    let receipt = provider
        .commit(publish.clone())
        .await
        .map_err(Error::from)?;
    require(
        matches!(receipt, CommitOutcome::Committed(_)),
        "sender journal and outbox did not commit",
    )?;
    require(
        provider
            .commit(publish.clone())
            .await
            .map_err(Error::from)?
            == receipt,
        "sender retry changed its receipt",
    )?;
    let messages = values(provider, paths.outbox(&source).map_err(Error::from)?).await?;
    require(
        messages.len() == 1 && values(provider, journal).await?.len() == 1,
        "sender retry duplicated an outbox or journal record",
    )?;
    let retained = crate::route::Publication::decode(&messages[0].value)
        .and_then(crate::route::Publication::delivery)
        .map_err(violation)?;
    require(
        retained == envelope,
        "outbox changed original sender, target, key or request bytes",
    )?;
    require(
        values(provider, inbox).await?.is_empty(),
        "source outbox wrote the remote inbox",
    )?;
    publish.effects[0].mutations[0] = CommitMutation::Append {
        path: paths.outbox(&source).map_err(Error::from)?,
        records: vec!["changed intent bytes".into()],
    };
    require(
        matches!(
            provider.commit(publish).await,
            Err(StreamError::IdempotencyMismatch)
        ),
        "changed sender bytes reused a spent intent key",
    )?;
    let relay = envelope
        .relay_request(&paths, &placement.node)
        .map_err(violation)?;
    require(
        crate::route::receipt(
            provider,
            &paths,
            &source,
            0,
            &envelope.request.idempotency_key,
        )
        .await
        .map_err(violation)?
        .is_none(),
        "unforwarded outbox acquired a relay receipt",
    )?;
    // Lose the account receipt after relay publication. The local outbox
    // cursor must remain unchanged and reconnect must resubmit the same intent.
    let interrupted = crate::route::forward(
        provider,
        &paths,
        &source,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |record| {
            let destination = Placement::read(provider, &paths, &record.to)
                .await?
                .ok_or(crate::route::Error::InvalidRecord)?;
            record
                .publish_relay(provider, &paths, &destination.node, Vec::new())
                .await?;
            Err(StreamError::Unavailable.into())
        },
    )
    .await;
    require(
        matches!(
            interrupted,
            Err(crate::route::Error::Stream(StreamError::Unavailable))
        ),
        "forwarder did not retain an unacknowledged account publication",
    )?;
    require(
        crate::route::receipt(
            provider,
            &paths,
            &source,
            0,
            &envelope.request.idempotency_key,
        )
        .await
        .map_err(violation)?
        .is_none(),
        "lost account receipt advanced local outbox progress",
    )?;
    for invalid in [
        Routed::Delivered(crate::route::Delivered { inbox: 0 }),
        Routed::Relayed {
            key: key(&root, "unrelated-intent")?,
            relay_seq: 0,
        },
    ] {
        let refused = crate::route::forward(
            provider,
            &paths,
            &source,
            async |_| Err(crate::route::Error::InvalidRecord),
            async |_| Ok(invalid.clone()),
        )
        .await;
        require(
            matches!(
                refused,
                Err(crate::route::Error::Stream(StreamError::InvalidResponse))
            ),
            "invalid account receipt advanced the outbox",
        )?;
        require(
            crate::route::receipt(
                provider,
                &paths,
                &source,
                0,
                &envelope.request.idempotency_key,
            )
            .await
            .map_err(violation)?
            .is_none(),
            "invalid receipt was retained",
        )?;
    }
    let forward = crate::route::forward(
        provider,
        &paths,
        &source,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |record| {
            if record != envelope {
                return Err(crate::route::Error::InvalidRecord);
            }
            let destination = Placement::read(provider, &paths, &record.to)
                .await?
                .ok_or(crate::route::Error::InvalidRecord)?;
            record
                .publish_relay(provider, &paths, &destination.node, Vec::new())
                .await
        },
    );
    until_ack(
        provider,
        paths.outbox_cursor(&source).map_err(Error::from)?,
        0,
        forward,
    )
    .await?;
    let admitted = crate::route::receipt(
        provider,
        &paths,
        &source,
        0,
        &envelope.request.idempotency_key,
    )
    .await
    .map_err(violation)?;
    require(
        admitted
            == Some(Routed::Relayed {
                key: envelope.request.idempotency_key.clone(),
                relay_seq: 0,
            }),
        "source acknowledgement lost its original relay receipt",
    )?;
    require(
        crate::route::receipt(
            provider,
            &paths,
            &source,
            2,
            &envelope.request.idempotency_key,
        )
        .await
        .map_err(violation)?
        .is_none(),
        "receipt beyond the acknowledged prefix was not pending",
    )?;
    let foreign_source = envelope.clone();
    let foreign_node = placement.node.clone();
    let source_path = paths.outbox(&foreign_node).map_err(Error::from)?;
    let mut foreign_effect = foreign_source.outbox_effect(&paths).map_err(violation)?;
    foreign_effect.conditions.push(CommitCondition::Absent {
        path: source_path.clone(),
    });
    let CommitMutation::Append { path, .. } = &mut foreign_effect.mutations[0] else {
        return Err(violation("outbox append missing"));
    };
    *path = source_path;
    let inserted = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key(&root, "foreign-source")?,
            effects: vec![foreign_effect],
        })
        .await
        .map_err(Error::from)?;
    require(
        matches!(inserted, CommitOutcome::Committed(_)),
        "foreign source fixture missing",
    )?;
    let refused = crate::route::forward(
        provider,
        &paths,
        &foreign_node,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |_| Err(StreamError::AccessDenied.into()),
    )
    .await;
    require(
        matches!(refused, Err(crate::route::Error::InvalidRecord)),
        "forwarder admitted a different node's claimed source",
    )?;
    let relayed = provider.commit(relay.clone()).await.map_err(Error::from)?;
    require(
        matches!(relayed, CommitOutcome::Committed(_)),
        "relay append did not commit",
    )?;
    require(
        provider.commit(relay).await.map_err(Error::from)? == relayed,
        "relay retry changed its receipt",
    )?;
    let original = envelope
        .publish_relay(provider, &paths, &placement.node, Vec::new())
        .await
        .map_err(violation)?;
    let mut unfunded = envelope.clone();
    unfunded.request.idempotency_key = key(&root, "unfunded-send")?;
    let soft_debit = CommitEffect {
        conditions: vec![CommitCondition::Present {
            path: StreamPath::new(format!("{root}/missing-credit")).map_err(Error::from)?,
        }],
        mutations: vec![CommitMutation::Append {
            path: StreamPath::new(format!("{root}/skipped-debit")).map_err(Error::from)?,
            records: vec!["unfunded debit".into()],
        }],
        soft: true,
    };
    require(
        matches!(
            unfunded
                .publish_relay(provider, &paths, &placement.node, vec![soft_debit])
                .await,
            Err(crate::route::Error::InvalidRecord)
        ),
        "a skipped storage debit admitted an unfunded relay",
    )?;
    let unfunded_key = unfunded
        .relay_request(&paths, &placement.node)
        .map_err(violation)?
        .idempotency_key;
    require(
        provider
            .inspect_idempotency(unfunded_key)
            .await
            .map_err(Error::from)?
            .is_none(),
        "refused storage admission spent the intent",
    )?;
    let debit = StreamPath::new(format!("{root}/later-storage-debit")).map_err(Error::from)?;
    let changed_guards = vec![CommitEffect::hard(
        vec![CommitCondition::Present {
            path: paths.placement(&envelope.to).map_err(Error::from)?,
        }],
        vec![CommitMutation::Append {
            path: debit.clone(),
            records: vec!["new debit".into()],
        }],
    )];
    require(
        envelope
            .publish_relay(provider, &paths, &placement.node, changed_guards)
            .await
            .map_err(violation)?
            == original,
        "changed account guards replaced the original relay result",
    )?;
    require(
        values(provider, debit).await?.is_empty(),
        "recovery applied a new storage debit",
    )?;
    require(
        envelope
            .publish_relay(provider, &paths, &source, Vec::new())
            .await
            .map_err(violation)?
            == original,
        "a changed destination published the same intent twice",
    )?;
    require(
        values(provider, paths.relay(&source).map_err(Error::from)?)
            .await?
            .is_empty(),
        "recovery wrote to the newly resolved relay",
    )?;
    let records = values(provider, paths.relay(&placement.node).map_err(Error::from)?).await?;
    require(
        records.len() == 1,
        "relay retry duplicated the original intent",
    )?;
    let restored = Envelope::decode(records[0].value.clone()).map_err(violation)?;
    require(
        restored == envelope,
        "relay altered the original ordinary-delivery request",
    )?;
    // Stop the real drain at the crash cut: ordinary delivery committed, but
    // its acknowledgement did not. Reopening must replay that exact request.
    let interrupted =
        crate::route::drain(
            provider,
            &paths,
            &placement.node,
            async |record| match provider.commit(record.envelope.request).await? {
                CommitOutcome::Committed(_) => Err(StreamError::Unavailable.into()),
                CommitOutcome::Conflict(_) => Err(crate::route::Error::InvalidRecord),
            },
        )
        .await;
    require(
        matches!(
            interrupted,
            Err(crate::route::Error::Stream(StreamError::Unavailable))
        ),
        "drain did not stop at the pre-ack crash cut",
    )?;
    require(
        values(
            provider,
            paths.relay_cursor(&placement.node).map_err(Error::from)?,
        )
        .await?
        .is_empty(),
        "failed delivery advanced the durable relay cursor",
    )?;
    drain_one(
        provider,
        &paths,
        &placement.node,
        &restored.request.idempotency_key,
        0,
        None,
    )
    .await?;
    let inbox = StreamPath::new(format!("{root}/recipient-inbox")).map_err(Error::from)?;
    require(
        values(provider, inbox.clone()).await?.len() == 1,
        "ordinary delivery replay duplicated input",
    )?;
    let mut following = envelope.clone();
    following.request.idempotency_key = key(&root, "following-send")?;
    following.request.effects[0].mutations[0] = CommitMutation::Append {
        path: inbox.clone(),
        records: vec!["following peer input".into()],
    };
    let accepted = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key(&root, "following-sender-step")?,
            effects: vec![following.outbox_effect(&paths).map_err(violation)?],
        })
        .await
        .map_err(Error::from)?;
    require(
        matches!(accepted, CommitOutcome::Committed(_)),
        "following sender publication refused",
    )?;
    require(
        crate::route::receipt(
            provider,
            &paths,
            &source,
            1,
            &following.request.idempotency_key,
        )
        .await
        .map_err(violation)?
        .is_none(),
        "pending send reused an earlier receipt",
    )?;
    let forward = crate::route::forward(
        provider,
        &paths,
        &source,
        async |_| Err(crate::route::Error::InvalidRecord),
        async |record| {
            record
                .publish_relay(provider, &paths, &placement.node, Vec::new())
                .await
        },
    );
    until_ack(
        provider,
        paths.outbox_cursor(&source).map_err(Error::from)?,
        1,
        forward,
    )
    .await?;
    require(
        crate::route::receipt(
            provider,
            &paths,
            &source,
            0,
            &envelope.request.idempotency_key,
        )
        .await
        .map_err(violation)?
            == admitted
            && crate::route::receipt(
                provider,
                &paths,
                &source,
                1,
                &following.request.idempotency_key,
            )
            .await
            .map_err(violation)?
                == Some(Routed::Relayed {
                    key: following.request.idempotency_key.clone(),
                    relay_seq: 1,
                }),
        "later outbox progress changed or lost an original admission receipt",
    )?;
    require(
        matches!(
            crate::route::receipt(
                provider,
                &paths,
                &source,
                0,
                &following.request.idempotency_key
            )
            .await,
            Err(crate::route::Error::Stream(StreamError::InvalidResponse))
        ),
        "source receipt was disclosed under another intent",
    )?;
    drain_one(
        provider,
        &paths,
        &placement.node,
        &following.request.idempotency_key,
        1,
        None,
    )
    .await?;
    let inputs = values(provider, inbox).await?;
    require(
        inputs.len() == 2 && inputs[1].value.as_ref() == b"following peer input",
        "reopened drain repeated acknowledged input or changed FIFO",
    )?;
    let mut changed = envelope.clone();
    changed.request.effects[0].mutations[0] = CommitMutation::Append {
        path: StreamPath::new(format!("{root}/recipient-inbox")).map_err(Error::from)?,
        records: vec!["different peer input".into()],
    };
    require(
        matches!(
            provider
                .commit(
                    changed
                        .relay_request(&paths, &placement.node)
                        .map_err(violation)?
                )
                .await,
            Err(StreamError::IdempotencyMismatch)
        ),
        "changed relay bytes reused the original intent",
    )?;
    require(
        matches!(
            changed
                .publish_relay(provider, &paths, &placement.node, Vec::new())
                .await,
            Err(crate::route::Error::Stream(
                StreamError::IdempotencyMismatch
            ))
        ),
        "recovered receipt admitted changed original envelope bytes",
    )?;
    let destination_relay = paths.relay(&placement.node).map_err(Error::from)?;
    let accepted_tail = provider
        .tail(destination_relay.clone())
        .await
        .map_err(Error::from)?;
    let notices = StreamPath::new(format!("{root}/sender-notices")).map_err(Error::from)?;
    for (index, (reason, body)) in [
        (Rejection::Closed, "closed recipient"),
        (Rejection::NoSuchActor, "unknown recipient"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut rejected = envelope.clone();
        rejected.request.idempotency_key = key(&root, &format!("rejected-{index}"))?;
        let notice = Envelope {
            source: NodeId::new(b"account-node").map_err(violation)?,
            sender: ActorReference::new(b"routing", b"nodes", b"router").map_err(violation)?,
            to: rejected.sender.clone(),
            request: CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: key(&root, &format!("notice-{index}"))?,
                effects: vec![CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: notices.clone(),
                        records: vec![body.into()],
                    }],
                )],
            },
        };
        let refused = rejected
            .publish_rejection(provider, &paths, reason, &notice, Vec::new())
            .await
            .map_err(violation)?;
        require(
            refused == Routed::Rejected(reason),
            "rejection outcome changed",
        )?;
        let published = provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: key(&root, &format!("rejected-sender-step-{index}"))?,
                effects: vec![rejected.outbox_effect(&paths).map_err(violation)?],
            })
            .await
            .map_err(Error::from)?;
        require(
            matches!(published, CommitOutcome::Committed(_)),
            "rejected send was not staged",
        )?;
        let forward = crate::route::forward(
            provider,
            &paths,
            &source,
            async |_| Err(crate::route::Error::InvalidRecord),
            async |record| {
                if record != rejected {
                    return Err(crate::route::Error::InvalidRecord);
                }
                record
                    .publish_rejection(provider, &paths, reason, &notice, Vec::new())
                    .await
            },
        );
        let outbox_sequence = 2 + index as u64;
        until_ack(
            provider,
            paths.outbox_cursor(&source).map_err(Error::from)?,
            outbox_sequence,
            forward,
        )
        .await?;
        require(
            crate::route::receipt(
                provider,
                &paths,
                &source,
                outbox_sequence,
                &rejected.request.idempotency_key,
            )
            .await
            .map_err(violation)?
                == Some(refused.clone()),
            "source receipt lost the original rejection",
        )?;
        require(
            envelope
                .publish_rejection(provider, &paths, reason, &notice, Vec::new())
                .await
                .map_err(violation)?
                == original,
            "a later refusal replaced an accepted relay outcome",
        )?;
        // A later route or refusal reason must recover the original decision,
        // not deliver a previously refused message or publish another Notice.
        require(
            rejected
                .publish_relay(provider, &paths, &placement.node, Vec::new())
                .await
                .map_err(violation)?
                == refused,
            "a retry delivered an already rejected message",
        )?;
        let different_reason = if reason == Rejection::Closed {
            Rejection::NoSuchActor
        } else {
            Rejection::Closed
        };
        let mut later_notice = notice.clone();
        later_notice.request.effects[0].mutations[0] = CommitMutation::Append {
            path: notices.clone(),
            records: vec!["later refusal".into()],
        };
        require(
            rejected
                .publish_rejection(
                    provider,
                    &paths,
                    different_reason,
                    &later_notice,
                    Vec::new(),
                )
                .await
                .map_err(violation)?
                == refused,
            "a retry changed the original refusal",
        )?;
        let mut altered = rejected.clone();
        altered.request.effects[0].mutations[0] = CommitMutation::Append {
            path: StreamPath::new(format!("{root}/recipient-inbox")).map_err(Error::from)?,
            records: vec!["changed refused input".into()],
        };
        require(
            matches!(
                altered
                    .publish_relay(provider, &paths, &placement.node, Vec::new())
                    .await,
                Err(crate::route::Error::Stream(
                    StreamError::IdempotencyMismatch
                ))
            ),
            "rejection recovery accepted changed intent bytes",
        )?;
        require(
            provider
                .tail(paths.relay(&source).map_err(Error::from)?)
                .await
                .map_err(Error::from)?
                == index as u64 + 1,
            "rejection retry duplicated its routed Notice",
        )?;
        drain_one(
            provider,
            &paths,
            &source,
            &notice.request.idempotency_key,
            index as u64,
            Some((&rejected, reason)),
        )
        .await?;
    }
    require(
        provider.tail(notices.clone()).await.map_err(Error::from)? == 2
            && provider
                .tail(destination_relay)
                .await
                .map_err(Error::from)?
                == accepted_tail,
        "rejection drain lost a Notice or delivered rejected content",
    )?;
    let received = values(provider, notices).await?;
    require(
        received[0].value.as_ref() == b"closed recipient"
            && received[1].value.as_ref() == b"unknown recipient",
        "rejection recovery replaced the original Notice bytes",
    )?;
    Ok(())
}
