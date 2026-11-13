//! Routing transaction invariants use the original backend, not a mock router.
use super::{Error, Failure};
use crate::{
    addressing::ActorReference,
    route::{Envelope, NodeId, Paths, Placement, Rejection, Routed},
};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    ReadRequest, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt as _;

async fn drain_one<P: StreamProvider + ?Sized>(
    provider: &P,
    paths: &Paths,
    node: &NodeId,
    expected: &IdempotencyKey,
    acknowledged: u64,
) -> Result<(), Failure> {
    let drain = crate::route::drain(provider, paths, node, async |envelope| {
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
    let call = crate::call::CallPath::new(&root, "send").map_err(violation)?;
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
            call.request("send".into(), crate::escrow::Escrow::new(None))
                .map_err(violation)?
                .into(),
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
    let retained = Envelope::decode(messages[0].value.clone()).map_err(violation)?;
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
    let claimed = provider
        .commit(
            call.accept(
                &crate::call::Accepted {
                    executor: "account-router".into(),
                    fence: None,
                },
                true,
            )
            .map_err(violation)?
            .request(key(&root, "forwarder-claim")?),
        )
        .await
        .map_err(Error::from)?;
    require(
        matches!(claimed, CommitOutcome::Committed(_)),
        "forwarder did not claim its original call",
    )?;
    // Lose the account receipt after relay publication. The local outbox
    // cursor must remain unchanged and reconnect must resubmit the same intent.
    let interrupted = crate::route::forward(provider, &paths, &source, async |record| {
        let destination = Placement::read(provider, &paths, &record.to)
            .await?
            .ok_or(crate::route::Error::InvalidRecord)?;
        record
            .publish_relay(provider, &paths, &destination.node, Vec::new())
            .await?;
        Err(StreamError::Unavailable.into())
    })
    .await;
    require(
        matches!(
            interrupted,
            Err(crate::route::Error::Stream(StreamError::Unavailable))
        ),
        "forwarder did not retain an unacknowledged account publication",
    )?;
    require(
        values(provider, paths.outbox_cursor(&source).map_err(Error::from)?)
            .await?
            .is_empty(),
        "lost account receipt advanced local outbox progress",
    )?;
    let completion = |relay_seq: u64| -> Result<_, crate::route::Error> {
        call.complete(&crate::call::Outcome::Ok(
            relay_seq.to_le_bytes().to_vec().into(),
        ))
        .map(Some)
        .map_err(|_| crate::route::Error::InvalidRecord)
    };
    let forward = crate::route::forward(provider, &paths, &source, async |record| {
        if record != envelope {
            return Err(crate::route::Error::InvalidRecord);
        }
        let destination = Placement::read(provider, &paths, &record.to)
            .await?
            .ok_or(crate::route::Error::InvalidRecord)?;
        let routed = record
            .publish_relay(provider, &paths, &destination.node, Vec::new())
            .await?;
        let Routed::Relayed { relay_seq, .. } = routed else {
            return Err(crate::route::Error::InvalidRecord);
        };
        completion(relay_seq)
    });
    until_ack(
        provider,
        paths.outbox_cursor(&source).map_err(Error::from)?,
        0,
        forward,
    )
    .await?;
    let completed = provider
        .head(call.log().clone())
        .await
        .map_err(Error::from)?;
    let acknowledged = provider
        .head(paths.outbox_cursor(&source).map_err(Error::from)?)
        .await
        .map_err(Error::from)?;
    let completed = completed
        .last
        .ok_or_else(|| violation("missing call receipt"))?;
    require(
        completed.commit_id
            == acknowledged
                .last
                .ok_or_else(|| violation("missing outbox ack"))?
                .commit_id
            && matches!(crate::call::Entry::decode(&completed).map_err(violation)?,
                crate::call::Entry::Terminal(crate::call::Outcome::Ok(value)) if value.as_ref() == 0u64.to_le_bytes())
            && provider
                .tail(call.log().clone())
                .await
                .map_err(Error::from)?
                == 4
            && provider
                .tail(call.settled().clone())
                .await
                .map_err(Error::from)?
                == 1,
        "relay receipt and outbox progress were not committed exactly once together",
    )?;
    let mut foreign_source = envelope.clone();
    foreign_source.source = placement.node.clone();
    let source_path = paths.outbox(&source).map_err(Error::from)?;
    let inserted = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key(&root, "foreign-source")?,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Present {
                    path: source_path.clone(),
                }],
                vec![CommitMutation::Append {
                    path: source_path,
                    records: vec![foreign_source.encode().into()],
                }],
            )],
        })
        .await
        .map_err(Error::from)?;
    require(
        matches!(inserted, CommitOutcome::Committed(_)),
        "foreign source fixture missing",
    )?;
    let refused = crate::route::forward(provider, &paths, &source, async |_| {
        Err(StreamError::AccessDenied.into())
    })
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
            async |envelope| match provider.commit(envelope.request).await? {
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
        .commit(
            following
                .relay_request(&paths, &placement.node)
                .map_err(violation)?,
        )
        .await
        .map_err(Error::from)?;
    require(
        matches!(accepted, CommitOutcome::Committed(_)),
        "following relay refused",
    )?;
    drain_one(
        provider,
        &paths,
        &placement.node,
        &following.request.idempotency_key,
        1,
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
    cancelled_forwarding(provider, &root, &envelope, &placement.node).await?;
    Ok(())
}

async fn cancelled_forwarding<P: StreamProvider + ?Sized>(
    provider: &P,
    root: &StreamPath,
    original: &Envelope,
    destination: &NodeId,
) -> Result<(), Failure> {
    use crate::call::{Accepted, CallPath, Entry, Outcome};
    let root = StreamPath::new(format!("{root}/cancelled-forwarding")).map_err(Error::from)?;
    let paths = Paths::new(root.clone());
    let cancelled = CallPath::new(&root, "cancelled").map_err(violation)?;
    let active = CallPath::new(&root, "active").map_err(violation)?;
    let mut first = original.clone();
    first.request.idempotency_key = key(&root, "first-message")?;
    let mut second = first.clone();
    second.request.idempotency_key = key(&root, "second-message")?;
    let created = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key(&root, "sender-publication")?,
            effects: vec![
                Placement {
                    actor: first.to.clone(),
                    node: destination.clone(),
                }
                .effect(&paths)
                .map_err(violation)?,
                CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: paths.outbox(&first.source).map_err(Error::from)?,
                        records: vec![first.encode().into(), second.encode().into()],
                    }],
                ),
                cancelled
                    .request("first message".into(), crate::escrow::Escrow::new(None))
                    .map_err(violation)?
                    .into(),
                active
                    .request("second message".into(), crate::escrow::Escrow::new(None))
                    .map_err(violation)?
                    .into(),
            ],
        })
        .await
        .map_err(Error::from)?;
    require(
        matches!(created, CommitOutcome::Committed(_)),
        "cancelled forwarding fixture did not commit",
    )?;
    // Prepare completion before cancellation: its eventual soft condition must
    // retain the terminal that wins, rather than blocking the ordered outbox.
    let mut prepared = Some(
        cancelled
            .complete(&Outcome::Ok(0u64.to_le_bytes().to_vec().into()))
            .map_err(violation)?,
    );
    let cancellation = provider
        .commit(
            cancelled
                .cancel("caller stopped")
                .map_err(violation)?
                .request(key(&root, "cancel-call")?),
        )
        .await
        .map_err(Error::from)?;
    require(
        matches!(cancellation, CommitOutcome::Committed(_)),
        "caller cancellation did not commit",
    )?;
    let claimed = provider
        .commit(
            active
                .accept(
                    &Accepted {
                        executor: "account-router".into(),
                        fence: None,
                    },
                    true,
                )
                .map_err(violation)?
                .request(key(&root, "claim-next-call")?),
        )
        .await
        .map_err(Error::from)?;
    require(
        matches!(claimed, CommitOutcome::Committed(_)),
        "next call was not claimed",
    )?;
    let forward = crate::route::forward(provider, &paths, &first.source, async |record| {
        let destination = Placement::read(provider, &paths, &record.to)
            .await?
            .ok_or(crate::route::Error::InvalidRecord)?;
        let routed = record
            .publish_relay(provider, &paths, &destination.node, Vec::new())
            .await?;
        let Routed::Relayed { relay_seq, .. } = routed else {
            return Err(crate::route::Error::InvalidRecord);
        };
        if record == first && relay_seq == 0 {
            Ok(Some(
                prepared.take().ok_or(crate::route::Error::InvalidRecord)?,
            ))
        } else if record == second && relay_seq == 1 {
            active
                .complete(&Outcome::Ok(relay_seq.to_le_bytes().to_vec().into()))
                .map(Some)
                .map_err(|_| crate::route::Error::InvalidRecord)
        } else {
            Err(crate::route::Error::InvalidRecord)
        }
    });
    until_ack(
        provider,
        paths.outbox_cursor(&first.source).map_err(Error::from)?,
        1,
        forward,
    )
    .await?;
    let cancelled_head = provider
        .head(cancelled.log().clone())
        .await
        .map_err(Error::from)?;
    let active_head = provider
        .head(active.log().clone())
        .await
        .map_err(Error::from)?;
    let ack = provider
        .head(paths.outbox_cursor(&first.source).map_err(Error::from)?)
        .await
        .map_err(Error::from)?;
    require(
        cancelled_head.tail == 3
            && matches!(Entry::decode(&cancelled_head.last.ok_or_else(|| violation("missing cancellation"))?)
                .map_err(violation)?, Entry::Terminal(Outcome::Cancelled(reason)) if reason == "caller stopped")
            && active_head.tail == 4
            && active_head
                .last
                .as_ref()
                .ok_or_else(|| violation("missing next call completion"))?
                .commit_id
                == ack
                    .last
                    .ok_or_else(|| violation("missing next outbox ack"))?
                    .commit_id
            && matches!(Entry::decode(active_head.last.as_ref().ok_or_else(|| violation("missing next receipt"))?)
                .map_err(violation)?, Entry::Terminal(Outcome::Ok(value)) if value.as_ref() == 1u64.to_le_bytes())
            && provider
                .tail(paths.relay(destination).map_err(Error::from)?)
                .await
                .map_err(Error::from)?
                == 2,
        "caller cancellation lost a durable message, replaced its terminal, or blocked the next outbox record",
    )?;
    Ok(())
}
