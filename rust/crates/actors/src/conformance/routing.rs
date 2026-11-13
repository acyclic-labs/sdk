//! Routing transaction invariants use the original backend, not a mock router.
use super::{Error, Failure};
use crate::{
    addressing::ActorReference,
    route::{Envelope, NodeId, Paths, Placement},
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
            .follow(acyclic_stream::FollowRequest::new(cursor, acknowledged))
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
        effects: vec![
            placement.effect(&paths).map_err(violation)?,
            paths.relay_creation(&placement.node).map_err(violation)?,
            paths.outbox_creation(&source).map_err(violation)?,
        ],
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
            .len()
            == 1,
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
        messages.len() == 2
            && messages[0].value.is_empty()
            && values(provider, journal).await?.len() == 1,
        "sender retry duplicated an outbox or journal record",
    )?;
    let retained = Envelope::decode(messages[1].value.clone()).map_err(violation)?;
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
    // Lose the account receipt after relay publication. The local outbox
    // cursor must remain unchanged and reconnect must resubmit the same intent.
    let interrupted = crate::route::forward(provider, &paths, &source, async |record| {
        let destination = Placement::read(provider, &paths, &record.to)
            .await?
            .ok_or(crate::route::Error::InvalidRecord)?;
        match provider
            .commit(record.relay_request(&paths, &destination.node)?)
            .await?
        {
            CommitOutcome::Committed(_) => Err(StreamError::Unavailable.into()),
            CommitOutcome::Conflict(_) => Err(crate::route::Error::InvalidRecord),
        }
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
            .len()
            == 1,
        "lost account receipt advanced local outbox progress",
    )?;
    let forward = crate::route::forward(provider, &paths, &source, async |record| {
        if record != envelope {
            return Err(crate::route::Error::InvalidRecord);
        }
        let destination = Placement::read(provider, &paths, &record.to)
            .await?
            .ok_or(crate::route::Error::InvalidRecord)?;
        match provider
            .commit(record.relay_request(&paths, &destination.node)?)
            .await?
        {
            CommitOutcome::Committed(_) => Ok(()),
            CommitOutcome::Conflict(_) => Err(crate::route::Error::InvalidRecord),
        }
    });
    until_ack(
        provider,
        paths.outbox_cursor(&source).map_err(Error::from)?,
        1,
        forward,
    )
    .await?;
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
    let records = values(provider, paths.relay(&placement.node).map_err(Error::from)?).await?;
    require(
        records.len() == 2 && records[0].value.is_empty(),
        "relay retry duplicated the original intent",
    )?;
    let restored = Envelope::decode(records[1].value.clone()).map_err(violation)?;
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
        .len()
            == 1,
        "failed delivery advanced the durable relay cursor",
    )?;
    drain_one(
        provider,
        &paths,
        &placement.node,
        &restored.request.idempotency_key,
        1,
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
        2,
    )
    .await?;
    let inputs = values(provider, inbox).await?;
    require(
        inputs.len() == 2 && inputs[1].value.as_ref() == b"following peer input",
        "reopened drain repeated acknowledged input or changed FIFO",
    )?;
    let mut changed = envelope;
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
    Ok(())
}
