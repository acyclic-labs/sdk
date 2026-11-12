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
        records.len() == 1,
        "relay retry duplicated the original intent",
    )?;
    let restored = Envelope::decode(records[0].value.clone()).map_err(violation)?;
    require(
        restored == envelope,
        "relay altered the original ordinary-delivery request",
    )?;
    let delivered = provider
        .commit(restored.request.clone())
        .await
        .map_err(Error::from)?;
    require(
        matches!(delivered, CommitOutcome::Committed(_)),
        "relay and inbox spent the same provider key",
    )?;
    // Reconstruct from the durable relay, as after loss between inbox commit
    // and acknowledgement. Only the ordinary request is submitted again.
    let replay = Envelope::decode(records[0].value.clone()).map_err(violation)?;
    require(
        provider.commit(replay.request).await.map_err(Error::from)? == delivered,
        "ordinary delivery replay changed its receipt",
    )?;
    let inbox = StreamPath::new(format!("{root}/recipient-inbox")).map_err(Error::from)?;
    require(
        values(provider, inbox).await?.len() == 1,
        "ordinary delivery replay duplicated input",
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
