//! One follow per destination, with acknowledgement after ordinary delivery.
use super::{Envelope, Error, NodeId, Paths};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, FollowRequest,
    IdempotencyKey, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use std::convert::Infallible;

#[derive(Clone, PartialEq, prost::Message)]
struct Cursor {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    next: u64,
}

/// Drain this node's relay on its already authenticated account provider.
/// The caller owns this future for the lifetime of its accepted NodeLink;
/// dropping it closes the one follow. Reopening starts at the committed cursor.
///
/// `deliver` must run the owning node's ordinary delivery, including admission
/// and a durable routed Notice for a rejected recipient. Only its successful
/// completion permits acknowledgement. A failure or crash before that commit
/// replays the original envelope and original inbox intent, never a new key.
/// Credentials and authority remain in the provider and delivery host.
pub async fn drain<P, D>(
    account: &P,
    paths: &Paths,
    node: &NodeId,
    mut deliver: D,
) -> Result<Infallible, Error>
where
    P: StreamProvider + ?Sized,
    D: AsyncFnMut(Envelope) -> Result<(), Error>,
{
    consume(
        account,
        paths.relay(node)?,
        paths.relay_cursor(node)?,
        async |envelope| {
            deliver(envelope).await?;
            Ok(Vec::new())
        },
    )
    .await
}

/// Forward the source node's durable outbox in order while its NodeLink is open.
/// `admit` must await account placement, authority and storage admission and
/// the committed relay receipt (or durable rejection Notice). No local cursor
/// advances while admission waits for credit or a transport is unavailable.
/// A reopened forwarder resubmits the identical envelope after a lost receipt.
/// The callback returns sender-local completion effects, such as the original
/// call journal's terminal receipt. They commit with the outbox cursor, so an
/// acknowledged publication cannot lose its caller's result on restart. Their
/// conditions retain the ordinary call cancellation and settlement rules.
/// Completion effects must be hard: a skipped receipt must not acknowledge
/// the outbox. They must not mutate the forwarder's cursor subtree.
pub async fn forward<P, D>(
    local: &P,
    paths: &Paths,
    node: &NodeId,
    mut admit: D,
) -> Result<Infallible, Error>
where
    P: StreamProvider + ?Sized,
    D: AsyncFnMut(Envelope) -> Result<Vec<CommitEffect>, Error>,
{
    consume(
        local,
        paths.outbox(node)?,
        paths.outbox_cursor(node)?,
        async |envelope: Envelope| {
            if envelope.source != *node {
                return Err(Error::InvalidRecord);
            }
            admit(envelope).await
        },
    )
    .await
}

async fn consume<P, D>(
    account: &P,
    relay: StreamPath,
    cursor_path: StreamPath,
    mut deliver: D,
) -> Result<Infallible, Error>
where
    P: StreamProvider + ?Sized,
    D: AsyncFnMut(Envelope) -> Result<Vec<CommitEffect>, Error>,
{
    let (mut next, mut cursor_tail) = match account.head(cursor_path.clone()).await {
        Ok(head) => {
            let record = head.last.ok_or(Error::InvalidRecord)?;
            let cursor = Cursor::decode(record.value.as_ref()).map_err(|_| Error::InvalidRecord)?;
            if cursor.version != 1 || cursor.next == 0 {
                return Err(Error::InvalidRecord);
            }
            (cursor.next, head.tail)
        }
        Err(error) if error.category() == StreamError::NotFound => (0, 0),
        Err(error) => return Err(error.into()),
    };
    let mut records = acyclic_stream::records_only(
        account
            .follow(FollowRequest {
                when_absent: acyclic_stream::WhenAbsent::Wait,
                ..FollowRequest::new(relay, next)
            })
            .await?,
    );
    while let Some(record) = records.try_next().await? {
        if record.sequence != next {
            return Err(Error::InvalidRecord);
        }
        let completion = deliver(Envelope::decode(&record.value)?).await?;
        if completion.iter().any(|effect| effect.soft) {
            return Err(Error::InvalidRecord);
        }
        let following = next.checked_add(1).ok_or(Error::InvalidRecord)?;
        let mut key = Sha256::new();
        key.update(b"acyclic.actors.route.ack.v1\0");
        for part in [cursor_path.as_str().as_bytes(), record.commit_id.as_bytes()] {
            key.update((part.len() as u64).to_le_bytes());
            key.update(part);
        }
        key.update(next.to_le_bytes());
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(key.finalize().to_vec())?,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Tail {
                    path: cursor_path.clone(),
                    expected: cursor_tail,
                }],
                vec![CommitMutation::Append {
                    path: cursor_path.clone(),
                    records: vec![
                        Cursor {
                            version: 1,
                            next: following,
                        }
                        .encode_to_vec()
                        .into(),
                    ],
                }],
            )],
        };
        request.effects.extend(completion);
        match account.commit(request).await? {
            CommitOutcome::Committed(_) => {
                next = following;
                cursor_tail = cursor_tail.checked_add(1).ok_or(Error::InvalidRecord)?;
            }
            CommitOutcome::Conflict(_) => return Err(crate::local::Error::Conflict.into()),
        }
    }
    Err(StreamError::Unavailable.into())
}
