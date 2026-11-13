//! One follow per destination, with acknowledgement after ordinary delivery.
use super::{Envelope, Error, NodeId, Paths};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, FollowRequest,
    IdempotencyKey, StreamError, StreamProvider, StreamProviderExt as _,
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

pub(super) fn creation(paths: &Paths, node: &NodeId) -> Result<CommitEffect, Error> {
    let relay = paths.relay(node)?;
    let cursor = paths.relay_cursor(node)?;
    Ok(CommitEffect::hard(
        vec![
            CommitCondition::Absent {
                path: relay.clone(),
            },
            CommitCondition::Absent {
                path: cursor.clone(),
            },
        ],
        vec![
            CommitMutation::Append {
                path: relay,
                records: vec![bytes::Bytes::new().into()],
            },
            CommitMutation::Append {
                path: cursor,
                records: vec![
                    Cursor {
                        version: 1,
                        next: 1,
                    }
                    .encode_to_vec()
                    .into(),
                ],
            },
        ],
    ))
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
    let relay = paths.relay(node)?;
    let cursor_path = paths.relay_cursor(node)?;
    let (mut next, mut cursor_tail) = match account.head(cursor_path.clone()).await {
        Ok(head) => {
            let record = head.last.ok_or(Error::InvalidRecord)?;
            let cursor = Cursor::decode(record.value.as_ref()).map_err(|_| Error::InvalidRecord)?;
            if cursor.version != 1 || cursor.next == 0 {
                return Err(Error::InvalidRecord);
            }
            (cursor.next, head.tail)
        }
        Err(error) => return Err(error.into()),
    };
    let mut records =
        acyclic_stream::records_only(account.follow(FollowRequest::new(relay, next)).await?);
    while let Some(record) = records.try_next().await? {
        if record.sequence != next {
            return Err(Error::InvalidRecord);
        }
        deliver(Envelope::decode(&record.value)?).await?;
        let following = next.checked_add(1).ok_or(Error::InvalidRecord)?;
        let mut key = Sha256::new();
        key.update(b"acyclic.actors.route.ack.v1\0");
        for part in [cursor_path.as_str().as_bytes(), record.commit_id.as_bytes()] {
            key.update((part.len() as u64).to_le_bytes());
            key.update(part);
        }
        key.update(next.to_le_bytes());
        let request = CommitRequest {
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
