//! One follow per destination, with acknowledgement after ordinary delivery.
use super::{Envelope, Error, NodeId, Paths, Publication, Routed, account::Reply};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitId, CommitMutation, CommitOutcome, CommitRequest,
    FollowRequest, IdempotencyKey, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt as _;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use std::convert::Infallible;

/// Original account publication of one relay record. This is a reference to
/// immutable data, not authority to send a Notice or access another node's relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayReceipt {
    /// Content-bound account commit that appended this record.
    pub commit_id: CommitId,
    /// Original sequence in the destination node's relay.
    pub sequence: u64,
}

impl RelayReceipt {
    /// Recover exactly this original record with one bounded read. The caller
    /// must authorize `node` before IO, then validate the returned envelope's
    /// relationship to its proposed reply or rejection Notice.
    pub async fn read<P: StreamProvider + ?Sized>(
        &self,
        account: &P,
        paths: &Paths,
        node: &NodeId,
    ) -> Result<Envelope, Error> {
        let relay = paths.relay(node)?;
        let record = account
            .record_at(relay.clone(), self.sequence)
            .await?
            .ok_or(Error::InvalidRecord)?;
        if record.sequence != self.sequence
            || record.commit_id != self.commit_id
            || record.origin != relay
        {
            return Err(Error::InvalidRecord);
        }
        Envelope::decode(record.value)
    }
}

/// Delivery plus the original account receipt needed to prove a later Notice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayRecord {
    /// Original ordinary-delivery request, without rewriting its intent or body.
    pub envelope: Envelope,
    /// Original relay position, retained across drain restarts and lost acks.
    pub receipt: RelayReceipt,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Cursor {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    next: u64,
    #[prost(message, optional, tag = "3")]
    receipt: Option<Reply>,
}

/// Read the original admission of one source outbox record, without scanning
/// other sends. `None` means it has not been acknowledged; wait on
/// `paths.outbox_cursor(node)` from `outbox_sequence`, never poll this query.
/// The owning host must authorize the sender's original intent before querying.
pub async fn receipt<P: StreamProvider + ?Sized>(
    local: &P,
    paths: &Paths,
    node: &NodeId,
    outbox_sequence: u64,
    intent: &IdempotencyKey,
) -> Result<Option<Routed>, Error> {
    let next = outbox_sequence.checked_add(1).ok_or(Error::InvalidRecord)?;
    let Some(record) = local
        .record_at(paths.outbox_cursor(node)?, outbox_sequence)
        .await?
    else {
        return Ok(None);
    };
    let cursor = Cursor::decode(record.value.as_ref()).map_err(|_| Error::InvalidRecord)?;
    if cursor.version != 1 || cursor.next != next {
        return Err(Error::InvalidRecord);
    }
    Ok(Some(
        cursor.receipt.ok_or(Error::InvalidRecord)?.routed(intent)?,
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
    D: AsyncFnMut(RelayRecord) -> Result<(), Error>,
{
    consume(
        account,
        paths.relay(node)?,
        paths.relay_cursor(node)?,
        async |record: acyclic_stream::Record| {
            deliver(RelayRecord {
                envelope: Envelope::decode(record.value)?,
                receipt: RelayReceipt {
                    commit_id: record.commit_id,
                    sequence: record.sequence,
                },
            })
            .await?;
            Ok(None)
        },
    )
    .await
}

/// Forward the source node's durable outbox in order while its NodeLink is open.
/// `admit` must await account placement, authority and storage admission and
/// the committed relay receipt (or durable rejection Notice). No local cursor
/// advances while admission waits for credit or a transport is unavailable.
/// A reopened forwarder resubmits the identical envelope after a lost receipt.
/// Its compact original receipt is the acknowledgement record itself. Callers
/// recover it through [`receipt`], including after restart or later admissions.
/// The forwarder owns no caller's call journal or cancellation terminal.
pub async fn forward<P, R, D>(
    local: &P,
    paths: &Paths,
    node: &NodeId,
    mut publish: R,
    mut admit: D,
) -> Result<Infallible, Error>
where
    P: StreamProvider + ?Sized,
    R: AsyncFnMut(Publication) -> Result<(), Error>,
    D: AsyncFnMut(Envelope) -> Result<Routed, Error>,
{
    consume(
        local,
        paths.outbox(node)?,
        paths.outbox_cursor(node)?,
        async |record: acyclic_stream::Record| match Publication::decode(&record.value)? {
            Publication::Delivery(envelope) => {
                if envelope.source != *node {
                    return Err(Error::InvalidRecord);
                }
                let key = envelope.request.idempotency_key.clone();
                Ok(Some(Reply::new(&key, admit(envelope).await?)?))
            }
            Publication::Placement(registration) => {
                if registration.placement.node != *node {
                    return Err(Error::InvalidRecord);
                }
                publish(Publication::Placement(registration)).await?;
                Ok(None)
            }
            Publication::Call(call) => {
                if call.node != *node {
                    return Err(Error::InvalidRecord);
                }
                publish(Publication::Call(call)).await?;
                Ok(None)
            }
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
    D: AsyncFnMut(acyclic_stream::Record) -> Result<Option<Reply>, Error>,
{
    let (mut next, mut cursor_tail) = match account.head(cursor_path.clone()).await {
        Ok(head) => {
            let record = head.last.ok_or(Error::InvalidRecord)?;
            let cursor = Cursor::decode(record.value.as_ref()).map_err(|_| Error::InvalidRecord)?;
            if cursor.version != 1 || cursor.next == 0 || cursor.next != head.tail {
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
        let commit_id = record.commit_id;
        let receipt = deliver(record).await?;
        let following = next.checked_add(1).ok_or(Error::InvalidRecord)?;
        let mut key = Sha256::new();
        key.update(b"acyclic.actors.route.ack.v1\0");
        for part in [cursor_path.as_str().as_bytes(), commit_id.as_bytes()] {
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
                            receipt,
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
