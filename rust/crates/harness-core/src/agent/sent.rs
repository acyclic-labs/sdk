//! A message's original delivery receipt, selected through its sender's intent.
//! The marker and inbox effect share one publication; recovery reads that exact
//! immutable envelope, never a current inbox tail or a conversation scan.
use super::{
    AgentAddress, AgentPath, AgentRecord, Delivered, Error, InboxRecord, IntentId, Sender,
};
use acyclic_actors::route::{self, Routed};
use acyclic_stream::{CommittedMutation, StreamError, StreamProviderExt as _};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

/// Original delivery or the lossless source position still awaiting admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SentDelivery {
    /// Ordinary owning-node delivery or retained account admission.
    Complete(Routed),
    /// The sender effect is durable; the original account receipt is pending.
    Pending {
        /// Sender-local acknowledgement stream.
        source: acyclic_stream::StreamPath,
        /// Original outbox record's sequence.
        from: u64,
    },
}

/// Public delivery observation, without an internal relay admission key.
/// Pending admission acknowledges durable sender-local storage, not receipt by
/// the recipient. It grants no read or follow authority over the named cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SentStatus<'a> {
    /// The owning node appended this inbox sequence.
    Committed { inbox: u64 },
    /// The recipient is closed.
    Closed,
    /// The account admitted this destination-relay sequence.
    Relayed { relay_seq: u64 },
    /// The destination could not accept the request.
    Rejected { reason: route::Rejection },
    /// Durable locally; awaiting the original account admission.
    PendingAdmission {
        /// Sender-local acknowledgement stream, observed with existing authority.
        source: std::borrow::Cow<'a, acyclic_stream::StreamPath>,
        /// Original outbox sequence, never a current tail.
        from: u64,
    },
}

impl SentStatus<'_> {
    /// Retain an observation after its original receipt is released. Only the
    /// pending cursor needs ownership; ordinary serialization borrows it.
    pub fn into_owned(self) -> SentStatus<'static> {
        match self {
            Self::Committed { inbox } => SentStatus::Committed { inbox },
            Self::Closed => SentStatus::Closed,
            Self::Relayed { relay_seq } => SentStatus::Relayed { relay_seq },
            Self::Rejected { reason } => SentStatus::Rejected { reason },
            Self::PendingAdmission { source, from } => SentStatus::PendingAdmission {
                source: std::borrow::Cow::Owned(source.into_owned()),
                from,
            },
        }
    }
}

impl SentDelivery {
    /// Borrow the same public status used by tools and external clients.
    pub fn status(&self) -> SentStatus<'_> {
        match self {
            Self::Complete(Routed::Delivered(delivered)) => SentStatus::Committed {
                inbox: delivered.inbox,
            },
            Self::Complete(Routed::Rejected(route::Rejection::Closed)) => SentStatus::Closed,
            Self::Complete(Routed::Rejected(route::Rejection::NoSuchActor)) => {
                SentStatus::Rejected {
                    reason: route::Rejection::NoSuchActor,
                }
            }
            Self::Complete(Routed::Relayed { relay_seq, .. }) => SentStatus::Relayed {
                relay_seq: *relay_seq,
            },
            Self::Pending { source, from } => SentStatus::PendingAdmission {
                source: std::borrow::Cow::Borrowed(source),
                from: *from,
            },
        }
    }
}

impl Serialize for SentDelivery {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.status().serialize(serializer)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SentRecord {
    pub to: AgentAddress,
    pub id: IntentId,
}

impl AgentRecord for SentRecord {}

/// The recipient and original outcome of this sender's committed message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentMessage {
    /// Exact recipient, including when its closed inbox skipped the effect.
    pub to: AgentAddress,
    /// Original caller-owned message identity.
    pub id: IntentId,
    /// The original outcome, or its source-admission event position.
    pub delivery: SentDelivery,
}

/// Settlement of one original send, without exposing the recipient's history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SentReplyStatus {
    /// The request or its continuing goal has not settled.
    Pending,
    /// The exact conversation position that answered the request.
    Answered {
        /// Committed answer position, without its content or file authority.
        sequence: u64,
    },
    /// The inbox was closed, or the accepted request was dropped.
    Dropped,
}

/// A pending reply's event position, selected with its committed checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyWatch {
    /// Original recipient from the sender's immutable receipt.
    pub to: AgentAddress,
    /// Original send intent, never an arbitrary recipient input.
    pub id: IntentId,
    /// Original accepted inbox sequence.
    pub inbox: u64,
    /// First status publication not covered by the observed checkpoint.
    pub from: u64,
}

/// One send's delivery and reply, with a lossless event position while pending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentReply {
    /// Immutable delivery receipt owned by the querying sender.
    pub message: SentMessage,
    /// Only the settlement of that original request.
    pub status: SentReplyStatus,
    /// Present while a local status or an owned routed Notice can settle it.
    pub watch: Option<super::Watch>,
}

/// Correlate only a send owned by `sender`. Same-node replies observe one
/// checkpoint/status cut. Routed replies use the sender's indexed inbox and
/// retain its observed tail, so a concurrent result cannot be missed. No
/// transcript or output body is returned.
pub async fn sent_reply<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &std::sync::Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    sender: &AgentPath,
    id: &IntentId,
) -> Result<Option<SentReply>, Error> {
    let Some(message) = sent(provider.as_ref(), sender, id).await? else {
        return Ok(None);
    };
    let inbox = match &message.delivery {
        SentDelivery::Complete(Routed::Delivered(delivered)) => delivered.inbox,
        SentDelivery::Pending { .. } | SentDelivery::Complete(Routed::Relayed { .. }) => {
            let (status, watch) =
                super::routing::notices::observe(provider.as_ref(), sender, id, &message.to)
                    .await?;
            return Ok(Some(SentReply {
                message,
                status,
                watch,
            }));
        }
        SentDelivery::Complete(Routed::Rejected(_)) => {
            return Ok(Some(SentReply {
                message,
                status: SentReplyStatus::Dropped,
                watch: None,
            }));
        }
    };
    let actor = message.to.actor(within)?;
    let agents = super::handler::Agents::new(super::actor_store(provider)?);
    let (mut head, cursor) = agents.head(&actor, message.to.path()).await?;
    let state = &mut head.state;
    let reply = super::reply(provider.as_ref(), state, cursor, id, inbox).await?;
    let (status, watch) = match reply {
        super::Reply::Answered { sequence, .. } => (SentReplyStatus::Answered { sequence }, None),
        super::Reply::Dropped => (SentReplyStatus::Dropped, None),
        super::Reply::Pending | super::Reply::Continuing { .. } => (
            SentReplyStatus::Pending,
            Some(super::Watch::Reply(ReplyWatch {
                to: message.to.clone(),
                id: id.clone(),
                inbox,
                from: state.status_tail,
            })),
        ),
    };
    Ok(Some(SentReply {
        message,
        status,
        watch,
    }))
}

/// Look up one original send by intent. `None` means this sender has not yet
/// published that effect. A marker whose envelope is unavailable is an error,
/// never a fabricated receipt or permission to deliver again.
pub async fn sent<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    sender: &AgentPath,
    id: &IntentId,
) -> Result<Option<SentMessage>, Error> {
    let Some((marker, record)) = original_marker(provider, sender, id).await? else {
        return Ok(None);
    };
    if let Some(source) = original_routed(provider, sender, &marker, &record).await? {
        let delivery = match route::receipt(
            provider,
            &source.paths,
            &source.envelope.source,
            source.sequence,
            &source.envelope.request.idempotency_key,
        )
        .await
        .map_err(super::routing::error)?
        {
            Some(receipt) => SentDelivery::Complete(receipt),
            None => SentDelivery::Pending {
                source: source
                    .paths
                    .outbox_cursor(&source.envelope.source)
                    .map_err(Error::Stream)?,
                from: source.sequence,
            },
        };
        return Ok(Some(SentMessage {
            to: marker.to,
            id: marker.id,
            delivery,
        }));
    }
    let envelope = provider
        .read_commit(record.commit_id)
        .await
        .map_err(Error::Stream)?;
    if envelope.commit_id != record.commit_id {
        return Err(Error::Record("send receipt publication differs"));
    }
    let inbox = marker.to.path().inbox()?;
    let mut delivery = Delivered::Closed;
    for mutation in envelope.mutations {
        let CommittedMutation::Append(append) = mutation else {
            continue;
        };
        if append.path != inbox {
            continue;
        }
        for record in append.records {
            if let InboxRecord::Message(message) | InboxRecord::FollowUp(message) =
                InboxRecord::decode(&record.value)?
                && message.id == *id
                && matches!(&message.from, Sender::Agent { path } if path == sender)
            {
                if matches!(delivery, Delivered::Committed { .. }) {
                    return Err(Error::Record("send intent was delivered more than once"));
                }
                delivery = Delivered::Committed {
                    inbox: record.sequence,
                };
            }
        }
    }
    Ok(Some(SentMessage {
        to: marker.to,
        id: marker.id,
        delivery: SentDelivery::Complete(match delivery {
            Delivered::Committed { inbox } => Routed::Delivered(route::Delivered { inbox }),
            Delivered::Closed => Routed::Rejected(route::Rejection::Closed),
        }),
    }))
}

async fn original_marker<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    sender: &AgentPath,
    id: &IntentId,
) -> Result<Option<(SentRecord, acyclic_stream::Record)>, Error> {
    let path = sender.sent_message(id)?;
    let head = match provider.head(path.clone()).await {
        Ok(head) => head,
        Err(error) if error.category() == StreamError::NotFound => return Ok(None),
        Err(error) => return Err(Error::Stream(error)),
    };
    let record = head
        .last
        .ok_or(Error::Record("send receipt marker is empty"))?;
    if head.tail != 1 || record.sequence != 0 || record.origin != path {
        return Err(Error::Record("send receipt marker is not immutable"));
    }
    let marker = SentRecord::decode(&record.value)?;
    if marker.id != *id {
        return Err(Error::Record("send receipt intent differs"));
    }
    Ok(Some((marker, record)))
}

struct RoutedSend {
    paths: route::Paths,
    envelope: route::Envelope,
    sequence: u64,
}

async fn original_routed<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    sender: &AgentPath,
    marker: &SentRecord,
    record: &acyclic_stream::Record,
) -> Result<Option<RoutedSend>, Error> {
    let outbox = record
        .headers
        .attributes()
        .iter()
        .find(|attribute| attribute.name == super::routing::OUTBOX)
        .map(|attribute| {
            let path = std::str::from_utf8(attribute.value.as_ref())
                .map_err(|_| Error::Record("send outbox path is not UTF-8"))?;
            acyclic_stream::StreamPath::new(path).map_err(Error::Stream)
        })
        .transpose()?;
    let attributes = record.headers.attributes();
    let source_intent = attributes
        .iter()
        .find(|attribute| attribute.name == super::routing::SOURCE_INTENT);
    if let Some(outbox) = outbox {
        let source_intent = source_intent.ok_or(Error::Record("send source index is missing"))?;
        let mut records = provider
            .read(acyclic_stream::ReadRequest {
                path: outbox.clone(),
                from: 0,
                limit: 1,
                key: Some(acyclic_stream::ReadKeySeek::exact(
                    source_intent.value.to_vec(),
                    u64::MAX,
                )),
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let source = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .ok_or(Error::Record("send source publication is missing"))?;
        if source.commit_id != record.commit_id || source.origin != outbox {
            return Err(Error::Record("send source publication differs"));
        }
        let (paths, node) = route::Paths::from_outbox(&outbox).map_err(super::routing::error)?;
        let envelope = route::Publication::decode(&source.value)
            .map_err(super::routing::error)?
            .delivery()
            .map_err(super::routing::error)?;
        let peer = super::PeerRoute::parse(&envelope)?;
        if envelope.source != node
            || peer.sender() != sender
            || peer.recipient() != marker.to.path()
            || marker.to.qualified_actor() != Some(&envelope.to)
            || peer.id() != &marker.id
        {
            return Err(Error::Record("send source intent differs"));
        }
        return Ok(Some(RoutedSend {
            paths,
            envelope,
            sequence: source.sequence,
        }));
    }
    if source_intent.is_some() {
        return Err(Error::Record("send source index has no outbox"));
    }
    Ok(None)
}

/// Recover the exact sender-owned request for an account rejection. The marker
/// and native source index prove its original commit, root, target and intent.
pub(super) async fn original_route<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    sender: &AgentPath,
    id: &IntentId,
) -> Result<route::Envelope, Error> {
    let (marker, record) = original_marker(provider, sender, id)
        .await?
        .ok_or(Error::Record("result has no owned send"))?;
    original_routed(provider, sender, &marker, &record)
        .await?
        .map(|source| source.envelope)
        .ok_or(Error::Record("result has no routed source"))
}
